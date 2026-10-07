'use strict';
// Synthetic accounting/control tests. No provider, microphone or financial state is used.
const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const core=require('../../netlify/functions/_britesGrowth.js'),voice=require('../../netlify/functions/_britesConciergeVoice.js');
const AT=Date.parse('2026-10-07T20:00:00Z'),WINDOW=45*60000;
const OWNER='synthetic-policy-root',TOKEN='a'.repeat(64),ADMIN='synthetic-only-operator',KEY='synthetic-only-provider-key';
const NATIVE='VoiceAllowances/native-sandbox',SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n';
const clone=value=>value===undefined?undefined:structuredClone(value);
class Database{
  constructor(rows){this.rows=new Map(rows.map(([k,v])=>[k,clone(v)]));this.tail=Promise.resolve();this.reads=0;this.writes=0;}
  collection(name){const db=this;return {firestore:db,doc(id){const key=name+'/'+id;return {id,key,async get(){db.reads++;return {exists:db.rows.has(key),data:()=>clone(db.rows.get(key))};},async set(value,options){db.write(key,value,options);}};},orderBy(field,direction){return {limit(limit){return {async get(){db.reads++;const docs=[...db.rows].filter(([k,v])=>k.startsWith(name+'/')&&Object.hasOwn(v,field)).sort((a,b)=>direction==='desc'?b[1][field]-a[1][field]:a[1][field]-b[1][field]).slice(0,limit).map(([k,v])=>({id:k.slice(name.length+1),data:()=>clone(v)}));return {docs,size:docs.length};}};}};}};}
  write(key,value,options){this.writes++;this.rows.set(key,options?.merge?{...this.rows.get(key),...clone(value)}:clone(value));}
  runTransaction(task){const result=this.tail.then(async()=>{const pending=[];const value=await task({get:async ref=>{assert.equal(pending.length,0,'all transaction reads precede writes');return ref.get();},set:(ref,data,options)=>pending.push([ref.key,clone(data),options])});for(const args of pending)this.write(...args);return value;});this.tail=result.catch(()=>{});return result;}
}
function fixture(options={}){
  let at=AT,sequence=0;
  const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'1',BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'10',BRITES_CONCIERGE_REALTIME_RESERVE_USD:'1',OPENAI_API_KEY:KEY,...options.env};
  const rows=[
    ['State/control',{enabled:true,aiEnabled:false,stopAt:core.STOP_AT}],
    ['State/controller',{owner:OWNER,tokenHash:core.hash(TOKEN),leaseUntil:AT+WINDOW}],
    ['State/checkpoint',{updatedAt:AT-1000,phase:'synthetic-private-checkpoint'}],
    ['VoiceUsage/preview-budget',{reservedCents:950,spentCents:0,calls:10,allocationCapCents:1000,at:AT-10000}],
    [NATIVE,{schema:1,id:'native-sandbox',kind:'realtime_voice',provider:'openai',allocatedCents:500,reservationCents:100,reservedCents:500,spentCents:0,calls:5,issuedAt:AT-600000,expiresAt:core.STOP_AT,lastStartedAt:AT-10000,authorizedBy:'synthetic-prior-root',checkpointUpdatedAt:AT-600001}]
  ];
  for(let n=1;n<=5;n++)rows.push(['VoiceUsage/session-'+crypto.createHash('sha256').update('old-native-'+n).digest('hex'),{schema:2,kind:'realtime_voice',provider:'openai',allocatedCents:100,startedAt:AT-600000+n*1000,reconcile:'provider_evidence_required',fundingSource:'native_allowance',allowanceId:'native-sandbox',stage:'call_verified',providerStatus:201,providerCode:'CALL_VERIFIED',callId:'rtc_old_synthetic_'+n,originalHistory:'synthetic-kept-call-'+n}]);
  for(let n=1;n<=10;n++)rows.push(['VoiceUsage/session-'+crypto.createHash('sha256').update('old-legacy-'+n).digest('hex'),{allocatedCents:n===10?50:100,startedAt:AT-1000000+n*1000,reconcile:'provider_evidence_required',originalHistory:'synthetic-unattributed-'+n}]);
  const db=new Database(rows),provider=[],deadlines=[];
  const service={namespace:options.namespace??'Brites_Growth_Sandbox',col:name=>db.collection(name),setup:async()=>clone(db.rows.get('State/control')),rateLimit:async()=>true};
  const fetch=async(url,init)=>{provider.push({url,method:init?.method});if(url.endsWith('/hangup'))return new Response(null,{status:200});assert.equal(url,'https://api.openai.com/v1/realtime/calls');if(options.providerFailure)throw Error('synthetic transport gap');if(options.providerRejection)return new Response('{"error":{"code":"insufficient_quota"}}',{status:429});const id='rtc_new_synthetic_'+(++sequence);return new Response(SDP,{status:201,headers:{Location:'/v1/realtime/calls/'+id,'x-request-id':'req_new_synthetic_'+sequence}});};
  const handler=voice.createHandler({env,service,fetch,now:()=>at,authorize:async request=>request.headers.get('X-Growth-Key')===ADMIN,scheduleHangup:async row=>{deadlines.push(clone(row));if(options.deadlineFailure)return false;await service.col('VoiceDeadlines').doc(row.callId).set({...row,state:'pending',at});return true;}});
  const request=(body,{operator=true,origin='https://preview.test'}={})=>new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{'Content-Type':'application/json',...(origin?{Origin:origin}:{}),...(operator?{'X-Growth-Key':ADMIN}:{})},body:JSON.stringify(body)});
  async function call(body,requestOptions){const response=await handler(request(body,requestOptions));return {response,value:await response.json()};}
  const authorize=(patch={},requestOptions)=>call({action:'authorize-voice',owner:OWNER,token:TOKEN,expectedUpdatedAt:db.rows.get('State/checkpoint')?.updatedAt??0,...patch},requestOptions);
  const continueOnce=(patch={},requestOptions)=>authorize({continueToConfiguredCeiling:true,...patch},requestOptions);
  const capability=()=>call({action:'capabilities'},{operator:false});
  const inspect=()=>call({action:'allocation'});
  const start=demoToken=>call({action:'start',sdp:SDP,demoToken},{operator:false});
  const bytes=()=>JSON.stringify([...db.rows]);
  const oldBytes=()=>JSON.stringify([...db.rows].filter(([key])=>key!==NATIVE));
  return {env,db,provider,deadlines,call,authorize,continueOnce,capability,inspect,start,bytes,oldBytes,now:()=>at,advance:ms=>{at+=ms;},setAt:value=>{at=value;},allowance:()=>clone(db.rows.get(NATIVE))};
}
async function promote(f){const answer=await f.continueOnce();assert.equal(answer.response.status,200);assert.equal(answer.value.continuationApplied,true);return answer;}
async function token(f){const answer=await f.capability();assert.equal(answer.response.status,200);assert.equal(answer.value.enabled,true);return answer.value.demoToken;}
function unchanged(f,before,writes){assert.equal(f.bytes(),before);assert.equal(f.db.writes,writes);assert.equal(f.provider.length,0);}

test('synthetic legacy and native holds pause voice until an explicit owned continuation',async()=>{
  const f=fixture(),before=f.bytes();assert.equal((await f.capability()).response.status,429);
  const ordinary=await f.authorize();assert.equal(ordinary.response.status,200);assert.equal(ordinary.value.reused,true);assert.equal(ordinary.value.allowance.limitCents,500);assert.equal(ordinary.value.allowance.state,'exhausted');unchanged(f,before,0);
});
test('one continuation preserves every original hold, call, checkpoint and expiry while making configured capacity available',async()=>{
  const f=fixture(),old=f.oldBytes(),native=f.allowance(),result=await promote(f),row=f.allowance();
  assert.equal(row.schema,2);assert.equal(row.allocatedCents,500);assert.equal(row.limitCents,1000);assert.equal(row.continuationCents,500);assert.equal(row.continuationCount,1);assert.equal(row.reservedCents,500);assert.equal(row.spentCents,0);assert.equal(row.calls,5);
  for(const key of ['issuedAt','expiresAt','lastStartedAt','authorizedBy','checkpointUpdatedAt'])assert.equal(row[key],native[key]);
  assert.equal(row.continuationAuthorizedBy,OWNER);assert.equal(row.continuationCheckpointUpdatedAt,AT-1000);assert.equal(row.continuedAt,AT);assert.equal(row.accountedCalls,0);assert.equal(row.accountedAllocationCents,0);
  assert.equal(result.value.allowance.availableCents,500);assert.equal(result.value.allowance.state,'available');assert.equal(result.value.providerCalls,0);assert.equal(result.value.refunds,0);assert.equal(result.value.usageSettled,false);assert.equal(result.value.legacyAllocationChanged,false);assert.equal(f.oldBytes(),old);assert.equal(f.db.writes,1);assert.equal(f.provider.length,0);
  const before=f.bytes(),reads=await Promise.all([f.capability(),f.inspect()]);assert.equal(reads[0].response.status,200);assert.equal(reads[1].value.nativeAllowance.limitCents,1000);assert.equal(reads[1].value.nativeAllowance.allocatedCents,500);assert.equal(reads[1].value.budget.reservedCents,950);assert.equal(reads[1].value.evidenceComplete,false);unchanged(f,before,1);
  assert.doesNotMatch(JSON.stringify(reads[1].value),/synthetic-only-|synthetic-prior-root|synthetic-policy-root|tokenHash|authorizedBy|AuthorizedBy|checkpointUpdatedAt|CheckpointUpdatedAt|originalHistory/);
});
test('eight simultaneous continuation requests commit once and all repeats reuse the same cumulative ceiling',async()=>{
  const f=fixture(),old=f.oldBytes(),results=await Promise.all(Array.from({length:8},()=>f.continueOnce()));
  assert.ok(results.every(x=>x.response.status===200));assert.equal(results.filter(x=>x.value.continuationApplied===true).length,1);assert.equal(results.filter(x=>x.value.reused===true).length,7);assert.equal(f.db.writes,1);assert.equal(f.oldBytes(),old);
  const before=f.bytes();for(let n=0;n<3;n++)assert.equal((await f.continueOnce()).value.continuationApplied,false);unchanged(f,before,1);
});
test('changing owner and renewing the lease cannot refill or extend the one-time policy',async()=>{
  const f=fixture();await promote(f);const native=f.allowance();f.advance(WINDOW+1000);const nextToken='d'.repeat(64);f.db.rows.set('State/controller',{owner:'synthetic-next-root',tokenHash:core.hash(nextToken),leaseUntil:f.now()+WINDOW});
  const before=f.bytes(),result=await f.continueOnce({owner:'synthetic-next-root',token:nextToken});assert.equal(result.response.status,200);assert.equal(result.value.reused,true);assert.deepEqual(f.allowance(),native);unchanged(f,before,1);
});
test('six simultaneous native starts can consume only the five additional slots; original unknowns stay held',async()=>{
  const f=fixture(),legacy=f.oldBytes();await promote(f);const tokens=await Promise.all(Array.from({length:6},()=>token(f))),responses=await Promise.all(tokens.map(t=>f.start(t)));
  assert.deepEqual(responses.map(x=>x.response.status).sort(),[200,200,200,200,200,429]);assert.equal(f.provider.length,5);assert.equal(f.deadlines.length,5);assert.equal(f.allowance().reservedCents,1000);assert.equal(f.allowance().spentCents,0);assert.equal(f.allowance().calls,10);
  assert.equal(JSON.stringify([...f.db.rows].filter(([key])=>JSON.parse(legacy).some(([old])=>old===key))),legacy);
  assert.equal((await f.capability()).response.status,429);const before=f.bytes(),writes=f.db.writes,result=await f.continueOnce();assert.equal(result.value.reused,true);assert.equal(result.value.continuationApplied,false);assert.equal(result.value.allowance.state,'exhausted');assert.equal(f.bytes(),before);assert.equal(f.db.writes,writes);
});
test('same-token starts and a replay cannot reserve twice under schema2',async()=>{
  const f=fixture();await promote(f);const demo=await token(f),responses=await Promise.all([f.start(demo),f.start(demo)]);assert.deepEqual(responses.map(x=>x.response.status).sort(),[200,401]);assert.equal(f.allowance().reservedCents,600);assert.equal(f.allowance().calls,6);assert.equal(f.provider.length,1);
  const before=f.bytes(),writes=f.db.writes;assert.equal((await f.start(demo)).response.status,401);assert.equal(f.bytes(),before);assert.equal(f.db.writes,writes);assert.equal(f.provider.length,1);
});
test('ending a newly started call closes its deadline but neither refunds nor settles any allocation',async()=>{
  const f=fixture();await promote(f);const started=await f.start(await token(f));assert.equal(started.response.status,200);const native=f.allowance(),legacy=clone(f.db.rows.get('VoiceUsage/preview-budget'));
  assert.equal((await f.call({action:'stop',stopToken:started.value.stopToken},{operator:false})).value.stopped,true);assert.deepEqual(f.allowance(),native);assert.deepEqual(f.db.rows.get('VoiceUsage/preview-budget'),legacy);assert.equal(f.allowance().spentCents,0);assert.equal((await f.continueOnce()).value.continuationApplied,false);
});
for(const failure of ['providerRejection','providerFailure','deadlineFailure'])test('schema2 '+failure+' retains the exact once-only hold without invented charges',async()=>{
  const f=fixture({[failure]:true});await promote(f);assert.equal((await f.start(await token(f))).response.status,503);assert.equal(f.allowance().reservedCents,600);assert.equal(f.allowance().spentCents,0);assert.equal(f.allowance().calls,6);assert.equal(f.db.rows.get('VoiceUsage/preview-budget').reservedCents,950);assert.equal((await f.continueOnce()).value.continuationApplied,false);
});
test('typed or non-provenance requests cannot use the continuation pool',async()=>{
  const f=fixture();await promote(f);const before=f.bytes(),writes=f.db.writes;
  assert.equal(await voice.createDemoBudgetReservation({namespace:'Brites_Growth_Sandbox',col:name=>f.db.collection(name),setup:async()=>clone(f.db.rows.get('State/control'))},{capUsd:10,now:f.now})(1,'synthetic-typed-current-id'),null);
  const service={namespace:'Brites_Growth_Sandbox',col:name=>f.db.collection(name),setup:async()=>clone(f.db.rows.get('State/control'))};
  assert.deepEqual(await voice.createDemoBudgetReservationResult(service,{capUsd:10,now:f.now,allowContinuation:true})(1,'synthetic-unqualified-id'),{granted:false,reason:'ALLOCATION_EXHAUSTED'});unchanged(f,before,writes);
});
for(const item of [
  {name:'guest',options:{operator:false},status:401},{name:'foreign origin',options:{origin:'https://foreign.test'},status:403},
  {name:'test-grant action',patch:{action:'authorize-test'},status:400},{name:'false flag',patch:{continueToConfiguredCeiling:false},status:400},{name:'string flag',patch:{continueToConfiguredCeiling:'true'},status:400},{name:'numeric flag',patch:{continueToConfiguredCeiling:1},status:400},
  ...['allocatedCents','limitCents','continuationCents','spentCents','refund','expiresAt'].map(key=>({name:'caller '+key,patch:{[key]:1000},status:400})),
  {name:'live namespace',fixture:{env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'}},status:403},{name:'live service',fixture:{namespace:'Brites_Growth_Live'},status:403}
])test('continuation refuses '+item.name+' before any storage/provider write',async()=>{
  const f=fixture(item.fixture),before=f.bytes(),answer=await f.continueOnce(item.patch,item.options);assert.equal(answer.response.status,item.status);unchanged(f,before,0);assert.equal(f.db.reads,0);
});
for(const shape of ['missing-controller','foreign-owner','wrong-token','expired-lease','string-lease','stale-checkpoint','legacy-writer','legacy-leaseUntil','legacy-expiresAt'])test('continuation preserves '+shape+' coordination',async()=>{
  const f=fixture();let patch={};
  if(shape==='missing-controller')f.db.rows.delete('State/controller');else if(shape==='foreign-owner')f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),owner:'synthetic-other'});else if(shape==='wrong-token')patch.token='b'.repeat(64);else if(shape==='expired-lease')f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),leaseUntil:AT});else if(shape==='string-lease')f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),leaseUntil:String(AT+WINDOW)});else if(shape==='stale-checkpoint')patch.expectedUpdatedAt=AT-1001;else if(shape==='legacy-writer')f.db.rows.set('State/checkpoint',{...f.db.rows.get('State/checkpoint'),activeWriter:'synthetic-old-writer',leaseUntil:AT+1000});else f.db.rows.set('State/checkpoint',{...f.db.rows.get('State/checkpoint'),lease:{owner:'synthetic-old-writer',[shape==='legacy-leaseUntil'?'leaseUntil':'expiresAt']:AT+1000}});
  const before=f.bytes(),answer=await f.continueOnce(patch);assert.equal(answer.response.status,409);unchanged(f,before,0);
});
test('lease expiry during transaction reads fails before the single migration write',async()=>{
  const f=fixture(),run=f.db.runTransaction.bind(f.db);f.db.runTransaction=task=>run(tx=>task({...tx,get:async ref=>{const row=await tx.get(ref);if(ref.key==='State/control')f.advance(WINDOW);return row;}}));const before=f.bytes();assert.equal((await f.continueOnce()).response.status,409);unchanged(f,before,0);
});
for(const reason of ['missing-allowance','available-allowance','expired-allowance','disabled-control','hard-stop'])test('continuation refuses '+reason+' without resetting history',async()=>{
  const f=fixture();if(reason==='missing-allowance')f.db.rows.delete(NATIVE);else if(reason==='available-allowance')f.db.rows.set(NATIVE,{...f.allowance(),reservedCents:400,calls:4});else if(reason==='expired-allowance')f.db.rows.set(NATIVE,{...f.allowance(),expiresAt:AT});else if(reason==='disabled-control')f.db.rows.set('State/control',{...f.db.rows.get('State/control'),enabled:false});else{f.setAt(core.STOP_AT);f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),leaseUntil:core.STOP_AT+WINDOW});}
  const before=f.bytes(),answer=await f.continueOnce();assert.equal(answer.response.status,reason==='disabled-control'?503:409);unchanged(f,before,0);
});
for(const cap of ['0','4','5','10.01','25'])test('configured continuation ceiling '+cap+' cannot bypass the absolute bound',async()=>{
  const f=fixture({env:{BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:cap}}),before=f.bytes();assert.equal((await f.continueOnce()).response.status,503);unchanged(f,before,0);
});
test('a lower already-configured ceiling is used exactly without accepting a caller-selected amount',async()=>{
  const f=fixture({env:{BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'7'}});await promote(f);assert.equal(f.allowance().limitCents,700);assert.equal(f.allowance().continuationCents,200);assert.equal((await f.capability()).value.enabled,true);assert.equal((await f.inspect()).value.nativeAllowance.availableCents,200);
});
for(const patch of [
  {schema:3},{limitCents:1001},{limitCents:500},{limitCents:'1000'},{continuationCents:499},{continuationCount:2},{continuationPolicy:'unproved-refill'},
  {continuedAt:AT-600001},{continuedAt:core.STOP_AT},{continuationAuthorizedBy:'invalid owner'},{continuationCheckpointUpdatedAt:'0'},
  {accountingVersion:2},{accountedCalls:1},{accountedAllocationCents:100},{spentCents:1},{reservedCents:400},{calls:6},{reservedCents:1100,calls:11},{allocatedCents:1000},{expiresAt:core.STOP_AT+1},{reservationCents:99}
])test('malformed or unproved schema2 accounting fails closed: '+JSON.stringify(patch),async()=>{
  const f=fixture();await promote(f);const demo=await token(f);f.db.rows.set(NATIVE,{...f.allowance(),...patch});const before=f.bytes(),writes=f.db.writes;
  assert.equal((await f.capability()).response.status,503);assert.equal((await f.continueOnce()).response.status,503);assert.equal((await f.start(demo)).response.status,503);unchanged(f,before,writes);
});
test('partial schema1 extension data cannot be promoted as a fresh original allocation',async()=>{
  const f=fixture();f.db.rows.set(NATIVE,{...f.allowance(),limitCents:1000});const before=f.bytes();assert.equal((await f.capability()).response.status,503);assert.equal((await f.continueOnce()).response.status,503);unchanged(f,before,0);
});
test('a future-dated continuation stays pending and cannot reserve or be reused as current authority',async()=>{
  const f=fixture();await promote(f);const demo=await token(f);f.db.rows.set(NATIVE,{...f.allowance(),continuedAt:AT+1000});const before=f.bytes(),writes=f.db.writes;
  const read=await f.inspect();assert.equal(read.response.status,200);assert.equal(read.value.nativeAllowance.state,'pending');assert.equal(read.value.nativeAllowance.effective,false);assert.equal(read.value.nativeAllowance.nextReservationFits,false);
  assert.equal((await f.capability()).response.status,429);assert.equal((await f.start(demo)).response.status,429);assert.equal((await f.continueOnce()).response.status,409);assert.equal((await f.authorize()).response.status,409);unchanged(f,before,writes);
  f.advance(1000);assert.equal((await f.capability()).response.status,200);assert.equal((await f.continueOnce()).value.continuationApplied,false);unchanged(f,before,writes);
});
for(const change of ['ceiling','reservation'])test('schema2 '+change+' configuration drift cannot reinterpret the stored continuation',async()=>{
  const f=fixture();await promote(f);const before=f.bytes(),writes=f.db.writes;if(change==='ceiling')f.env.BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP='9';else f.env.BRITES_CONCIERGE_REALTIME_RESERVE_USD='2';assert.equal((await f.capability()).response.status,503);assert.equal((await f.authorize()).response.status,503);unchanged(f,before,writes);
});
test('operator continuation does not expose its authority or synthesize invoice cost',async()=>{
  const f=fixture(),answer=await promote(f);assert.doesNotMatch(JSON.stringify(answer.value),/synthetic-only-|synthetic-policy-root|synthetic-prior-root|tokenHash|AuthorizedBy|CheckpointUpdatedAt|originalHistory/);assert.equal(answer.value.usageSettled,false);assert.equal(answer.value.refunds,0);assert.equal(answer.value.providerCalls,0);assert.equal(answer.value.allowance.accountedCalls,0);assert.equal(answer.value.allowance.spentCents,0);
});
