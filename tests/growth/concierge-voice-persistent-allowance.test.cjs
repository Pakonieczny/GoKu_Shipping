'use strict';
// Native multi-session allowance with atomic synthetic storage and provider doubles.
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


const ALLOWANCE='VoiceAllowances/native-sandbox';
const enabled=(f,changes={},requestOptions={})=>authorize(f,{action:'authorize-voice',...changes},requestOptions);
const allowance=f=>f.db.rows.get(ALLOWANCE);
const inspect=async f=>{const response=await f.handler(f.request({action:'allocation'},{operator:true}));return {response,value:await response.json()};};
const bytes=f=>JSON.stringify([...f.db.rows]);

for(const item of [
  {name:'guest',request:{operator:false},status:401},
  {name:'foreign origin',request:{origin:'https://foreign.test'},status:403},
  {name:'caller-selected money',body:{allocatedCents:1000},status:400},
  {name:'caller-selected expiry',body:{expiresAt:core.STOP_AT},status:400},
  {name:'caller-selected refund',body:{refund:true},status:400},
  {name:'invalid owner',body:{owner:'invalid owner'},status:400},
  {name:'invalid lease token',body:{token:'z'.repeat(64)},status:400},
  {name:'string revision',body:{expectedUpdatedAt:String(BASE_AT-1000)},status:400},
  {name:'live environment',env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'},status:403},
  {name:'implicit namespace',env:{BRITES_GROWTH_NAMESPACE:undefined},status:403},
  {name:'live service',namespace:'Brites_Growth_Live',status:403},
  {name:'disabled sandbox',env:{BRITES_GROWTH_SANDBOX:'0'},status:403}
])test('persistent authorization refuses '+item.name+' before storage/provider work',async()=>{
  const f=fixture(item),before=bytes(f),result=await enabled(f,item.body,item.request);
  assert.equal(result.response.status,item.status);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,0);assert.equal(f.db.counts.reads,0);assert.equal(f.counts.setup,0);assert.equal(f.provider.length,0);
});

test('one fixed persistent allowance lasts until the hard stop without changing previous holds, grants, checkpoint or provider activity',async()=>{
  const f=fixture();await authorize(f);const before=bytes(f),legacy=f.legacyBytes(),oldGrant=clone(f.grant()),{response,value}=await enabled(f);
  assert.equal(response.status,200);assert.equal(value.ok,true);assert.equal(value.reused,false);assert.equal(value.providerCalls,0);assert.equal(value.refunds,0);assert.equal(value.legacyAllocationChanged,false);
  assert.equal(value.allowance.allocatedCents,500);assert.equal(value.allowance.reservationCents,100);assert.equal(value.allowance.reservedCents,0);assert.equal(value.allowance.state,'available');assert.equal(value.allowance.expiresAt,core.STOP_AT);
  assert.ok(value.allowance.expiresAt-BASE_AT>WINDOW_MS);assert.equal(f.legacyBytes(),legacy);assert.deepEqual(f.grant(),oldGrant);assert.equal(f.provider.length,0);
  assert.equal(JSON.stringify([...f.db.rows].filter(([key])=>key!==ALLOWANCE)),before);assert.doesNotMatch(JSON.stringify(value),/tokenHash|synthetic-only-operator|synthetic-only-provider-key|originalHistory/);assert.equal(JSON.stringify(value).includes(TOKEN),false);
});

test('simultaneous authorizations create one cumulative document, and all later owners reuse capacity without refilling it',async()=>{
  const f=fixture(),initial=f.savedBytes(),results=await Promise.all(Array.from({length:8},()=>enabled(f)));
  assert.ok(results.every(({response,value})=>response.status===200&&value.ok));assert.equal(results.filter(({value})=>!value.reused).length,1);assert.equal(f.db.counts.writes,1);assert.equal(f.savedBytes(),initial);
  const first=clone(allowance(f));assert.equal((await nativeStart(f,await token(f))).status,200);
  f.advance(WINDOW_MS+60000);const newToken='d'.repeat(64);f.db.rows.set('State/controller',{owner:'synthetic-next-root',tokenHash:core.hash(newToken),leaseUntil:f.now()+WINDOW_MS,updatedAt:f.now()});
  const before=bytes(f),writes=f.db.counts.writes,result=await enabled(f,{owner:'synthetic-next-root',token:newToken});
  assert.equal(result.response.status,200);assert.equal(result.value.reused,true);assert.equal(result.value.allowance.reservedCents,100);assert.equal(result.value.allowance.allocatedCents,500);assert.equal(result.value.allowance.issuedAt,first.issuedAt);assert.equal(result.value.allowance.expiresAt,first.expiresAt);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);
});

for(const shape of ['missing','foreign','expired','malformed','stale','legacy-activeWriter','legacy-leaseUntil','legacy-expiresAt'])test('persistent authorization preserves '+shape+' coordination and cannot write without an active current owned lease',async()=>{
  const f=fixture();if(shape==='missing')f.db.rows.delete('State/controller');
  else if(shape==='foreign')f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),owner:'other-root'});
  else if(shape==='expired')f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),leaseUntil:f.now()});
  else if(shape==='malformed')f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),leaseUntil:String(f.now()+60000)});
  else if(shape==='stale')f.db.rows.set('State/checkpoint',{...f.db.rows.get('State/checkpoint'),updatedAt:BASE_AT+1});
  else if(shape==='legacy-activeWriter')f.db.rows.set('State/checkpoint',{...f.db.rows.get('State/checkpoint'),activeWriter:'initial-root',leaseUntil:f.now()+60000});
  else f.db.rows.set('State/checkpoint',{...f.db.rows.get('State/checkpoint'),lease:{owner:'initial-root',[shape==='legacy-leaseUntil'?'leaseUntil':'expiresAt']:f.now()+60000}});
  const before=bytes(f),result=await enabled(f,shape==='stale'?{expectedUpdatedAt:BASE_AT-1000}:{});assert.equal(result.response.status,409);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,0);assert.equal(f.provider.length,0);
});

test('an owner whose lease expires while transaction reads finish cannot authorize an allowance',async()=>{
  const f=fixture(),run=f.db.runTransaction.bind(f.db);f.db.runTransaction=task=>run(tx=>task({...tx,get:async ref=>{const value=await tx.get(ref);if(ref.key==='State/control')f.advance(WINDOW_MS);return value;}}));
  const before=bytes(f),result=await enabled(f);assert.equal(result.response.status,409);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,0);assert.equal(f.provider.length,0);
});

test('availability stays read-only across hours and after repair lease expiry; each explicit native start reserves once',async()=>{
  const f=fixture();await enabled(f);const legacy=f.legacyBytes(),before=bytes(f),writes=f.db.counts.writes;
  f.advance(2*60*60000);const checks=await Promise.all(Array.from({length:5},()=>capability(f)));assert.ok(checks.every(({response,value})=>response.status===200&&value.enabled));assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
  for(let n=1;n<=3;n++){assert.equal((await nativeStart(f,await token(f))).status,200);assert.equal(allowance(f).reservedCents,n*100);assert.equal(allowance(f).calls,n);f.advance(60*60000);}
  assert.equal(f.counts.providerStarts,3);assert.equal(f.legacyBytes(),legacy);assert.equal(allowance(f).spentCents,0);assert.equal(allowance(f).expiresAt,core.STOP_AT);
});

test('six simultaneous native starts can reserve only five sessions under the fixed cumulative ceiling',async()=>{
  const f=fixture();await authorize(f);await enabled(f);const oldGrant=clone(f.grant()),legacy=f.legacyBytes(),tokens=await Promise.all(Array.from({length:6},()=>token(f))),responses=await Promise.all(tokens.map(value=>nativeStart(f,value)));
  assert.deepEqual(responses.map(response=>response.status).sort(),[200,200,200,200,200,429]);assert.equal(f.counts.providerStarts,5);assert.equal(allowance(f).reservedCents,500);assert.equal(allowance(f).spentCents,0);assert.equal(allowance(f).calls,5);assert.equal(f.deadlines.length,5);assert.equal(f.legacyBytes(),legacy);assert.deepEqual(f.grant(),oldGrant);
  assert.equal((await capability(f)).response.status,429);assert.equal((await enabled(f)).value.allowance.state,'exhausted');assert.deepEqual(f.grant(),oldGrant);
  const attempts=[...f.db.rows].filter(([key,value])=>key.startsWith('VoiceUsage/session-')&&value.fundingSource==='native_allowance');assert.equal(attempts.length,5);assert.ok(attempts.every(([,row])=>row.allowanceId==='native-sandbox'&&row.allocatedCents===100&&row.stage==='call_verified'));
});

test('same-token concurrent starts and subsequent replays never reserve or call twice',async()=>{
  const f=fixture();await enabled(f);const demo=await token(f),responses=await Promise.all([nativeStart(f,demo),nativeStart(f,demo)]);assert.deepEqual(responses.map(response=>response.status).sort(),[200,401]);assert.equal(f.counts.providerStarts,1);assert.equal(allowance(f).reservedCents,100);
  const before=bytes(f),writes=f.db.counts.writes,replay=await nativeStart(f,demo);assert.equal(replay.status,401);assert.equal((await replay.json()).code,'VOICE_SESSION_REUSED');assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.counts.providerStarts,1);
});

test('typed conversations and non-provenance callers cannot spend the native-only persistent allowance',async()=>{
  const f=fixture();await enabled(f);const before=bytes(f),writes=f.db.counts.writes;
  assert.equal(await voice.createDemoBudgetReservation(f.service,{capUsd:10,now:f.now})(.05,'synthetic-typed-session-id'),null);
  const result=await voice.createDemoBudgetReservationResult(f.service,{capUsd:10,now:f.now,allowContinuation:true})(1,'synthetic-unlinked-native-id');assert.deepEqual(result,{granted:false,reason:'ALLOCATION_EXHAUSTED'});
  assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
});

test('old authorization replay returns its original record but new legacy slots cannot replenish persistent capacity',async()=>{
  const f=fixture();await authorize(f);await enabled(f);const grant=clone(f.grant()),before=bytes(f),writes=f.db.counts.writes,replay=await authorize(f);assert.equal(replay.response.status,200);assert.equal(replay.value.reused,true);assert.deepEqual(f.grant(),grant);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);
  const next='e'.repeat(64);f.db.rows.set('State/controller',{owner:'synthetic-other-root',tokenHash:core.hash(next),leaseUntil:f.now()+WINDOW_MS,updatedAt:f.now()});
  const after=bytes(f),denied=await authorize(f,{owner:'synthetic-other-root',token:next});assert.equal(denied.response.status,409);assert.equal(bytes(f),after);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
});

for(const outcome of ['provider-rejection','transport-uncertainty','deadline-uncertainty','deadline-and-hangup-uncertainty'])test('persistent '+outcome+' keeps its full hold and never starts a paid automatic retry',async()=>{
  const options=outcome==='provider-rejection'?{provider:async()=>Response.json({error:{code:'insufficient_quota',message:'PRIVATE_PROVIDER_DETAIL'}},{status:429,headers:{'x-request-id':'req_rejected'}})}:outcome==='transport-uncertainty'?{provider:async()=>{throw Error('PRIVATE_TRANSPORT_DETAIL');}}:{deadlineFailure:true,hangupFailure:outcome==='deadline-and-hangup-uncertainty'};
  const f=fixture(options);await enabled(f);const demo=await token(f),legacy=f.legacyBytes(),response=await nativeStart(f,demo),value=await response.json();assert.equal(response.status,503);assert.doesNotMatch(JSON.stringify(value),/PRIVATE_|synthetic-only-provider-key/);assert.equal(f.counts.providerStarts,1);assert.equal(allowance(f).reservedCents,100);assert.equal(allowance(f).spentCents,0);assert.equal(f.legacyBytes(),legacy);
  const attempt=[...f.db.rows].find(([key,row])=>key.startsWith('VoiceUsage/session-')&&row.fundingSource==='native_allowance')[1];assert.equal(attempt.reconcile,'provider_evidence_required');assert.equal(attempt.stage,outcome==='provider-rejection'?'provider_rejected':outcome==='transport-uncertainty'?'unknown':'deadline_failed');
  const before=bytes(f),writes=f.db.counts.writes;assert.equal((await nativeStart(f,demo)).status,401);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.counts.providerStarts,1);assert.equal((await capability(f)).response.status,200);
});

for(const boundary of ['control','hard'])test('persistent '+boundary+' stop limits authorization, availability and the actual call deadline',async()=>{
  const at=boundary==='hard'?core.STOP_AT-60000:BASE_AT,stopAt=boundary==='hard'?core.STOP_AT+86400000:at+60000,f=fixture({at,control:{stopAt}}),result=await enabled(f),expected=boundary==='hard'?core.STOP_AT:stopAt;
  assert.equal(result.response.status,200);assert.equal(result.value.allowance.expiresAt,expected);const response=await nativeStart(f,await token(f)),answer=await response.json();assert.equal(response.status,200);assert.equal(answer.expiresAt,expected);assert.equal(answer.maxDurationMs,60000);assert.equal(f.deadlines[0].expiresAt,expected);
  f.setAt(expected);const before=bytes(f),writes=f.db.counts.writes,calls=f.provider.length;assert.equal((await capability(f)).response.status,503);assert.equal((await enabled(f)).response.status,409);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,calls);
});

test('a stored earlier expiry cannot be extended by changing control or a new controller lease',async()=>{
  const f=fixture({control:{stopAt:BASE_AT+60000}});await enabled(f);f.advance(60001);f.db.rows.set('State/control',{...f.db.rows.get('State/control'),stopAt:core.STOP_AT});
  const before=bytes(f),writes=f.db.counts.writes,result=await enabled(f);assert.equal(result.response.status,200);assert.equal(result.value.reused,true);assert.equal(result.value.allowance.state,'expired');assert.equal(result.value.allowance.expiresAt,BASE_AT+60000);assert.equal((await capability(f)).response.status,429);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);
});

test('a call near the immutable allowance expiry cannot run past it when a later control stop is increased',async()=>{
  const f=fixture({control:{stopAt:BASE_AT+60000}});await enabled(f);f.db.rows.set('State/control',{...f.db.rows.get('State/control'),stopAt:core.STOP_AT});f.advance(30000);
  const response=await nativeStart(f,await token(f)),answer=await response.json();assert.equal(response.status,200);assert.equal(answer.expiresAt,BASE_AT+60000);assert.equal(answer.maxDurationMs,30000);assert.equal(f.deadlines[0].expiresAt,BASE_AT+60000);assert.equal(allowance(f).expiresAt,BASE_AT+60000);
});

test('shortening the stored stop blocks later starts even with a recently valid demo token',async()=>{
  const f=fixture();await enabled(f);const demo=await token(f);f.db.rows.set('State/control',{...f.db.rows.get('State/control'),stopAt:f.now()+1000});f.advance(1000);
  const before=bytes(f),writes=f.db.counts.writes;assert.equal((await nativeStart(f,demo)).status,503);assert.equal((await capability(f)).response.status,503);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
});

for(const stopDuringProvider of ['hard-stop','disabled-control'])test(stopDuringProvider+' after a reservation closes the accepted call and preserves its hold',async()=>{
  let f;f=fixture({provider:async()=>{if(stopDuringProvider==='hard-stop')f.setAt(core.STOP_AT);else f.db.rows.set('State/control',{...f.db.rows.get('State/control'),enabled:false});return new Response(SDP,{status:201,headers:{Location:'/v1/realtime/calls/rtc_stop','x-request-id':'req_stop'}});}});await enabled(f);
  const response=await nativeStart(f,await token(f)),value=await response.json();assert.equal(response.status,503);assert.equal(value.code,'VOICE_RUNTIME_DISABLED');assert.equal(f.counts.providerStarts,1);assert.equal(f.counts.hangups,1);assert.equal(f.deadlines.length,0);assert.equal(allowance(f).reservedCents,100);
  const row=[...f.db.rows].find(([key,row])=>key.startsWith('VoiceUsage/session-')&&row.fundingSource==='native_allowance')[1];assert.equal(row.providerCode,'SANDBOX_STOP_REACHED');assert.equal(row.hangupConfirmed,true);assert.equal(row.reconcile,'provider_evidence_required');
});

for(const patch of [{reservedCents:'100'},{spentCents:1},{calls:'0'},{reservedCents:501},{reservedCents:100,calls:0},{allocatedCents:600},{provider:'untrusted'},{reservationCents:'100'},{lastStartedAt:BASE_AT},{expiresAt:core.STOP_AT+1},{issuedAt:0}])test('invalid persistent record fails closed without rewriting it: '+JSON.stringify(patch),async()=>{
  const f=fixture();await enabled(f);const demo=await token(f);f.db.rows.set(ALLOWANCE,{...allowance(f),...patch});const before=bytes(f),writes=f.db.counts.writes,cap=await capability(f),response=await nativeStart(f,demo),result=await enabled(f);
  assert.equal(cap.response.status,503);assert.equal(cap.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(response.status,503);assert.equal((await response.json()).code,'VOICE_GUARD_UNAVAILABLE');assert.equal(result.response.status,503);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
});

test('malformed old budget still fails closed even when a valid native allowance exists',async()=>{
  for(const patch of [{reservedCents:'900',spentCents:100,calls:10},{reservedCents:900,spentCents:null,calls:10},{reservedCents:Number.MAX_SAFE_INTEGER,spentCents:1,calls:10}]){
    const f=fixture();await enabled(f);const demo=await token(f);f.db.rows.set('VoiceUsage/preview-budget',patch);const before=bytes(f),writes=f.db.counts.writes;
    assert.equal((await capability(f)).response.status,503);assert.equal((await nativeStart(f,demo)).status,503);assert.equal((await enabled(f)).response.status,503);assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
  }
});

test('read-only operator allocation shows persistent holds and provenance separately from original history',async()=>{
  const f=fixture();await authorize(f);await enabled(f);assert.equal((await nativeStart(f,await token(f))).status,200);const before=bytes(f),writes=f.db.counts.writes,calls=f.provider.length,{response,value}=await inspect(f);
  assert.equal(response.status,200);assert.equal(value.readOnly,true);assert.equal(value.financialWrites,0);assert.equal(value.providerCalls,0);assert.equal(value.refunds,0);assert.equal(value.evidenceComplete,false);
  assert.equal(value.nativeAllowance.exists,true);assert.equal(value.nativeAllowance.valid,true);assert.equal(value.nativeAllowance.state,'available');assert.equal(value.nativeAllowance.allocatedCents,500);assert.equal(value.nativeAllowance.reservedCents,100);assert.equal(value.nativeAllowance.availableCents,400);assert.equal(value.nativeAllowance.calls,1);assert.equal(value.nativeAllowance.expiresAt,core.STOP_AT);
  assert.equal(value.budget.reservedCents,900);assert.equal(value.budget.spentCents,100);assert.equal(value.budget.attempts,10);assert.equal(value.recordWindow.heldCents,100);assert.equal(value.nativeContinuations.heldCentsInReadWindow,0);assert.ok(value.records.some(row=>row.fundingSource==='native_allowance'&&row.allowanceId==='native-sandbox'&&row.callId));
  assert.equal(bytes(f),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,calls);assert.doesNotMatch(JSON.stringify(value),/synthetic-only-provider-key|synthetic-only-operator|tokenHash|authorizedBy|checkpointUpdatedAt/);
});

test('native co-selection uses the exact selected piece approved story, qualified meanings and neutral evidence without unsolicited hover speech',()=>{
  const config=voice.sessionConfig({});assert.ok(config.tools.some(tool=>tool.name==='find_jewellery'));assert.ok(config.tools.some(tool=>tool.name==='inspect_jewellery'));
  assert.match(config.instructions,/after find_jewellery returns its checked approved public story/);assert.match(config.instructions,/one short interesting connection about that exact motif/);assert.match(config.instructions,/find_jewellery with its checked owned product URL and the shopper/);assert.match(config.instructions,/inspect_jewellery verifies live options and does not supply a researched story/);
  assert.match(config.instructions,/neutral citations and cultural context/);assert.match(config.instructions,/can represent/);assert.match(config.instructions,/never invent a universal meaning/);assert.match(config.instructions,/do not turn a hover into unsolicited speech/);assert.match(config.instructions,/When a tool cannot verify something, say so/);
});
