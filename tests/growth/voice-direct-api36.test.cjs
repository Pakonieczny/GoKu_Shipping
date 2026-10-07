'use strict';
// Synthetic storage/transport only. Nothing here contacts a provider or device.
const test=require('node:test'),assert=require('node:assert/strict');
const voice=require('../../netlify/functions/_britesConciergeVoice.js'),core=require('../../netlify/functions/_britesGrowth.js');
const AT=Date.parse('2026-10-08T00:00:00Z'),SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n',KEY='synthetic-existing-provider-key';
const MONEY=new Set(['VoiceUsage','VoiceAllowances','VoiceContinuations','Usage']);
const copy=value=>value===undefined?undefined:structuredClone(value);
function fixture(options={}){
  let at=AT,tail=Promise.resolve(),transaction=0,callNumber=0;
  const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'1',OPENAI_API_KEY:KEY,...options.env};
  const rows=new Map([['State/control',{enabled:true,aiEnabled:false,aiDailyUsdCap:0,stopAt:core.STOP_AT,...options.control}]]),accesses=[],writes=[],provider=[],operations=[],rateLimits=[];
  if(options.ledgers!=='missing'){
    for(const [name,id,value]of [
      ['VoiceUsage','preview-budget',{reservedCents:905,spentCents:0,calls:10,allocationCapCents:1000}],
      ['VoiceAllowances','native-sandbox',{reservedCents:1000,spentCents:0,calls:10,limitCents:1000}],
      ['VoiceContinuations','active',{grantId:'synthetic-historical-only'}],
      ['Usage','2026-10-07',{reservedUsd:100,spentUsd:10,calls:100}]
    ])rows.set(name+'/'+id,options.ledgers==='malformed'?{reservedCents:'invalid',spentUsd:null,calls:-1,privateHistory:'preserve-exactly'}:value);
  }
  const financial=()=>JSON.stringify([...rows].filter(([key])=>MONEY.has(key.split('/')[0])));
  const beforeFinancial=financial(),failStages=new Set(options.failStages||[]);
  const db={runTransaction(task){const result=tail.then(async()=>{
    const ordinal=++transaction,pending=[];
    const answer=await task({get:async ref=>{
      assert.equal(pending.length,0,'transaction reads precede writes');
      if(options.failRead?.(ref.key,ordinal))throw Error('synthetic private read failure');
      return ref.get();
    },set(ref,value){if(failStages.has(value.stage))throw Error('synthetic private write failure');pending.push([ref,copy(value)]);}});
    for(const [ref,value]of pending){rows.set(ref.key,value);writes.push(ref.key);operations.push('stored:'+value.stage);}
    return answer;
  });tail=result.catch(()=>{});return result;}};
  const service={namespace:options.namespace??'Brites_Growth_Sandbox',setup:async()=>copy(rows.get('State/control')),rateLimit:async(name,limit)=>{rateLimits.push({name,limit});return options.rateLimit!==false;},col(name){
    accesses.push(name);assert.equal(MONEY.has(name),false,'native voice must not access historical money collection '+name);
    return {firestore:db,doc(id){const key=name+'/'+id;return {id,key,get:async()=>({exists:rows.has(key),data:()=>copy(rows.get(key))}),set:async(value,opts)=>{rows.set(key,opts?.merge?{...rows.get(key),...copy(value)}:copy(value));writes.push(key);}};}};
  }};
  const fetch=async(url,init)=>{
    provider.push({url,method:init?.method});
    if(url.endsWith('/hangup')){operations.push('provider:hangup');if(options.hangupThrows)throw Error('synthetic private cleanup failure');return new Response(null,{status:options.hangupStatus??200});}
    assert.equal(url,'https://api.openai.com/v1/realtime/calls');assert.equal(init.headers.Authorization,'Bearer '+KEY);operations.push('provider:start');
    const current=[...rows.values()].filter(row=>row.kind==='realtime_voice'&&row.stage==='provider_requested');assert.ok(current.length>0,'dispatch journal is durable before the provider request');
    if(options.providerThrows)throw Error('synthetic private provider failure');
    if(options.reject)return Response.json({error:{code:'insufficient_quota',message:'synthetic private account detail'}},{status:429,headers:{'x-request-id':'req_rejected_synthetic'}});
    if(options.afterDispatch)options.afterDispatch(rows);
    const id='rtc_direct_synthetic_'+(++callNumber),response=new Response(options.badSdp?'not-an-sdp':SDP,{status:201,headers:{Location:'/v1/realtime/calls/'+id,'x-request-id':'req_direct_synthetic_'+callNumber}});
    if(options.unreadableSdp)response.text=async()=>{throw Error('synthetic private body failure');};
    return response;
  };
  const handler=voice.createHandler({env,service,fetch,now:()=>at,authorize:async req=>req.headers.get('X-Growth-Key')==='synthetic-operator',reserveBudget:async()=>{throw Error('an obsolete financial injection must not run');},scheduleHangup:async row=>{
    operations.push('deadline');if(options.deadlineFailure)return false;await service.col('VoiceDeadlines').doc(row.callId).set({...row,at,state:'pending'});return true;
  }});
  const request=(body,{operator=false,origin='https://preview.test'}={})=>new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{'Content-Type':'application/json',...(origin?{Origin:origin}:{}),...(operator?{'X-Growth-Key':'synthetic-operator'}:{})},body:JSON.stringify(body)});
  const call=async(body,requestOptions)=>{const response=await handler(request(body,requestOptions));return {response,value:await response.json()};};
  const capability=requestOptions=>call({action:'capabilities'},requestOptions);
  const start=(demoToken,requestOptions)=>call({action:'start',sdp:SDP,demoToken},requestOptions);
  const freshStart=async requestOptions=>{const cap=await capability(requestOptions);assert.equal(cap.response.status,200);return {token:cap.value.demoToken,...await start(cap.value.demoToken,requestOptions)};};
  const sessions=()=>[...rows].filter(([key])=>key.startsWith('VoiceSessions/'));
  const unchangedMoney=()=>{assert.equal(financial(),beforeFinancial);assert.equal(accesses.some(name=>MONEY.has(name)),false);assert.equal(writes.some(key=>MONEY.has(key.split('/')[0])),false);};
  return {env,rows,service,provider,operations,accesses,writes,rateLimits,failStages,call,capability,start,freshStart,sessions,unchangedMoney,now:()=>at,advance:ms=>at+=ms};
}
for(const ledgers of ['exhausted','malformed','missing'])test('direct native API works with '+ledgers+' obsolete ledgers, missing dollar configuration and disabled bulk AI',async()=>{
  const f=fixture({ledgers}),answer=await f.freshStart();assert.equal(answer.response.status,200);assert.equal(answer.value.sdp,SDP);assert.equal(answer.value.maxDurationMs,120000);assert.equal(answer.value.expiresAt,AT+120000);
  const [[id,row]]=f.sessions();assert.match(id,/^VoiceSessions\/session-[a-f0-9]{64}$/);assert.equal(row.id,id.split('/')[1]);assert.equal(row.schema,1);assert.equal(row.kind,'realtime_voice');assert.equal(row.provider,'openai');assert.equal(row.stage,'call_verified');assert.equal(row.callId,'rtc_direct_synthetic_1');assert.equal(row.providerRequestId,'req_direct_synthetic_1');assert.equal(row.startedAt,AT);assert.equal(row.tokenExpiresAt,AT+600000);assert.equal(row.outcomeAt,AT);
  assert.ok(f.operations.indexOf('stored:provider_requested')<f.operations.indexOf('provider:start'));assert.ok(f.operations.indexOf('stored:call_verified')>f.operations.indexOf('deadline'));assert.equal(f.provider.length,1);assert.doesNotMatch(JSON.stringify(row),/allocated|reserved|spent|refund|cents|usd/i);f.unchangedMoney();
});
test('nonsense obsolete amount configuration cannot block direct native voice',async()=>{
  const f=fixture({env:{BRITES_CONCIERGE_REALTIME_RESERVE_USD:'invalid',BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'-1'},control:{aiEnabled:false,aiDailyUsdCap:undefined}});assert.equal((await f.freshStart()).response.status,200);f.unchangedMoney();
});
test('operator-only mode also uses signed one-start tokens and ignores bulk AI budget switches',async()=>{
  const f=fixture({env:{BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'0'}});assert.equal((await f.capability()).response.status,401);
  const cap=await f.capability({operator:true});assert.equal(cap.response.status,200);assert.equal(typeof cap.value.demoToken,'string');assert.equal((await f.start(cap.value.demoToken,{operator:true})).response.status,200);assert.equal((await f.start(cap.value.demoToken,{operator:true})).response.status,401);assert.equal(f.provider.length,1);f.unchangedMoney();
});
test('capability and recheck reads acquire no session, microphone or provider request',async()=>{
  const f=fixture(),tokens=[];for(let n=0;n<4;n++){const answer=await f.capability();assert.equal(answer.response.status,200);assert.equal(answer.value.enabled,true);assert.equal(answer.value.optIn,true);assert.equal(answer.value.usage,'provider_account');tokens.push(answer.value.demoToken);}
  assert.equal(new Set(tokens).size,4);assert.equal(f.sessions().length,0);assert.equal(f.writes.length,0);assert.equal(f.provider.length,0);f.unchangedMoney();
});
test('twelve separate explicit starts exceed the retired ten-slot scheme without consulting historical balances',async()=>{
  const f=fixture();for(let n=0;n<12;n++)assert.equal((await f.freshStart()).response.status,200);
  assert.equal(f.sessions().length,12);assert.equal(f.provider.length,12);assert.equal(f.rateLimits.filter(row=>row.name==='realtime-preview-start'&&row.limit===3).length,12);assert.equal(f.rateLimits.filter(row=>row.name.startsWith('realtime-preview-caller-')&&row.limit===2).length,12);f.unchangedMoney();
});
test('eight simultaneous starts with one signed token create one journal and one provider call',async()=>{
  const f=fixture(),cap=await f.capability(),answers=await Promise.all(Array.from({length:8},()=>f.start(cap.value.demoToken)));
  assert.deepEqual(answers.map(x=>x.response.status).sort(),[200,401,401,401,401,401,401,401]);assert.equal(f.sessions().length,1);assert.equal(f.provider.length,1);assert.ok(answers.filter(x=>x.response.status===401).every(x=>x.value.code==='VOICE_SESSION_REUSED'));
  const before=JSON.stringify(f.sessions());assert.equal((await f.start(cap.value.demoToken)).response.status,401);assert.equal(JSON.stringify(f.sessions()),before);assert.equal(f.provider.length,1);f.unchangedMoney();
});
test('dispatch journal write failure withholds paid submission and does not invent a financial reservation',async()=>{
  const f=fixture({failStages:['provider_requested']}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(answer.value.sdp,undefined);assert.equal(f.provider.length,0);assert.equal(f.sessions().length,0);assert.equal(f.rows.get('VoiceDiagnostics/last-start').code,'PROVENANCE_UNAVAILABLE');f.unchangedMoney();
  f.failStages.clear();assert.equal((await f.start(answer.token)).response.status,200);assert.equal(f.sessions().length,1);assert.equal(f.provider.length,1);f.unchangedMoney();
});
for(const collection of ['VoiceSessions','State'])test('initial '+collection+' transaction read failure withholds provider dispatch',async()=>{
  const f=fixture({failRead:(key,ordinal)=>ordinal===1&&key.startsWith(collection+'/')}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(f.provider.length,0);assert.equal(f.sessions().length,0);f.unchangedMoney();
});
for(const hangupStatus of [200,404,503])test('accepted journal write failure hangs up only its exact call, withholds SDP and handles status'+hangupStatus,async()=>{
  const f=fixture({failStages:['call_verified'],hangupStatus}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(answer.value.sdp,undefined);assert.equal(answer.value.stopToken,undefined);
  assert.deepEqual(f.provider.map(row=>row.url),['https://api.openai.com/v1/realtime/calls','https://api.openai.com/v1/realtime/calls/rtc_direct_synthetic_1/hangup']);const [[,row]]=f.sessions();assert.equal(row.stage,'unknown');assert.equal(row.callId,'rtc_direct_synthetic_1');assert.equal(row.providerRequestId,'req_direct_synthetic_1');assert.equal(row.providerCode,'PROVENANCE_UNAVAILABLE');assert.equal(row.hangupConfirmed,hangupStatus!==503);assert.equal(f.rows.get('VoiceDeadlines/rtc_direct_synthetic_1').state,hangupStatus===503?'pending':'closed');f.unchangedMoney();
});
test('accepted journal read failure also withholds SDP and preserves known provider linkage on recovery',async()=>{
  const f=fixture({failRead:(key,ordinal)=>ordinal===2&&key.startsWith('VoiceSessions/')}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.sdp,undefined);assert.equal(f.provider.length,2);assert.equal(f.sessions()[0][1].callId,'rtc_direct_synthetic_1');assert.equal(f.sessions()[0][1].stage,'unknown');f.unchangedMoney();
});
test('persistent accepted-journal and hangup transport failures retain the exact durable deadline backup',async()=>{
  const f=fixture({failStages:['call_verified','unknown'],hangupThrows:true}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.sdp,undefined);assert.equal(f.sessions()[0][1].stage,'provider_requested');const row=f.rows.get('VoiceDeadlines/rtc_direct_synthetic_1');assert.equal(row.callId,'rtc_direct_synthetic_1');assert.equal(row.state,'pending');assert.equal(row.expiresAt,AT+120000);assert.equal(f.provider.length,2);f.unchangedMoney();
});
test('a runtime pause observed in the journal transaction blocks dispatch despite stale setup data',async()=>{
  const f=fixture(),cap=await f.capability(),stale=copy(f.rows.get('State/control'));f.service.setup=async()=>stale;f.rows.set('State/control',{...stale,enabled:false});const answer=await f.start(cap.value.demoToken);assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_RUNTIME_DISABLED');assert.equal(f.provider.length,0);assert.equal(f.sessions().length,0);f.unchangedMoney();
});
for(const change of ['paused','stopped'])test('runtime '+change+' after accepted provider start triggers exact cleanup without releasing SDP',async()=>{
  const f=fixture({afterDispatch:rows=>rows.set('State/control',{...rows.get('State/control'),...(change==='paused'?{enabled:false}:{stopAt:AT})})}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_RUNTIME_DISABLED');assert.equal(answer.value.sdp,undefined);assert.equal(f.provider.at(-1).url.endsWith('/rtc_direct_synthetic_1/hangup'),true);assert.equal(f.sessions()[0][1].stage,'deadline_failed');f.unchangedMoney();
});
test('durable deadline enqueue failure closes the known call and withholds SDP without touching money history',async()=>{
  const f=fixture({deadlineFailure:true}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(answer.value.sdp,undefined);assert.equal(f.provider.at(-1).url.endsWith('/rtc_direct_synthetic_1/hangup'),true);assert.equal(f.sessions()[0][1].stage,'deadline_failed');assert.equal(f.sessions()[0][1].hangupConfirmed,true);f.unchangedMoney();
});
for(const failure of ['badSdp','unreadableSdp'])test(failure+' accepted answer keeps server-observed call linkage and cleans up exactly',async()=>{
  const f=fixture({[failure]:true}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.sdp,undefined);assert.equal(f.provider.at(-1).url.endsWith('/rtc_direct_synthetic_1/hangup'),true);assert.equal(f.sessions()[0][1].callId,'rtc_direct_synthetic_1');assert.equal(f.sessions()[0][1].hangupConfirmed,true);f.unchangedMoney();
});
for(const failure of ['reject','providerThrows'])test(failure+' is journalled privately without automatic retry or historical financial changes',async()=>{
  const f=fixture({[failure]:true}),answer=await f.freshStart();assert.equal(answer.response.status,503);assert.equal(answer.value.sdp,undefined);assert.doesNotMatch(JSON.stringify(answer.value),/synthetic|private|insufficient_quota|Bearer/);assert.equal(f.sessions()[0][1].stage,failure==='reject'?'provider_rejected':'unknown');assert.equal(f.provider.length,1);
  assert.equal((await f.start(answer.token)).response.status,401);assert.equal(f.provider.length,1);f.unchangedMoney();
});
for(const action of ['authorize-test','authorize-voice'])test(action+' is an operator-only fixed410 retirement without any lease, money or provider action',async()=>{
  const f=fixture(),body={action,owner:'synthetic-private-owner',token:'synthetic-private-token',continueToConfiguredCeiling:true,allocatedCents:999999};assert.equal((await f.call(body)).response.status,401);
  const one=await f.call(body,{operator:true}),two=await f.call({action},{operator:true});assert.equal(one.response.status,410);assert.deepEqual(one.value,two.value);assert.equal(one.value.code,'VOICE_ALLOCATION_AUTHORIZATION_RETIRED');assert.doesNotMatch(JSON.stringify(one.value),/synthetic|999999/);assert.equal(f.accesses.length,0);assert.equal(f.writes.length,0);assert.equal(f.provider.length,0);f.unchangedMoney();
});
test('signed stop cleans up the exact journalled call without creating a second start or modifying historical money',async()=>{
  const f=fixture(),answer=await f.freshStart(),before=JSON.stringify(f.sessions());assert.equal(answer.response.status,200);assert.equal((await f.call({action:'stop',stopToken:answer.value.stopToken})).value.stopped,true);assert.equal(f.provider.length,2);assert.equal(f.provider.at(-1).url.endsWith('/rtc_direct_synthetic_1/hangup'),true);assert.equal(f.rows.get('VoiceDeadlines/rtc_direct_synthetic_1').state,'closed');assert.equal(JSON.stringify(f.sessions()),before);f.unchangedMoney();
});
test('rate limits, disabled sandbox, exact service scope and stop date remain effective without money gates',async()=>{
  const rate=fixture({rateLimit:false}),denied=await rate.freshStart();assert.equal(denied.response.status,429);assert.equal(rate.provider.length,0);assert.equal(rate.sessions().length,0);rate.unchangedMoney();
  for(const options of [{control:{enabled:false}},{control:{stopAt:AT}},{namespace:'Brites_Growth_Live'}]){const f=fixture(options),answer=await f.capability();assert.equal(answer.response.status,503);assert.equal(f.provider.length,0);assert.equal(f.sessions().length,0);f.unchangedMoney();}
  for(const patch of [{BRITES_CONCIERGE_REALTIME_ENABLED:'0'},{BRITES_GROWTH_SANDBOX:'0'},{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'}]){const f=fixture({env:patch}),answer=await f.capability();assert.equal(answer.value.enabled,false);assert.equal(f.provider.length,0);assert.equal(f.sessions().length,0);f.unchangedMoney();}
});
test('foreign origin, unknown fields and invalid signed tokens cannot create a native session',async()=>{
  const f=fixture(),cap=await f.capability();assert.equal((await f.start(cap.value.demoToken,{origin:'https://other.test'})).response.status,403);
  for(const body of [{action:'start',sdp:SDP},{action:'start',sdp:SDP,demoToken:cap.value.demoToken+'x'},{action:'start',sdp:SDP,demoToken:cap.value.demoToken,allocatedUsd:10},{action:'start',sdp:'invalid',demoToken:cap.value.demoToken}])assert.ok([400,401].includes((await f.call(body)).response.status));
  f.advance(600001);assert.equal((await f.start(cap.value.demoToken)).response.status,401);assert.equal(f.provider.length,0);assert.equal(f.sessions().length,0);f.unchangedMoney();
});
