'use strict';
// Atomic synthetic storage and provider responses only; no network or credentials.
const assert=require('node:assert/strict');
const core=require('../../netlify/functions/_britesGrowth.js');
const voice=require('../../netlify/functions/_britesConciergeVoice.js');
const AT=Date.parse('2026-10-07T23:00:00Z');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n';
const OPERATOR='synthetic-only-operator',KEY='synthetic-only-provider-key';
const ENV={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'1',OPENAI_API_KEY:KEY};
const clone=value=>value===undefined?undefined:structuredClone(value);
class Database{
  constructor(rows=[],{failStages=[],missingOutcomeRecord=false}={}){this.rows=new Map(rows.map(([key,value])=>[key,clone(value)]));this.tail=Promise.resolve();this.counts={reads:0,writes:0};this.queries=[];this.writes=[];this.operations=[];this.failStages=new Set(failStages);this.missingOutcomeRecord=missingOutcomeRecord;this.transactions=0;}
  snapshot(key){return {exists:this.rows.has(key),data:()=>clone(this.rows.get(key))};}
  write(key,value,options){this.counts.writes++;this.writes.push(key);this.rows.set(key,options?.merge?{...this.rows.get(key),...clone(value)}:clone(value));if(value.stage)this.operations.push('stored:'+value.stage);}
  collection(name){
    const db=this;
    return {firestore:db,doc(id){const key=name+'/'+id;return {id,key,firestore:db,async get(){db.counts.reads++;return db.snapshot(key);},async set(value,options){db.write(key,value,options);}};},orderBy(field,direction){return {limit(limit){db.queries.push({name,field,direction,limit});return {async get(){db.counts.reads++;const docs=[...db.rows].filter(([key,value])=>key.startsWith(name+'/')&&value&&Object.hasOwn(value,field)).sort((a,b)=>{const order=a[1][field]>b[1][field]?1:a[1][field]<b[1][field]?-1:0;return direction==='desc'?-order:order;}).slice(0,limit).map(([key,value])=>({id:key.slice(name.length+1),data:()=>clone(value)}));return {docs,size:docs.length};}};}};}};
  }
  runTransaction(task){
    const result=this.tail.then(async()=>{
      const ordinal=++this.transactions,pending=[];
      const value=await task({get:async ref=>{assert.equal(pending.length,0,'Firestore reads must precede writes');if(this.missingOutcomeRecord&&ordinal>1&&ref.key.startsWith('VoiceSessions/'))return {exists:false,data:()=>undefined};return ref.get();},set:(ref,data,options)=>{if(ref.key.startsWith('VoiceSessions/')&&this.failStages.has(data.stage))throw Error('PRIVATE_SYNTHETIC_STORAGE');pending.push([ref.key,clone(data),options]);}});
      for(const args of pending)this.write(...args);return value;
    });this.tail=result.catch(()=>{});return result;
  }
}
function historicalRows(at=AT){return [
  ['VoiceUsage/preview-budget',{reservedCents:950,spentCents:0,calls:10,allocationCapCents:1000,at:at-1000}],
  ['VoiceUsage/session-'+'a'.repeat(64),{allocatedCents:100,startedAt:at-10000,reconcile:'provider_evidence_required',originalHistory:'PRIVATE_PRESERVE_HISTORY'}],
  ['VoiceAllowances/native-sandbox',{schema:1,id:'native-sandbox',kind:'realtime_voice',provider:'openai',allocatedCents:500,reservationCents:100,reservedCents:500,spentCents:0,calls:5,issuedAt:at-600000,expiresAt:core.STOP_AT,lastStartedAt:at-10000,authorizedBy:'PRIVATE_PAST_OWNER',checkpointUpdatedAt:at-600001}],
  ['Usage/2026-10-07',{reservedUsd:8,spentUsd:3,calls:7}],
  ['State/controller',{owner:'PRIVATE_CONTROLLER',tokenHash:'b'.repeat(64),leaseUntil:at+45*60000}],
  ['State/checkpoint',{updatedAt:at-1000,phase:'PRIVATE_CHECKPOINT',savedResearch:'preserve'}]
];}
function request(body,{operator=false,origin='https://preview.test'}={}){return new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{'Content-Type':'application/json',...(origin?{Origin:origin}:{}),...(operator?{'X-Growth-Key':OPERATOR}:{})},body:JSON.stringify(body)});}
function fixture(options={}){
  let at=options.at??AT,sequence=0;
  const env={...ENV,...options.env},control={enabled:true,aiEnabled:false,aiDailyUsdCap:0,stopAt:core.STOP_AT,...options.control};
  const db=new Database([['State/control',control],...historicalRows(at),...(options.rows||[])],options),provider=[],deadlines=[],rates=[],counts={setup:0,starts:0,hangups:0,auth:0};
  const service={namespace:options.namespace??'Brites_Growth_Sandbox',col:name=>db.collection(name),setup:async()=>{counts.setup++;return clone(db.rows.get('State/control'));},rateLimit:async(key,limit)=>{rates.push({key,limit});return options.rateLimit?options.rateLimit(key,limit):true;}};
  const fetch=async(url,init)=>{
    provider.push({url,init});if(url.includes('/v1/models/'))return options.provider?options.provider(url,init):Response.json({id:'gpt-realtime-2.1'});
    if(url.endsWith('/hangup')){counts.hangups++;db.operations.push('provider:hangup');if(options.hangupThrows)throw Error('PRIVATE_HANGUP');return new Response(null,{status:options.hangupStatus??200});}
    assert.equal(url,'https://api.openai.com/v1/realtime/calls');counts.starts++;db.operations.push('provider:start');if(options.provider)return options.provider(url,init);
    const id=options.callId||'rtc_synthetic_'+(++sequence);return new Response(options.answer??SDP,{status:201,headers:{Location:'/v1/realtime/calls/'+id,'x-request-id':options.requestId||'req_synthetic_'+sequence}});
  };
  const handler=voice.createHandler({env,service,fetch,now:()=>at,authorize:async req=>{counts.auth++;return req.headers.get('X-Growth-Key')===OPERATOR;},scheduleHangup:async row=>{deadlines.push(clone(row));db.operations.push('deadline');if(options.deadlineFailure)return false;await service.col('VoiceDeadlines').doc(row.callId).set({...row,state:'pending',at});return true;},...options.handler});
  const call=async(body,requestOptions)=>{const response=await handler(request(body,requestOptions));return {response,value:await response.json()};};
  const token=async requestOptions=>{const answer=await call({action:'capabilities'},requestOptions);assert.equal(answer.response.status,200);assert.equal(answer.value.enabled,true);assert.equal(typeof answer.value.demoToken,'string');return answer.value.demoToken;};
  const start=async(demoToken,requestOptions)=>call({action:'start',sdp:SDP,demoToken:demoToken??await token(requestOptions)},requestOptions);
  const journal=()=>[...db.rows].filter(([key])=>key.startsWith('VoiceSessions/')).map(([,value])=>clone(value));
  const moneyBytes=()=>JSON.stringify([...db.rows].filter(([key])=>/^(?:VoiceUsage|VoiceAllowances|VoiceContinuations|Usage)\//.test(key)));
  const protectedBytes=()=>JSON.stringify([...db.rows].filter(([key])=>/^(?:VoiceUsage|VoiceAllowances|VoiceContinuations|Usage|State)\//.test(key)));
  return {env,db,rows:db.rows,service,provider,calls:provider,deadlines,rates,counts,handler,call,token,start,journal,moneyBytes,protectedBytes,now:()=>at,advance:ms=>{at+=ms;},setAt:value=>{at=value;}};
}
module.exports={AT,SDP,ENV,KEY,OPERATOR,clone,Database,historicalRows,request,fixture};
