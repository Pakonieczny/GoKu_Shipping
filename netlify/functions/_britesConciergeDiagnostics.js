'use strict';

const {randomUUID}=require('node:crypto');
const {performance}=require('node:perf_hooks');

// Neither stage names nor error categories can come from shopper text, URLs,
// exception messages or provider responses. Keep the private read projection as
// restrictive as the write projection, including when older records are read.
const STAGES=Object.freeze(['request','storage_connect','catalogue_connect','service_connect','rate_limit','control_state','catalogue_search','catalogue_product','catalogue_persist','holds_read','knowledge_read','policy_read','conversation','message_event','client_event']);
const CATEGORIES=Object.freeze(['timeout','rate_limited','access','network','unavailable','invalid_data','configuration','unclassified','policy_unverified']);
const OUTCOMES=Object.freeze(['ok','degraded','failed','limited']);
const COLLECTION='ConciergeDiagnostics';
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const MAX_DURATION_MS=300000;
const validNamespace=value=>typeof value==='string'&&/^Brites_Growth_(Sandbox|Live)$/.test(value);
const bounded=(value,max=MAX_DURATION_MS)=>Number.isFinite(value)&&value>=0?Math.min(max,Math.round(value)):0;
const boundedTimeout=value=>Number.isFinite(value)?Math.max(1,Math.min(2000,Math.round(value))):750;
function safeField(object,key){try{return object&&object[key];}catch{return undefined;}}
function errorCategory(error){
  const name=safeField(error,'name'),code=safeField(error,'code');
  const status=safeField(error,'status')??safeField(error,'statusCode');
  if(['AbortError','TimeoutError'].includes(name)||['ETIMEDOUT','DEADLINE_EXCEEDED',4,408].includes(code)||status===408)return 'timeout';
  if(['RESOURCE_EXHAUSTED',8,429].includes(code)||status===429)return 'rate_limited';
  if(['PERMISSION_DENIED','UNAUTHENTICATED',7,16,401,403].includes(code)||[401,403].includes(status))return 'access';
  if(['ECONNRESET','ECONNREFUSED','EAI_AGAIN','ENOTFOUND','ENETUNREACH','EPIPE'].includes(code))return 'network';
  if(['UNAVAILABLE',14,502,503,504].includes(code)||[502,503,504].includes(status))return 'unavailable';
  if(name==='SyntaxError'||['INVALID_ARGUMENT',3].includes(code))return 'invalid_data';
  return 'unclassified';
}
function sanitize(record,{reference}={}){
  if(!record||typeof record!=='object'||Array.isArray(record)||safeField(record,'schema')!==1||safeField(record,'kind')!=='concierge_diagnostic')return null;
  const id=safeField(record,'reference'),at=safeField(record,'at'),outcome=safeField(record,'outcome'),statusCode=safeField(record,'statusCode');
  if(typeof id!=='string'||!UUID.test(id)||(reference!==undefined&&reference!==id)||!Number.isSafeInteger(at)||at<=0||at>8640000000000000||!OUTCOMES.includes(outcome)||!Number.isInteger(statusCode)||statusCode<100||statusCode>599)return null;
  const seen=new Set(),rawStages=safeField(record,'stages');
  const stages=(Array.isArray(rawStages)?rawStages:[]).slice(0,STAGES.length).flatMap(entry=>{
    const stage=safeField(entry,'stage');if(!STAGES.includes(stage)||seen.has(stage))return [];seen.add(stage);
    const categories=safeField(entry,'categories');
    return [{stage,calls:bounded(safeField(entry,'calls'),1000),completed:bounded(safeField(entry,'completed'),1000),failed:bounded(safeField(entry,'failed'),1000),degraded:bounded(safeField(entry,'degraded'),1000),inFlight:bounded(safeField(entry,'inFlight'),1000),durationMs:bounded(safeField(entry,'durationMs')),maxDurationMs:bounded(safeField(entry,'maxDurationMs')),categories:[...new Set((Array.isArray(categories)?categories:[]).filter(x=>CATEGORIES.includes(x)))].slice(0,CATEGORIES.length)}];
  });
  const failureStage=safeField(record,'failureStage');
  return {schema:1,kind:'concierge_diagnostic',reference:id,at,durationMs:bounded(safeField(record,'durationMs')),outcome,statusCode,failureStage:STAGES.includes(failureStage)?failureStage:null,stages};
}

function createRecorder({writeTimeoutMs=750,optionalTimeoutMs=750,now=Date.now,clock=()=>performance.now()}={}){
  const reference=randomUUID(),started=clock(),stages=new Map(),errorStages=new WeakMap();
  let service=null,flushing=null;
  function stageEntry(stage){
    if(!STAGES.includes(stage))throw new TypeError('Unsupported diagnostic stage.');
    if(!stages.has(stage))stages.set(stage,{stage,calls:0,completed:0,failed:0,degraded:0,inFlight:0,durationMs:0,maxDurationMs:0,categories:new Set()});
    return stages.get(stage);
  }
  function remember(error,stage){
    if(error&&(typeof error==='object'||typeof error==='function')&&!errorStages.has(error))errorStages.set(error,stage);
  }
  async function run(stage,operation){
    const entry=stageEntry(stage),at=clock();entry.calls++;entry.inFlight++;
    try{const value=await operation();entry.completed++;return value;}
    catch(error){entry.failed++;entry.categories.add(errorCategory(error));remember(error,stage);throw error;}
    finally{const elapsed=bounded(clock()-at);entry.inFlight--;entry.durationMs=bounded(entry.durationMs+elapsed);entry.maxDurationMs=Math.max(entry.maxDurationMs,elapsed);}
  }
  function degraded(stage,category='unclassified'){
    const entry=stageEntry(stage);entry.degraded++;entry.categories.add(CATEGORIES.includes(category)?category:'unclassified');
  }
  function attach(value){service=value;}
  async function optionalMessage(operation){
    let timer;
    try{
      await run('message_event',()=>Promise.race([Promise.resolve().then(operation),new Promise((resolve,reject)=>{timer=setTimeout(()=>{const error=new Error();error.name='TimeoutError';reject(error);},boundedTimeout(optionalTimeoutMs));})]));
    }catch{}finally{if(timer)clearTimeout(timer);}
  }
  function snapshot({outcome='ok',statusCode=200,error}={}){
    let failureStage=null;
    if(outcome==='failed'){
      failureStage=error&&(typeof error==='object'||typeof error==='function')?errorStages.get(error):null;
      if(!failureStage){const entry=stageEntry('request');entry.calls++;entry.failed++;entry.categories.add(errorCategory(error));failureStage='request';}
    }
    const entries=[...stages.values()];
    if(outcome==='ok'&&entries.some(x=>x.failed||x.degraded))outcome='degraded';
    return sanitize({schema:1,kind:'concierge_diagnostic',reference,at:now(),durationMs:bounded(clock()-started),outcome,statusCode,failureStage,stages:entries.map(x=>({...x,categories:[...x.categories]}))});
  }
  function flush(options={}){
    if(flushing)return flushing;
    flushing=(async()=>{
      let timer;
      try{
        if(!service||!validNamespace(service.namespace)||typeof service.col!=='function')return {saved:false};
        const record=snapshot(options);if(!record)return {saved:false};
        // A failed or stalled diagnostics write cannot replace the answer or
        // original error. There is no retry, including after the time limit.
        const write=Promise.resolve().then(()=>service.col(COLLECTION).doc(reference).set(record)).then(()=>({saved:true}),()=>({saved:false}));
        const timeout=boundedTimeout(writeTimeoutMs);
        return await Promise.race([write,new Promise(resolve=>{timer=setTimeout(()=>resolve({saved:false}),timeout);})]);
      }catch{return {saved:false};}finally{if(timer)clearTimeout(timer);}
    })();
    return flushing;
  }
  return {reference,run,degraded,attach,optionalMessage,flush};
}

async function read({db,namespace,limit=20}={}){
  if(!validNamespace(namespace))throw new TypeError('Invalid diagnostic namespace.');
  if(!db||typeof db.collection!=='function')throw new TypeError('Diagnostic storage is unavailable.');
  const boundedLimit=typeof limit==='number'&&Number.isFinite(limit)?Math.max(1,Math.min(50,Math.trunc(limit))):20;
  const result=await db.collection(namespace+'_'+COLLECTION).orderBy('at','desc').limit(boundedLimit).get();
  const documents=Array.isArray(result?.docs)?result.docs:[];
  return {schema:1,diagnostics:documents.slice(0,boundedLimit).flatMap(doc=>{let value;try{value=sanitize(doc.data(),{reference:doc.id});}catch{return [];}return value?[value]:[];})};
}

module.exports={STAGES,CATEGORIES,COLLECTION,validNamespace,errorCategory,sanitize,createRecorder,read};
