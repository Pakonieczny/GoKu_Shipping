'use strict';

const crypto=require('node:crypto');
const library=require('./_britesCharmMeaningLibrary.js');
const definitions=require('./_britesCharmStoryBootstrap.json');
const SITE_ID='dd7556cf-ad1d-4e4d-abb2-c854cd27368e';
const BRANCH='codex/brites-growth-2026-10';
const NAMESPACE='Brites_Growth_Sandbox';
const RELEASE='charm-stories-52';
const MARKER_ID='concierge-library-release52';
const EXPECTED_COUNT=28;
const ENV_KEYS=['FIREBASE_PROJECT_ID','FIREBASE_CLIENT_EMAIL','FIREBASE_PRIVATE_KEY','BRITES_GROWTH_NAMESPACE'];
const plain=value=>!!value&&typeof value==='object'&&!Array.isArray(value);
const identifier=value=>typeof value==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(value);
const hash=value=>crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');

// Netlify verifies the event signature before invoking the reserved filename.
// These additional checks restrict that trusted event to the existing sandbox
// Git publishing workflow. No HTTP path or browser-provided authority is added.
function checkedDeploy(body,at=Date.now()){
  if(!plain(body)||!plain(body.payload))return null;
  // The legacy event contract guarantees payload, not a separate site object.
  // An optional site envelope may corroborate the payload but cannot conflict.
  if(Object.hasOwn(body,'site')&&(!plain(body.site)||body.site.id!==SITE_ID))return null;
  const payload=body.payload,publishedAt=typeof payload.published_at==='string'?Date.parse(payload.published_at):NaN;
  if(payload.site_id!==SITE_ID||payload.state!=='ready'||payload.context!=='production'||payload.branch!==BRANCH||payload.manual_deploy!==false||!identifier(payload.id)||!identifier(payload.build_id)||typeof payload.commit_ref!=='string'||!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/i.test(payload.commit_ref)||!Number.isFinite(publishedAt)||publishedAt<=0||publishedAt>at+60000||payload.error_message!=null&&payload.error_message!==''||payload.review_id!=null)return null;
  return {deployId:payload.id,buildId:payload.build_id,commitRef:payload.commit_ref.toLowerCase(),publishedAt,siteId:SITE_ID,branch:BRANCH};
}

function createDeployMigration({makeDb,createLibrary=library.createLibrary,now=Date.now,bootstrapDefinitions=definitions}={}){
  const shape=plain(bootstrapDefinitions)&&bootstrapDefinitions.schema===1&&bootstrapDefinitions.release===RELEASE&&bootstrapDefinitions.provenance==='agent_researched'&&Array.isArray(bootstrapDefinitions.records)&&bootstrapDefinitions.records.length===EXPECTED_COUNT;
  const expectedIds=shape?bootstrapDefinitions.records.map(record=>record?.id):[];
  const validDefinitions=shape&&expectedIds.every(id=>typeof id==='string'&&/^[a-z0-9][a-z0-9_-]{0,79}$/.test(id))&&new Set(expectedIds).size===EXPECTED_COUNT;
  const definitionHash=validDefinitions?hash(bootstrapDefinitions):null;
  function complete(value){return plain(value)&&value.schema===1&&value.completed===true&&value.release===RELEASE&&value.definitionHash===definitionHash&&value.namespace===NAMESPACE&&value.siteId===SITE_ID&&value.branch===BRANCH&&value.validPresentCount===EXPECTED_COUNT&&Number.isFinite(value.checkedAt)&&value.checkedAt>0&&value.checkedAt<=now()+60000;}
  function inspected(records,at){
    if(!Array.isArray(records)||records.length>96)return null;
    const byId=new Map();
    for(const record of records){if(expectedIds.includes(record?.id)&&library.validateRecord(record,at,true)){if(byId.has(record.id))return null;byId.set(record.id,record);}}
    if(byId.size!==EXPECTED_COUNT)return null;
    return {validPresentCount:byId.size,activeCount:[...byId.values()].filter(record=>record.status==='active').length};
  }
  async function run(body,env={}){
    const deploy=checkedDeploy(body,now());
    if(!deploy)return {ok:false,status:'ignored'};
    if(!validDefinitions)return {ok:false,status:'incomplete'};
    if(env.BRITES_GROWTH_NAMESPACE&&env.BRITES_GROWTH_NAMESPACE!==NAMESPACE)return {ok:false,status:'ignored'};
    if(ENV_KEYS.slice(0,3).some(key=>typeof env[key]!=='string'||!env[key].trim()))return {ok:false,status:'unavailable'};
    try{
      const connect=makeDb||require('./_britesGrowth.js').makeDb;
      const db=connect({...env,BRITES_GROWTH_NAMESPACE:NAMESPACE});
      const marker=db.collection(NAMESPACE+'_State').doc(MARKER_ID),prior=await marker.get();
      if(prior.exists&&complete(prior.data()))return {ok:true,status:'already_complete',release:RELEASE,definitionHash,validPresentCount:EXPECTED_COUNT};
      const store=createLibrary({db,namespace:NAMESPACE,now}),seed=await store.bootstrap();
      if(seed?.ok!==true||seed.release!==RELEASE||seed.collection!==NAMESPACE+'_CharmStories'||!Number.isSafeInteger(seed.created)||seed.created<0||!Number.isSafeInteger(seed.existing)||seed.existing<0||seed.created+seed.existing!==EXPECTED_COUNT)return {ok:false,status:'incomplete'};
      const readback=await store.status();
      if(readback?.collection!==NAMESPACE+'_CharmStories'||readback.provenance!=='agent_researched'||!inspected(readback.records,now()))return {ok:false,status:'incomplete'};
      const receipt=await db.runTransaction(async tx=>{
        const current=await tx.get(marker);
        if(current.exists&&complete(current.data()))return null;
        // Recheck actual stored content after the final await; a delayed marker
        // must never make expired inspection dates look like completed work.
        const checkedAt=now(),counts=inspected(readback.records,checkedAt);
        if(!counts)return null;
        const value={schema:1,completed:true,release:RELEASE,definitionHash,namespace:NAMESPACE,...deploy,...counts,created:seed.created,existing:seed.existing,checkedAt,provenance:'agent_researched'};
        tx.set(marker,value);
        return value;
      });
      if(receipt)return {ok:true,status:'completed',...receipt};
      const concurrent=await marker.get();
      return concurrent.exists&&complete(concurrent.data())?{ok:true,status:'already_complete',release:RELEASE,definitionHash,validPresentCount:EXPECTED_COUNT}:{ok:false,status:'incomplete'};
    }catch{
      // Do not expose or log errors that can contain credentials, URLs or raw
      // platform payloads. Without a completion marker, another deploy retries.
      return {ok:false,status:'unavailable'};
    }
  }
  return {run,definitionHash};
}

function createRequestHandler({getEnv=key=>globalThis.Netlify?.env?.get(key),log=()=>{},...options}={}){
  const migration=createDeployMigration(options);
  return async request=>{
    const response=status=>new Response(null,{status,headers:{'Cache-Control':'no-store'}});
    if(request?.method!=='POST')return response(405);
    let body;
    try{const raw=await request.text();if(Buffer.byteLength(raw,'utf8')>256*1024)return response(413);body=JSON.parse(raw);}catch{return response(400);}
    if(!checkedDeploy(body,(options.now||Date.now)()))return response(204);
    let env;
    try{env=Object.fromEntries(ENV_KEYS.map(key=>[key,getEnv(key)]));}catch{return response(503);}
    const result=await migration.run(body,env);
    if(result.ok){
      if(result.status==='completed')try{log({event:'concierge_library_seed_complete',release:result.release,definitionHash:result.definitionHash,namespace:result.namespace,siteId:result.siteId,deployId:result.deployId,commitRef:result.commitRef,created:result.created,existing:result.existing,validPresentCount:result.validPresentCount,activeCount:result.activeCount,checkedAt:result.checkedAt});}catch{}
      return response(204);
    }
    return response(result.status==='ignored'?204:503);
  };
}

module.exports={SITE_ID,BRANCH,NAMESPACE,RELEASE,MARKER_ID,EXPECTED_COUNT,ENV_KEYS,checkedDeploy,createDeployMigration,createRequestHandler};
