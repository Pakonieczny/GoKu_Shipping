'use strict';
// Public listing metadata only. Never store tokens, receipts or buyer information.
// Cached images remain available even when the live-read budget is exhausted.
const DAY = 86400000;
function createImageCache({db, fetch, env=process.env, now=Date.now, sleep=ms=>new Promise(r=>setTimeout(r,ms)), meter=null}) {
  const crypto=require('node:crypto'), flights=new Map(),memory=new Map();
  const key=crypto.createHash('sha256').update(String(env.CLIENT_ID||'unconfigured')).digest('hex').slice(0,24);
  const budgetRef=db.collection('EtsyApi_Config').doc('listingImages_'+key);
  const capValue=Number(env.ETSY_IMAGE_MAX_DAILY_CALLS ?? 25);
  const cap=Number.isFinite(capValue)?Math.max(0,Math.min(1000,Math.floor(capValue))):25;
  const normalize=data=>(Array.isArray(data)?data:data?.results||data?.images||[]).map((x,i)=>({...x,rank:Number(x.rank)||i+1,url_570xN:x.url_570xN||x.url||x.url_fullxfull||null})).filter(x=>x.url_570xN).sort((a,b)=>a.rank-b.rank);
  const result=(images,source,status=200,retryAt=0)=>({images,source,status,retryAt});
  async function read(id,{cacheOnly=false}={}) {
    id=String(id||'');if(!/^\d{3,20}$/.test(id))return result([],'invalid',400);
    const flightKey=id+':'+cacheOnly;
    const hit=memory.get(flightKey);
    if(hit && hit.until>now())return {...hit.value,source:hit.value.images.length?'cache':hit.value.source,etsyCalls:0};
    if(flights.has(flightKey))return flights.get(flightKey);
    const task=load(id,cacheOnly).catch(()=>result([],'cache-unavailable',503));
    flights.set(flightKey,task);try{const value=await task;memory.delete(flightKey);memory.set(flightKey,{value,until:now()+(value.images.length?900000:60000)});while(memory.size>1000)memory.delete(memory.keys().next().value);return value;}finally{flights.delete(flightKey);}
  }
  async function load(id,cacheOnly) {
    const ref=db.collection('Etsy_Listing_Image_Cache').doc(id);
    const cached=(await ref.get()).data()||{};
    const images=normalize(cached.images);
    if(images.length)return result(images,'cache'); // No automatic expiry/refetch on scrolling.
    // Reuse the existing listing catalog before spending even one new Etsy call.
    const catalog=(await db.collection('EtsyMail_Listings').doc(id).get()).data()||{};
    const saved=normalize(catalog.images);
    if(saved.length)return result(saved,'catalog');
    if(cacheOnly)return result([],'sandbox-cache-only');
    if(cached.retryAt>now())return result([],'paused',429,cached.retryAt);
    if(cached.emptyUntil>now())return result([],'cached-empty');
    if(!env.CLIENT_ID)return result([],'unconfigured',503);
    const lease=crypto.randomUUID();
    const reserved=await db.runTransaction(async tx=>{
      const [imageSnap,budgetSnap,usageSnap]=await Promise.all([tx.get(ref),tx.get(budgetRef),tx.get(db.collection('EtsyApi_Config').doc('usage'))]);
      const item=imageSnap.data()||{},budget=budgetSnap.data()||{},usage=usageSnap.data()||{},t=now();
      if(normalize(item.images).length)return result(normalize(item.images),'cache');
      if(item.leaseUntil>t)return result([],'loading',202,item.leaseUntil);
      if(item.emptyUntil>t)return result([],'cached-empty');
      const stamps=(budget.attempts||[]).filter(x=>Number.isFinite(x)&&x>t-DAY);
      let retryAt=Math.max(budget.blockedUntil||0,item.retryAt||0);
      if(stamps.length>=cap)retryAt=Math.max(retryAt,stamps.length?stamps[0]+DAY:t+DAY);
      const etsy=usage.etsy||{};
      if(etsy.reported_at>t-DAY && etsy.remaining_today!=null && etsy.remaining_today<=Math.max(10,(Number(etsy.limit_per_day)||0)*.2))retryAt=Math.max(retryAt,etsy.reported_at+DAY);
      const startAt=Math.max(t,budget.nextAt||0);
      if(startAt>t+6000)retryAt=Math.max(retryAt,startAt);
      if(retryAt>t)return result([],'budget-paused',429,retryAt);
      tx.set(ref,{...item,lease,leaseUntil:t+30000});
      tx.set(budgetRef,{...budget,attempts:[...stamps,startAt],nextAt:startAt+1000,cap,updatedAt:t});
      return {reserved:true,startAt};
    });
    if(!reserved.reserved)return reserved;
    if(reserved.startAt>now())await sleep(reserved.startAt-now());
    // Another image may have received a 429 while this reservation waited.
    const latest=(await budgetRef.get()).data()||{};
    if(latest.blockedUntil>now())return result([],'budget-paused',429,latest.blockedUntil);
    let response,output=[],retryAt=0,status=502;
    try {
      const secret=env.CLIENT_SECRET||env.ETSY_SHARED_SECRET;
      response=await fetch(`https://api.etsy.com/v3/application/listings/${id}/images`,{headers:{'x-api-key':secret?`${env.CLIENT_ID}:${secret}`:env.CLIENT_ID},timeout:10000});
      status=response.status;
      if(response.ok)output=normalize(await response.json());
      if(status===429){
        const raw=response.headers?.get?.('retry-after'),seconds=Number(raw);
        retryAt=Math.max(now()+60000,raw&&Number.isFinite(seconds)?now()+seconds*1000:Date.parse(raw)||now()+3600000);
      }else if(!response.ok)retryAt=now()+3600000;
      const remaining=response.headers?.get?.('x-remaining-today');
      if(remaining!=null && Number(remaining)<=0)retryAt=Math.max(retryAt,now()+DAY);
    }catch(_){retryAt=now()+3600000;}
    // Count actual attempts, including failures, in the existing verified usage meter.
    if(meter){meter.recordCall('charm-sorter-images',response);await meter.flushNow();}
    await db.runTransaction(async tx=>{
      const [a,b]=await Promise.all([tx.get(ref),tx.get(budgetRef)]),item=a.data()||{},budget=b.data()||{};
      if(item.lease===lease)tx.set(ref,{images:output,fetchedAt:now(),leaseUntil:0,retryAt,emptyUntil:status===200&&!output.length?now()+DAY:0});
      if(status!==200 || (response?.headers?.get?.('x-remaining-today')!=null && Number(response.headers.get('x-remaining-today'))<=0))tx.set(budgetRef,{...budget,blockedUntil:Math.max(budget.blockedUntil||0,retryAt)});
    });
    return {...result(output,output.length?'etsy':retryAt?'paused':'empty',status,retryAt),etsyCalls:1};
  }
  return {read};
}
let singleton;
function instance(){return singleton||(singleton=createImageCache({db:require('./firebaseAdmin').firestore(),fetch:require('node-fetch'),meter:require('./_etsyApiUsage')}));}
module.exports={createImageCache,read:(...args)=>instance().read(...args)};
