'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {createImageCache}=require('../../netlify/functions/_etsyImageCache');
function fixture({cap=25,status=200,headers={},broken=false}={}){
 const data=new Map();let clock=Date.UTC(2026,8,22,23,59,50),calls=0,reads=0,serial=Promise.resolve();
 const ref=path=>({path,get:async()=>{reads++;if(broken)throw Error('Firestore unavailable');return{data:()=>structuredClone(data.get(path)),exists:data.has(path)}}});
 const db={collection:name=>({doc:id=>ref(name+'/'+id)}),runTransaction:fn=>{const p=serial.then(async()=>{let wrote=false;const writes=[];const result=await fn({get:r=>{assert(!wrote);return r.get();},set:(r,v)=>{wrote=true;writes.push(()=>data.set(r.path,structuredClone(v)));}});writes.forEach(f=>f());return result});serial=p.catch(()=>{});return p;}};
 const options={db,env:{CLIENT_ID:'test',ETSY_IMAGE_MAX_DAILY_CALLS:String(cap)},now:()=>clock,sleep:async ms=>{clock+=ms;},fetch:async url=>{calls++;if(status==='network')throw Error('network');return {status,ok:status===200,headers:{get:k=>headers[k]??null},json:async()=>({results:url.includes('/listings/batch?')?new URL(url).searchParams.get('listing_ids').split(',').map(listing_id=>({listing_id,images:[{rank:2,url_570xN:'second.jpg'},{rank:1,url_570xN:'first.jpg'}]})):[{rank:2,url_570xN:'second.jpg'},{rank:1,url_570xN:'first.jpg'}]})}}};
 return {data,api:()=>createImageCache(options),calls:()=>calls,reads:()=>reads,advance:ms=>clock+=ms};
}
(async()=>{
 let f=fixture(),a=f.api();
 assert.equal((await a.read('123',{cacheOnly:true})).source,'sandbox-cache-only');assert.equal(f.calls(),0);
 f.data.set('EtsyMail_Listings/123',{images:[{url:'saved.jpg'}]});f.advance(60001);
 assert.equal((await a.read('123',{cacheOnly:true})).images[0].url_570xN,'saved.jpg');assert.equal((await a.read('123')).source,'catalog');assert.equal(f.calls(),0);
 const warmReads=f.reads();await Promise.all(Array.from({length:100},()=>a.read('123')));assert.equal(f.reads(),warmReads,'warm image requests use no Firebase reads');
 assert.equal((await a.read('../123')).status,400);assert.equal(f.calls(),0);
 f=fixture();a=f.api();const b=f.api();
 await Promise.all(Array.from({length:30},(_,i)=>(i%2?a:b).read('456')));assert.equal(f.calls(),1,'independent instances share one distributed reservation');
 assert.equal((await f.api().read('456')).images[0].url_570xN,'first.jpg');assert.equal(f.calls(),1,'cold start uses durable image cache');
 f.advance(40*86400000);await f.api().read('456');assert.equal(f.calls(),1,'scrolling never expires/re-fetches saved photos');
 f=fixture({cap:2});a=f.api();await Promise.all(['100','101','102','103'].map(id=>a.read(id)));assert.equal(f.calls(),2,'atomic rolling budget holds across different listing IDs');
 f.advance(60000);assert.equal((await a.read('104')).status,429);assert.equal(f.calls(),2,'midnight does not reset rolling allowance');
 f.advance(86400000);assert.equal((await a.read('104')).status,200);assert.equal(f.calls(),3);
 f=fixture({status:429,headers:{'retry-after':'7200'}});a=f.api();const limited=await a.read('123');assert.equal(limited.status,429);assert(limited.retryAt);await f.api().read('456');assert.equal(f.calls(),1,'429 blocks other IDs and cold instances without retry');
 f.advance(3600000);await f.api().read('789');assert.equal(f.calls(),1,'full Retry-After is respected');
 f=fixture({status:'network'});await f.api().read('123');await f.api().read('456');assert.equal(f.calls(),1,'network failure counts and opens the shared circuit');
 f=fixture({broken:true});assert.equal((await f.api().read('123')).status,503);assert.equal(f.calls(),0,'cache/budget failures never fall through to Etsy');
 f=fixture({cap:0});await f.api().read('123');assert.equal(f.calls(),0,'zero disables new photo lookups');
 f=fixture();f.data.set('EtsyApi_Config/usage',{etsy:{reported_at:Date.UTC(2026,8,22,23,59,49),remaining_today:4,limit_per_day:100}});await f.api().read('123');assert.equal(f.calls(),0,'known low shared-key quota reserves remaining allowance');
 // Bulk preparation shares the existing quota, leases and durable image cache.
 f=fixture();a=f.api();const ids=Array.from({length:100},(_,i)=>String(1000+i));
 const bulk=await a.readMany(ids);assert.equal(f.calls(),1);assert.equal(Object.values(bulk).reduce((n,r)=>n+(r.etsyCalls||0),0),1);assert(Object.values(bulk).every(r=>r.images[0].url_570xN==='first.jpg'));
 const reads=f.reads();await a.readMany(ids);assert.equal(f.calls(),1);assert.equal(f.reads(),reads,'warm batches make no Firebase reads');
 await f.api().readMany(ids);assert.equal(f.calls(),1,'cold instances recover the durable batch without Etsy');
 f=fixture();await Promise.all([f.api().readMany(ids),f.api().readMany(ids)]);assert.equal(f.calls(),1,'overlapping instances reserve each listing once');
 f=fixture({cap:1});await f.api().readMany(['123','456']);const paused=await f.api().readMany(['789']);assert.equal(f.calls(),1);assert.equal(paused['789'].source,'budget-paused');
 f.data.set('EtsyMail_Listings/987',{images:[{url:'cached.jpg'}]});const saved=await f.api().readMany(['987'],{cacheOnly:true});assert.equal(saved['987'].images[0].url_570xN,'cached.jpg');assert.equal(f.calls(),1,'cached photos remain available during a pause');
 f=fixture({status:429,headers:{'retry-after':'7200'}});await f.api().readMany(['123','456']);await f.api().readMany(['789']);assert.equal(f.calls(),1,'batch 429 shares the single-listing circuit');
 f=fixture({cap:0});await f.api().readMany(ids);assert.equal(f.calls(),0);
 f=fixture({broken:true});await f.api().readMany(ids);assert.equal(f.calls(),0,'batch fails closed on Firestore failure');
 f=fixture();a=f.api();await a.read('123',{cacheOnly:true});await a.readMany(['123']);assert.equal((await a.read('123',{cacheOnly:true})).images.length,2,'preparation replaces a cached miss immediately');
 // Execute the actual front-end loader, proving one request, no OAuth and no retry on 429.
 const source=fs.readFileSync(require.resolve('../../design-1.html'),'utf8');
 const loader=source.slice(source.indexOf('const __imagesCache'),source.indexOf('/** Route an external image'));
 let requests=0;const context={Map,NS:':test',FN:'/fn',runImageTask:fn=>fn(),fetch:async()=>{requests++;return {ok:false,status:429};}};
 vm.runInNewContext(loader+';globalThis.load=fetchListingImages;',context);
 await Promise.all(Array.from({length:30},()=>context.load('123')));await context.load('123');assert.equal(requests,1,'browser dedupes and never retries 429');
 const api=source.slice(source.indexOf('async function apiFetch('),source.indexOf('/** Receipt fetch with retries'));
 let fetches=0,refreshes=0;const local=new Map(),ctx={Math,Number,String,Date,JSON,Response,SANDBOX:false,ETSY_LS:'meter',ETSY_GUARD:{brakeMs:300000},etsyMeter:{brakeUntil:0},isEtsyPath:()=>true,localStorage:{getItem:k=>local.get(k),setItem:(k,v)=>local.set(k,v)},ensureFreshToken:async()=>{refreshes++;},TOKEN_KEYS:{access:'token'},FN:'/fn',runEtsyTask:fn=>fn(),fetch:async()=>{fetches++;return new Response('{}',{status:429,headers:{'retry-after':'7200'}})}};
 vm.runInNewContext(api+';globalThis.read=apiFetch;',ctx);await ctx.read('/listOpenOrders');await ctx.read('/etsyOrderProxy');assert.equal(fetches,1);assert.equal(refreshes,1,'subsequent paused order reads do not refresh tokens');
 for(const match of source.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))if(match[1].trim()&&!match[0].includes('type="module"'))new vm.Script(match[1]);
 assert(!fs.readFileSync(require.resolve('../../netlify/functions/etsySandbox'),'utf8').includes('api.etsy.com'),'sandbox has no direct Etsy URL');
 const bridge=fs.readFileSync(require.resolve('../../charm-nest-bridge.js'),'utf8');
 const cadence=bridge.match(/const interval = \(\) => ([^;]+);/)[1];
 const interval=(sandbox,minutes)=>vm.runInNewContext(cadence,{WORKSPACE_SANDBOX:sandbox,S:{settings:{pollMinutes:minutes}}});
 assert.equal(interval(false,1),600000);assert.equal(interval(true,1),600000);assert.equal(interval(false,30),1800000);
 console.log('Etsy image protections OK: sandbox/catalog zero calls, concurrent/cold-start cache, rolling budget, key reserve, shared cooldown, fail-closed storage, no browser retry or token refresh; no live Etsy requests used.');
})().catch(e=>{console.error(e);process.exitCode=1});
