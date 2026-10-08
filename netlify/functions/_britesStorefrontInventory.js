'use strict';

// An isolated cache of already public Shopify product.js facts. It is separate
// from research, merchant ranking, the Admin mirror and cart/payment writes.
// Cached descriptions can be answered immediately, while each release still
// reads reviewed holds and preserves the exact product's original timestamp.
const crypto=require('node:crypto');
const categories=require('./_britesStorefrontSeed');
// Match the browser's 75-second lead: its first 32-second page precedes the
// remaining parallel pages. Refresh early without extending any fact's TTL.
const CACHE_MS=5*60000,REFRESH_AHEAD_MS=75000,MAXIMUM=160,PAGE_LIMIT=24,CONCURRENCY=6,READ_MS=7000,DEADLINE_MS=28000;
const shared=new Map(),clone=value=>structuredClone(value);
const hash=value=>crypto.createHash('sha256').update(typeof value==='string'?value:JSON.stringify(value)).digest('hex');
const handleValid=value=>typeof value==='string'&&/^[a-z0-9_-]{1,180}$/.test(value);
const idValid=value=>typeof value==='string'&&/^gid:\/\/shopify\/Product\/[1-9]\d{0,19}$/.test(value);
function failure(message){return Object.assign(Error(message),{publicInventory:true});}
function parsePage({offset=0,limit=PAGE_LIMIT}={}){
  const integer=(value,defaultValue)=>value==null?defaultValue:typeof value==='number'?value:typeof value==='string'&&/^(?:0|[1-9]\d{0,2})$/.test(value)?Number(value):NaN;
  const start=integer(offset,0),size=integer(limit,PAGE_LIMIT);
  if(!Number.isInteger(start)||start<0||start>=MAXIMUM||!Number.isInteger(size)||size<1||size>PAGE_LIMIT)throw failure('Invalid inventory page: use an offset from 0 to 159 and a limit from 1 to 24.');
  return {offset:start,limit:size};
}
async function bounded(promise,ms){
  let timer;const expiry=new Promise((_,reject)=>{timer=setTimeout(()=>reject(failure('The published inventory read timed out.')),Math.max(1,ms));timer.unref?.();});
  try{return await Promise.race([Promise.resolve(promise),expiry]);}finally{clearTimeout(timer);}
}
function createInventory({shopify,service,core,now=Date.now,cache}={}){
  if(!shopify||typeof shopify.seed!=='function'||!core?.productProjection||!core?.applyProductIssues||!core?.publicUrl||!service||typeof service.productIssues!=='function')throw failure('The public inventory reader is unavailable.');
  const namespace=service.namespace||'Brites_Growth_Sandbox';
  if(!/^Brites_Growth_(?:Sandbox|Live)$/.test(namespace))throw failure('The isolated inventory namespace is required.');
  if(!cache){if(!shared.has(namespace))shared.set(namespace,{});cache=shared.get(namespace);}
  cache.products||=new Map();cache.pendingProducts||=new Map();
  const ref=name=>typeof service.col==='function'?service.col('StorefrontInventory').doc(name):null;
  const productRef=handle=>'p-'+hash(handle).slice(0,40);
  const currentTimestamp=value=>Number.isFinite(value)&&value<=now()+60000&&now()-value<CACHE_MS;
  const ownedIdentity=(product,identity)=>{
    if(!idValid(product?.id)||!handleValid(product?.handle)||identity&&(product.id!==identity.id||product.handle!==identity.handle))return false;
    const href=core.publicUrl(product.url,true);if(!href)return false;
    const path=new URL(href).pathname.replace(/\/$/,'');
    return path==='/products/'+product.handle||new RegExp('^/[a-z]{2}(?:-[a-z]{2})?/products/'+product.handle+'$','i').test(path);
  };
  function safeProduct(value,identity,{exact=false}={}){
    if(!ownedIdentity(value,identity)||!currentTimestamp(value.checkedAt))return null;
    const product=core.productProjection(value);
    if(!product.title||!product.type||!/^[A-Z]{3}$/.test(product.currency||'')||!ownedIdentity(product,identity)||!Array.isArray(product.options)||product.options.length>3||!Array.isArray(product.variants)||product.variants.length>250)return null;
    if(exact){
      if(product.variantsComplete!==true||!product.variants.length)return null;
      const names=new Set(product.options.map(option=>option.name));
      if(names.size!==product.options.length||product.options.some(option=>typeof option.name!=='string'||!option.name||!Array.isArray(option.values)||!option.values.length||option.values.length>250||new Set(option.values).size!==option.values.length))return null;
      const variants=new Set();
      for(const variant of product.variants){
        if(!/^gid:\/\/shopify\/ProductVariant\/[1-9]\d{0,19}$/.test(variant?.id||'')||variants.has(variant.id)||String(variant.id).split('/').at(-1)!==String(variant.numericId)||!Number.isFinite(variant.price)||variant.price<0||typeof variant.available!=='boolean'||!Array.isArray(variant.options)||variant.options.length!==product.options.length)return null;
        variants.add(variant.id);const seen=new Set();
        for(const option of variant.options){const group=product.options.find(group=>group.name===option.name);if(!group||seen.has(option.name)||!group.values.includes(option.value))return null;seen.add(option.name);}
      }
    }
    return {...product,storeCategories:categories.categories(product).filter(category=>categories.CATEGORIES.includes(category))};
  }
  function manifestProduct(value){
    // The manifest stays below Firestore's document limit. Full options,
    // descriptions and galleries live in independent exact-product records.
    return {id:value.id,handle:value.handle,title:value.title,type:value.type,url:value.url,currency:value.currency,image:typeof value.image==='string'?value.image.slice(0,1200):null,imageAlt:value.imageAlt||null,images:(value.images||[]).slice(0,1).map(image=>({url:image.url.slice(0,1200),altText:image.altText||null})),description:String(value.description||'').slice(0,1200),options:value.options.map(option=>({name:option.name,values:[]})),variants:[],variantsComplete:false,checkedAt:value.checkedAt,storeCategories:value.storeCategories};
  }
  function validManifest(value){
    if(!value||value.schema!==1||!currentTimestamp(value.checkedAt)||value.expiresAt!==value.checkedAt+CACHE_MS||!Array.isArray(value.products)||!value.products.length||value.products.length>MAXIMUM)return null;
    const products=[],ids=new Set(),handles=new Set();
    for(const raw of value.products){const p=safeProduct(raw);if(!p||ids.has(p.id)||handles.has(p.handle)||!p.storeCategories.length)return null;ids.add(p.id);handles.add(p.handle);products.push(p);}
    const fingerprint=hash(products.map(p=>[p.id,p.handle]).sort((a,b)=>a[1].localeCompare(b[1])));
    if(value.fingerprint!==fingerprint)return null;
    return {schema:1,products,fingerprint,checkedAt:value.checkedAt,expiresAt:value.expiresAt,seedPartial:value.seedPartial===true};
  }
  async function persist(name,value){const doc=ref(name);if(!doc)return false;try{await bounded(doc.set(value),2000);return true;}catch{return false;}}
  async function readSaved(name){const doc=ref(name);if(!doc)return null;try{const snapshot=await bounded(doc.get(),2000);return snapshot?.exists?snapshot.data():null;}catch{return null;}}
  async function manifest(refresh=true){
    const usable=value=>value&&now()+(refresh?REFRESH_AHEAD_MS:0)<value.expiresAt&&(!refresh||!value.seedPartial||now()-value.checkedAt<30000);
    if(usable(cache.manifest))return cache.manifest;
    if(!refresh){
      // A shopper's fact question must not start or wait on120 network reads.
      // Hydrate just the small public manifest; the caller can use one exact
      // product.js read if the upfront inventory has not populated it yet.
      const saved=validManifest(await readSaved('v1'));if(saved){cache.manifest=saved;return saved;}return null;
    }
    if(!cache.pendingManifest)cache.pendingManifest=(async()=>{
      const saved=validManifest(await readSaved('v1'));if(usable(saved)){cache.manifest=saved;return saved;}
      const result=await bounded(shopify.seed(),20000),rows=Array.isArray(result?.products)?result.products:[];
      if(!rows.length||rows.length>MAXIMUM)throw failure('The published test inventory could not be checked.');
      const products=[],ids=new Set(),handles=new Set();
      for(const row of rows){const product=safeProduct(row);if(!product||!product.storeCategories.length||ids.has(product.id)||handles.has(product.handle))throw failure('The published test inventory identities conflict.');ids.add(product.id);handles.add(product.handle);products.push(product);}
      const checkedAt=now(),value={schema:1,products,fingerprint:hash(products.map(p=>[p.id,p.handle]).sort((a,b)=>a[1].localeCompare(b[1]))),checkedAt,expiresAt:checkedAt+CACHE_MS,seedPartial:result.seed?.partial===true};
      cache.manifest=value;await persist('v1',{...value,products:products.map(manifestProduct)});return value;
    })();
    try{return await cache.pendingManifest;}finally{cache.pendingManifest=null;}
  }
  function cachedRecord(value,identity){
    if(!value||value.schema!==1||value.id!==identity.id||value.handle!==identity.handle||value.checkedAt!==value.product?.checkedAt||value.expiresAt!==value.checkedAt+CACHE_MS||!currentTimestamp(value.checkedAt))return null;
    const product=safeProduct(value.product,identity,{exact:true});
    return product?{schema:1,id:product.id,handle:product.handle,product,checkedAt:product.checkedAt,expiresAt:product.checkedAt+CACHE_MS}:null;
  }
  async function getRecord(identity,refresh,deadline){
    const usable=record=>record&&(!refresh||now()+REFRESH_AHEAD_MS<record.expiresAt&&!record.product.variants.some(variant=>variant.availabilityKnown===false));
    const memory=cachedRecord(cache.products.get(identity.handle),identity);if(usable(memory))return {record:memory,fromCache:true};
    const saved=memory||cachedRecord(await readSaved(productRef(identity.handle)),identity);if(usable(saved)){cache.products.set(identity.handle,saved);return {record:saved,fromCache:true};}
    if(!refresh)return null;
    if(typeof shopify.publicByHandle!=='function')throw failure('The public exact product reader is required.');
    let promise=cache.pendingProducts.get(identity.handle);
    if(!promise){
      promise=(async()=>{
        if(now()>=deadline)throw failure('The published inventory read timed out.');
        const raw=await bounded(shopify.publicByHandle(identity.handle,Math.min(READ_MS,Math.max(1,deadline-now()))),Math.min(READ_MS,Math.max(1,deadline-now())));
        const product=safeProduct(raw,identity,{exact:true});if(!product)throw failure('The exact published product could not be checked.');
        const record={schema:1,id:product.id,handle:product.handle,product,checkedAt:product.checkedAt,expiresAt:product.checkedAt+CACHE_MS};
        cache.products.set(identity.handle,record);await persist(productRef(identity.handle),record);return record;
      })();cache.pendingProducts.set(identity.handle,promise);
    }
    try{return {record:await promise,fromCache:false};}finally{if(cache.pendingProducts.get(identity.handle)===promise)cache.pendingProducts.delete(identity.handle);}
  }
  async function holds(products){
    const ids=[...new Set(products.map(product=>product.id))],issues=[];
    // The service deliberately limits one read to100 identities. Never lose
    // a reviewed hold on the final20 products of the120-piece test sample.
    for(let i=0;i<ids.length;i+=100){const records=await service.productIssues(ids.slice(i,i+100));if(!Array.isArray(records))throw failure('The current product holds could not be checked.');issues.push(...records);}
    return issues;
  }
  function provisional(identity){
    return {...core.productProjection(identity),storeCategories:identity.storeCategories,variantsComplete:false,variants:(identity.variants||[]).map(variant=>{const clean=core.productProjection({...identity,variants:[variant]}).variants[0];return {...clean,available:false,availabilityKnown:false};}).filter(variant=>variant.id),detailState:'unconfirmed',cached:true};
  }
  async function read(page){
    // One budget covers the seed, durable reads, exact facts and reviewed
    // holds. Slow cold starts must not spend a new full budget at each stage.
    const {offset,limit}=parsePage(page),started=Date.now(),deadline=now()+DEADLINE_MS;
    const remaining=()=>Math.max(0,Math.min(deadline-now(),DEADLINE_MS-(Date.now()-started)));
    const index=await bounded(manifest(),remaining()),selected=index.products.slice(offset,offset+limit),results=[];
    if(!remaining())throw failure('The published inventory read timed out.');
    // Begin the independent hold check before the public detail batches, and
    // settle its rejection immediately while those batches are still running.
    const issueCheck=bounded(holds(index.products),Math.min(15000,remaining())).then(issues=>({issues}),error=>({error}));
    for(let i=0;i<selected.length;i+=CONCURRENCY){
      if(!remaining()){for(const product of selected.slice(i))results.push(provisional(product));break;}
      const group=selected.slice(i,i+CONCURRENCY),checks=await Promise.allSettled(group.map(product=>{const budget=remaining();return bounded(getRecord(product,true,now()+budget),budget);}));
      for(let j=0;j<group.length;j++){const checked=checks[j];results.push(checked.status==='fulfilled'&&checked.value?{...clone(checked.value.record.product),detailState:'checked',cached:checked.value.fromCache}:provisional(group[j]));}
      if(!remaining()){for(const product of selected.slice(i+group.length))results.push(provisional(product));break;}
    }
    // Cache facts independently of holds. A new reviewed issue must apply
    // even when all descriptions/options were loaded by an earlier visitor.
    const reviewed=await issueCheck;if(reviewed.error)throw reviewed.error;
    const products=core.applyProductIssues(results,reviewed.issues);
    const detailsLoaded=index.products.filter(product=>{const record=cachedRecord(cache.products.get(product.handle),product);return record&&!record.product.variants.some(variant=>variant.availabilityKnown===false);}).length;
    const unconfirmed=products.filter(product=>product.detailState!=='checked'||product.variants.some(variant=>variant.availabilityKnown===false)).length;
    const pageExpiry=products.filter(product=>product.detailState==='checked').map(product=>product.checkedAt+CACHE_MS);
    return {products,live:true,checkedAt:now(),inventory:{schema:1,total:index.products.length,offset,limit,loaded:products.length,detailsLoaded,ready:detailsLoaded===index.products.length&&!index.seedPartial&&!unconfirmed,partial:index.seedPartial||unconfirmed>0,sourcePartial:index.seedPartial,catalogueComplete:false,expiresAt:pageExpiry.length?Math.min(...pageExpiry):index.expiresAt,fingerprint:index.fingerprint},pageInfo:{hasNextPage:offset+products.length<index.products.length,nextOffset:offset+products.length<index.products.length?offset+products.length:null}};
  }
  async function lookup(handle){
    if(!handleValid(handle))return null;
    const index=await manifest(false);if(!index)return null;const identity=index.products.find(product=>product.handle===handle);if(!identity)return null;
    const value=await getRecord(identity,false,now());if(!value)return null;
    const product=core.applyProductIssues([clone(value.record.product)],await bounded(holds([identity]),15000))[0];
    return {product:{...product,detailState:'checked',cached:true},checkedAt:value.record.checkedAt,expiresAt:value.record.expiresAt,fromCache:true};
  }
  function peek(){
    const index=cache.manifest;if(!index||now()>=index.expiresAt)return null;
    const products=index.products.flatMap(identity=>{const record=cachedRecord(cache.products.get(identity.handle),identity);return record?[{...clone(record.product),detailState:'checked',cached:true,cartHold:true,recommendationHold:true}]:[];});
    return {products,checkedAt:index.checkedAt,expiresAt:index.expiresAt,catalogueComplete:false,holdsChecked:false,ready:products.length===index.products.length,total:index.products.length};
  }
  return {read,lookup,peek};
}
module.exports={createInventory,parsePage,CACHE_MS,REFRESH_AHEAD_MS,MAXIMUM,PAGE_LIMIT,CONCURRENCY,READ_MS,DEADLINE_MS};
