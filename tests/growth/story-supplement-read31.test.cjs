'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const core=require('../../netlify/functions/_britesGrowth');
const source=fs.readFileSync(path.join(__dirname,'../../netlify/functions/britesGrowthApi.js'),'utf8').replace(/^import \w+ from .*;\s*$/gm,'').replace('export default async (req,context) => {','return async (req,context) => {').replace(/export const config = [\s\S]*$/,'');
const PRODUCT='gid://shopify/Product/731',SECOND='gid://shopify/Product/732',KEY='synthetic-story-read-key',PRIVATE='SYNTHETIC_PRIVATE_STORAGE_FAILURE';
const record={schema:1,productId:PRODUCT,handle:'rabbit-story-test',productUrl:'https://britesjewelry.com/products/rabbit-story-test',baseDossierVersion:core.hash('approved-base'),sources:[{id:'museum-rabbit',url:'https://museum.example.edu/exhibits/rabbits',title:'Rabbit imagery',excerpt:'Interpretations vary by place and period.',checkedAt:1791372000000,reviewed:true}],meanings:[{text:'A rabbit may evoke a personal sense of renewal.',context:'A qualified personal interpretation.',kind:'interpretation',sourceIds:['museum-rabbit']}],status:'approved',version:core.hash('approved-story'),savedAt:1791372001000,productCheckedAt:1791372000000};
function fixture({namespace='Brites_Growth_Sandbox',failRead=false,records=[record]}={}){
  const calls={reads:[],writes:0,product:0,authFallback:0,rate:0};
  const db={collection:()=>({doc:()=>({get:async()=>{calls.authFallback++;return{exists:false};},set:async()=>{calls.writes++;throw Error('Read must not write.');}})})};
  const service={namespace,storySupplements:async ids=>{calls.reads.push(ids);if(failRead)throw Error(PRIVATE+' '+KEY);return records;},saveStorySupplement:async()=>{calls.writes++;throw Error('Read must not save.');},saveProducts:async()=>{calls.writes++;throw Error('Read must not mirror.');},rateLimit:async()=>{calls.rate++;return true;},col:()=>{calls.writes++;throw Error('Read must not enumerate collections.');}};
  const injected={...core,makeDb:()=>db,createShopify:()=>({byHandle:async()=>{calls.product++;throw Error('Stored story read must not fetch a product.');}}),createGrowthService:()=>service};
  const handler=new Function('core','demandStore','controllerStore','receiptSandboxCheck','etsyCacheReadOnly','historicalLookup','conciergeDiagnostics','Netlify',source)(injected,{},{},{},{},{},{},{env:{get:name=>name==='BRITES_GROWTH_ADMIN_KEY'?KEY:undefined}});
  const call=async({ids=PRODUCT,key=KEY,op='story-supplements',method='GET',body}={})=>{const params=new URLSearchParams({ids});const request=new Request('https://preview.test/api/growth/'+op+'?'+params,{method,headers:{Origin:'https://preview.test',...(key?{'X-Growth-Key':key}:{})},...(['GET','HEAD','OPTIONS'].includes(method)?{}:{body:body||'{}'})});const response=await handler(request,{params:{op},ip:'synthetic'});return{status:response.status,body:response.status===204?null:await response.json(),headers:response.headers};};return{calls,call};
}
test('authenticated exact stored supplement read preserves source and base-version evidence without writes or live calls',async()=>{
  const f=fixture(),r=await f.call();assert.equal(r.status,200);assert.deepEqual(r.body,{supplements:[record]});assert.deepEqual(f.calls.reads,[[PRODUCT]]);assert.equal(f.calls.writes,0);assert.equal(f.calls.product,0);assert.equal(f.calls.rate,0);assert.equal(f.calls.authFallback,0);assert.equal(r.headers.get('cache-control'),'no-store');assert.equal(JSON.stringify(r.body).includes(KEY),false);
});
test('empty stored read means no supplement returned and is distinct from approved public knowledge',async()=>{
  const f=fixture({records:[]}),r=await f.call();assert.equal(r.status,200);assert.deepEqual(r.body,{supplements:[]});assert.equal(r.body.products,undefined);assert.equal(f.calls.writes,0);
});
test('duplicate exact IDs are deduplicated while caller order is preserved',async()=>{
  const f=fixture();assert.equal((await f.call({ids:PRODUCT+','+SECOND+','+PRODUCT})).status,200);assert.deepEqual(f.calls.reads,[[PRODUCT,SECOND]]);
});
for(const ids of ['',',',PRODUCT+',','gid://shopify/Product/0','gid://shopify/Product/0001','gid://shopify/ProductVariant/731','gid://shopify/Product/-1','gid://shopify/Product/123456789012345678901','https://britesjewelry.com/products/rabbit','../Research','gid://shopify/Product/1/Secrets',Array.from({length:21},(_,i)=>'gid://shopify/Product/'+(i+1)).join(',')])test('invalid or unbounded caller IDs fail before storage reads: '+ids,async()=>{
  const f=fixture(),r=await f.call({ids});assert.equal(r.status,400);assert.equal(f.calls.reads.length,0);assert.equal(f.calls.writes,0);
});
test('at most20 exact IDs are accepted without scanning the collection',async()=>{
  const ids=Array.from({length:20},(_,i)=>'gid://shopify/Product/'+(i+1)),f=fixture();assert.equal((await f.call({ids:ids.join(',')})).status,200);assert.deepEqual(f.calls.reads,[ids]);assert.equal(f.calls.writes,0);
});
for(const key of [null,'wrong-synthetic-key'])test('missing or incorrect operator authentication cannot reveal stored sources: '+key,async()=>{
  const f=fixture(),r=await f.call({key});assert.equal(r.status,401);assert.equal(f.calls.reads.length,0);assert.equal(f.calls.writes,0);assert.deepEqual(r.body,{error:'Operator sign-in required.'});assert.ok(!JSON.stringify(r.body).includes(record.sources[0].excerpt));
});
test('private read remains sandbox-only and never borrows the live store',async()=>{
  const f=fixture({namespace:'Brites_Growth_Live'}),r=await f.call();assert.equal(r.status,403);assert.equal(f.calls.reads.length,0);assert.equal(f.calls.writes,0);
});
for(const method of ['POST','PUT','PATCH','DELETE','HEAD'])test('the stored read rejects '+method+' without becoming a write route',async()=>{
  const f=fixture(),r=await f.call({method,body:JSON.stringify(record)});assert.equal(r.status,405);assert.equal(f.calls.reads.length,0);assert.equal(f.calls.writes,0);
});
test('singular story-supplement GET remains POST-only and plural endpoint never becomes public',async()=>{
  const f=fixture();assert.equal((await f.call({op:'story-supplement'})).status,405);assert.equal(f.calls.reads.length,0);const line=source.match(/const publicOps=new Set\([^\n]+/)[0];assert.doesNotMatch(line,/story-supplements?/);assert.equal((await f.call({method:'POST',key:null})).status,401);assert.equal(f.calls.writes,0);
});
test('OPTIONS returns no record and performs no private read',async()=>{
  const f=fixture(),r=await f.call({method:'OPTIONS',key:null});assert.equal(r.status,204);assert.equal(r.body,null);assert.equal(f.calls.reads.length,0);assert.equal(f.calls.writes,0);
});
test('storage errors are retryable without copying credential or provider exception text',async()=>{
  const f=fixture({failRead:true}),r=await f.call();assert.equal(r.status,503);assert.match(r.body.error,/could not be read/);assert.doesNotMatch(JSON.stringify(r.body),new RegExp(PRIVATE+'|'+KEY));assert.equal(f.calls.reads.length,1);assert.equal(f.calls.writes,0);
});
