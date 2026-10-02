'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const core=require('../../netlify/functions/_britesGrowth');
const source=fs.readFileSync(path.join(__dirname,'../../netlify/functions/britesGrowthApi.js'),'utf8')
  .replace("import core from './_britesGrowth.js';",'')
  .replace("import demandStore from './_britesGrowthDemandStore.js';",'')
  .replace("import receiptSandboxCheck from './_britesGrowthReceiptSandboxCheck.js';",'')
  .replace("import etsyCacheReadOnly from './_britesGrowthEtsyCacheReadOnly.js';",'')
  .replace("import controllerStore from './_britesGrowthController.js';",'')
  .replace('export default async (req,context) => {','return async (req,context) => {')
  .replace(/export const config = [\s\S]*$/, '');
const product={id:'gid://shopify/Product/301',handle:'verified-bunny',url:'https://britesjewelry.com/products/verified-bunny',title:'Bunny necklace',type:'Necklace',description:'Pendant and chain included.',currency:'USD',options:[{name:'Necklace Length',values:['18 inches']}],variants:[{id:'gid://shopify/ProductVariant/302',numericId:'302',price:54,available:true,title:'18 inches',sku:'Bunny301',options:[]}],variantsComplete:true,checkedAt:Date.now()};
const issue={productId:product.id,issues:[{id:'inspection',kind:'identity',detail:'Private inspection diagnosis.',status:'open',blocks:['recommendation','cart'],evidence:[{url:product.url,text:'Private evidence',checkedAt:Date.now()}]}]};
function fixture(){
  let savedIssue=null,write=null;
  const row={rank:1,title:'Bunny necklace',handle:'verified-bunny',sku:'Bunny301',leaseUntil:0};
  const db={collection:()=>({doc:()=>({get:async()=>({exists:true,data:()=>({passcode:'owner-code'})})})}),runTransaction:async fn=>fn({get:async()=>({exists:true,data:()=>row}),update:(_r,value)=>{write=value;}})};
  const service={rateLimit:async()=>true,saveProducts:async()=>{},getProduct:async id=>id===product.id?product:null,productIssues:async()=>[issue],research:async()=>[],recordProductIssue:async value=>{savedIssue=value;return{ok:true,productId:value.productId};},col:()=>({doc:()=>({})})};
  const injected={...core,makeDb:()=>db,createShopify:()=>({byHandle:async()=>product,search:async()=>({products:[product],pageInfo:{}})}),createGrowthService:()=>service};
  const handler=new Function('core','demandStore','controllerStore','receiptSandboxCheck','etsyCacheReadOnly','Netlify',source)(injected,require('../../netlify/functions/_britesGrowthDemandStore'),require('../../netlify/functions/_britesGrowthController'),require('../../netlify/functions/_britesGrowthReceiptSandboxCheck'),require('../../netlify/functions/_britesGrowthEtsyCacheReadOnly'),{env:{get:k=>k==='BRITES_GROWTH_ADMIN_KEY'?'operator-key':undefined}});
  return{handler,getSaved:()=>savedIssue,getWrite:()=>write};
}
async function call(f,op,{method='GET',body,key,query=''}={}){
  const req=new Request('https://brites-growth-sandbox.netlify.app/api/growth/'+op+query,{method,headers:{...(key?{'X-Growth-Key':key}:{}),...(body?{'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})});
  const result=await f.handler(req,{params:{op},ip:'test'});return{status:result.status,body:await result.json()};
}
test('live public product and catalogue apply persistent holds without diagnosis leakage',async()=>{
  const f=fixture();for(const op of ['product','catalogue']){const r=await call(f,op);assert.equal(r.status,200);const p=r.body.product||r.body.products[0];assert.equal(p.cartHold,true);assert.equal(p.recommendationHold,true);assert(!JSON.stringify(r.body).includes('Private'));}
});
test('issue diagnosis is operator-only and cannot be written anonymously',async()=>{
  const f=fixture();assert.equal((await call(f,'issues')).status,401);assert.equal((await call(f,'issue',{method:'POST',body:issue})).status,401);assert.equal(f.getSaved(),null);
});
test('controller coordination and checkpoints remain operator-only',async()=>{
  const f=fixture();for(const op of ['controller','checkpoint','checkpoint-read','receipt-sandbox-check','etsy-cache-state'])assert.equal((await call(f,op,{method:'POST',body:{action:'claim',owner:'anonymous',value:{phase:'overwrite'}}})).status,401);
  assert.equal((await call(f,'controller',{method:'POST',body:{action:'invalid'},key:'operator-key'})).status,400);
});
test('reviewed issue ingestion requires fresh exact current-product evidence',async()=>{
  const f=fixture();const valid={productId:product.id,issues:[{...issue.issues[0],reviewed:true}]};
  for(const payload of [{...valid,productId:'gid://shopify/Product/999'},{...valid,issues:[{...valid.issues[0],reviewed:false}]},{...valid,issues:[{...valid.issues[0],evidence:[{url:'https://example.com/competitor',text:'another offer',checkedAt:Date.now()}]}]},{...valid,issues:[{...valid.issues[0],evidence:[{url:product.url,text:'Old observation',checkedAt:Date.now()-31*86400000}]}]}])assert.equal((await call(f,'issue',{method:'POST',body:payload,key:'operator-key'})).status,400);
  assert.equal(f.getSaved(),null);assert.equal((await call(f,'issue',{method:'POST',body:valid,key:'operator-key'})).status,200);assert.equal(f.getSaved().productId,product.id);
});
test('public knowledge suppresses interpretations for a meaning hold',async()=>{
  const f=fixture();const r=await call(f,'knowledge',{query:'?ids='+encodeURIComponent(product.id)});assert.equal(r.status,200);assert.deepEqual(r.body.products,[]);
});
test('claimed exact match methods require the original ranked identity',async()=>{
  const f=fixture();const base={rank:1,productId:product.id,handle:product.handle,match:{method:'exact_sku',evidence:'Inspected published variant',checkedAt:Date.now()}};
  assert.equal((await call(f,'match',{method:'POST',body:base,key:'operator-key'})).status,200);
  const wrong={...base,match:{...base.match,method:'exact_title'}};
  product.title='Different motif necklace';try{const r=await call(f,'match',{method:'POST',body:wrong,key:'operator-key'});assert.equal(r.status,400);assert.match(r.body.error,/Exact title/);}finally{product.title='Bunny necklace';}
});

test('receipt storage checks reject caller receipt data and non-sandbox services before any write',async()=>{
  const f=fixture();
  assert.equal((await call(f,'receipt-sandbox-check',{method:'POST',body:{requestId:'synthetic-caller-id'},key:'operator-key'})).status,400);
  assert.equal((await call(f,'receipt-sandbox-check',{method:'POST',body:{},key:'operator-key'})).status,403);
  assert.equal((await call(f,'receipt-sandbox-check',{key:'operator-key'})).status,405);
  assert.equal(f.getWrite(),null);
});
