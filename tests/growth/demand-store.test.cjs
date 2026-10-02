'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {createDemandStore}=require('../../netlify/functions/_britesGrowthDemandStore');
function fixture(){
  let time=Date.parse('2026-10-02T03:00:00Z'),version='v1',issues=[];const records=new Map();
  const product={id:'gid://shopify/Product/1',handle:'bunny'},service={getProduct:async id=>id===product.id?product:null,research:async()=>[{productId:product.id,handle:product.handle,status:'approved',version}],productIssues:async()=>issues,col:name=>{assert.equal(name,'Demand');return{doc:key=>({set:async value=>records.set(key,structuredClone(value)),get:async()=>({exists:records.has(key),data:()=>structuredClone(records.get(key))})})};}};
  const store=createDemandStore(service,{now:()=>time}),evidence=()=>({schemaVersion:1,evidenceKind:'product_demand_and_ad_outcomes',readOnly:true,sandboxReadOnly:true,productId:product.id,handle:product.handle,dossierVersion:version,at:time,sources:{shopping:{state:'unavailable'}},shopping:{scope:'verified_product_and_variant_offer_ids',rows:[]},search:{scope:'currently_aligned_search_ad_groups'},pmax:{scope:'campaign_context_only',productAttributionVerified:false},planner:{state:'unavailable'}});
  return{store,evidence,id:product.id,records,service,setVersion:value=>version=value,setIssues:value=>issues=value,advance:ms=>time+=ms};
}
test('exact version-bound evidence persists and unavailable observations remain unavailable',async()=>{
  const f=fixture();await f.store.save(f.evidence());const rows=await f.store.read([f.id]);assert.equal(rows.length,1);assert.equal(rows[0].state,'current');assert.equal(rows[0].evidence.sources.shopping.state,'unavailable');assert.equal(rows[0].evidence.planner.state,'unavailable');assert.equal(rows[0].promotionAllowed,true);
});
test('foreign, stale, future, unbounded and mutated evidence cannot be saved',async()=>{
  const f=fixture(),e=f.evidence();for(const bad of [{...e,productId:'gid://shopify/Product/2'},{...e,handle:'foreign'},{...e,dossierVersion:'old'},{...e,readOnly:false},{...e,sandboxReadOnly:false},{...e,at:e.at-2*86400000},{...e,at:e.at+120000},{...e,padding:'x'.repeat(250000)}])await assert.rejects(f.store.save(bad));assert.equal(f.records.size,0);
});
test('a revised dossier or elapsed day marks saved evidence stale',async()=>{
  const f=fixture();await f.store.save(f.evidence());f.setVersion('v2');assert.equal((await f.store.read([f.id]))[0].state,'stale');assert.equal((await f.store.read([f.id]))[0].promotionAllowed,false);f.setVersion('v1');f.advance(86400001);assert.equal((await f.store.read([f.id]))[0].state,'stale');
});
test('new holds and unavailable issue reads are evaluated at read time',async()=>{
  const f=fixture();await f.store.save(f.evidence());f.setIssues([{productId:f.id,issues:[{kind:'material',status:'open',blocks:['recommendation','cart']}]}]);const row=(await f.store.read([f.id]))[0];assert.equal(row.promotionAllowed,false);assert.equal(row.productHolds.cartHold,true);f.service.productIssues=async()=>{throw Error('temporary storage failure');};const unavailable=(await f.store.read([f.id]))[0];assert.equal(unavailable.productIssueState,'unavailable');assert.equal(unavailable.promotionAllowed,false);
});
test('missing and cross-bound saved records are not returned',async()=>{
  const f=fixture();assert.deepEqual(await f.store.read([f.id]),[]);await f.store.save(f.evidence());for(const [key,value] of f.records)f.records.set(key,{...value,productId:'gid://shopify/Product/2'});assert.deepEqual(await f.store.read([f.id]),[]);await assert.rejects(f.store.read(Array.from({length:21},()=>f.id)),/20/);
});
