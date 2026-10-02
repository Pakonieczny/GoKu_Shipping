'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const core=require('../../netlify/functions/_britesGrowth');
const NOW=Date.parse('2026-10-03T12:00:00Z'),id=n=>'gid://shopify/Product/'+n;
const turn=()=>new Promise(resolve=>setImmediate(resolve));
function deferred(){let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};}
function readFixture(){
  const calls=[],pending=[];let active=0,maxActive=0;
  const db={collection:name=>({doc:key=>({get:()=>{const gate=deferred();active++;maxActive=Math.max(maxActive,active);calls.push({name,key});pending.push(gate);return gate.promise.finally(()=>{active--;});}})})};
  return {service:core.createGrowthService({db}),calls,pending,maxActive:()=>maxActive};
}
function settle(gate,value){gate.resolve({exists:value!==null,data:()=>value});}

for(const method of ['research','productIssues','storySupplements'])test(method+' uses six-read chunks, ordered results and a barrier between chunks',async()=>{
  const f=readFixture(),ids=Array.from({length:15},(_,n)=>id(n+1)),run=f.service[method](ids);
  assert.equal(f.calls.length,6);
  for(let start=0;start<ids.length;start+=6){
    const end=Math.min(start+6,ids.length);
    for(let n=end-1;n>start;n--)settle(f.pending[n],{productId:ids[n]});
    await turn();assert.equal(f.calls.length,end,'next chunk waits for the slowest read');
    settle(f.pending[start],{productId:ids[start]});await turn();
    assert.equal(f.calls.length,Math.min(end+6,ids.length));
  }
  assert.deepEqual((await run).map(row=>row.productId),ids);assert.equal(f.maxActive(),6);
});

for(const [method,cap] of [['research',20],['productIssues',100],['storySupplements',20]])test(method+' preserves dedupe, original cap before identity filtering, and missing-document omission',async()=>{
  const calls=[],rows=new Map(),ids=Array.from({length:cap+5},(_,n)=>id(n+1));
  for(const productId of ids)rows.set(core.hash(productId).slice(0,40),{productId});
  rows.delete(core.hash(ids[4]).slice(0,40));
  const db={collection:()=>({doc:key=>({get:async()=>{calls.push(key);return {exists:rows.has(key),data:()=>rows.get(key)};}})})};
  const service=core.createGrowthService({db}),result=await service[method](['invalid',ids[0],ids[0],...ids.slice(1)]);
  assert.equal(calls.length,cap-1);assert.deepEqual(result.map(row=>row.productId),ids.slice(0,cap-1).filter(productId=>productId!==ids[4]));
});

test('a failed hold read rejects the whole lookup, never starts a later chunk or returns partial holds',async()=>{
  const f=readFixture(),run=f.service.productIssues(Array.from({length:15},(_,n)=>id(n+1))),rejection=assert.rejects(run,/hold read failed/);
  for(let n=0;n<6;n++)settle(f.pending[n],{productId:id(n+1),issues:[]});await turn();assert.equal(f.calls.length,12);
  f.pending[7].reject(Error('hold read failed'));await rejection;
  for(let n=6;n<12;n++)if(n!==7)settle(f.pending[n],{productId:id(n+1),issues:[]});await turn();assert.equal(f.calls.length,12);
});

test('live namespace never reads sandbox story supplements',async()=>{
  const db={collection:()=>{throw Error('must not read');}},service=core.createGrowthService({db,env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'}});
  assert.deepEqual(await service.storySupplements([id(1)]),[]);
});

const product=(n=1,price=54)=>({id:id(n),handle:'wolf-necklace-'+n,url:'https://britesjewelry.com/products/wolf-necklace-'+n,title:'Wolf Necklace',type:'Necklace',description:'Wolf pendant with its chain included.',tags:['wolf'],currency:'USD',image:null,options:[],variants:[{id:'gid://shopify/ProductVariant/'+n,numericId:String(n),title:'Sterling Silver',price,available:true,options:[]}],variantsComplete:true,checkedAt:NOW});
function conciergeFixture(){const calls=[],service={saveProducts:async()=>{calls.push('save');},productIssues:async()=>{calls.push('issues');return [];},research:async()=>{calls.push('research');return [];},storySupplements:async()=>{calls.push('story');return [];}};return {calls,service,shopify:{search:async()=>({products:[product()]})},message:'Wolf silver necklace',now:()=>NOW};}

test('catalogue persistence and hold lookup overlap, but knowledge and recommendations await both',async()=>{
  const f=conciergeFixture(),save=deferred(),issues=deferred();let finished=false;
  f.service.saveProducts=()=>{f.calls.push('save');return save.promise;};f.service.productIssues=()=>{f.calls.push('issues');return issues.promise;};
  const run=core.concierge(f).then(value=>{finished=true;return value;});await turn();assert.deepEqual(f.calls,['save','issues']);
  issues.resolve([]);await turn();assert.equal(finished,false);assert.deepEqual(f.calls,['save','issues']);
  save.resolve();const result=await run;assert.equal(result.products.length,1);assert.deepEqual(f.calls,['save','issues','research','story']);
});

for(const failed of ['saveProducts','productIssues'])test('failed '+failed+' prevents downstream knowledge, AI and all advice',async()=>{
  const f=conciergeFixture();f.service[failed]=async()=>{throw Error('required stage unavailable');};f.ai=async()=>{throw Error('AI must not run');};
  await assert.rejects(core.concierge(f),/required stage unavailable/);assert(!f.calls.includes('research'));assert(!f.calls.includes('story'));
});

test('recall persistence and full combined hold lookup overlap, with no recommendation before both settle',async()=>{
  const f=conciergeFixture(),save=deferred(),issues=deferred();let saves=0,lookups=0,finished=false,seenIds;
  f.message='Wolf silver necklace under 65 USD';f.shopify.search=async()=>({products:[product(1,75)]});f.shopify.byHandle=async()=>product(2,54);
  f.service.catalogueCandidateHandles=async()=>[product(2).handle];
  f.service.saveProducts=()=>++saves===2?save.promise:Promise.resolve();
  f.service.productIssues=ids=>{if(++lookups===2){seenIds=ids;return issues.promise;}return Promise.resolve([]);};
  const run=core.concierge(f).then(value=>{finished=true;return value;});await turn();assert.equal(saves,2);assert.equal(lookups,2);assert.deepEqual(seenIds,[id(1),id(2)]);
  save.resolve();await turn();assert.equal(finished,false);assert(!f.calls.includes('research'));
  issues.resolve([{productId:id(2),issues:[{kind:'identity',status:'open',blocks:['recommendation','cart']}]}]);
  assert.deepEqual((await run).products,[]);
});

test('research and supplements start together; either failure suppresses partial meanings',async()=>{
  for(const failed of ['research','storySupplements']){
    const f=conciergeFixture(),research=deferred(),story=deferred();let finished=false;
    f.service.research=()=>{f.calls.push('research');return research.promise;};f.service.storySupplements=()=>{f.calls.push('story');return story.promise;};
    const run=core.concierge(f).then(value=>{finished=true;return value;});await turn();assert.deepEqual(f.calls,['save','issues','research','story']);assert.equal(finished,false);
    const dossier={productId:id(1),status:'approved',sources:[{id:'museum',url:'https://example.org/museum/wolf',title:'Museum interpretation',reviewed:true,checkedAt:NOW}],meanings:[{text:'A wolf may evoke a personal sense of companionship.',context:'Personal interpretation.',kind:'interpretation',sourceIds:['museum']}]};
    if(failed==='research'){story.resolve([]);await turn();research.reject(Error('unavailable'));}else{research.resolve([dossier]);await turn();story.reject(Error('unavailable'));}
    const result=await run;assert.equal(result.knowledgeUnavailable,true);assert.deepEqual(result.meanings,[]);assert.equal(result.products.length,1);assert.equal(result.aiUsed,false);
  }
});
