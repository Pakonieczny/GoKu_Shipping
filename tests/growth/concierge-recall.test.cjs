'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const core=require('../../netlify/functions/_britesGrowth.js');

const NOW=Date.parse('2026-10-03T12:00:00Z');
const clone=value=>structuredClone(value);
function product(id,overrides={}){
  const handle=overrides.handle||'baseball-necklace-'+id;
  return {id:'gid://shopify/Product/'+id,handle,title:'Baseball Necklace',url:'https://britesjewelry.com/products/'+handle,image:null,imageAlt:'',currency:'USD',description:'A baseball pendant with an included chain.',type:'Necklace',tags:['baseball'],options:[{name:'Necklace Length',values:['14 Inches']}],variants:[{id:'gid://shopify/ProductVariant/'+id+'1',numericId:String(id)+'1',title:'Gold Filled / 14 Inches / No engraving',sku:'',price:76,available:true,options:[{name:'Metal',value:'Gold Filled'},{name:'Necklace Length',value:'14 Inches'},{name:'Engraving',value:'No engraving'}]}],variantsComplete:true,checkedAt:NOW,...overrides};
}
function fixture({predictive=[product(1)],mirror=['mini-baseball'],live={},namespace='Brites_Growth_Sandbox'}={}){
  const calls={search:0,handles:[],mirror:0,saved:[]};
  const service={namespace,saveProducts:async rows=>calls.saved.push(rows.map(x=>x.handle)),productIssues:async()=>[],research:async()=>[],catalogueCandidateHandles:async()=>{calls.mirror++;return mirror;}};
  const shopify={search:async()=>{calls.search++;return {products:clone(predictive)};},byHandle:async handle=>{calls.handles.push(handle);const value=live[handle];if(value instanceof Error)throw value;return value===undefined?null:clone(value);}};
  return {calls,service,shopify};
}

test('verified mini baseball necklace repairs the observed predictive recall miss',async()=>{
  const mini=product(8499905888419,{handle:'mini-baseball',variants:[{...product(9).variants[0],id:'gid://shopify/ProductVariant/47832872779939',numericId:'47832872779939',price:59}]});
  const f=fixture({predictive:[1,2,3,4].map(n=>product(n,{handle:'predictive-'+n,variants:[{...product(n).variants[0],price:72+n}]})),live:{'mini-baseball':mini}});
  const answer=await core.concierge({...f,now:()=>NOW,message:'My father loves baseball. Find a complete gold filled baseball necklace with its chain under 65 USD. No earrings.'});
  assert.deepEqual(answer.products.map(x=>x.handle),['mini-baseball']);
  assert.equal(answer.products[0].minPrice,59);assert.equal(answer.products[0].currency,'USD');assert.equal(answer.products[0].variants[0].available,true);
  assert.equal(f.calls.mirror,1);assert.deepEqual(f.calls.handles,['mini-baseball']);assert.deepEqual(f.calls.saved.at(-1),['mini-baseball']);
});

test('mirror details cannot bypass live type, exclusion, availability, price or currency checks',async()=>{
  const wrongType=product(2,{handle:'mini-baseball',type:'Earrings',title:'Baseball Stud Earrings',options:[],variants:[{...product(2).variants[0],title:'Gold Filled Studs',price:12}]});
  const f=fixture({live:{'mini-baseball':wrongType}});
  const answer=await core.concierge({...f,now:()=>NOW,message:'Gold baseball necklace under 65 USD, no earrings'});
  assert.deepEqual(answer.products,[]);assert.equal(f.calls.mirror,1);assert.deepEqual(f.calls.handles,['mini-baseball']);
});

test('an unavailable or over-budget live refresh remains ineligible',async()=>{
  for(const variant of [{price:59,available:false},{price:66,available:true}]){
    const f=fixture({live:{'mini-baseball':product(3,{handle:'mini-baseball',variants:[{...product(3).variants[0],...variant}]})}});
    const answer=await core.concierge({...f,now:()=>NOW,message:'Baseball necklace under 65 USD'});
    assert.deepEqual(answer.products,[]);
  }
});

test('a mismatched shopper currency is disclosed and never silently converted',async()=>{
  const f=fixture({predictive:[product(1,{type:'Earrings',title:'Baseball Earrings',options:[]})],live:{'mini-baseball':product(4,{handle:'mini-baseball',variants:[{...product(4).variants[0],price:59}]})}});
  const answer=await core.concierge({...f,now:()=>NOW,message:'Baseball necklace under $65 CAD',context:{currency:'CAD'}});
  assert.equal(answer.products[0].handle,'mini-baseball');assert.equal(answer.products[0].budgetApplied,false);assert.equal(answer.currencyMismatch,true);assert.match(answer.reply,/haven’t applied your CAD budget/);
});

test('fallback is bounded, deduplicated, repairs empty motif results and skips eligible results',async()=>{
  const handles=Array.from({length:30},(_,i)=>'candidate-'+i);
  const f=fixture({mirror:[...handles,handles[0]],live:Object.fromEntries(handles.map(h=>[h,null]))});
  await core.concierge({...f,now:()=>NOW,message:'Baseball necklace under 65 USD'});assert.equal(f.calls.handles.length,15);assert.equal(new Set(f.calls.handles).size,15);
  const empty=fixture({predictive:[],live:{'mini-baseball':product(8,{handle:'mini-baseball',variants:[{...product(8).variants[0],price:59}]})}});const recalled=await core.concierge({...empty,now:()=>NOW,message:'Baseball necklace'});assert.equal(empty.calls.mirror,1);assert.equal(recalled.products[0].handle,'mini-baseball');
  const eligible=fixture({predictive:[product(7,{variants:[{...product(7).variants[0],price:59}]})]});await core.concierge({...eligible,now:()=>NOW,message:'Baseball necklace under 65 USD'});assert.equal(eligible.calls.mirror,0);
});

test('empty generic, context and meaning requests never start broad mirror recall',async()=>{
  for(const scenario of [{message:'Show me a necklace'},{message:'Tell me the meaning of this necklace',context:{currentHandle:'missing-handle'}}]){
    const f=fixture({predictive:[]});await core.concierge({...f,now:()=>NOW,...scenario});assert.equal(f.calls.mirror,0);
  }
});

test('all failed live refreshes surface verification failure without exposing error text',async()=>{
  const f=fixture({mirror:['mini-baseball'],live:{'mini-baseball':new Error('PRIVATE_STOREFRONT_DETAIL')}});
  await assert.rejects(core.concierge({...f,now:()=>NOW,message:'Baseball necklace under 65 USD'}),error=>error.message==='The matching published pieces could not be checked.');
});

test('sandbox mirror lookup returns only bounded handle hints and never stored product fields',async()=>{
  const calls=[];
  const rows=[product(10,{handle:'baseball-earrings',type:'Earrings',title:'Private seller note',tags:['baseball_necklace'],customerEmail:'private@example.com'}),product(11,{handle:'mini-baseball',type:'Necklace',title:'Baseball Necklace',tags:['baseball_necklace'],customerEmail:'private@example.com'})];
  const db={collection:name=>({where:(field,op,value)=>({limit:n=>({get:async()=>{calls.push({name,field,op,value,n});return {docs:value==='baseball_necklace'?rows.map(value=>({data:()=>value})):[]};}})})})};
  const sandbox=core.createGrowthService({db,env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'},shopify:{},now:()=>NOW});
  const result=await sandbox.catalogueCandidateHandles({interests:['baseball'],query:'baseball',type:'necklace',excludedTypes:['earrings']},1);
  assert.deepEqual(result,['mini-baseball']);assert.deepEqual(calls.map(x=>x.value),['baseball','baseball_necklace']);assert.ok(calls.every(x=>x.n===60));assert.doesNotMatch(JSON.stringify(result),/private|seller|@/i);
  const live=core.createGrowthService({db,env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'},shopify:{},now:()=>NOW});assert.deepEqual(await live.catalogueCandidateHandles({interests:['baseball']},15),[]);assert.equal(calls.length,2);
});
