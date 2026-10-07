'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const seed=require('../../netlify/functions/_britesStorefrontSeed'),core=require('../../netlify/functions/_britesGrowth');
const NOW=Date.parse('2026-10-07T21:00:00Z');
function product(id,category='stud-earrings'){
  const meta={
    'regular-necklaces':['Silver Chain Necklace','Necklace','Necklace Length'],
    'beady-necklaces':['Padlock Beady Necklace','Necklace','Necklace Length'],
    'stud-earrings':['Cherry Stud Earrings','Earrings','Metal Choice'],
    'hoop-earrings':['Gold Huggie Hoop Earrings','Earrings','Hoop Size'],
    'charm-only':['Puzzle Necklace Charm','Charm','Charm Type']
  }[category];
  return {id:'gid://shopify/Product/'+id,handle:'piece-'+id,title:meta[0],type:meta[1],image:'https://cdn.shopify.com/'+id+'.jpg',url:'https://britesjewelry.com/products/piece-'+id,currency:'USD',checkedAt:NOW,options:[{name:meta[2],values:['Published value']}],variants:[{id:'gid://shopify/ProductVariant/'+(10000+id),numericId:10000+id,title:'Published value',price:51,available:true,options:[{name:meta[2],value:'Published value'}]}],variantsComplete:true};
}
function fixture({rows,failPages=[],failSearch=false,cache={},clock=()=>NOW,searchRows=[]}={}){
  const calls={pages:[],searches:[]},pool=rows||Array.from({length:250},(_,i)=>product(i+1,seed.CATEGORIES[i%5]));
  const reader=seed.createSeedReader({cache,now:clock,isDiscovery:core.isStorefrontDiscoveryProduct,project:core.productProjection,
    readPage:async n=>{calls.pages.push(n);if(failPages.includes(n))throw Error('PRIVATE_UPSTREAM_ERROR');return {products:n===1?pool:[],pageInfo:{hasNextPage:n===1}};},
    readSearch:async q=>{calls.searches.push(q);if(failSearch)throw Error('PRIVATE_SEARCH_ERROR');return {products:searchRows};}});
  return {reader,calls};
}
test('strict source categories separate chains, beads, studs, actual hoops and charm attachments',()=>{
  for(const name of seed.CATEGORIES)assert.deepEqual(seed.categories(product(1,name)),[name]);
  assert.deepEqual(seed.categories({...product(1,'regular-necklaces'),type:'Charm',title:'Cable Chain Necklace'}),['regular-necklaces']);
  assert.deepEqual(seed.categories({...product(1,'charm-only'),title:'Huggie Necklace Charm',options:[{name:'Charm Type',values:['Huggie CHARM SET']}]}),['charm-only']);
  assert.deepEqual(seed.categories({...product(1,'regular-necklaces'),title:'Gemstone Bead Necklace'}),[]);
  assert.deepEqual(seed.categories({...product(1),title:'Custom Charm Studio Credit',type:'Studio Credit'}),[]);
});
test('bounded public seed returns120 distinct photographed listings and honest category counts',async()=>{
  const f=fixture(),r=await f.reader.read();assert.equal(r.products.length,120);assert.equal(new Set(r.products.map(p=>p.id)).size,120);
  assert.deepEqual(r.seed.categoryCounts,Object.fromEntries(seed.CATEGORIES.map(name=>[name,24])));assert.equal(r.seed.complete,true);assert.equal(r.seed.partial,false);
  assert.deepEqual(r.pageInfo,{hasNextPage:false,endCursor:null});assert.deepEqual(f.calls.pages,[1,2]);assert.equal(f.calls.searches.length,4);
  assert.ok(r.products.every(p=>p.image&&p.storeCategories.length===1));
});
test('small category windows are neither padded nor presented as storewide totals',async()=>{
  const rows=Array.from({length:112},(_,i)=>product(i+1));for(let i=0;i<4;i++)rows.push(product(200+i,seed.CATEGORIES.filter(x=>x!=='stud-earrings')[i]));
  const r=await fixture({rows}).reader.read();assert.equal(r.products.length,116);assert.equal(r.seed.complete,true);assert.equal(r.seed.categoryCounts['hoop-earrings'],1);
  assert.equal(r.seed.categoryCounts['stud-earrings'],112);assert.equal(r.seed.loaded,116);
});
test('duplicate windows do not increase counts and conflicting ID or handle fails closed',async()=>{
  const p=product(1),r=await fixture({rows:[p,p],searchRows:[p]}).reader.read();assert.equal(r.seed.loaded,1);assert.equal(r.seed.complete,false);
  await assert.rejects(fixture({rows:[p,{...p,handle:'different'}]}).reader.read(),/identities conflict/);
  await assert.rejects(fixture({rows:[p,{...p,id:'gid://shopify/Product/2'}]}).reader.read(),/identities conflict/);
});
test('missing categories, failed windows and absent photos remain explicit partial coverage',async()=>{
  const rows=Array.from({length:110},(_,i)=>product(i+1));rows.push({...product(500,'hoop-earrings'),image:null});
  const r=await fixture({rows,failPages:[2],failSearch:true}).reader.read();assert.equal(r.seed.loaded,110);assert.equal(r.seed.complete,false);assert.equal(r.seed.partial,true);
  assert.equal(r.seed.sourcePages,2);assert.deepEqual(r.seed.unfilledCategories,seed.CATEGORIES.filter(x=>x!=='stud-earrings'));assert.doesNotMatch(JSON.stringify(r),/PRIVATE_/);
});
test('a later bounded public page fills small subtype pools even when the first pages already exceed100',async()=>{
  const calls=[],reader=seed.createSeedReader({now:()=>NOW,isDiscovery:core.isStorefrontDiscoveryProduct,project:core.productProjection,
    readPage:async page=>{calls.push(page);const rows=page===1?Array.from({length:120},(_,i)=>product(i+1)):page===2?Array.from({length:50},(_,i)=>product(i+201,'charm-only')):page===3?['regular-necklaces','beady-necklaces','hoop-earrings'].flatMap((c,j)=>Array.from({length:20},(_,i)=>product(500+j*20+i,c))):[];return {products:rows,pageInfo:{hasNextPage:page<3}};},readSearch:async()=>({products:[]})});
  const r=await reader.read();assert.deepEqual(calls,[1,2,3]);assert.equal(r.seed.loaded,120);assert.equal(r.seed.complete,true);assert.ok(Object.values(r.seed.categoryCounts).every(n=>n>=20));
});
test('raw public cache coalesces requests, returns independent copies and expires without stale fallback',async()=>{
  let time=NOW;const cache={},f=fixture({cache,clock:()=>time});const [a,b]=await Promise.all([f.reader.read(),f.reader.read()]);assert.equal(f.calls.pages.length,2);
  a.products[0].title='Changed by caller';assert.notEqual(a.products[0].title,b.products[0].title);assert.notEqual((await f.reader.read()).products[0].title,a.products[0].title);
  time+=seed.CACHE_MS+1;await f.reader.read();assert.equal(f.calls.pages.length,4);
  const bad=fixture({rows:[],failPages:[1,2,3,4],failSearch:true,cache,clock:()=>time+seed.CACHE_MS+1});await assert.rejects(bad.reader.read(),/could not be checked/);
});
test('seed public adapter cannot use configured Admin credentials, guesses no availability and bounds page size',async()=>{
  const calls=[],raw={id:1,handle:'piece-1',title:'Silver Stud Earrings',product_type:'Earrings',options:[{name:'Metal Choice'}],images:[{src:'https://cdn.shopify.com/1.jpg'}],variants:[{id:10001,title:'Silver',price:'51.00',option1:'Silver'}]};
  const fetch=async (url,options)=>{calls.push({url,options});let body=url.endsWith('/cart.js')?{currency:'CAD'}:url.includes('/search/suggest.json')?{resources:{results:{products:[]}}}:{products:[raw]};return {ok:true,status:200,json:async()=>body};};
  const r=await core.createShopify({env:{SHOPIFY_STORE:'synthetic.myshopify.com',SHOPIFY_CLIENT_ID:'TEST',SHOPIFY_CLIENT_SECRET:'SYNTHETIC_TEST_SECRET'},fetch,now:()=>NOW}).seed();
  assert.equal(r.products[0].variants[0].available,false);assert.equal(r.products[0].currency,'CAD');assert.equal(r.products[0].variants[0].price,51);
  assert.ok(calls.every(c=>c.url.startsWith('https://britesjewelry.com/')&&!c.url.includes('/admin/')&&!c.options.method&&c.options.redirect==='error'));
  assert.doesNotMatch(JSON.stringify(calls),/SYNTHETIC_TEST_SECRET/);
});
const apiSource=fs.readFileSync(path.join(__dirname,'../../netlify/functions/britesGrowthApi.js'),'utf8').replace(/^import\s+\w+\s+from\s+['"][^'"]+['"];\s*$/gm,'').replace('export default async (req,context) => {','return async (req,context) => {').replace(/export const config = [\s\S]*$/,'');
function apiFixture({failHolds=false}={}){
  const rows=Array.from({length:120},(_,i)=>product(i+1,seed.CATEGORIES[i%5])),calls={seed:0,writes:0,holdIds:[]};
  const shop={seed:async()=>{calls.seed++;return {products:rows.map(p=>({...p,storeCategories:seed.categories(p)})),pageInfo:{hasNextPage:false,endCursor:null},checkedAt:NOW,seed:{schema:1,loaded:120,minimum:100,target:120,complete:true,categoryCounts:seed.counts(rows),unfilledCategories:[],sourcePages:2,partial:false}};}};
  const service={rateLimit:async()=>true,saveProducts:async()=>{calls.writes++;},productIssues:async ids=>{calls.holdIds.push(ids);if(failHolds&&calls.holdIds.length===2)throw Error('PRIVATE_SECOND_HOLD_ERROR');return ids.includes(rows[119].id)?[{productId:rows[119].id,issues:[{kind:'identity',status:'open',blocks:['cart','recommendation'],detail:'PRIVATE_HOLD'}]}]:[];}};
  const injected={...core,makeDb:()=>({}),createShopify:()=>shop,createGrowthService:()=>service};
  const handler=new Function('core','demandStore','controllerStore','receiptSandboxCheck','etsyCacheReadOnly','historicalLookup','conciergeDiagnostics','Netlify',apiSource)(injected,{},{},{},{},{},{},{env:{get:()=>undefined}});
  return {handler,calls};
}
async function apiCall(f,q='?seed=1'){const r=await f.handler(new Request('https://brites-growth-sandbox.netlify.app/api/growth/catalogue'+q),{params:{op:'catalogue'},ip:'synthetic-seed34'});return {status:r.status,body:await r.json()};}
test('seed API reads fresh holds for every product beyond100 and performs no mirror writes',async()=>{
  const f=apiFixture(),r=await apiCall(f);assert.equal(r.status,200);assert.equal(r.body.seed.loaded,120);assert.equal(f.calls.writes,0);
  assert.deepEqual(f.calls.holdIds.map(x=>x.length),[100,20]);assert.equal(r.body.products[119].cartHold,true);assert.equal(r.body.products[119].recommendationHold,true);
  assert.ok(r.body.products.every(p=>p.storeCategories.length===1));assert.doesNotMatch(JSON.stringify(r.body),/PRIVATE_|sku/);
  await apiCall(f);assert.equal(f.calls.holdIds.length,4);
});
test('seed mixing requests fail before reads; a later hold failure exposes no partial seed or private cause',async()=>{
  const f=apiFixture();for(const q of ['?seed=1&browse=1','?seed=1&cursor=storefront:2','?seed=1&q=bunny'])assert.equal((await apiCall(f,q)).status,400);assert.equal(f.calls.seed,0);
  const r=await apiCall(apiFixture({failHolds:true}));assert.equal(r.status,400);assert.equal(r.body.products,undefined);assert.doesNotMatch(JSON.stringify(r.body),/PRIVATE_/);
});
