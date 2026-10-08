'use strict';
// Real backend routing with synthetic public GET results. No provider calls,
// Admin requests, private inventory claims or purchase writes are performed.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const core=require('../../netlify/functions/_britesGrowth');
const discovery=require('../../netlify/functions/_britesCatalogueDiscovery');
const seed=require('../../netlify/functions/_britesStorefrontSeed');
const voice=require('../../netlify/functions/_britesConciergeVoice');
const NOW=Date.parse('2026-10-08T05:00:00Z'),clone=value=>structuredClone(value);
function product(id,category=seed.CATEGORIES[(id-1)%5]){
  const [title,type,option]= {
    'regular-necklaces':['Leaf Chain Necklace','Necklace','Necklace Length'],
    'beady-necklaces':['Leaf Beady Necklace','Necklace','Necklace Length'],
    'stud-earrings':['Leaf Stud Earrings','Earrings','Metal Choice'],
    'hoop-earrings':['Leaf Huggie Hoop Earrings','Earrings','Hoop Size'],
    'charm-only':['Leaf Necklace Charm','Charm','Charm Type']
  }[category],value=category.includes('necklaces')?'18 Inches':category==='charm-only'?'Necklace CHARM':category==='hoop-earrings'?'8.5mm':'Sterling Silver';
  const options=[{name:'Metal Choice',value:'Sterling Silver'},...(option==='Metal Choice'?[]:[{name:option,value}])];
  return {id:'gid://shopify/Product/'+id,handle:'public-piece-'+id,title:title+' '+id,type,url:'https://britesjewelry.com/products/public-piece-'+id,currency:'USD',description:category.includes('necklaces')?'Includes a chain.':'An exact published '+(category==='charm-only'?'standalone charm; chain and hoops not included.':'pair of earrings.'),image:'https://cdn.shopify.com/'+id+'.jpg',tags:['leaf'],options:options.map(o=>({name:o.name,values:[o.value]})),variantsComplete:true,checkedAt:NOW,variants:[{id:'gid://shopify/ProductVariant/'+(10000+id),numericId:String(10000+id),title:options.map(o=>o.value).join(' / '),price:50+id%11,available:true,options,sku:'PRIVATE_SKU'}]};
}
function fixture({rows=Array.from({length:120},(_,i)=>product(i+1)),searchRows=[],seedFailure=false,searchFailure=false,fresh,issues=[],issueFailure}={}){
  const calls={seed:0,search:[],handles:[],saved:[],holds:[]};
  const reader=discovery.createDiscovery({now:()=>NOW,readSeed:async()=>{calls.seed++;if(seedFailure)throw Error('PRIVATE_SEED_FAILURE');return {products:clone(rows),seed:{partial:false}};},readSearch:async query=>{calls.search.push(query);if(searchFailure)throw Error('PRIVATE_SEARCH_FAILURE');return {products:clone(searchRows)};}});
  const shopify={discover:reader.read,search:async()=>{throw Error('The old ten-result-only path must not be used.');},byHandle:async handle=>{calls.handles.push(handle);if(fresh)return fresh(handle);return clone(rows.find(p=>p.handle===handle)||null);}};
  const service={saveProducts:async products=>{calls.saved.push(clone(products));},productIssues:async ids=>{calls.holds.push([...ids]);if(issueFailure&&issueFailure(ids,calls.holds.length))throw Error('PRIVATE_HOLD_FAILURE');return clone(issues.filter(issue=>ids.slice(0,100).includes(issue.productId)));},research:async()=>[],storySupplements:async()=>[]};
  return {rows,calls,shopify,service,now:()=>NOW};
}
function otterRows(){const rows=Array.from({length:120},(_,i)=>product(i+1));rows[117].title='Otter Stud Earrings';rows[117].tags=['otter'];return rows;}
const ask=(f,message='Find otter stud earrings',extra={})=>core.concierge({...f,message,...extra});

for(const [message,category,type] of [['Show regular necklaces','regular-necklaces','necklace'],['Show beady necklaces','beady-necklaces','necklace'],['Show stud earrings','stud-earrings','earrings'],['Show hoop earrings','hoop-earrings','earrings'],['Show loose charms','charm-only','charm']])test('precise category replaces the old motif without becoming a literal query: '+message,()=>{
  const p=core.intentFrom(message,[],core.intentFrom('Bunny gold necklace under $80'));
  assert.equal(p.storeCategory,category);assert.equal(p.type,type);assert.equal(p.query,'');assert.deepEqual(p.interests,[]);assert.equal(p.budget,80);assert.equal(p.metal,'gold');
  const hits=core.rankProducts(seed.CATEGORIES.map((c,i)=>product(i+1,c)),{...p,metal:null},NOW);
  assert.equal(hits.length,1);assert.deepEqual(seed.categories(hits[0]),[category]);
});
test('category refinements persist through price answers and clear on broad browse/reset',()=>{
  const before=core.intentFrom('Beady necklaces');assert.equal(core.intentFrom('Under $65',[],before).storeCategory,'beady-necklaces');
  assert.equal(core.intentFrom('Show necklaces',[],before).storeCategory,null);assert.equal(core.intentFrom('Show all pieces',[],before).storeCategory,null);
  assert.equal(core.shopperPreferences({storeCategory:'PRIVATE_OWNER_CATEGORY'}).storeCategory,null);
});
test('necklace subtype qualifiers preserve intervening known and new motifs and budgets',()=>{
  for(const [phrase,category,motif] of [['beaded bunny','beady-necklaces','bunny'],['bead otter','beady-necklaces','otter'],['regular bunny','regular-necklaces','bunny'],['regular otter','regular-necklaces','otter']]){
    const p=core.intentFrom('Show '+phrase+' necklaces under $55');
    assert.equal(p.storeCategory,category);assert.equal(p.type,'necklace');assert.equal(p.query,motif);assert.equal(p.budget,55);
    const rows=seed.CATEGORIES.map((c,i)=>{const p=product(i+1,c);p.title=motif+' '+p.title;p.tags=[motif];return p;});
    assert.deepEqual(core.rankProducts(rows,p,NOW).map(p=>seed.categories(p)[0]),[category]);
  }
  const previous=core.intentFrom('Bunny gold necklace under $80');
  for(const phrase of ['Show beaded necklaces','Show bead necklaces']){const p=core.intentFrom(phrase,[],previous);assert.equal(p.storeCategory,'beady-necklaces');assert.equal(p.query,'');}
  const contrasted=core.intentFrom('Not regular necklaces; show beaded bunny necklaces under $55');assert.equal(contrasted.storeCategory,'beady-necklaces');assert.equal(contrasted.query,'bunny');
  assert.equal(core.intentFrom('I like regular hoops').storeCategory,'hoop-earrings');
});
test('Huggie charm attachments cannot become hoops or regular necklaces',()=>{
  const charm={...product(1,'charm-only'),title:'Huggie Necklace Charm',variants:[{...product(1,'charm-only').variants[0],title:'Sterling Silver / Huggie CHARM SET'}]};
  assert.deepEqual(core.rankProducts([charm],core.intentFrom('Hoop earrings'),NOW),[]);assert.deepEqual(core.rankProducts([charm],core.intentFrom('Regular necklaces'),NOW),[]);
  const hit=core.rankProducts([charm],core.intentFrom('Charm-only'),NOW)[0];assert.equal(hit.partsOnly,true);assert.equal(hit.variants[0].title,'Sterling Silver / Huggie CHARM SET');
});
test('global discovery finds the 118th listing outside predictive results and the current six-card tray',async()=>{
  const rows=otterRows(),f=fixture({rows,searchRows:rows.slice(0,10),fresh:handle=>{const p=clone(rows.find(p=>p.handle===handle));p.variants[0].price=79;return p;}}),answer=await ask(f,undefined,{context:{currentHandle:rows[0].handle,productHandles:rows.slice(0,6).map(p=>p.handle)}});
  assert.deepEqual(answer.products.map(p=>p.id),[rows[117].id]);assert.equal(answer.products[0].minPrice,79);assert.equal(answer.products[0].variants[0].price,79);
  assert.deepEqual(f.calls.handles,[rows[117].handle]);assert.deepEqual(f.calls.holds.map(ids=>ids.length),[100,20,1]);assert.equal(f.calls.saved.flat().length,1);
  assert.equal(answer.discovery.sampledListings,120);assert.equal(answer.discovery.catalogueComplete,false);assert.equal(answer.discovery.exactListingsChecked,1);
  assert.doesNotMatch(JSON.stringify(answer),/PRIVATE_|sku|orders|owner/i);
});
test('independently reviewed hold beyond row100 remains authoritative before recommendation',async()=>{
  const rows=otterRows(),f=fixture({rows,issues:[{productId:rows[117].id,issues:[{kind:'identity',status:'open',blocks:['recommendation','cart'],detail:'PRIVATE_HOLD'}]}]}),answer=await ask(f);
  assert.deepEqual(answer.products,[]);assert.deepEqual(f.calls.holds.map(ids=>ids.length),[100,20]);assert.equal(f.calls.handles.length,0);assert.equal(f.calls.saved.length,0);assert.doesNotMatch(JSON.stringify(answer),/PRIVATE_HOLD/);
});
test('unknown seed availability can select an exact read but never supplies final stock or price',async()=>{
  const rows=otterRows();rows[117].variants[0].available=false;rows[117].variants[0].availabilityKnown=false;
  const f=fixture({rows,fresh:handle=>{const p=clone(rows.find(p=>p.handle===handle));p.variants[0].available=true;p.variants[0].availabilityKnown=true;p.variants[0].price=81;return p;}}),answer=await ask(f);
  assert.equal(answer.products.length,1);assert.equal(answer.products[0].minPrice,81);assert.equal(answer.products[0].variants[0].available,true);assert.equal(answer.products[0].variants[0].availabilityKnown,undefined);
  const unavailable=fixture({rows,fresh:handle=>{const p=clone(rows.find(p=>p.handle===handle));p.variants[0].available=false;delete p.variants[0].availabilityKnown;return p;}});
  assert.deepEqual((await ask(unavailable)).products,[]);
});
test('cached material/budget matches cannot survive changed exact options or unavailable stock',async()=>{
  for(const mutate of [p=>p.variants[0].price=150,p=>p.variants[0].available=false,p=>{p.variants[0].title='14k Gold Filled';p.variants[0].options=[{name:'Metal Choice',value:'14k Gold Filled'}];}]){
    const rows=otterRows(),f=fixture({rows,fresh:handle=>{const p=clone(rows.find(p=>p.handle===handle));mutate(p);return p;}});
    assert.deepEqual((await ask(f,'Find otter stud earrings in silver under $90')).products,[]);
  }
});
test('fresh identity substitution and wrong owned product path cannot release cached recommendations',async()=>{
  for(const mutate of [p=>p.id='gid://shopify/Product/999',p=>p.handle='another-piece',p=>p.url='https://britesjewelry.com/products/another-piece',p=>p.url='https://competitor.invalid/products/public-piece-118']){
    const rows=otterRows(),f=fixture({rows,fresh:handle=>{const p=clone(rows.find(p=>p.handle===handle));mutate(p);return p;}});
    assert.deepEqual((await ask(f)).products,[]);assert.equal(f.calls.saved.length,0);
  }
});
test('failed exact read withholds cached facts, while independent successful reads stay partial',async()=>{
  const rows=otterRows(),failed=fixture({rows,fresh:()=>{throw Error('PRIVATE_EXACT_FAILURE');}});await assert.rejects(ask(failed),/^Error: The matching published pieces could not be checked\.$/);
  rows[112].title='Otter Stud Earrings Two';rows[112].tags=['otter'];
  const f=fixture({rows,fresh:handle=>{if(handle===rows[117].handle)throw Error('PRIVATE_EXACT_FAILURE');return clone(rows.find(p=>p.handle===handle));}}),answer=await ask(f);
  assert.deepEqual(answer.products.map(p=>p.id),[rows[112].id]);assert.equal(answer.discovery.partial,true);assert.doesNotMatch(JSON.stringify(answer),/PRIVATE_EXACT_FAILURE/);
});
test('exact-read shortlist expands after unavailable or newly held variants but stays bounded18',async()=>{
  const rows=Array.from({length:30},(_,i)=>product(i+1,'stud-earrings'));rows.forEach((p,i)=>{p.title='Otter Stud Earrings '+i;p.tags=['otter'];p.variants[0].price=30+i;});
  const f=fixture({rows,fresh:handle=>{const p=clone(rows.find(p=>p.handle===handle));if(Number(p.id.split('/').at(-1))<=12)p.variants[0].available=false;return p;}}),answer=await ask(f);
  assert.equal(answer.products.length,6);assert.equal(f.calls.handles.length,18);assert.deepEqual(answer.products.map(p=>p.id),rows.slice(12,18).map(p=>p.id));
  const held=fixture({rows});held.service.productIssues=async ids=>{held.calls.holds.push([...ids]);return held.calls.holds.length===1?[]:ids.filter(id=>Number(id.split('/').at(-1))<=6).map(productId=>({productId,issues:[{kind:'identity',status:'open',blocks:['recommendation','cart']}]}));};
  const result=await ask(held);assert.equal(result.products.length,6);assert.equal(held.calls.handles.length,12);assert.deepEqual(result.products.map(p=>p.id),rows.slice(6,12).map(p=>p.id));
});
test('discovery source failures remain qualified; no source failure is exposed as absent inventory',async()=>{
  const p=product(1),f=fixture({rows:[],searchRows:[p],seedFailure:true}),read=await f.shopify.discover('leaf');assert.equal(read.products.length,1);assert.equal(read.discovery.partial,true);assert.equal(read.discovery.catalogueComplete,false);assert.doesNotMatch(JSON.stringify(read),/PRIVATE_(?:SEED|SEARCH)_FAILURE/);
  await assert.rejects(fixture({rows:[],seedFailure:true,searchFailure:true}).shopify.discover('leaf'),/^Error: The published catalogue discovery could not be checked\.$/);
});
test('discovery deduplicates fresh identities, refuses conflicting rebinding and caps every source',async()=>{
  const p=product(1),newer={...clone(p),checkedAt:NOW+1,title:'Updated published title'};
  const f=fixture({rows:[p],searchRows:[newer]});assert.equal((await f.shopify.discover('leaf')).products[0].title,newer.title);
  await assert.rejects(fixture({rows:[p],searchRows:[{...p,handle:'rebound-handle'}]}).shopify.discover('leaf'),/identities conflict/);
  const capped=await fixture({rows:Array.from({length:170},(_,i)=>product(i+1)),searchRows:Array.from({length:70},(_,i)=>product(i+1000))}).shopify.discover('leaf');
  assert.equal(capped.products.length,220);assert.equal(capped.discovery.sampledListings,160);assert.equal(capped.discovery.predictiveListings,60);
});
test('reset uses the bounded sample without replaying an old query or inventing a full-store count',async()=>{
  const f=fixture(),read=await f.shopify.discover('PRIVATE_OLD_QUERY',{reset:true});assert.equal(read.products.length,120);assert.deepEqual(f.calls.search,[]);assert.equal(read.discovery.catalogueComplete,false);
});
test('public Shopify discovery cannot use configured Admin credentials and preserves unknown availability',async()=>{
  const calls=[],raw={id:1,handle:'public-piece-1',title:'Leaf Stud Earrings',product_type:'Earrings',options:[{name:'Metal Choice'}],images:[{src:'https://cdn.shopify.com/1.jpg'}],variants:[{id:10001,title:'Sterling Silver',price:'51.00',option1:'Sterling Silver'}]};
  const fetch=async(url,options)=>{calls.push({url,options});return {ok:true,status:200,json:async()=>url.endsWith('/cart.js')?{currency:'CAD'}:url.includes('/search/suggest.json')?{resources:{results:{products:[]}}}:{products:[raw]}};};
  const shop=core.createShopify({env:{SHOPIFY_STORE:'synthetic.myshopify.com',SHOPIFY_CLIENT_ID:'TEST',SHOPIFY_CLIENT_SECRET:'PRIVATE_ADMIN_SECRET'},fetch,now:()=>NOW}),read=await shop.discover('stud earrings');
  assert.equal(read.products[0].variants[0].available,false);assert.equal(read.products[0].variants[0].availabilityKnown,false);assert.equal(core.productProjection(read.products[0]).variants[0].availabilityKnown,false);
  assert.equal(read.products[0].currency,'CAD');assert.ok(calls.every(c=>c.url.startsWith('https://britesjewelry.com/')&&!c.url.includes('/admin/')&&!c.options.method));assert.doesNotMatch(JSON.stringify(calls),/PRIVATE_ADMIN_SECRET/);
});

const apiSource=fs.readFileSync(require.resolve('../../netlify/functions/britesGrowthApi.js'),'utf8').replace(/^import\s+\w+\s+from\s+['"][^'"]+['"];\s*$/gm,'').replace('export default async (req,context) => {','return async (req,context) => {').replace(/export const config = [\s\S]*$/,'');
function endpoint(f){
  const injected={...core,makeDb:()=>({}),createShopify:()=>f.shopify,createGrowthService:()=>({...f.service,rateLimit:async()=>true})};
  return new Function('core','demandStore','controllerStore','receiptSandboxCheck','etsyCacheReadOnly','historicalLookup','conciergeDiagnostics','keywordRevision','Netlify',apiSource)(injected,{},{},{},{},{},{},{},{env:{get:()=>undefined}});
}
async function catalogue(f){const response=await endpoint(f)(new Request('https://brites-growth-sandbox.netlify.app/api/growth/catalogue?q=otter+stud+earrings'),{params:{op:'catalogue'},ip:'synthetic37'});return {status:response.status,body:await response.json()};}
test('actual catalogue search route exposes all checked public candidates and every hold with no mirror sync',async()=>{
  const rows=otterRows(),f=fixture({rows,issues:[{productId:rows[117].id,issues:[{kind:'identity',status:'open',blocks:['cart','recommendation'],detail:'PRIVATE_OWNER_ISSUE'}]}]}),read=await catalogue(f);
  assert.equal(read.status,200);assert.equal(read.body.products.length,120);assert.equal(read.body.products[117].cartHold,true);assert.equal(read.body.products[117].recommendationHold,true);
  assert.deepEqual(f.calls.holds.map(ids=>ids.length),[100,20]);assert.equal(f.calls.saved.length,0);assert.equal(read.body.discovery.catalogueComplete,false);assert.doesNotMatch(JSON.stringify(read.body),/PRIVATE_|sku/);
});
test('failed hold read after100 cannot expose a partial catalogue or private exception',async()=>{
  const f=fixture({issueFailure:(_,n)=>n===2}),read=await catalogue(f);assert.equal(read.status,400);assert.equal(read.body.products,undefined);assert.doesNotMatch(JSON.stringify(read.body),/PRIVATE_|HOLD_FAILURE/);
  await assert.rejects(ask(fixture({rows:otterRows(),issueFailure:(_,n)=>n===2})),/PRIVATE_HOLD_FAILURE/);
});
test('native tool protocol accepts each real category and preserves exact price/charm/confirmation distinctions',()=>{
  const config=voice.sessionConfig(),tool=config.tools.find(t=>t.name==='control_storefront');
  for(const filter of seed.CATEGORIES){assert.ok(tool.parameters.properties.filter.enum.includes(filter));assert.deepEqual(voice.validateToolArguments({type:'filter',filter},'control_storefront'),{type:'filter',filter});}
  assert.equal(voice.validateToolArguments({type:'filter',filter:'PRIVATE_OWNER_CATEGORY'},'control_storefront'),null);
  assert.match(config.instructions,/selectedVariant.*selected exact variant/);assert.match(config.instructions,/minPrice is a starting price/);assert.match(config.instructions,/gold-filled, gold-plated and solid gold are distinct/);assert.match(config.instructions,/standalone published charm/);assert.match(config.instructions,/separate shopper confirmation/);
});
