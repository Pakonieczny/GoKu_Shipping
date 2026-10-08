'use strict';
// Exact public readers and isolated cache storage, never provider or live writes.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const inventory=require('../../netlify/functions/_britesStorefrontInventory');
const core=require('../../netlify/functions/_britesGrowth');
const seed=require('../../netlify/functions/_britesStorefrontSeed');
const NOW=Date.parse('2026-10-08T05:20:00Z'),clone=value=>structuredClone(value);
function product(index,checkedAt=NOW){
  const category=seed.CATEGORIES[(index-1)%5],[title,type,axis,value]={
    'regular-necklaces':['Leaf Chain Necklace','Necklace','Necklace Length','18 Inches'],
    'beady-necklaces':['Leaf Beady Necklace','Necklace','Necklace Length','16 Inches'],
    'stud-earrings':['Leaf Stud Earrings','Earrings','Charm Type','Stud Earring PAIR'],
    'hoop-earrings':['Leaf Huggie Hoop Earrings','Earrings','Hoop Size','8.5mm'],
    'charm-only':['Leaf Necklace Charm','Charm','Charm Type','Necklace CHARM']
  }[category];
  const options=[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled']},{name:axis,values:[value]}],handle='public-inventory-'+index;
  return {id:'gid://shopify/Product/'+index,handle,title:title+' '+index,type,url:'https://britesjewelry.com/products/'+handle,currency:'USD',description:'Published detailed facts for this exact piece '+index+'. Chain and engraving choices are shown on its product page.',tags:['PRIVATE_CATALOGUE_SKU'],options,variantsComplete:true,checkedAt,image:'https://cdn.shopify.com/'+index+'-1.jpg',images:Array.from({length:5},(_,i)=>({url:'https://cdn.shopify.com/'+index+'-'+(i+1)+'.jpg',altText:'Exact product view '+(i+1)})),variants:options[0].values.map((metal,i)=>({id:'gid://shopify/ProductVariant/'+(index*100+i+1),numericId:String(index*100+i+1),title:metal+' / '+value,price:45+index+i*20,available:true,options:[{name:'Metal Choice',value:metal},{name:axis,value}],sku:'PRIVATE_SKU',inventory_quantity:913})),metafields:{private:'PRIVATE_ADMIN'},notes:'PRIVATE_GIFT',ranking:25};
}
function fixture({rows=Array.from({length:120},(_,i)=>product(i+1)),records=new Map(),cache={},fresh,issues=[],seedPartial=false,issueFailure,clock={value:NOW}}={}){
  const calls={seed:0,exact:[],holds:[],saves:[],storageReads:[],active:0,maxActive:0},now=()=>clock.value;
  const shopify={seed:async()=>{calls.seed++;return {products:clone(rows),seed:{partial:seedPartial}};},publicByHandle:async(handle,timeout)=>{calls.exact.push({handle,timeout});calls.active++;calls.maxActive=Math.max(calls.maxActive,calls.active);await Promise.resolve();try{return fresh?await fresh(handle):clone(rows.find(p=>p.handle===handle)||null);}finally{calls.active--;}}};
  const service={namespace:'Brites_Growth_Sandbox',productIssues:async ids=>{calls.holds.push([...ids]);if(issueFailure)throw Error('PRIVATE_HOLD_FAILURE');return clone(issues.filter(issue=>ids.includes(issue.productId)));},col:name=>{assert.equal(name,'StorefrontInventory');return {doc:key=>({get:async()=>{calls.storageReads.push(key);return {exists:records.has(key),data:()=>clone(records.get(key))};},set:async value=>{calls.saves.push({key,value:clone(value)});records.set(key,clone(value));}})};}};
  return {rows,records,cache,calls,clock,now,shopify,service,reader:inventory.createInventory({shopify,service,core,now,cache})};
}
test('all120 exact detailed products are reachable upfront in bounded public batches',async()=>{
  const f=fixture(),seen=[];
  for(let offset=0;offset<120;offset+=24){const read=await f.reader.read({offset,limit:24});seen.push(...read.products);assert.equal(read.live,true);assert.equal(read.inventory.total,120);assert.equal(read.inventory.catalogueComplete,false);assert.equal(read.inventory.partial,false);assert.equal(read.pageInfo.nextOffset,offset<96?offset+24:null);assert.equal(read.inventory.ready,offset===96);}
  assert.equal(new Set(seen.map(p=>p.id)).size,120);assert.equal(f.calls.seed,1);assert.equal(f.calls.exact.length,120);assert.equal(f.calls.maxActive,6);assert.ok(f.calls.exact.every(call=>call.timeout<=7000));
  assert.deepEqual(Object.fromEntries(seed.CATEGORIES.map(category=>[category,seen.filter(p=>p.storeCategories.includes(category)).length])),Object.fromEntries(seed.CATEGORIES.map(category=>[category,24])));
  for(const p of seen){assert.equal(p.detailState,'checked');assert.equal(p.description,f.rows.find(row=>row.id===p.id).description);assert.equal(p.images.length,5);assert.equal(p.options.length,2);assert.equal(p.variants.length,2);assert.equal(p.variantsComplete,true);assert.equal(p.variants[1].price,p.variants[0].price+20);assert.equal(p.variants[1].options[0].value,'14k Gold Filled');assert.equal(p.checkedAt,NOW);}
  assert.deepEqual(f.calls.holds.map(ids=>ids.length),[100,20,100,20,100,20,100,20,100,20]);
});
test('public release and durable cache contain no SKU, private stocks, notes, rank or raw issues',async()=>{
  const f=fixture({issues:[{productId:'gid://shopify/Product/118',issues:[{kind:'identity',status:'open',blocks:['cart','recommendation'],detail:'PRIVATE_REVIEW'}]}]}),read=await f.reader.read({offset:96,limit:24});
  assert.equal(read.products.find(p=>p.id==='gid://shopify/Product/118').cartHold,true);
  assert.equal(read.products.find(p=>p.id==='gid://shopify/Product/118').recommendationHold,true);
  assert.doesNotMatch(JSON.stringify(read),/PRIVATE_|inventory_quantity|metafields|ranking|sku|notes/i);
  assert.doesNotMatch(JSON.stringify(f.calls.saves),/PRIVATE_|inventory_quantity|metafields|ranking|sku|notes|issues/i);
  assert.equal(f.calls.saves.length,25);assert.equal(f.calls.saves[0].key,'v1');assert.ok(JSON.stringify(f.calls.saves[0].value).length<850000);
});
test('cold function hydrates exact public durable facts without any new seed or product network read',async()=>{
  const warm=fixture();await warm.reader.read({offset:0,limit:24});
  const cold=fixture({records:warm.records,fresh:()=>{throw Error('PRIVATE_NETWORK_NOT_NEEDED');}}),result=await cold.reader.lookup(warm.rows[7].handle);
  assert.equal(cold.calls.seed,0);assert.equal(cold.calls.exact.length,0);assert.equal(result.fromCache,true);assert.equal(result.product.id,warm.rows[7].id);assert.equal(result.product.description,warm.rows[7].description);assert.equal(result.checkedAt,NOW);assert.equal(result.expiresAt,NOW+inventory.CACHE_MS);assert.deepEqual(cold.calls.holds,[[warm.rows[7].id]]);
  result.product.variants[0].price=999;assert.equal((await cold.reader.lookup(warm.rows[7].handle)).product.variants[0].price,warm.rows[7].variants[0].price);
});
test('repeated warm page is immediate cached details but rereads ALL reviewed hold identities',async()=>{
  const issues=[],f=fixture({issues});await f.reader.read({offset:96,limit:24});issues.push({productId:f.rows[117].id,issues:[{kind:'identity',status:'open',blocks:['cart','recommendation']} ]});
  const repeat=await f.reader.read({offset:96,limit:24});assert.equal(f.calls.exact.length,24);assert.equal(f.calls.seed,1);assert.ok(repeat.products.every(p=>p.cached));assert.equal(repeat.products[21].cartHold,true);assert.deepEqual(f.calls.holds.map(ids=>ids.length),[100,20,100,20]);
});
test('fact lookup on a cold empty cache never starts a full seed or per-product read',async()=>{
  const f=fixture();assert.equal(await f.reader.lookup(f.rows[119].handle),null);assert.equal(f.calls.seed,0);assert.equal(f.calls.exact.length,0);assert.deepEqual(f.calls.storageReads,['v1']);
});
test('duplicate concurrent pages share pending seed and every exact public reader',async()=>{
  let release;const gate=new Promise(resolve=>release=resolve),f=fixture({fresh:async handle=>{await gate;return clone(f.rows.find(p=>p.handle===handle));}});
  const first=f.reader.read({offset:0,limit:6}),second=f.reader.read({offset:0,limit:6});setImmediate(release);const both=await Promise.all([first,second]);
  assert.equal(f.calls.seed,1);assert.equal(f.calls.exact.length,6);assert.deepEqual(both[0].products.map(p=>p.id),both[1].products.map(p=>p.id));
});
test('an unavailable exact published variant remains unavailable with its current exact price',async()=>{
  const f=fixture({fresh:handle=>{const p=product(Number(handle.split('-').at(-1)));p.variants[0].available=false;p.variants[0].price=81;return p;}}),read=await f.reader.read({offset:0,limit:1});
  assert.equal(read.products[0].detailState,'checked');assert.equal(read.products[0].variants[0].available,false);assert.equal(read.products[0].variants[0].price,81);assert.equal(read.products[0].variants[0].availabilityKnown,undefined);
});
test('public exact stock omission stays explicitly unknown, never a sold-out assertion',async()=>{
  const f=fixture({fresh:handle=>{const p=product(Number(handle.split('-').at(-1)));p.variants[0].available=false;p.variants[0].availabilityKnown=false;return p;}}),read=await f.reader.read({offset:0,limit:1});
  assert.equal(read.products[0].detailState,'checked');assert.equal(read.products[0].variants[0].availabilityKnown,false);assert.equal(read.inventory.partial,true);assert.equal(read.inventory.ready,false);
});
test('partial exact read failure keeps identity with unknown option availability while checked neighbors survive',async()=>{
  const f=fixture({fresh:handle=>{if(handle.endsWith('-2'))throw Error('PRIVATE_EXACT_FAILURE');return product(Number(handle.split('-').at(-1)));}}),read=await f.reader.read({offset:0,limit:3});
  assert.equal(read.products.length,3);assert.deepEqual(read.products.map(p=>p.detailState),['checked','unconfirmed','checked']);assert.equal(read.products[1].id,f.rows[1].id);assert.ok(read.products[1].variants.every(v=>v.available===false&&v.availabilityKnown===false));assert.equal(read.inventory.partial,true);assert.equal(read.inventory.detailsLoaded,2);assert.doesNotMatch(JSON.stringify(read),/PRIVATE_EXACT_FAILURE/);
});
for(const [label,change] of [
  ['another product ID',p=>p.id='gid://shopify/Product/999'],
  ['another handle',p=>p.handle='other-listing'],
  ['another owned path',p=>p.url='https://britesjewelry.com/products/other-listing'],
  ['an unowned path',p=>p.url='https://other.example/products/public-inventory-1'],
  ['a stale exact timestamp',p=>p.checkedAt=NOW-inventory.CACHE_MS],
  ['a future exact timestamp',p=>p.checkedAt=NOW+60001],
  ['incomplete variants',p=>p.variantsComplete=false],
  ['a variant option absent from exact product groups',p=>p.variants[0].options[0].value='Solid Gold'],
  ['a duplicate variant',p=>p.variants.push(clone(p.variants[0]))],
  ['an invalid currency',p=>p.currency='PRIVATE_MONEY']
])test('exact cache refuses substitution or incomplete evidence: '+label,async()=>{
  const f=fixture({fresh:handle=>{const p=product(Number(handle.split('-').at(-1)));change(p);return p;}}),read=await f.reader.read({offset:0,limit:1});
  assert.equal(read.products[0].id,f.rows[0].id);assert.equal(read.products[0].detailState,'unconfirmed');assert.equal(read.inventory.detailsLoaded,0);assert.equal(await f.reader.lookup(f.rows[0].handle),null);assert.equal(f.calls.saves.length,1);
});
test('expired exact product facts are not reused, and reloading refreshes public facts once',async()=>{
  const clock={value:NOW},f=fixture({clock,fresh:handle=>product(Number(handle.split('-').at(-1)),clock.value)});await f.reader.read({offset:0,limit:1});clock.value=NOW+inventory.CACHE_MS;
  f.rows.forEach(p=>p.checkedAt=clock.value);
  assert.equal(await f.reader.lookup(f.rows[0].handle),null);assert.equal(f.calls.exact.length,1);
  const fresh=await f.reader.read({offset:0,limit:1});assert.equal(fresh.products[0].checkedAt,clock.value);assert.equal(fresh.products[0].cached,false);assert.equal(f.calls.exact.length,2);assert.equal(f.calls.seed,2);
});
test('tampered durable identity fails closed without rebinding a current seed listing',async()=>{
  const warm=fixture();await warm.reader.read({offset:0,limit:1});const key=warm.calls.saves.find(row=>row.key.startsWith('p-')).key,record=warm.records.get(key);record.product.id='gid://shopify/Product/999';warm.records.set(key,record);
  const cold=fixture({records:warm.records});assert.equal(await cold.reader.lookup(warm.rows[0].handle),null);assert.equal(cold.calls.exact.length,0);const read=await cold.reader.read({offset:0,limit:1});assert.equal(read.products[0].id,warm.rows[0].id);assert.equal(cold.calls.exact.length,1);
});
test('an independent reviewed hold failure rejects the entire page rather than releasing unchecked pieces',async()=>{
  const f=fixture({issueFailure:true});await assert.rejects(f.reader.read({offset:96,limit:24}),/PRIVATE_HOLD_FAILURE/);assert.equal(f.calls.holds[0].length,100);
});
test('seed conflicts and duplicate identities cannot become an inventory manifest',async()=>{
  const rows=[product(1),{...product(2),id:product(1).id}],f=fixture({rows});await assert.rejects(f.reader.read({offset:0,limit:1}),/identities conflict/);assert.equal(f.calls.exact.length,0);assert.equal(f.calls.saves.length,0);
});
test('peek is read-only warm knowledge, explicitly unverified for cart/recommendation holds',async()=>{
  const f=fixture();assert.equal(f.reader.peek(),null);await f.reader.read({offset:0,limit:1});const peek=f.reader.peek();assert.equal(peek.holdsChecked,false);assert.equal(peek.products.length,1);assert.equal(peek.products[0].cartHold,true);assert.equal(peek.products[0].recommendationHold,true);assert.equal(peek.catalogueComplete,false);
});
test('invalid or out-of-bound page parameters are rejected before reads',async()=>{
  const f=fixture();for(const page of [{offset:-1},{offset:160},{offset:0.5},{offset:'01'},{offset:[]},{limit:0},{limit:25},{limit:'24&handle=other'},{limit:[]}])await assert.rejects(f.reader.read(page),/offset/);
  assert.equal(f.calls.seed,0);assert.deepEqual(inventory.parsePage({offset:'120',limit:'24'}),{offset:120,limit:24});
});
const apiSource=fs.readFileSync(require.resolve('../../netlify/functions/britesGrowthApi.js'),'utf8').replace(/^import\s+\w+\s+from\s+['"][^'"]+['"];\s*$/gm,'').replace('export default async (req,context) => {','return async (req,context) => {').replace(/export const config = [\s\S]*$/,'');
function endpoint(f,{allow=true,namespace='Brites_Growth_Sandbox'}={}){
  const environments=[],injected={...core,makeDb:()=>({}),createShopify:({env})=>{environments.push(clone(env));return f.shopify;},createGrowthService:()=>({...f.service,namespace,rateLimit:async()=>allow})};
  const cache={},implementation={...inventory,createInventory:options=>inventory.createInventory({...options,cache,now:f.now})};
  return {environments,handler:new Function('core','demandStore','controllerStore','receiptSandboxCheck','etsyCacheReadOnly','historicalLookup','conciergeDiagnostics','keywordRevision','storefrontInventory','Netlify',apiSource)(injected,{},{},{},{},{},{},{},implementation,{env:{get:key=>key.startsWith('SHOPIFY_')?'PRIVATE_ADMIN_SECRET':undefined}})};
}
const request=(endpoint,query='',method='GET')=>endpoint.handler(new Request('https://brites-growth-sandbox.netlify.app/api/growth/inventory'+query,{method}),{params:{op:'inventory'},ip:'synthetic38'});
test('actual public endpoint forces exact public reader, allholds and isolated namespace',async()=>{
  const f=fixture(),api=endpoint(f),response=await request(api,'?offset=96&limit=24'),body=await response.json();
  assert.equal(response.status,200);assert.equal(body.products.length,24);assert.equal(body.products[0].id,f.rows[96].id);assert.equal(body.inventory.total,120);assert.equal(body.inventory.catalogueComplete,false);assert.equal(api.environments.length,2);assert.deepEqual(api.environments[1],{});assert.deepEqual(f.calls.holds.map(ids=>ids.length),[100,20]);assert.equal(response.headers.get('Cache-Control'),'no-store');assert.doesNotMatch(JSON.stringify(body),/PRIVATE_|sku|inventory_quantity/i);
});
test('endpoint public rate guard, method and offset validation reject before exact reads',async()=>{
  const f=fixture();assert.equal((await request(endpoint(f,{allow:false}))).status,429);assert.equal((await request(endpoint(f),'','POST')).status,405);assert.equal((await request(endpoint(f),'?offset=-1')).status,400);assert.equal((await request(endpoint(f),'?limit=25')).status,400);assert.equal((await request(endpoint(f,{namespace:'Brites_Growth_Live'}))).status,403);assert.equal(f.calls.exact.length,0);assert.equal(f.calls.seed,0);
});
test('endpoint private storage failure cannot release exactfacts or operator error details',async()=>{
  const response=await request(endpoint(fixture({issueFailure:true})),'?offset=96&limit=24'),body=await response.json();assert.ok([400,503].includes(response.status));assert.equal(body.products,undefined);assert.doesNotMatch(JSON.stringify(body),/PRIVATE_|HOLD_FAILURE/);
});
