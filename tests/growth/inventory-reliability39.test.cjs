'use strict';
// Exercise the actual page and exact public reader under incomplete, changing
// and unavailable inventory responses. Every request is a read-only fixture.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),crypto=require('node:crypto');
const {JSDOM,VirtualConsole}=require('jsdom');
const Inventory=require('../../netlify/functions/_britesStorefrontInventory'),core=require('../../netlify/functions/_britesGrowth');
const html=fs.readFileSync(require.resolve('../../concierge-sandbox.html'),'utf8'),script=fs.readFileSync(require.resolve('../../concierge-sandbox.js'),'utf8');
const categories=['regular-necklaces','beady-necklaces','stud-earrings','hoop-earrings','charm-only'],clone=value=>structuredClone(value),settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
function product(index,checkedAt=Date.now()){
  const category=categories[index%5],noun=['Necklace','Beady Necklace','Stud Earrings','Hoop Earrings','Necklace Charm'][index%5],type=index%5===4?'Charm':index%5<2?'Necklace':'Earrings',handle='reliable-piece-'+index;
  const charmOption=index%5===4?[{name:'Charm Type',values:['Necklace CHARM']}]:[];
  return {id:'gid://shopify/Product/'+(3900+index),handle,title:'Reliable '+index+' '+noun,type,url:'https://britesjewelry.com/products/'+handle,currency:'USD',description:'Exact public description for piece '+index+'.',image:'https://cdn.shopify.com/'+handle+'.jpg',images:[{url:'https://cdn.shopify.com/'+handle+'.jpg',altText:'Exact piece'}],storeCategories:[category],checkedAt,detailState:'checked',variantsComplete:true,options:[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled']},...charmOption],variants:['Sterling Silver','14k Gold Filled'].map((metal,i)=>({id:'gid://shopify/ProductVariant/'+(39000+index*10+i),numericId:String(39000+index*10+i),title:metal,price:50+i*20,available:true,options:[{name:'Metal Choice',value:metal},...charmOption.map(option=>({name:option.name,value:option.values[0]}))]}))};
}
function fingerprint(rows){return crypto.createHash('sha256').update(JSON.stringify(rows.map(p=>[p.id,p.handle]).sort((a,b)=>a[1].localeCompare(b[1])))).digest('hex');}
function catalogue(rows,checkedAt,partial=false){return {live:true,checkedAt,products:rows.map(p=>({...p,detailState:'unconfirmed',variants:p.variants.map(v=>({...v,available:false,availabilityKnown:false}))})),pageInfo:{hasNextPage:false,endCursor:null},seed:{schema:1,target:120,minimum:100,loaded:rows.length,complete:rows.length>=100&&categories.every(c=>rows.some(p=>p.storeCategories.includes(c))),partial,sourcePages:1,categoryCounts:Object.fromEntries(categories.map(c=>[c,rows.filter(p=>p.storeCategories.includes(c)).length])),unfilledCategories:categories.filter(c=>!rows.some(p=>p.storeCategories.includes(c)))}};}
async function page(t,{failCatalogue=false,failOffsets=[],sourcePartial=false,hangOffset=null,wait=true}={}){
  const errors=[],virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(html,{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole}),w=dom.window,d=w.document,clock={value:Date.now()};
  const model={rows:Array.from({length:120},(_,i)=>product(i,clock.value)),failOffsets:new Set(failOffsets),sourcePartial,failCatalogue,hangOffset,mixedOffset:null,duplicateOffset:null},requests=[],timers=new Map();let sequence=0;
  w.Date.now=()=>clock.value;w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};
  const nativeSet=w.setTimeout.bind(w),nativeClear=w.clearTimeout.bind(w);w.setTimeout=(fn,delay,...args)=>{if(delay===32000||String(fn).includes('preloadInventory')){const id=-(++sequence);timers.set(id,{fn,delay});return id;}return nativeSet(fn,delay,...args);};w.clearTimeout=id=>{if(id<0)timers.delete(id);else nativeClear(id);};
  w.fetch=async(raw,init={})=>{
    const url=new URL(raw,w.location.href);requests.push({url,init});let body;
    if(url.pathname==='/api/growth/inventory'){
      const offset=Number(url.searchParams.get('offset'));if(model.failOffsets.has(offset))throw Error('Synthetic public inventory failure');
      if(model.hangOffset===offset)await new Promise((resolve,reject)=>init.signal.addEventListener('abort',()=>reject(Error('Synthetic aborted read')),{once:true}));
      let rows=model.rows.slice(offset,offset+24);if(model.duplicateOffset===offset)rows=[model.rows[0],...rows.slice(1)];
      body={live:true,checkedAt:clock.value,products:rows,inventory:{schema:1,total:model.rows.length,offset,limit:24,loaded:rows.length,partial:model.sourcePartial,sourcePartial:model.sourcePartial,fingerprint:model.mixedOffset===offset?'f'.repeat(64):fingerprint(model.rows)},pageInfo:{hasNextPage:offset+24<model.rows.length,nextOffset:offset+24<model.rows.length?offset+24:null}};
    }else if(url.pathname==='/api/growth/catalogue'){if(model.failCatalogue)throw Error('Synthetic catalogue failure');body=catalogue(model.rows,clock.value,model.sourcePartial);}
    else if(url.pathname==='/api/growth/product')body={live:true,checkedAt:clock.value,product:model.rows.find(p=>p.handle===url.searchParams.get('handle'))};
    else if(url.pathname==='/api/growth/storefront-services')body={schema:1,guidance:{},conflicts:[],offers:{items:[]}};
    else throw Error('Unexpected fixture request '+url.pathname);
    return {ok:true,json:async()=>clone(body)};
  };
  t.after(()=>w.close());w.eval(script);const store=w.BritesSandboxStorefront,bootstrap=store.preloadInventory();await settle();if(wait){await bootstrap;await settle();}
  return {w,d,clock,model,requests,timers,store,errors,async retry(){await store.preloadInventory({retry:true});await settle();},async runTimer(delay){const entry=[...timers].find(([,timer])=>delay==null?String(timer.fn).includes('preloadInventory'):timer.delay===delay);assert(entry,'Expected inventory timer');timers.delete(entry[0]);entry[1].fn();await store.preloadInventory();await settle();}};
}
test('successful exact preload populates the actual grid when both starting catalogue reads fail',async t=>{
  const f=await page(t,{failCatalogue:true});assert.equal(f.store.inventoryStatus().ready,true);assert.equal(f.store.snapshot().loadedPieces.length,120);assert.equal(f.d.querySelectorAll('.piece-card').length,24);assert.doesNotMatch(f.d.querySelector('#result-summary').textContent,/unavailable/);assert.equal((await f.store.execute({type:'open',handle:f.model.rows[119].handle})).ok,true);assert.equal(f.requests.filter(call=>call.url.pathname==='/api/growth/product').length,0);assert.equal(f.errors.length,0);
});
test('a partial source cannot announce complete knowledge even if every returned exact row is checked',async t=>{
  const f=await page(t,{sourcePartial:true}),status=f.store.inventoryStatus();assert.equal(status.loaded,120);assert.equal(status.ready,false);assert.equal(status.partial,true);assert.equal(status.catalogueComplete,false);assert.doesNotMatch(f.d.querySelector('#inventory-status').textContent,/products ready/);f.model.sourcePartial=false;await f.runTimer();assert.equal(f.store.inventoryStatus().ready,true);
});
test('a failed final page is retried promptly while checked neighbors remain locally readable',async t=>{
  const f=await page(t,{failOffsets:[96]});assert.equal(f.store.inventoryStatus().loaded,96);assert.equal(f.store.inventoryStatus().ready,false);assert.equal((await f.store.readProduct(f.model.rows[0].handle)).cached,true);assert([...f.timers.values()].some(timer=>timer.delay===2000&&String(timer.fn).includes('preloadInventory')));f.model.failOffsets.clear();await f.runTimer();assert.equal(f.store.inventoryStatus().ready,true);assert.equal(f.store.getInventory().length,120);
});
test('repeated unavailable pages use bounded backoff instead of waiting for five-minute fact expiry',async t=>{
  const f=await page(t,{failOffsets:[96]});const delays=[];for(let i=0;i<6;i++){const timer=[...f.timers.values()].find(timer=>String(timer.fn).includes('preloadInventory'));delays.push(timer.delay);await f.runTimer();}assert.deepEqual(delays,[2000,4000,8000,16000,30000,30000]);assert.equal(f.store.inventoryStatus().partial,true);
});
test('a stuck preload page has an independent abort deadline and leaves recoverable partial knowledge',async t=>{
  const f=await page(t,{hangOffset:96,wait:false});assert.equal(f.store.inventoryStatus().loading,true);await f.runTimer(32000);assert.equal(f.store.inventoryStatus().loading,false);assert.equal(f.store.inventoryStatus().loaded,96);assert.equal(f.store.inventoryStatus().partial,true);const stalled=f.requests.find(call=>call.url.pathname==='/api/growth/inventory'&&call.url.searchParams.get('offset')==='96');assert.equal(stalled.init.signal.aborted,true);f.model.hangOffset=null;await f.runTimer();assert.equal(f.store.inventoryStatus().ready,true);
});
test('replacing the manifest removes retired members from knowledge and collection identity counts',async t=>{
  const f=await page(t),removed=f.model.rows.slice(0,10);f.clock.value+=1000;f.model.rows=f.model.rows.slice(10).concat(Array.from({length:10},(_,i)=>product(140+i,f.clock.value))).map(p=>({...p,checkedAt:f.clock.value}));await f.retry();assert.equal(f.store.inventoryStatus().total,120);assert.equal(f.store.inventoryStatus().loaded,120);assert.equal(f.store.inventoryStatus().ready,true);assert.equal(f.store.getInventory().length,120);assert.equal(f.store.snapshot().inventoryPieces.length,120);assert.equal(f.store.snapshot().loadedPieces.length,120);for(const p of removed){assert.equal(await f.store.readProduct(p.handle),null);assert.equal(f.store.snapshot().inventoryPieces.some(row=>row.handle===p.handle),false);}assert.equal((await f.store.execute({type:'search',query:'Reliable 149'})).products[0].handle,'reliable-piece-149');
});
for(const [label,key] of [['different manifest fingerprints','mixedOffset'],['a repeated product from another page','duplicateOffset']])test('preload rejects '+label+' without claiming complete inventory',async t=>{
  const f=await page(t);f.model[key]=96;await f.retry();assert.equal(f.store.inventoryStatus().ready,false);assert.equal(f.store.inventoryStatus().partial,true);assert([...f.timers.values()].some(timer=>timer.delay===2000&&String(timer.fn).includes('preloadInventory')));f.model[key]=null;await f.runTimer();assert.equal(f.store.inventoryStatus().ready,true);
});
test('expired remembered details cannot replace newly published unknown availability',async t=>{
  const f=await page(t);f.clock.value+=600000;f.model.failOffsets=new Set([0,24,48,72,96]);const result=await f.store.execute({type:'search',query:f.model.rows[0].title});assert.equal(result.ok,true);assert.equal(await f.store.readProduct(f.model.rows[0].handle),null);assert.match(f.d.querySelector('[data-product-handle="'+f.model.rows[0].handle+'"]').textContent,/Availability checked/);assert.equal(result.products[0].variants[0].available,false);assert.equal(result.products[0].variants[0].availabilityKnown,false);
});
test('newer exact prices still receive newly reviewed holds from an older timestamped fact page',async t=>{
  const f=await page(t),p=f.model.rows[0];f.store.presentProducts([{...p,checkedAt:f.clock.value+1000,variants:p.variants.map(v=>({...v,price:v.price+10}))}]);f.model.rows[0]={...p,cartHold:true,recommendationHold:true};await f.retry();const cached=await f.store.readProduct(p.handle);assert.equal(cached.product.variants[0].price,60);assert.equal(cached.product.cartHold,true);assert.equal(cached.product.recommendationHold,true);await f.store.execute({type:'open',handle:p.handle});await f.store.execute({type:'select-option',handle:p.handle,variantId:p.variants[0].id});assert.equal((await f.store.execute({type:'review-add',handle:p.handle})).ok,false);
});
test('an unchanged background refresh preserves the real focused option controls and open image dialog',async t=>{
  const f=await page(t),p=f.model.rows[0];await f.store.execute({type:'open',handle:p.handle});await f.store.execute({type:'select-option',handle:p.handle,variantId:p.variants[1].id});await f.store.execute({type:'product-quantity',handle:p.handle,quantity:2});await f.store.execute({type:'options',handle:p.handle,optionName:'Metal Choice'});await f.store.execute({type:'review-add',handle:p.handle});const input=f.d.querySelector('.product-quantity input'),menu=f.d.querySelector('.option-menu'),review=f.d.querySelector('.product-review');input.focus();
  f.clock.value+=1000;f.model.rows=f.model.rows.map(row=>({...row,checkedAt:f.clock.value}));await f.retry();assert.equal(f.d.querySelector('.product-quantity input'),input);assert.equal(f.d.activeElement,input);assert.equal(f.d.querySelector('.option-menu'),menu);assert.equal(menu.hidden,false);assert.equal(f.d.querySelector('.product-review'),review);assert.equal(f.store.snapshot().productControls.itemTotalPrice,140);assert.equal((await f.store.readProduct(p.handle)).checkedAt,f.clock.value);
  await f.store.execute({type:'zoom',handle:p.handle});const dialog=f.d.querySelector('#storefront-image-dialog'),focus=f.d.activeElement;assert(dialog.hasAttribute('open'));f.clock.value+=1000;f.model.rows=f.model.rows.map(row=>({...row,checkedAt:f.clock.value}));await f.retry();assert.equal(f.d.querySelector('#storefront-image-dialog'),dialog);assert.equal(dialog.hasAttribute('open'),true);assert.equal(f.d.activeElement,focus);assert.equal(f.store.snapshot().productControls.variantId,p.variants[1].id);assert.equal(f.store.snapshot().productControls.quantity,2);
});
function reader({count=120,clock={value:Date.now()},seedCost=0,exactCost=0,unknownIndex=null}={}){
  const rows=Array.from({length:count},(_,i)=>product(i,clock.value)),calls={seed:0,exact:[],holds:[],order:[]};
  const shopify={seed:async()=>{calls.seed++;clock.value+=seedCost;return {products:rows.map(p=>({...p,checkedAt:clock.value})),seed:{partial:false}};},publicByHandle:async(handle,timeout)=>{calls.order.push('exact');calls.exact.push({handle,timeout});clock.value+=exactCost;const p=clone(rows.find(p=>p.handle===handle));p.checkedAt=clock.value;if(rows.indexOf(rows.find(p=>p.handle===handle))===unknownIndex)p.variants[0].availabilityKnown=false;return p;}};
  const service={namespace:'Brites_Growth_Sandbox',productIssues:async ids=>{calls.order.push('holds');calls.holds.push(ids);return ids.includes(rows.at(-1).id)?[{productId:rows.at(-1).id,issues:[{kind:'identity',status:'open',blocks:['cart','recommendation']}]}]:[];}};
  return {rows,clock,calls,instance:Inventory.createInventory({shopify,service,core,now:()=>clock.value,cache:{}})};
}
test('the cold seed consumes the same deadline as exact products and all scoped holds',async()=>{
  const f=reader({seedCost:20000,exactCost:9000}),response=await f.instance.read({offset:0,limit:24});assert.equal(f.calls.exact.length,1);assert(f.calls.exact[0].timeout<=8000);assert.equal(f.calls.order[0],'holds');assert.deepEqual(f.calls.holds.map(ids=>ids.length),[100,20]);assert.equal(response.products.length,24);assert.equal(response.inventory.ready,false);assert.equal(response.inventory.partial,true);assert(response.products.slice(1).every(p=>p.detailState==='unconfirmed'));
});
test('a seed that exhausts the overall deadline never starts another exact or hold budget',async()=>{
  const f=reader({seedCost:Inventory.DEADLINE_MS+1});await assert.rejects(f.instance.read({offset:0,limit:24}),/timed out/);assert.equal(f.calls.exact.length,0);assert.equal(f.calls.holds.length,0);
});
test('reviewed holds begin before exact detail reads, including the final twenty inventory identities',async()=>{
  const f=reader(),response=await f.instance.read({offset:96,limit:24});assert.equal(f.calls.order[0],'holds');assert.deepEqual(f.calls.holds.map(ids=>ids.length),[100,20]);assert.equal(response.products.at(-1).cartHold,true);assert.equal(response.products.at(-1).recommendationHold,true);assert.equal(response.inventory.sourcePartial,false);
});
test('unknown availability elsewhere in the inventory prevents a neighbor page from reporting full readiness',async()=>{
  const f=reader({count:3,unknownIndex:0});await f.instance.read({offset:0,limit:1});const response=await f.instance.read({offset:1,limit:2});assert.equal(response.inventory.detailsLoaded,2);assert.equal(response.inventory.ready,false);assert.equal(response.products.length,2);
});
test('background refresh replaces expiring facts before cache expiry while ordinary fact lookup stays instant',async()=>{
  const f=reader({count:1});await f.instance.read({offset:0,limit:1});const original=f.clock.value;f.clock.value+=Inventory.CACHE_MS-15000;const before=await f.instance.lookup(f.rows[0].handle);assert.equal(before.checkedAt,original);assert.equal(f.calls.exact.length,1);const fresh=await f.instance.read({offset:0,limit:1});assert.equal(f.calls.seed,2);assert.equal(f.calls.exact.length,2);assert.equal(fresh.products[0].checkedAt,f.clock.value);assert.equal(fresh.products[0].cached,false);
});
test('cached descriptions with unknown stock are rechecked on recovery instead of blocking readiness for five minutes',async()=>{
  const p=product(0);let exact=0;const service={namespace:'Brites_Growth_Sandbox',productIssues:async()=>[]},shopify={seed:async()=>({products:[p],seed:{partial:false}}),publicByHandle:async()=>{exact++;return {...p,variants:p.variants.map(v=>exact===1?{...v,available:false,availabilityKnown:false}:v)};}};
  const instance=Inventory.createInventory({shopify,service,core,cache:{}}),first=await instance.read({offset:0,limit:1});assert.equal(first.inventory.ready,false);assert.equal((await instance.lookup(p.handle)).product.variants[0].availabilityKnown,false);const recovered=await instance.read({offset:0,limit:1});assert.equal(recovered.products[0].variants[0].available,true);assert.equal(recovered.products[0].variants[0].availabilityKnown,undefined);assert.equal(recovered.inventory.ready,true);assert.equal(exact,2);
});
test('a partial seed is revisited within thirty seconds while completed facts stay cached',async()=>{
  const clock={value:Date.now()},p=product(0,clock.value);let seeds=0,exact=0;const service={namespace:'Brites_Growth_Sandbox',productIssues:async()=>[]},shopify={seed:async()=>({products:[{...p,checkedAt:clock.value}],seed:{partial:++seeds===1}}),publicByHandle:async()=>{exact++;return {...p,checkedAt:clock.value};}};
  const instance=Inventory.createInventory({shopify,service,core,now:()=>clock.value,cache:{}});assert.equal((await instance.read({offset:0,limit:1})).inventory.sourcePartial,true);assert.equal((await instance.read({offset:0,limit:1})).inventory.ready,false);assert.equal(seeds,1);clock.value+=30000;const recovered=await instance.read({offset:0,limit:1});assert.equal(seeds,2);assert.equal(exact,1);assert.equal(recovered.inventory.sourcePartial,false);assert.equal(recovered.inventory.ready,true);
});
