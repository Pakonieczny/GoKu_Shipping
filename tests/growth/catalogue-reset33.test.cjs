'use strict';
// Production host/bridge with synthetic GET-only catalogue responses. These
// regressions establish query scope and cancellation, not live voice or GPU.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Bridge=require('../../brites-storefront-bridge.js');
const html=fs.readFileSync(require.resolve('../../concierge-sandbox.html'),'utf8');
const source=fs.readFileSync(require.resolve('../../concierge-sandbox.js'),'utf8');
const clone=value=>JSON.parse(JSON.stringify(value)),tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
function deferred(){let resolve;return {promise:new Promise(done=>{resolve=done;}),resolve:value=>resolve(value)};}
function product(index,title,type){const handle='checked-piece-'+index;return {id:'gid://shopify/Product/'+index,handle,title,type,description:'Sterling silver jewelry.',url:'https://britesjewelry.com/products/'+handle,currency:'USD',image:'https://cdn.shopify.com/'+handle+'.jpg',variants:[{id:'gid://shopify/ProductVariant/'+(index+1000),numericId:String(index+1000),title:'Sterling Silver',available:true,price:40+index,options:[{name:'Metal',value:'Sterling Silver'}]}],options:[],variantsComplete:true};}
const necklace=product(1,'Bunny Pendant Necklace','Necklaces'),earrings=product(2,'Moon Stud Earrings','Earrings'),ring=product(3,'Moon Ring','Rings');
function response(products,pageInfo={hasNextPage:false,endCursor:null}){return {ok:true,json:async()=>clone({products,pageInfo,live:true})};}
function fixture(t,{query='',browse=[necklace,earrings],catalogue,session={}}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM(html,{url:'https://sandbox.example/concierge-sandbox.html'+query,runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,calls=[];
  Object.entries(session).forEach(([key,value])=>w.sessionStorage.setItem(key,value));
  w.HTMLElement.prototype.scrollIntoView=function(){};
  w.fetch=async(raw,init={})=>{const url=new URL(raw,w.location.href);calls.push({url,init});
    if(url.pathname==='/api/growth/catalogue')return catalogue?catalogue(url,init):response(url.searchParams.has('q')?[url.searchParams.get('q').includes('bunny')?necklace:earrings]:browse);
    if(url.pathname==='/api/growth/product')return {ok:true,json:async()=>({product:[necklace,earrings,ring].find(p=>p.handle===url.searchParams.get('handle')),live:true})};
    if(url.pathname==='/api/growth/storefront-services')return {ok:true,json:async()=>({services:{guidance:{},offers:{items:[]}}})};
    throw Error('Unexpected route '+url.pathname);
  };
  w.eval(source);t.after(()=>w.close());
  const bridge=Bridge.create({storefront:()=>w.BritesSandboxStorefront});t.after(()=>bridge.destroy());
  return {w,d,calls,errors,bridge,get store(){return w.BritesSandboxStorefront;},cards:()=>[...d.querySelectorAll('.piece-card')].map(e=>e.dataset.productHandle),say:async text=>{const resolved=bridge.resolve(text);assert.equal(resolved.ok,true,text);return bridge.execute(resolved.action);}};
}
test('necklace search → earrings → full catalogue uses independent scope and exact live identities',async t=>{
  const h=fixture(t);await settle();assert.equal((await h.say('Show bunny necklaces')).ok,true);assert.deepEqual(h.cards(),[necklace.handle]);assert.equal(h.store.snapshot().filter,'necklaces');
  assert.equal((await h.say('Show earrings')).ok,true);assert.deepEqual(h.cards(),[earrings.handle]);assert.equal(h.store.snapshot().search,'');assert.equal(h.store.snapshot().currentHandle,'');
  assert.equal((await h.say('Go back to the full list')).ok,true);assert.deepEqual(h.cards(),[necklace.handle,earrings.handle]);assert.equal(h.store.snapshot().filter,'all');assert.equal(h.d.querySelector('#store-search').value,'');assert.equal(h.errors.length,0);
});
test('a fresh untyped search clears a previous category and keeps authoritative query matches',async t=>{
  const h=fixture(t);await settle();await h.store.execute({type:'filter',filter:'necklaces'});
  const out=await h.store.execute({type:'search',query:'moon earrings under $60'});assert.equal(out.ok,true);assert.equal(h.store.snapshot().filter,'all');assert.equal(h.store.snapshot().search,'moon earrings under $60');assert.deepEqual(h.cards(),[earrings.handle]);assert.equal(out.products[0].variants[0].id,earrings.variants[0].id);
});
test('editing remains a local preview while checked category plurals are not applied twice',async t=>{
  const h=fixture(t);await settle();await h.say('Show bunny necklaces');assert.deepEqual(h.cards(),[necklace.handle]);
  const input=h.d.querySelector('#store-search'),before=h.calls.length;input.value='moon';input.dispatchEvent(new h.w.Event('input',{bubbles:true}));assert.deepEqual(h.cards(),[]);assert.equal(h.calls.length,before);
  await h.store.execute({type:'search',query:'moon earrings'});assert.deepEqual(h.cards(),[earrings.handle]);
});
test('clearing the search field restores all loaded pieces without losing browse continuation',async t=>{
  const h=fixture(t,{catalogue:url=>url.searchParams.has('q')?response([necklace]):response([necklace,earrings],{hasNextPage:true,endCursor:'storefront:2'})});await settle();await h.say('Show bunny necklaces');
  const before=h.calls.length,input=h.d.querySelector('#store-search');input.value='';input.dispatchEvent(new h.w.Event('input',{bubbles:true}));assert.deepEqual(h.cards(),[necklace.handle,earrings.handle]);assert.equal(h.store.snapshot().search,'');assert.equal(h.store.snapshot().filter,'all');assert.ok(h.d.querySelector('#collection-more button'));assert.equal(h.calls.length,before);
});
test('availability and sorting remain refinements of the current checked query',async t=>{
  const h=fixture(t);await settle();await h.say('Show bunny necklaces');await h.store.execute({type:'filter',filter:'available'});await h.store.execute({type:'sort',sort:'price-desc'});
  assert.equal(h.store.snapshot().search,'bunny necklaces');assert.equal(h.store.snapshot().filter,'available');assert.deepEqual(h.cards(),[necklace.handle]);
});
test('category and all-piece controls leave a model selection and restore the browse continuation',async t=>{
  const h=fixture(t,{catalogue:url=>url.searchParams.has('cursor')?response([ring]):response([necklace,earrings],{hasNextPage:true,endCursor:'storefront:2'})});await settle();h.store.presentProducts([necklace]);assert.deepEqual(h.cards(),[necklace.handle]);
  await h.store.execute({type:'filter',filter:'earrings'});assert.deepEqual(h.cards(),[earrings.handle]);assert.ok(h.d.querySelector('#collection-more button'));
  await h.store.execute({type:'filter',filter:'all'});h.d.querySelector('#collection-more button').click();await settle();assert.deepEqual(h.cards(),[necklace.handle,earrings.handle,ring.handle]);assert.equal(h.calls.find(c=>c.url.searchParams.has('cursor')).url.searchParams.get('cursor'),'storefront:2');
});
test('all-piece and category controls preserve session bag and gift preferences',async t=>{
  const saved=[{productId:necklace.id,title:necklace.title,variantId:necklace.variants[0].numericId,variant:necklace.variants[0].title,price:41,currency:'USD'}],prefs={wrapping:true,giftPackage:true,giftNote:'A kind note.'};
  const session={'brites-sandbox-cart':JSON.stringify(saved),'brites-sandbox-gift-preferences':JSON.stringify(prefs)},h=fixture(t,{session});await settle();await h.say('Show bunny necklaces');await h.say('Show earrings');await h.say('Clear filters');
  for(const [key,value]of Object.entries(session))assert.equal(h.w.sessionStorage.getItem(key),value);assert.ok(h.calls.every(c=>!c.init.method&&!c.init.body));
});
test('a full-list reset aborts a delayed search even when the read ignores cancellation',async t=>{
  const delayed=deferred(),h=fixture(t,{catalogue:url=>url.searchParams.has('q')?delayed.promise:response([necklace,earrings])});await settle();
  const older=h.store.execute({type:'search',query:'bunny',filter:'necklaces'});await h.store.execute({type:'filter',filter:'all'});assert.equal(h.calls.find(c=>c.url.searchParams.has('q')).init.signal.aborted,true);delayed.resolve(response([necklace]));assert.equal((await older).ok,false);assert.deepEqual(h.cards(),[necklace.handle,earrings.handle]);assert.equal(h.store.snapshot().search,'');assert.equal(h.store.snapshot().filter,'all');
});
test('category transition cancels pending old search and cannot restore its stale subject',async t=>{
  const delayed=deferred(),h=fixture(t,{catalogue:url=>url.searchParams.has('q')?delayed.promise:response([necklace,earrings])});await settle();const older=h.store.execute({type:'search',query:'bunny',filter:'necklaces'});await h.say('Show earrings');delayed.resolve(response([necklace]));assert.equal((await older).ok,false);assert.deepEqual(h.cards(),[earrings.handle]);assert.equal(h.store.snapshot().filter,'earrings');
});
test('a reset from a product deep link loads the broad collection when no cache exists',async t=>{
  const h=fixture(t,{query:'?product='+necklace.handle});await settle();assert.equal(h.store.snapshot().currentHandle,necklace.handle);await h.say('Return to the collection');assert.deepEqual(h.cards(),[necklace.handle,earrings.handle]);assert.equal(h.store.snapshot().pageKind,'collection');assert.equal(h.store.snapshot().currentHandle,'');assert.equal(h.w.location.search,'');assert.ok(h.calls.some(c=>c.url.search==='?browse=1'));
});
for(const action of [{type:'filter',filter:'earrings'},{type:'filter',filter:'all'},{type:'sort',sort:'price-desc'}])test('an early '+action.type+' control supersedes the initial read without stranding an empty collection',async t=>{
  const initial=deferred();let reads=0;const h=fixture(t,{catalogue:()=>++reads===1?initial.promise:response([necklace,earrings])});
  const out=await h.store.execute(action);assert.equal(out.ok,true);assert.equal(h.store.snapshot().loading,false);assert.deepEqual(h.cards(),action.filter==='earrings'?[earrings.handle]:action.sort?[earrings.handle,necklace.handle]:[necklace.handle,earrings.handle]);
  initial.resolve(response([ring]));await settle();assert.equal(h.cards().includes(ring.handle),false);assert.equal(h.store.snapshot().loading,false);
});
test('Back/Forward collection restores all pieces and original continuation without new history entries',async t=>{
  const h=fixture(t,{catalogue:url=>response([necklace,earrings],{hasNextPage:true,endCursor:'storefront:2'})});await settle();await h.store.execute({type:'search',query:'bunny',filter:'necklaces'});await h.store.execute({type:'open',handle:necklace.handle});const length=h.w.history.length;
  await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('Missing history traversal.')),1000);h.w.addEventListener('popstate',()=>{clearTimeout(timer);resolve();},{once:true});h.w.history.back();});await settle();
  assert.equal(h.store.snapshot().filter,'all');assert.equal(h.store.snapshot().search,'');assert.deepEqual(h.cards(),[necklace.handle,earrings.handle]);assert.ok(h.d.querySelector('#collection-more button'));assert.equal(h.w.history.length,length);
});
test('a category absent from one loaded page is not declared absent from the catalogue',async t=>{
  const h=fixture(t,{catalogue:()=>response([necklace],{hasNextPage:true,endCursor:'storefront:2'})});await settle();const out=await h.say('Show earrings');assert.equal(out.ok,true);assert.match(out.reply,/loaded pages.*Explore more pieces/);assert.doesNotMatch(out.reply,/no earrings in (?:the )?catalog/i);assert.ok(h.d.querySelector('#collection-more button'));
});
for(const message of ['Show all','Show all pieces again','Show the full list','Go back','Take me back to the previous collection','Return to the collection','Start over','Start fresh','Reset the search','Clear the search results'])test('current reset request recognizes '+message,()=>{const result=Bridge.resolve(message,{contextRevision:1,pageKind:'collection',search:'bunny',filter:'necklaces',visiblePieces:[]});assert.equal(result.ok,true);assert.deepEqual(result.action,{type:'filter',filter:'all'});});
for(const message of ['Say "go back"','Yesterday I said start over','If I say go back','Do not show all pieces','Clear the cart','Go back to the Moon Ring'])test('reset cannot acquire unsupported authority from '+message,()=>{const result=Bridge.resolve(message,{contextRevision:1,pageKind:'collection',visiblePieces:[]});assert.equal(result.ok,false);});
