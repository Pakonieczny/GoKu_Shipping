'use strict';
// Actual host and bridge with synthetic, GET-only shop responses. This checks
// discovery adoption and cancellation; it does not establish live audio or GPU.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Bridge=require('../../brites-storefront-bridge.js');
const html=fs.readFileSync(require.resolve('../../concierge-sandbox.html'),'utf8');
const source=fs.readFileSync(require.resolve('../../concierge-sandbox.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
function deferred(){let resolve;return {promise:new Promise(done=>{resolve=done;}),resolve};}
function product(index,title,type){const handle='revision-piece-'+index;return {id:'gid://shopify/Product/'+index,handle,title,type,description:'Sterling silver jewelry.',url:'https://britesjewelry.com/products/'+handle,currency:'USD',variants:[{id:'gid://shopify/ProductVariant/'+(index+1000),numericId:String(index+1000),title:'Sterling Silver',available:true,price:40+index}],options:[],variantsComplete:true};}
const necklace=product(1,'Bunny Pendant Necklace','Necklaces'),earrings=product(2,'Moon Stud Earrings','Earrings'),ring=product(3,'Moon Ring','Rings');
function response(products,pageInfo={hasNextPage:false,endCursor:null}){return {ok:true,json:async()=>JSON.parse(JSON.stringify({products,pageInfo,live:true}))};}
function fixture(t,{query='',catalogue,session={}}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM(html,{url:'https://sandbox.example/concierge-sandbox.html'+query,runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,calls=[],views=[];
  Object.entries(session).forEach(([key,value])=>w.sessionStorage.setItem(key,value));
  w.HTMLElement.prototype.scrollIntoView=function(){};
  d.addEventListener('brites-storefront:context',e=>views.push(JSON.parse(JSON.stringify(e.detail))));
  w.fetch=async(raw,init={})=>{const url=new URL(raw,w.location.href);calls.push({url,init});
    if(url.pathname==='/api/growth/catalogue')return catalogue?catalogue(url,init):response(url.searchParams.has('q')?[url.searchParams.get('q').includes('bunny')?necklace:earrings]:[necklace,earrings,ring]);
    if(url.pathname==='/api/growth/product')return {ok:true,json:async()=>({product:[necklace,earrings,ring].find(p=>p.handle===url.searchParams.get('handle')),live:true})};
    if(url.pathname==='/api/growth/storefront-services')return {ok:true,json:async()=>({services:{guidance:{},offers:{items:[]}}})};
    throw Error('Unexpected route '+url.pathname);
  };
  w.eval(source);t.after(()=>w.close());
  const bridge=Bridge.create({storefront:()=>w.BritesSandboxStorefront});t.after(()=>bridge.destroy());
  return {w,d,calls,views,errors,bridge,get store(){return w.BritesSandboxStorefront;},revision:()=>w.BritesSandboxStorefront.snapshot().discoveryRevision,cards:()=>[...d.querySelectorAll('.piece-card')].map(e=>e.dataset.productHandle)};
}
test('initial browse starts at discovery revision zero and bridge retains it',async t=>{
  const h=fixture(t);assert.equal(h.revision(),0);await settle();assert.equal(h.revision(),0);assert.equal(h.bridge.snapshot().discoveryRevision,0);assert.deepEqual(h.cards(),[necklace.handle,earrings.handle,ring.handle]);assert.equal(h.errors.length,0);
});
test('each completed search adopts a new subject, including the same query and empty reset',async t=>{
  const h=fixture(t);await settle();
  for(const [query,revision]of [['bunny necklace',1],['bunny necklace',2],['moon earrings',3],['',4]]){const out=await h.store.execute({type:'search',query});assert.equal(out.ok,true);assert.equal(out.snapshot.discoveryRevision,revision);assert.equal(h.bridge.snapshot().discoveryRevision,revision);}
  assert.deepEqual(h.cards(),[necklace.handle,earrings.handle,ring.handle]);
});
test('pending search leaves the revision unchanged until its current checked view is ready',async t=>{
  const delayed=deferred(),h=fixture(t,{catalogue:url=>url.searchParams.has('q')?delayed.promise:response([necklace,earrings])});await settle();
  const pending=h.store.execute({type:'search',query:'moon earrings'});assert.equal(h.revision(),0);assert.equal(h.store.snapshot().loading,true);assert.ok(h.views.some(v=>v.search==='moon earrings'&&v.loading&&v.discoveryRevision===0));
  delayed.resolve(response([earrings]));const out=await pending;assert.equal(out.ok,true);assert.equal(h.revision(),1);assert.equal(out.snapshot.loading,false);assert.deepEqual(h.cards(),[earrings.handle]);
});
test('explicit category and all controls adopt discovery even when repeated',async t=>{
  const h=fixture(t);await settle();
  for(const [filter,revision]of [['earrings',1],['earrings',2],['all',3],['all',4],['available',5]]){const out=await h.store.execute({type:'filter',filter});assert.equal(out.ok,true);assert.equal(out.snapshot.discoveryRevision,revision);assert.equal(out.snapshot.filter,filter);}
});
test('model selection and full browse have identical blank/all scope but different discovery revisions',async t=>{
  const h=fixture(t);await settle();const chosen=h.store.presentProducts([necklace]);assert.equal(chosen.ok,true);assert.equal(chosen.snapshot.discoveryRevision,1);assert.deepEqual(h.cards(),[necklace.handle]);
  const all=await h.store.execute({type:'filter',filter:'all'});assert.equal(all.ok,true);assert.equal(all.snapshot.discoveryRevision,2);assert.equal(all.snapshot.search,chosen.snapshot.search);assert.equal(all.snapshot.filter,chosen.snapshot.filter);assert.deepEqual(h.cards(),[necklace.handle,earrings.handle,ring.handle]);
});
test('gaze, scroll, sorting and highlighting update context without replacing discovery',async t=>{
  const h=fixture(t);await settle();await h.store.execute({type:'filter',filter:'earrings'});const before=h.store.snapshot(),card=h.d.querySelector('.piece-card');
  card.dispatchEvent(new h.w.MouseEvent('pointerover',{bubbles:true}));assert.equal(h.store.snapshot().focusedHandle,earrings.handle);assert.equal(h.revision(),before.discoveryRevision);
  h.w.dispatchEvent(new h.w.Event('scroll'));await h.store.execute({type:'sort',sort:'price-desc'});await h.store.execute({type:'highlight',section:'catalogue'});
  assert.equal(h.revision(),before.discoveryRevision);assert.ok(h.store.snapshot().contextRevision>before.contextRevision);
});
test('loading another browse page extends the subject without changing discovery',async t=>{
  const h=fixture(t,{catalogue:url=>url.searchParams.has('cursor')?response([ring]):response([necklace,earrings],{hasNextPage:true,endCursor:'public:next'})});await settle();await h.store.execute({type:'filter',filter:'all'});const before=h.revision();h.d.querySelector('#collection-more button').click();await settle();
  assert.equal(h.revision(),before);assert.deepEqual(h.cards(),[necklace.handle,earrings.handle,ring.handle]);
});
test('clearing a checked search restores cached broad discovery once while typing stays local',async t=>{
  const h=fixture(t);await settle();await h.store.execute({type:'search',query:'bunny'});const input=h.d.querySelector('#store-search'),calls=h.calls.length;
  input.value='bunny n';input.dispatchEvent(new h.w.Event('input',{bubbles:true}));assert.equal(h.revision(),1);assert.equal(h.calls.length,calls);
  input.value='';input.dispatchEvent(new h.w.Event('input',{bubbles:true}));assert.equal(h.revision(),2);assert.equal(h.store.snapshot().filter,'all');assert.deepEqual(h.cards(),[necklace.handle,earrings.handle,ring.handle]);
  input.dispatchEvent(new h.w.Event('input',{bubbles:true}));assert.equal(h.revision(),2);assert.equal(h.calls.length,calls);
});
test('accepted presentation increments discovery on a collection and the current product',async t=>{
  const h=fixture(t);await settle();assert.equal(h.store.presentProducts([necklace]).snapshot.discoveryRevision,1);assert.equal(h.store.presentProducts([necklace]).snapshot.discoveryRevision,2);await h.store.execute({type:'open',handle:necklace.handle});assert.equal(h.revision(),2);
  const current=h.store.presentProducts([necklace]);assert.equal(current.ok,true);assert.equal(current.snapshot.pageKind,'product');assert.equal(current.snapshot.discoveryRevision,3);assert.equal(current.snapshot.currentHandle,necklace.handle);
});
test('invalid filter, search, presentation and already-canceled requests cannot advance discovery',async t=>{
  const h=fixture(t);await settle();const controller=new h.w.AbortController();controller.abort();
  for(const action of [{type:'filter',filter:'secret'},{type:'search',query:'x'.repeat(251)},{type:'search',query:'bunny'}]){const out=await h.store.execute(action,action.query==='bunny'?{signal:controller.signal}:{});assert.equal(out.ok,false);assert.equal(h.revision(),0);}
  assert.equal(h.store.presentProducts([{...necklace,id:'invalid'}]).ok,false);assert.equal(h.revision(),0);assert.deepEqual(h.cards(),[necklace.handle,earrings.handle,ring.handle]);
});
test('failed upstream search has no new completed discovery revision',async t=>{
  const h=fixture(t,{catalogue:url=>url.searchParams.has('q')?{ok:false,json:async()=>({})}:response([necklace,earrings])});await settle();const out=await h.store.execute({type:'search',query:'bunny'});assert.equal(out.ok,false);assert.equal(h.revision(),0);assert.equal(h.store.snapshot().loading,false);
});
test('canceled old query cannot advance discovery or replace a newer category view',async t=>{
  const delayed=deferred(),h=fixture(t,{catalogue:url=>url.searchParams.has('q')?delayed.promise:response([necklace,earrings])});await settle();
  const older=h.store.execute({type:'search',query:'bunny',filter:'necklaces'});await h.store.execute({type:'filter',filter:'earrings'});const acceptedViews=h.views.length;assert.equal(h.revision(),1);assert.equal(h.store.snapshot().search,'');assert.deepEqual(h.cards(),[earrings.handle]);
  delayed.resolve(response([necklace]));assert.equal((await older).ok,false);assert.equal(h.revision(),1);assert.equal(h.views.length,acceptedViews);assert.deepEqual(h.cards(),[earrings.handle]);
});
for(const action of [{type:'filter',filter:'earrings'},{type:'sort',sort:'price-desc'}])test('early '+action.type+' delegated browse commits its discovery exactly once or not at all',async t=>{
  const initial=deferred();let reads=0;const h=fixture(t,{catalogue:()=>++reads===1?initial.promise:response([necklace,earrings])});const out=await h.store.execute(action);assert.equal(out.ok,true);assert.equal(h.revision(),action.type==='filter'?1:0);initial.resolve(response([ring]));await settle();assert.equal(h.revision(),action.type==='filter'?1:0);assert.equal(h.cards().includes(ring.handle),false);
});
test('cached browser Back restores the prior exact discovery scope and preserves history length',async t=>{
  const h=fixture(t);await settle();await h.store.execute({type:'search',query:'bunny',filter:'necklaces'});await h.store.execute({type:'open',handle:necklace.handle});const length=h.w.history.length;
  await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('Missing history traversal.')),1000);h.w.addEventListener('popstate',()=>{clearTimeout(timer);resolve();},{once:true});h.w.history.back();});await settle();
  assert.equal(h.revision(),2);assert.equal(h.store.snapshot().filter,'necklaces');assert.equal(h.store.snapshot().search,'bunny');assert.deepEqual(h.cards(),[necklace.handle]);assert.equal(h.w.history.length,length);
});
test('a broad popstate reset from a deep link fetches and commits discovery once',async t=>{
  const h=fixture(t,{query:'?product='+necklace.handle});await settle();assert.equal(h.revision(),0);h.w.history.replaceState({},'','/concierge-sandbox.html');h.w.dispatchEvent(new h.w.PopStateEvent('popstate'));await settle();assert.equal(h.revision(),1);assert.deepEqual(h.cards(),[necklace.handle,earrings.handle,ring.handle]);
});
test('discovery resets preserve session bag and gift values without outbound mutation',async t=>{
  const session={'brites-sandbox-cart':JSON.stringify([{productId:necklace.id,title:necklace.title,variantId:necklace.variants[0].numericId,variant:necklace.variants[0].title,price:41,currency:'USD'}]),'brites-sandbox-gift-preferences':JSON.stringify({wrapping:true,giftPackage:true,giftNote:'A kind test note.'})},h=fixture(t,{session});await settle();await h.store.execute({type:'search',query:'bunny'});h.store.presentProducts([earrings]);await h.store.execute({type:'filter',filter:'all'});
  for(const [key,value]of Object.entries(session))assert.equal(h.w.sessionStorage.getItem(key),value);assert.ok(h.calls.every(c=>!c.init.method&&!c.init.body));
});
for(const value of [undefined,-1,1.5,'1','scope:1',NaN,Infinity,Number.MAX_SAFE_INTEGER+1,{},null])test('bridge rejects invalid discovery revision '+String(value),()=>{const out=Bridge.sanitizeSnapshot({discoveryRevision:value,internal:'private',visiblePieces:[]});assert.equal(out.discoveryRevision,0);assert.equal(Object.hasOwn(out,'internal'),false);});
for(const value of [0,1,73,Number.MAX_SAFE_INTEGER])test('bridge preserves safe nonnegative discovery revision '+value,()=>{assert.equal(Bridge.sanitizeSnapshot({discoveryRevision:value}).discoveryRevision,value);});
