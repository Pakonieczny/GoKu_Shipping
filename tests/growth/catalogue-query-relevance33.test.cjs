'use strict';
// Actual production host and bridge with synthetic GET-only responses. The
// published ten-row order below was observed; these product/variant IDs are
// deliberately synthetic. This suite does not establish live voice or GPU.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Bridge=require('../../brites-storefront-bridge.js');
const html=fs.readFileSync(require.resolve('../../concierge-sandbox.html'),'utf8');
const source=fs.readFileSync(require.resolve('../../concierge-sandbox.js'),'utf8');
const clone=value=>JSON.parse(JSON.stringify(value)),tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
function deferred(){let resolve;return {promise:new Promise(done=>{resolve=done;}),resolve};}
function product(id,title,patch={}){const handle='relevance-piece-'+id;return {id:'gid://shopify/Product/'+id,handle,title,type:'Necklaces',description:'Synthetic live listing.',url:'https://britesjewelry.com/products/'+handle,currency:'USD',image:'https://cdn.shopify.com/'+handle+'.jpg',variantsComplete:true,options:[{name:'Metal',values:['Sterling Silver']}],variants:[{id:'gid://shopify/ProductVariant/'+id+'01',numericId:id+'01',title:'Sterling Silver',price:40+id,available:true,options:[{name:'Metal',value:'Sterling Silver'}]}],...patch};}
const observedNames=['Script Name Necklace','Gemstone Bead Necklace','Lowercase Initial Necklace','Lowercase Pendant Necklace','Decorative Initial Pendant Necklace','Cable Chain Necklace','Crescent Moon Pendant Necklace','Saturn Pendant Necklace','Mountain Pendant Necklace','Leaf Pendant Cable Necklace'];
const observedHandles=['custom-name-necklace','gemstone-choker-necklace','lowercase-initial','lowercase-initial-gold-necklace','initial-charm-necklace','team-charm-necklace-for-everyday-layering','tiny-moon-necklace','saturn-pendant-necklace-for-celestial-layering','mountain-charm-necklace','leaf-necklace-dainty'];
const moonRows=observedNames.map((title,index)=>{const handle=observedHandles[index];return product(index+1,title,{handle,url:'https://britesjewelry.com/products/'+handle});});
function response(products,pageInfo={hasNextPage:false,endCursor:null}){return {ok:true,json:async()=>clone({live:true,products,pageInfo})};}
function fixture(t,{products=moonRows,catalogue,pageInfo={hasNextPage:false,endCursor:null},session={}}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(html,{url:'https://sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,reads=[],views=[];
  // This catalogue-only fixture does not serve /inventory. Its unrelated
  // failed-preload retry can otherwise enter the strict paging read ledger
  // under full-suite load. Complete preload/retry remains exercised by the
  // inventory and actual 120-row browse suites; paging assertions stay exact.
  delete d.body.dataset.inventoryPreload;
  Object.entries(session).forEach(([key,value])=>w.sessionStorage.setItem(key,value));
  w.HTMLElement.prototype.scrollIntoView=function(){};
  d.addEventListener('brites-storefront:context',event=>views.push(clone(event.detail)));
  w.fetch=async(raw,init={})=>{const url=new URL(raw,w.location.href);reads.push({url,init});if(url.pathname==='/api/growth/catalogue')return catalogue?catalogue(url,init):response(products,pageInfo);if(url.pathname==='/api/growth/storefront-services')return {ok:true,json:async()=>({services:{guidance:{},offers:{items:[]}}})};throw Error('Unexpected read '+url.pathname);};
  w.eval(source);t.after(()=>w.close());const bridge=Bridge.create({storefront:()=>w.BritesSandboxStorefront});t.after(()=>bridge.destroy());
  const h={w,d,reads,views,errors,bridge,get store(){return w.BritesSandboxStorefront;},handles(){return [...d.querySelectorAll('.piece-card')].map(card=>card.dataset.productHandle);},async ready(){await settle();},async say(message){const resolved=bridge.resolve(message);assert.equal(resolved.ok,true,message);return bridge.execute(resolved.action);}};return h;
}

test('the observed OR necklace results retain only a checked moon piece, with its exact variants',async t=>{
  const h=fixture(t);await h.ready();const before=h.store.snapshot(),result=await h.store.execute({type:'search',query:'moon necklaces',filter:'necklaces'});
  assert.equal(result.ok,true);assert.deepEqual(h.handles(),['tiny-moon-necklace']);assert.deepEqual(result.products.map(p=>p.id),[moonRows[6].id]);assert.deepEqual(result.products[0].variants,moonRows[6].variants);assert.equal(result.products[0].minPrice,47);assert.equal(result.snapshot.search,'moon necklaces');assert.equal(result.snapshot.filter,'necklaces');assert.equal(result.snapshot.discoveryRevision,before.discoveryRevision+1);assert.equal(result.snapshot.loading,false);assert.deepEqual(result.snapshot.visiblePieces.map(p=>p.handle),['tiny-moon-necklace']);assert.equal(h.reads.filter(r=>r.url.searchParams.has('q')).length,1);assert.equal(h.reads.at(-1).url.searchParams.get('q'),'moon necklaces');assert.equal(h.errors.length,0);
});
test('the actual typed bridge request cannot publish generic necklaces as moon matches',async t=>{
  const h=fixture(t);await h.ready();const result=await h.say('Show moon necklaces');assert.equal(result.ok,true);assert.deepEqual(h.handles(),['tiny-moon-necklace']);assert.equal(h.d.querySelector('#result-summary').textContent,'1 of 1 loaded pieces matching “moon necklaces” · necklaces');assert.match(result.reply,/checked search results/i);
});
test('body cross-sell copy and substring coincidence cannot supply a missing motif',async t=>{
  const rows=[product(20,'Leaf Necklace',{description:'Wear with our moon earrings.'}),product(21,'Moonstruck Necklace'),product(22,'Crescent Moon Necklace'),product(23,'Moonstone Necklace'),product(24,'Leaf Necklace',{tags:['moonstone','styled alongside moon earrings']})],h=fixture(t,{products:rows});await h.ready();await h.store.execute({type:'search',query:'moon necklaces',filter:'necklaces'});assert.deepEqual(h.handles(),['relevance-piece-22']);
});
test('checked category plurals and price language keep a literal motif without stale categories',async t=>{
  const earring=product(30,'Moon Stud Earrings',{type:'Earrings'});earring.variants[0].price=55;const h=fixture(t,{products:[moonRows[0],moonRows[6],earring]});await h.ready();await h.store.execute({type:'filter',filter:'necklaces'});const result=await h.store.execute({type:'search',query:'moon earrings under $60'});assert.equal(result.snapshot.filter,'all');assert.equal(result.snapshot.search,'moon earrings under $60');assert.deepEqual(h.handles(),[earring.handle]);assert.deepEqual(result.products[0].variants,earring.variants);assert.equal(result.products[0].currency,'USD');
});
test('literal aliases and inflections match the same symbol without unrelated category rows',async t=>{
  const bunny=product(40,'Bunny Pendant Necklace'),rabbit=product(41,'Rabbit Pendant Necklace'),daisy=product(42,'Daisy Pendant Necklace'),h=fixture(t,{products:[bunny,rabbit,daisy,moonRows[0]]});await h.ready();for(const query of ['rabbit necklaces','bunnies necklaces']){await h.store.execute({type:'search',query,filter:'necklaces'});assert.deepEqual(h.handles(),[bunny.handle,rabbit.handle]);}await h.store.execute({type:'search',query:'daisies necklaces',filter:'necklaces'});assert.deepEqual(h.handles(),[daisy.handle]);
});
test('exact names and numbered titles retain all their substantive name terms',async t=>{
  const exact=product(51,'Rhino Profile Stud Earrings',{type:'Earrings'}),other=product(52,'Rhino Stud Earrings',{type:'Earrings'}),numbered=product(53,'Bunny Necklace 7'),another=product(54,'Bunny Necklace 8'),h=fixture(t,{products:[exact,other,numbered,another]});await h.ready();await h.store.execute({type:'search',query:'Rhino Profile Stud Earrings'});assert.deepEqual(h.handles(),[exact.handle]);await h.store.execute({type:'search',query:'Bunny Necklace 7'});assert.deepEqual(h.handles(),[numbered.handle]);
});
test('explicit checked motif tags supplement live names without promotional phrase matches',async t=>{
  const tagged=product(61,'Celestial Pendant',{tags:['motif:moon']}),plain=product(62,'Celestial Pendant',{tags:['moon']}),promo=product(63,'Flower Necklace',{tags:['gift alongside moon necklaces']}),h=fixture(t,{products:[tagged,plain,promo]});await h.ready();await h.store.execute({type:'search',query:'moon necklaces',filter:'necklaces'});assert.deepEqual(h.handles(),[tagged.handle,plain.handle]);
});
test('no source-bound motif match produces a qualified empty result with a full-list recovery',async t=>{
  const h=fixture(t,{products:[moonRows[0],moonRows[1]],pageInfo:{hasNextPage:true,endCursor:'public:next'}});await h.ready();const result=await h.store.execute({type:'search',query:'moon necklaces',filter:'necklaces'});assert.equal(result.ok,true);assert.deepEqual(result.products,[]);assert.deepEqual(result.snapshot.visiblePieces,[]);assert.equal(result.snapshot.loading,false);assert.match(result.message,/these checked results/i);assert.doesNotMatch(result.message,/no moon.*(?:in the|in our) (?:catalogue|shop)/i);assert.match(h.d.querySelector('.empty-collection').textContent,/Show all pieces/);assert.equal(h.d.querySelector('#collection-more button'),null);h.d.querySelector('.empty-collection button').click();await settle();assert.deepEqual(h.handles(),[moonRows[0].handle,moonRows[1].handle]);assert.ok(h.d.querySelector('#collection-more button'));assert.equal(h.store.snapshot().search,'');assert.equal(h.store.snapshot().filter,'all');
});
test('category/material/budget-only searches match exact checked choices without applying prose as literal names',async t=>{
  const silver=product(70,'Daisy Stud Earrings',{type:'Earrings'}),gold=product(71,'Moon Stud Earrings',{type:'Earrings'});silver.variants[0].price=45;gold.options[0].values=['14k Gold Filled'];gold.variants[0]={...gold.variants[0],price:55,title:'14k Gold Filled',options:[{name:'Metal',value:'14k Gold Filled'}]};const earrings=[silver,gold],h=fixture(t,{products:earrings});await h.ready();for(const [query,expected] of [['earrings',earrings],['gold earrings under $60',[gold]],['silver earrings between USD 40 and 80',[silver]],['earrings budget 50 dollars',[silver]],['all pieces',earrings]]){const result=await h.store.execute({type:'search',query});assert.equal(result.ok,true,query);assert.deepEqual(h.handles(),expected.map(p=>p.handle),query);assert.equal(result.snapshot.search,query);}
});
test('blank and all-piece reset restore exact browse order, continuation and saved shopper choices',async t=>{
  const session={'brites-sandbox-gift-preferences':JSON.stringify({wrapping:true,giftNote:'Synthetic private note'}),'brites-sandbox-cart':'[]'},h=fixture(t,{pageInfo:{hasNextPage:true,endCursor:'public:next'},session});await h.ready();await h.store.execute({type:'search',query:'moon necklaces',filter:'necklaces'});await h.store.execute({type:'filter',filter:'all'});assert.deepEqual(h.handles(),observedHandles);assert.ok(h.d.querySelector('#collection-more button'));await h.store.execute({type:'search',query:''});assert.deepEqual(h.handles(),observedHandles);for(const [key,value]of Object.entries(session))assert.equal(h.w.sessionStorage.getItem(key),value);assert.ok(h.reads.every(r=>!r.init.method&&!r.init.body));
});
test('a late constrained search cannot replace a completed new-category view',async t=>{
  const delayed=deferred(),earrings=product(80,'Moon Stud Earrings',{type:'Earrings'}),h=fixture(t,{products:[moonRows[6],earrings],catalogue:url=>url.searchParams.has('q')?delayed.promise:response([moonRows[6],earrings])});await h.ready();const pending=h.store.execute({type:'search',query:'moon necklaces',filter:'necklaces'});await h.store.execute({type:'filter',filter:'earrings'});const completed=h.store.snapshot(),published=h.views.length;delayed.resolve(response(moonRows));assert.equal((await pending).ok,false);assert.deepEqual(h.handles(),[earrings.handle]);assert.equal(h.store.snapshot().discoveryRevision,completed.discoveryRevision);assert.equal(h.views.length,published);
});
test('constrained checked results preserve local paging, exact stable sorting and read-only authority',async t=>{
  const rows=Array.from({length:55},(_,i)=>product(100+i,'Moon Pendant Necklace '+i)),h=fixture(t,{products:[moonRows[0],...rows]});await h.ready();const result=await h.store.execute({type:'search',query:'moon necklaces',filter:'necklaces',sort:'price-desc'});assert.equal(result.products.length,24);assert.equal(h.handles()[0],rows[54].handle);assert.match(h.d.querySelector('#result-summary').textContent,/24 of 55 loaded pieces/);const count=h.reads.length;h.d.querySelector('#collection-more button').click();await settle();assert.equal(h.handles().length,48);h.d.querySelector('#collection-more button').click();await settle();assert.equal(h.handles().length,55);assert.equal(h.reads.length,count);assert.equal(h.d.querySelector('#collection-more button'),null);assert.ok(h.reads.every(r=>!r.init.method&&!r.init.body));
});
