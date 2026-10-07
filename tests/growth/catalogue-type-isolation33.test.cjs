'use strict';
// Production catalogue UI with synthetic published product facts. No live
// source type, material, price, variant or historical choice is invented here.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const html=fs.readFileSync(require.resolve('../../concierge-sandbox.html'),'utf8'),source=fs.readFileSync(require.resolve('../../concierge-sandbox.js'),'utf8');
const clone=value=>JSON.parse(JSON.stringify(value)),settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
function product(id,title,type='',patch={}){return {id:'gid://shopify/Product/'+id,handle:'catalogue-piece-'+id,title,type,url:'https://britesjewelry.com/products/catalogue-piece-'+id,description:'Synthetic source: this can coordinate with earrings, necklaces, bracelets, rings and charms.',currency:'USD',image:'https://cdn.shopify.com/catalogue-piece-'+id+'.jpg',images:[],options:[{name:'Metal',values:['Sterling Silver']}],variantsComplete:true,variants:[{id:'gid://shopify/ProductVariant/'+id+'01',numericId:id+'01',title:'Sterling Silver',price:id,available:true,options:[{name:'Metal',value:'Sterling Silver'}]}],...patch};}
const rows=[
  product(1,'Chain extender — tiny heart','Custom Charm Studio'),
  product(2,'Engraving on your custom charm','Custom Charm Studio'),
  product(3,'Custom design fee','Custom Charm Studio'),
  product(4,'Custom Charm — Stud Earrings (pair)','Custom Charm Studio'),
  product(5,'Custom Charm — Huggie Hoops (pair)','Custom Charm Studio'),
  product(6,'Personalized custom charm studs — engraving optional','Custom Charm Studio'),
  product(7,'Wildcat','Earrings'),
  product(8,'Compass Necklace','Necklaces'),
  product(9,'Moon Charm','Charms'),
  product(10,'Engraved Signet Rings','Rings'),
  product(11,'Petal Bracelets','Bracelets'),
  product(12,'Bunny Stud Earrings',''),
  product(13,'Little Moon','',{description:'Coordinates with sterling silver earrings.'})
];
function fixture(t,{products=rows,pageInfo={hasNextPage:false,endCursor:null}}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));const dom=new JSDOM(html,{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,requests=[],contexts=[];
  w.HTMLElement.prototype.scrollIntoView=function(){};d.addEventListener('brites-storefront:context',event=>contexts.push(clone(event.detail)));
  w.fetch=async raw=>{const url=new URL(raw,w.location.href);requests.push(url);let data;
    if(url.pathname==='/api/growth/catalogue')data={live:true,products:clone(url.searchParams.get('q')==='bunny'?products.filter(p=>p.title.includes('Bunny')):products),pageInfo:clone(pageInfo)};
    else if(url.pathname==='/api/growth/product')data={live:true,product:clone(products.find(p=>p.handle===url.searchParams.get('handle')))};
    else if(url.pathname==='/api/growth/storefront-services')data={schema:1,guidance:{},conflicts:[],offers:{items:[],status:'published_not_checkout_validated'}};
    else throw Error('Unexpected synthetic route '+url.pathname);
    return {ok:true,json:async()=>data};
  };
  w.eval(source);t.after(()=>w.close());return {w,d,errors,requests,contexts,get store(){return w.BritesSandboxStorefront;},cards(){return [...d.querySelectorAll('.piece-card')];},handles(){return this.cards().map(card=>card.dataset.productHandle);},async ready(){await settle();},cart(){return w.sessionStorage.getItem('brites-sandbox-cart');}};
}

test('earrings exclude studio services and extenders while preserving exact custom studs and huggies',async t=>{
  const h=fixture(t);await h.ready();const before=h.store.snapshot(),count=h.requests.length;const result=await h.store.execute({type:'filter',filter:'earrings'});
  assert.equal(result.ok,true);assert.deepEqual(h.handles(),[4,5,6,7,12].map(id=>'catalogue-piece-'+id));assert.deepEqual(result.products.map(p=>p.id),[4,5,6,7,12].map(id=>'gid://shopify/Product/'+id));assert.deepEqual(result.products[0].variants,rows[3].variants);assert.equal(result.products[0].type,'Custom Charm Studio');assert.equal(result.products[0].currency,'USD');assert.equal(result.products[0].minPrice,4);assert.equal(h.requests.length,count);assert.equal(result.snapshot.discoveryRevision,before.discoveryRevision+1);assert.equal(result.snapshot.loading,false);assert.equal(result.snapshot.filter,'earrings');assert.equal(result.snapshot.search,'');assert.equal(h.cart(),null);assert.deepEqual(h.errors,[]);
});

test('complete words avoid studio, studying and studded-name substring false positives',async t=>{
  const products=[product(20,'Studio service','Studio'),product(21,'Studying moon necklace','Necklace'),product(22,'Studded bracelet','Bracelet'),product(23,'Tiny stud',''),product(24,'Tiny huggie',''),product(25,'Tiny hoops','')],h=fixture(t,{products});await h.ready();await h.store.execute({type:'filter',filter:'earrings'});assert.deepEqual(h.handles(),[23,24,25].map(id=>'catalogue-piece-'+id));
});

test('a missing piece type uses actual title evidence and never body-only category mentions',async t=>{
  const h=fixture(t,{products:[product(31,'Little Moon','',{description:'This would complement earrings or a necklace.'}),product(32,'Moon Stud Earrings',''),product(33,'Moon Pendant','',{description:'Wear with earrings.'})]});await h.ready();await h.store.execute({type:'filter',filter:'earrings'});assert.deepEqual(h.handles(),['catalogue-piece-32']);await h.store.execute({type:'filter',filter:'necklaces'});assert.deepEqual(h.handles(),['catalogue-piece-33']);await h.store.execute({type:'filter',filter:'rings'});assert.equal(h.cards().length,0);assert.match(h.d.querySelector('.empty-collection').textContent,/No loaded pieces match/);assert.equal(h.store.snapshot().loading,false);await h.store.execute({type:'filter',filter:'all'});assert.deepEqual(h.handles(),[31,32,33].map(id=>'catalogue-piece-'+id));
});

test('only explicit single-category tags supplement missing structural type',async t=>{
  const h=fixture(t,{products:[product(40,'Tiny Flower','',{tags:['category:Earrings']}),product(41,'Tiny Clover','',{tags:['product_type:Studs']}),product(42,'Tiny Heart','',{tags:['Earrings','match with earrings','category:Earrings and necklaces']}),product(43,'Tiny Star','Custom Charm Studio',{tags:['category:Charms']})]});await h.ready();await h.store.execute({type:'filter',filter:'earrings'});assert.deepEqual(h.handles(),[40,41].map(id=>'catalogue-piece-'+id));await h.store.execute({type:'filter',filter:'charms'});assert.deepEqual(h.handles(),['catalogue-piece-43']);
});

test('a studio category itself never makes a service listing a charm',async t=>{
  const h=fixture(t,{products:[...rows.slice(0,3),product(51,'Cloud Charm','Custom Charm Studio')]});await h.ready();await h.store.execute({type:'filter',filter:'charms'});assert.deepEqual(h.handles(),['catalogue-piece-51']);
});

test('explicit utility or service titles cannot acquire a jewellery category from misleading type or tags',async t=>{
  const h=fixture(t,{products:[product(60,'Chain extender for necklace','Necklaces'),product(61,'Custom design fee','Earrings',{tags:['category:Earrings']}),product(62,'Engraving on your custom charm','Charms'),product(63,'Custom engraving','Earrings'),product(64,'Custom engraving charm studs','Custom Charm Studio')]});await h.ready();await h.store.execute({type:'filter',filter:'earrings'});assert.deepEqual(h.handles(),['catalogue-piece-64']);await h.store.execute({type:'filter',filter:'necklaces'});assert.equal(h.cards().length,0);await h.store.execute({type:'filter',filter:'charms'});assert.deepEqual(h.handles(),['catalogue-piece-64']);await h.store.execute({type:'filter',filter:'all'});assert.equal(h.cards().length,5);
});

test('a category switch leaves a narrowed query and restores the full checked scope without service pollution',async t=>{
  const h=fixture(t);await h.ready();await h.store.execute({type:'search',query:'bunny',filter:'necklaces'});const search=h.store.snapshot();assert.equal(search.search,'bunny');const result=await h.store.execute({type:'filter',filter:'earrings'});assert.equal(result.snapshot.search,'');assert.ok(result.snapshot.discoveryRevision>search.discoveryRevision);assert.deepEqual(h.handles(),[4,5,6,7,12].map(id=>'catalogue-piece-'+id));assert.equal(result.products.every(p=>!['catalogue-piece-1','catalogue-piece-2','catalogue-piece-3'].includes(p.handle)),true);assert.equal(h.cart(),null);
});

test('all and availability views retain public source rows and exact facts without a category exclusion',async t=>{
  const products=rows.map((p,i)=>({...clone(p),variants:p.variants.map(v=>({...clone(v),available:i!==1}))})),h=fixture(t,{products});await h.ready();const all=await h.store.execute({type:'filter',filter:'all'});assert.deepEqual(h.handles(),products.map(p=>p.handle));assert.deepEqual(all.products.map(p=>p.type),products.map(p=>p.type));await h.store.execute({type:'filter',filter:'available'});assert.ok(h.handles().includes('catalogue-piece-1'));assert.ok(h.handles().includes('catalogue-piece-3'));assert.ok(!h.handles().includes('catalogue-piece-2'));assert.equal(h.cart(),null);
});

test('plural rings and bracelets classify from structural title or live type without borrowed descriptions',async t=>{
  const h=fixture(t);await h.ready();await h.store.execute({type:'filter',filter:'rings'});assert.deepEqual(h.handles(),['catalogue-piece-10']);await h.store.execute({type:'filter',filter:'bracelets'});assert.deepEqual(h.handles(),['catalogue-piece-11']);await h.store.execute({type:'filter',filter:'necklaces'});assert.deepEqual(h.handles(),['catalogue-piece-8']);
});
