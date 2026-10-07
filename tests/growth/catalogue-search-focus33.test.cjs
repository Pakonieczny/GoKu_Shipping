'use strict';
// Actual production form/navigation handlers with synthetic public responses.
// JSDOM has no default Enter activation: requestSubmit and detail=0 clicks
// exercise the same handlers as the separately observed Chrome keyboard route.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const html=fs.readFileSync(require.resolve('../../concierge-sandbox.html'),'utf8'),source=fs.readFileSync(require.resolve('../../concierge-sandbox.js'),'utf8');
const clone=value=>JSON.parse(JSON.stringify(value)),settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
function deferred(){let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};}
function product(id,title){return {id:'gid://shopify/Product/'+id,handle:'focus-piece-'+id,title,type:'Earrings',description:'Synthetic current sterling silver description.',url:'https://britesjewelry.com/products/focus-piece-'+id,currency:'USD',image:'https://cdn.shopify.com/focus-piece-'+id+'.jpg',images:[],options:[{name:'Metal',values:['Sterling Silver']}],variantsComplete:true,variants:[{id:'gid://shopify/ProductVariant/'+id+'01',numericId:id+'01',title:'Sterling Silver',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]}]};}
const initial=[product(1,'Daisy Stud Earrings'),product(2,'Bee Stud Earrings')],daisies=[product(3,'Small Daisy Stud Earrings'),product(4,'Large Daisy Stud Earrings')];
function fixture(t){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));const dom=new JSDOM(html,{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document,requests=[],scrolls=[],gates=new Map();let lookupGate=null;
  w.matchMedia=()=>({matches:false});w.HTMLElement.prototype.scrollIntoView=function(value){scrolls.push({node:this,value:clone(value)});};
  w.fetch=async raw=>{const url=new URL(raw,w.location.href);requests.push(url);let data;
    if(url.pathname==='/api/growth/catalogue'){
      const query=url.searchParams.get('q');if(gates.has(query))return gates.get(query).promise;
      data={live:true,products:clone(query==='daisy'?daisies:query==='bee'?[initial[1]]:initial),pageInfo:{hasNextPage:false,endCursor:null}};
    }else if(url.pathname==='/api/growth/product'){
      if(lookupGate)return lookupGate.promise;
      data={live:true,product:clone([...initial,...daisies].find(p=>p.handle===url.searchParams.get('handle')))};
    }else if(url.pathname==='/api/growth/storefront-services')data={schema:1,guidance:{},conflicts:[],offers:{items:[],status:'published_not_checkout_validated'}};
    else throw Error('Unexpected synthetic route '+url.pathname);
    return {ok:true,json:async()=>data};
  };
  w.eval(source);t.after(()=>w.close());const h={w,d,errors,requests,scrolls,gates,get store(){return w.BritesSandboxStorefront;},input(){return d.querySelector('#store-search');},sort(){return d.querySelector('#store-sort');},outside(){return d.querySelector('header a[href]');},async ready(){await settle();},submit(text){const input=h.input();input.focus();input.value=text;input.dispatchEvent(new w.Event('input',{bubbles:true}));input.form.requestSubmit();return input;},response(products=daisies,ok=true){return {ok,json:async()=>({live:true,products:clone(products),pageInfo:{hasNextPage:false,endCursor:null}})};},lookup(gate){lookupGate=gate;},activateLink({mouse=false}={}){const anchor=d.querySelector('.piece-card a');anchor.focus();anchor.dispatchEvent(new w.MouseEvent('click',{bubbles:true,cancelable:true,button:0,detail:mouse?1:0}));return anchor;}};return h;
}
function activateBack(h,{mouse=false,focused=true}={}){const back=h.d.querySelector('.back-link');assert.equal(back.textContent,'← Back to the collection');if(focused)back.focus();back.dispatchEvent(new h.w.MouseEvent('click',{bubbles:true,cancelable:true,button:0,detail:mouse?1:0}));return back;}

test('a focused search submission keeps the exact input through loading and result completion',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.gates.set('daisy',gate);const input=h.submit('daisy'),revision=h.store.snapshot().discoveryRevision;
  assert.equal(input.isConnected,true);assert.equal(h.input(),input);assert.equal(h.d.activeElement,input);assert.equal(h.store.snapshot().loading,true);assert.equal(h.store.snapshot().search,'daisy');assert.equal(h.requests.filter(u=>u.searchParams.get('q')==='daisy').length,1);
  gate.resolve(h.response());await settle();assert.equal(h.d.activeElement,input);assert.equal(h.input(),input);assert.equal(h.store.snapshot().loading,false);assert.ok(h.store.snapshot().discoveryRevision>revision);assert.deepEqual([...h.d.querySelectorAll('.piece-card h3')].map(n=>n.textContent),daisies.map(p=>p.title));assert.equal(h.w.sessionStorage.getItem('brites-sandbox-cart'),null);assert.deepEqual(h.errors,[]);
});

for(const location of ['sort','outside'])test('search completion respects focus moved to '+location+' during the read',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.gates.set('daisy',gate);h.submit('daisy');const focused=location==='sort'?h.sort():h.outside();focused.focus();assert.equal(h.d.activeElement,focused);gate.resolve(h.response());await settle();assert.equal(h.d.activeElement,focused);assert.equal(focused.isConnected,true);assert.equal(h.store.snapshot().loading,false);
});

test('a focused Search button is retained rather than refocusing the text field',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.gates.set('daisy',gate);const input=h.input();input.value='daisy';input.dispatchEvent(new h.w.Event('input',{bubbles:true}));const submit=input.form.querySelector('button[type=submit]');submit.focus();input.form.requestSubmit(submit);assert.equal(h.d.activeElement,submit);gate.resolve(h.response());await settle();assert.equal(h.d.activeElement,submit);assert.equal(submit.isConnected,true);
});

test('failed search keeps the input available and does not override a subsequent focus choice',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.gates.set('daisy',gate);const input=h.submit('daisy');gate.resolve(h.response([],false));await settle();assert.equal(h.input(),input);assert.equal(h.d.activeElement,input);assert.equal(h.store.snapshot().loading,false);assert.match(h.d.querySelector('#storefront-status').textContent,/temporarily unavailable/);
  const next=deferred();h.gates.set('bee',next);h.submit('bee');const outside=h.outside();outside.focus();next.resolve(h.response([],false));await settle();assert.equal(h.d.activeElement,outside);
});

test('typing a replacement query cancels an older read without dropping focus or replaying old results',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.gates.set('daisy',gate);const input=h.submit('daisy');h.submit('bee');await settle();assert.equal(h.input(),input);assert.equal(h.d.activeElement,input);assert.equal(h.store.snapshot().search,'bee');gate.resolve(h.response());await settle();assert.equal(h.d.activeElement,input);assert.equal(h.store.snapshot().search,'bee');assert.deepEqual([...h.d.querySelectorAll('.piece-card h3')].map(n=>n.textContent),[initial[1].title]);
});

test('programmatic search and category/sort changes update controls without stealing outside focus',async t=>{
  const h=fixture(t);await h.ready();const input=h.input(),sort=h.sort(),outside=h.outside();outside.focus();await h.store.execute({type:'search',query:'daisy',sort:'price-desc',filter:'earrings'});assert.equal(h.d.activeElement,outside);assert.equal(h.input(),input);assert.equal(h.sort(),sort);assert.equal(input.value,'daisy');assert.equal(sort.value,'price-desc');assert.equal(h.d.querySelector('[data-filter=earrings]').getAttribute('aria-pressed'),'true');
  await h.store.execute({type:'filter',filter:'all'});assert.equal(input.value,'');assert.equal(h.d.activeElement,outside);await h.store.execute({type:'sort',sort:'title-asc'});assert.equal(sort.value,'title-asc');assert.equal(h.d.activeElement,outside);
});

test('retained controls keep selection range and install no duplicate search listeners',async t=>{
  const h=fixture(t);await h.ready();const input=h.input();input.focus();input.value='daisy';input.dispatchEvent(new h.w.Event('input',{bubbles:true}));input.setSelectionRange(1,4);input.form.requestSubmit();assert.equal(input.selectionStart,1);assert.equal(input.selectionEnd,4);await settle();h.submit('bee');await settle();h.submit('daisy');await settle();assert.equal(h.input(),input);assert.equal(h.requests.filter(u=>u.searchParams.get('q')==='daisy').length,2);assert.equal(h.requests.filter(u=>u.searchParams.get('q')==='bee').length,1);
});

test('keyboard activation of a focused product link transfers focus and reveal to the exact product heading',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.lookup(gate);const anchor=h.activateLink();assert.equal(h.d.activeElement,anchor);gate.resolve({ok:true,json:async()=>({live:true,product:clone(initial[0])})});await settle();const heading=h.d.querySelector('.product-copy h1');assert.equal(heading.textContent,initial[0].title);assert.equal(h.d.activeElement,heading);assert.equal(heading.tabIndex,-1);assert.ok(h.scrolls.some(s=>s.node===heading&&s.value.block==='start'));assert.equal(h.store.snapshot().currentHandle,initial[0].handle);
});

test('a changed keyboard navigation focus intent never overrides the newer focus choice',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.lookup(gate);h.activateLink();const outside=h.outside();outside.focus();gate.resolve({ok:true,json:async()=>({live:true,product:clone(initial[0])})});await settle();assert.equal(h.d.activeElement,outside);const heading=h.d.querySelector('.product-copy h1');assert.equal(heading.tabIndex,-1);assert.equal(h.scrolls.some(s=>s.node===heading),false);
});

test('a backgrounded page cannot reclaim focus or start a heading reveal after keyboard navigation',async t=>{
  const h=fixture(t);await h.ready();let hidden=false;Object.defineProperty(h.d,'hidden',{get:()=>hidden});const gate=deferred();h.lookup(gate);h.activateLink();hidden=true;gate.resolve({ok:true,json:async()=>({live:true,product:clone(initial[0])})});await settle();const heading=h.d.querySelector('.product-copy h1');assert.notEqual(h.d.activeElement,heading);assert.equal(h.scrolls.some(s=>s.node===heading),false);
});

test('mouse and programmatic detail navigation retain existing focus behavior',async t=>{
  const h=fixture(t);await h.ready();h.activateLink({mouse:true});await settle();assert.notEqual(h.d.activeElement,h.d.querySelector('.product-copy h1'));await h.store.execute({type:'search',query:''});const outside=h.outside();outside.focus();await h.store.execute({type:'open',handle:initial[1].handle});assert.equal(h.d.activeElement,outside);assert.equal(h.scrolls.some(s=>s.node===h.d.querySelector('.product-copy h1')),false);
});

test('a late search cannot resurrect detached controls or reclaim focus from a newly opened detail view',async t=>{
  const h=fixture(t);await h.ready();const gate=deferred();h.gates.set('daisy',gate);const old=h.submit('daisy');await h.store.execute({type:'open',handle:initial[1].handle});const select=h.d.querySelector('#piece-variant');select.focus();gate.resolve(h.response());await settle();assert.equal(h.store.snapshot().pageKind,'product');assert.equal(h.store.snapshot().currentHandle,initial[1].handle);assert.equal(old.isConnected,false);assert.equal(h.d.activeElement,select);await h.store.execute({type:'search',query:''});assert.notEqual(h.input(),old);const next=h.submit('bee');await settle();assert.equal(h.d.activeElement,next);assert.equal(h.requests.filter(u=>u.searchParams.get('q')==='bee').length,1);
});

test('keyboard Back focuses the explicit restored collection heading after the checked reset completes',async t=>{
  const h=fixture(t);await h.ready();await h.store.execute({type:'search',query:'daisy',filter:'earrings'});await h.store.execute({type:'open',handle:daisies[0].handle});const before=h.store.snapshot().discoveryRevision,gate=deferred();h.gates.set(null,gate);const back=activateBack(h);assert.equal(back.isConnected,false);assert.equal(h.store.snapshot().loading,true);assert.equal(h.store.snapshot().search,'');assert.equal(h.store.snapshot().filter,'all');assert.equal(h.d.activeElement,h.d.body);gate.resolve(h.response(initial));await settle();const heading=h.d.querySelector('.collection-heading h2');assert.equal(h.d.activeElement===heading,true,'completed keyboard Back focuses the restored collection heading');assert.equal(heading.tabIndex,-1);assert.ok(h.scrolls.some(s=>s.node===heading&&s.value.block==='start'));assert.equal(h.store.snapshot().discoveryRevision,before+1);assert.deepEqual([...h.d.querySelectorAll('.piece-card')].map(card=>card.dataset.productHandle),initial.map(p=>p.handle));assert.equal(h.w.location.search,'');assert.deepEqual(h.errors,[]);
});

for(const location of ['input','sort','outside'])test('keyboard Back respects the new '+location+' focus while its reset is pending',async t=>{
  const h=fixture(t);await h.ready();await h.store.execute({type:'open',handle:initial[0].handle});const gate=deferred();h.gates.set(null,gate);activateBack(h);const selected=location==='input'?h.input():location==='sort'?h.sort():h.outside();selected.focus();gate.resolve(h.response(initial));await settle();assert.equal(h.d.activeElement,selected);assert.equal(h.scrolls.some(s=>s.node===h.d.querySelector('.collection-heading h2')),false);assert.equal(h.store.snapshot().loading,false);
});

test('keyboard Back cannot reclaim collection focus when the pending reset completes in a hidden page',async t=>{
  const h=fixture(t);await h.ready();let hidden=false;Object.defineProperty(h.d,'hidden',{get:()=>hidden});await h.store.execute({type:'open',handle:initial[0].handle});const gate=deferred();h.gates.set(null,gate);activateBack(h);hidden=true;gate.resolve(h.response(initial));await settle();assert.notEqual(h.d.activeElement,h.d.querySelector('.collection-heading h2'));assert.equal(h.scrolls.some(s=>s.node===h.d.querySelector('.collection-heading h2')),false);
});

test('mouse, unfocused synthetic and programmatic collection reset do not acquire keyboard Back focus authority',async t=>{
  const h=fixture(t);await h.ready();await h.store.execute({type:'open',handle:initial[0].handle});activateBack(h,{mouse:true});await settle();assert.notEqual(h.d.activeElement,h.d.querySelector('.collection-heading h2'));await h.store.execute({type:'open',handle:initial[0].handle});const outside=h.outside();outside.focus();activateBack(h,{focused:false});await settle();assert.equal(h.d.activeElement,outside);await h.store.execute({type:'open',handle:initial[0].handle});outside.focus();await h.store.execute({type:'search',query:''});assert.equal(h.d.activeElement,outside);assert.equal(h.scrolls.some(s=>s.node===h.d.querySelector('.collection-heading h2')),false);
});

test('a cancelled keyboard Back cannot reclaim focus or replace a newer checked detail view',async t=>{
  const h=fixture(t);await h.ready();await h.store.execute({type:'open',handle:initial[0].handle});const gate=deferred();h.gates.set(null,gate);activateBack(h);await h.store.execute({type:'open',handle:initial[1].handle});const select=h.d.querySelector('#piece-variant');select.focus();gate.resolve(h.response(initial));await settle();assert.equal(h.d.activeElement,select);assert.equal(h.store.snapshot().pageKind,'product');assert.equal(h.store.snapshot().currentHandle,initial[1].handle);assert.equal(h.d.querySelector('.collection-heading h2'),null);
});

test('a failed keyboard Back read cannot claim a checked reset or move focus to a success target',async t=>{
  const h=fixture(t);await h.ready();await h.store.execute({type:'open',handle:initial[0].handle});const before=h.store.snapshot().discoveryRevision,gate=deferred();h.gates.set(null,gate);activateBack(h);gate.resolve(h.response([],false));await settle();assert.notEqual(h.d.activeElement,h.d.querySelector('.collection-heading h2'));assert.equal(h.store.snapshot().discoveryRevision,before);assert.equal(h.store.snapshot().loading,false);assert.match(h.d.querySelector('#storefront-status').textContent,/temporarily unavailable/);assert.equal(h.scrolls.some(s=>s.node===h.d.querySelector('.collection-heading h2')),false);
});
