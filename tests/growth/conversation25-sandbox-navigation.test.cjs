'use strict';
// Same-document sandbox navigation, synthetic live catalogue and DOM only.
// The adapter below proves object/lifecycle preservation, not a provider call,
// an audible conversation, actual microphone access or GPU appearance.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../concierge-sandbox.js'),'utf8');
const widgetSource=fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){await tick();await tick();}
const clone=value=>JSON.parse(JSON.stringify(value));
function product(handle='bunny-1',id='1'){return {id:'gid://shopify/Product/'+id,handle,url:'https://britesjewelry.com/products/'+handle,title:handle==='bunny-1'?'Bunny Necklace':'Silver Cat Necklace',description:'An everyday piece with a personal story.',currency:'USD',variants:[{id:'gid://shopify/ProductVariant/'+id+'01',numericId:id+'01',title:'Sterling Silver',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]}],variantsComplete:true,suggestedVariantId:'gid://shopify/ProductVariant/'+id+'01',minPrice:54,why:'A personal symbol.'};}
const response=(body,ok=true)=>({ok,status:ok?200:503,json:async()=>clone(body)});
function deferred(){let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};}
function harness(t,options={}){
  const errors=[],console=new VirtualConsole();console.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM('<!doctype html><html lang="en"><body><header><a id="home" href="/concierge-sandbox.html">Brites</a></header><main id="shop-content"><h1>Original home</h1><div id="demo-products"></div></main><aside id="persistent">Existing application</aside></body></html>',{url:options.url||'https://growth-sandbox.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:console});
  const win=dom.window,document=win.document,network=[],events=[];
  document.addEventListener('brites-concierge:page',()=>events.push(win.location.href));
  win.fetch=async(raw,init={})=>{const url=new URL(raw,win.location.href);network.push({url,init});
    if(url.pathname==='/api/growth/catalogue')return response({products:[product()]});
    if(url.pathname==='/api/growth/product')return options.lookup?options.lookup(url.searchParams.get('handle'),network.at(-1)):response({product:product(url.searchParams.get('handle'),url.searchParams.get('handle')==='bunny-1'?'1':'2')});
    if(url.pathname==='/api/concierge'&&init.body&&JSON.parse(init.body).event)return response({});
    throw Error('Unexpected route/write: '+url.pathname);
  };
  win.eval(source);t.after(()=>win.close());
  return {win,document,network,events,errors,main:document.querySelector('#shop-content'),navigate:handle=>win.BritesSandboxNavigate(handle),productRequests:()=>network.filter(call=>call.url.pathname==='/api/growth/product')};
}

test('checked same-host navigation changes only the sandbox product view and emits page context after history update',async t=>{
  const h=harness(t);await settle();const aside=h.document.querySelector('#persistent'),length=h.win.history.length;assert.equal(await h.navigate('bunny-1'),true);assert.equal(h.win.location.origin,'https://growth-sandbox.example');assert.equal(h.win.location.pathname,'/concierge-sandbox.html');assert.equal(h.win.location.search,'?product=bunny-1');assert.equal(h.win.history.length,length+1);assert.equal(h.main.querySelector('h1').textContent,'Bunny Necklace');assert.match(h.main.textContent,/Sterling Silver|available/);assert.equal(h.document.querySelector('#persistent'),aside);assert.deepEqual(h.events,[h.win.location.href]);const request=h.productRequests()[0];assert.equal(request.url.pathname,'/api/growth/product');assert.equal(request.url.searchParams.get('handle'),'bunny-1');assert.equal(request.init.cache,'no-store');assert.equal(request.init.method,undefined);assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('URL/history and existing content remain unchanged while the live product check is pending',async t=>{
  const pending=deferred(),h=harness(t,{lookup:()=>pending.promise});await settle();const before=h.win.location.href,html=h.main.innerHTML,length=h.win.history.length,lookup=h.navigate('bunny-1');await settle();assert.equal(h.win.location.href,before);assert.equal(h.main.innerHTML,html);assert.equal(h.win.history.length,length);assert.deepEqual(h.events,[]);pending.resolve(response({product:product()}));assert.equal(await lookup,true);assert.equal(h.main.querySelector('h1').textContent,'Bunny Necklace');
});

for(const handle of ['', '../bunny', 'bunny/1', 'bunny?admin=1', 'bunny#1', 'https://evil.example/bunny', 'bunny%2f1', 'bunny_1', 'a'.repeat(181)])test('invalid sandbox product handle is refused before fetch: '+JSON.stringify(handle.slice(0,40)),async t=>{
  const h=harness(t);await settle();const before=h.main.innerHTML,url=h.win.location.href;assert.equal(await h.navigate(handle),false);assert.equal(h.productRequests().length,0);assert.equal(h.main.innerHTML,before);assert.equal(h.win.location.href,url);assert.deepEqual(h.events,[]);
});

const malformed=[
  ['wrong returned handle',{...product(),handle:'silver-cat'}],
  ['missing product ID',(()=>{const p=product();delete p.id;return p;})()],
  ['non-Shopify product ID',{...product(),id:'private-account-1'}],
  ['variant-shaped product ID',{...product(),id:'gid://shopify/ProductVariant/1'}],
  ['wrong product URL',{...product(),url:'https://britesjewelry.com/products/silver-cat'}],
  ['missing description',(()=>{const p=product();delete p.description;return p;})()],
  ['invalid currency',{...product(),currency:'USD<script>'}],
  ['invalid variant price',{...product(),variants:[{...product().variants[0],price:null}]}]
];
for(const [label,p]of malformed)test('malformed live catalogue product leaves view/history intact: '+label,async t=>{
  const h=harness(t,{lookup:()=>response({product:p})});await settle();const before=h.main.innerHTML,url=h.win.location.href,length=h.win.history.length;assert.equal(await h.navigate('bunny-1'),false);assert.equal(h.main.innerHTML,before);assert.equal(h.win.location.href,url);assert.equal(h.win.history.length,length);assert.deepEqual(h.events,[]);
});

test('latest requested product wins an out-of-order live response race',async t=>{
  const bunny=deferred(),cat=deferred(),h=harness(t,{lookup:handle=>handle==='bunny-1'?bunny.promise:cat.promise});await settle();const first=h.navigate('bunny-1'),second=h.navigate('silver-cat');cat.resolve(response({product:product('silver-cat','2')}));assert.equal(await second,true);const url=h.win.location.href,length=h.win.history.length;assert.match(h.main.textContent,/Silver Cat Necklace/);bunny.resolve(response({product:product()}));assert.equal(await first,false);assert.equal(h.win.location.href,url);assert.equal(h.win.history.length,length);assert.equal(h.events.length,1);assert.doesNotMatch(h.main.textContent,/Bunny Necklace/);
});

test('an older successful lookup cannot navigate after the latest requested product failed',async t=>{
  const pending=deferred(),h=harness(t,{lookup:handle=>handle==='bunny-1'?pending.promise:response({error:'Latest product unavailable'},false)});await settle();const before=h.main.innerHTML,url=h.win.location.href,old=h.navigate('bunny-1');assert.equal(await h.navigate('silver-cat'),false);pending.resolve(response({product:product()}));assert.equal(await old,false);assert.equal(h.main.innerHTML,before);assert.equal(h.win.location.href,url);assert.deepEqual(h.events,[]);
});

test('initial checked product URL renders without pushing another history entry',async t=>{
  const h=harness(t,{url:'https://growth-sandbox.example/concierge-sandbox.html?product=bunny-1'}),length=h.win.history.length;await settle();assert.equal(h.main.querySelector('h1').textContent,'Bunny Necklace');assert.equal(h.win.history.length,length);assert.equal(h.productRequests().length,1);assert.deepEqual(h.events,[h.win.location.href]);
});

test('catalogue text is rendered as text, and real product links open outside the sandbox with opener isolation',async t=>{
  const p={...product(),title:'<script>window.catalogueInjected=true</script>',description:'<img src=x onerror="window.catalogueInjected=true">'},h=harness(t,{lookup:()=>response({product:p})});await settle();assert.equal(await h.navigate('bunny-1'),true);assert.equal(h.main.querySelector('h1').textContent,p.title);assert.equal(h.win.catalogueInjected,undefined);assert.equal(h.main.querySelector('script'),null);assert.equal(h.main.querySelector('img'),null);const link=h.main.querySelector('a');assert.equal(link.href,p.url);assert.equal(link.target,'_blank');assert.equal(link.rel,'noopener noreferrer');
});

for(const kind of ['non-ok response','request rejected','invalid JSON'])test('failed live lookup does not navigate or destroy existing content: '+kind,async t=>{
  const h=harness(t,{lookup:()=>kind==='non-ok response'?response({error:'Unavailable'},false):kind==='request rejected'?Promise.reject(Error('Offline')):{ok:true,json:async()=>{throw Error('Malformed JSON');}}});await settle();const before=h.main.innerHTML,url=h.win.location.href;assert.equal(await h.navigate('bunny-1'),false);assert.equal(h.main.innerHTML,before);assert.equal(h.win.location.href,url);assert.deepEqual(h.events,[]);
});

test('nested same-host product anchor is delegated without a document reload',async t=>{
  const h=harness(t);await settle();const link=h.document.querySelector('#demo-products a'),nested=link.querySelector('h3');const click=new h.win.MouseEvent('click',{bubbles:true,cancelable:true,button:0});nested.dispatchEvent(click);assert.equal(click.defaultPrevented,true);await settle();assert.equal(h.win.location.search,'?product=bunny-1');assert.equal(h.productRequests().length,1);assert.equal(h.events.length,1);assert.ok(!h.errors.some(error=>/navigation/i.test(error.message)));
});

test('delegation ignores external host, non-product route and modified/new-tab link gestures',async t=>{
  const h=harness(t);await settle();const variants=[{href:'https://other.example/concierge-sandbox.html?product=bunny-1'},{href:'/brites-growth.html?product=bunny-1'},{href:'/concierge-sandbox.html?cart=1'},{href:'/concierge-sandbox.html?product=bunny-1',target:'_blank'},{href:'/concierge-sandbox.html?product=bunny-1',ctrlKey:true},{href:'/concierge-sandbox.html?product=bunny-1',metaKey:true},{href:'/concierge-sandbox.html?product=bunny-1',button:1}];for(const valueof of variants){const link=h.document.createElement('a');link.href=valueof.href;if(valueof.target)link.target=valueof.target;h.document.body.appendChild(link);const event=new h.win.MouseEvent('click',{bubbles:true,cancelable:true,button:valueof.button||0,ctrlKey:valueof.ctrlKey||false,metaKey:valueof.metaKey||false});link.dispatchEvent(event);}await settle();assert.equal(h.productRequests().length,0);assert.deepEqual(h.events,[]);
});

test('product popstate checks the destination and does not push another history entry',async t=>{
  const h=harness(t);await settle();assert.equal(await h.navigate('bunny-1'),true);assert.equal(await h.navigate('silver-cat'),true);h.win.history.replaceState({},'','/concierge-sandbox.html?product=bunny-1');const length=h.win.history.length;h.win.dispatchEvent(new h.win.PopStateEvent('popstate'));await settle();assert.equal(h.main.querySelector('h1').textContent,'Bunny Necklace');assert.equal(h.win.history.length,length);assert.equal(h.events.at(-1),h.win.location.href);assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);
});

test('actual widget keeps its explicitly started synthetic voice instance and updates page context through product navigation',async t=>{
  const h=harness(t);await settle();const counts={create:0,start:0,stop:0,dispose:0},contexts=[],current=h.document.createElement('script');current.src='https://growth-sandbox.example/brites-concierge.js';current.dataset.sandbox='true';Object.defineProperty(h.document,'currentScript',{get:()=>current});h.win.BritesConciergeAvatar={create:()=>({setState(){},setEmotion(){},setVisible(){},setPaused(){},setLevel(){},clearFocus(){},focusProduct(){},cue(){},triggerGreeting(){},retry(){},destroy(){}})};h.win.BritesConciergeVoice={create:()=>{counts.create++;return {start:async()=>{counts.start++;return true;},stop:async()=>{counts.stop++;},dispose:async()=>{counts.dispose++;},updateContext:value=>contexts.push(clone(value))};}};h.win.eval(widgetSource);const host=h.document.querySelector('brites-concierge'),root=host.shadowRoot;h.win.BritesConcierge.open();await settle();Array.from(root.querySelectorAll('button')).find(b=>b.textContent==='Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]').dispatchEvent(new h.win.Event('load'));await settle();const before={...counts};assert.equal(before.create,1);assert.equal(before.start,1);assert.equal(await h.navigate('bunny-1'),true);assert.equal(h.document.querySelector('brites-concierge'),host);assert.deepEqual(counts,before);assert.equal(root.querySelector('.voice-state').dataset.active,'true');assert.equal(contexts.at(-1).pageKind,'product');assert.equal(contexts.at(-1).currentHandle,'bunny-1');assert.equal(h.productRequests().length,2,'one checked view read and one widget page-piece read');assert.ok(h.productRequests().every(call=>call.url.searchParams.get('handle')==='bunny-1'&&call.init.method===undefined&&call.init.cache==='no-store'));assert.ok(h.network.every(call=>['/api/growth/catalogue','/api/growth/product','/api/concierge'].includes(call.url.pathname)));assert.equal(h.win.sessionStorage.getItem('brites-sandbox-cart'),null);h.win.BritesConcierge.close();
});
