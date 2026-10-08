'use strict';
// The same checked synthetic products as the guided panel fixture, mounted
// with the actual page, bridge and widget. No provider or commerce calls.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const source=Object.fromEntries(['concierge-sandbox.html','concierge-sandbox.js','brites-storefront-bridge.js','brites-concierge.js'].map(name=>[name,fs.readFileSync(require.resolve('../../'+name),'utf8')]));
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{await new Promise(setImmediate);await new Promise(setImmediate);};
function piece(index,engraving){
  const handle=index?'maple-earrings':'daisy-necklace';
  const product={id:'gid://shopify/Product/'+(39100+index),handle,title:index?'Maple Earrings':'Daisy Necklace',type:index?'Earrings':'Necklace',description:'Exact published synthetic silver and gold filled piece.',url:'https://britesjewelry.com/products/'+handle,currency:'USD',image:'https://cdn.shopify.com/'+handle+'-1.jpg',images:[1,2,3].map(n=>({url:'https://cdn.shopify.com/'+handle+'-'+n+'.jpg'})),options:[{name:'Metal Choice',values:['Sterling Silver','Gold Filled']},{name:'Chain Length',values:['16 inch','18 inch']}],variantsComplete:true,detailState:'checked',checkedAt:Date.now(),variants:['Sterling Silver','Gold Filled'].flatMap((metal,m)=>['16 inch','18 inch'].map((length,l)=>({id:'gid://shopify/ProductVariant/'+(391000+index*10+m*2+l),numericId:String(391000+index*10+m*2+l),title:metal+' / '+length,price:50+m*20+l*5,available:true,options:[{name:'Metal Choice',value:metal},{name:'Chain Length',value:length}]})))};
  if(engraving&&!index){product.options.push({name:'Engraving',values:['None','Engraved']});product.variants.forEach(v=>{v.title+=' / None';v.options.push({name:'Engraving',value:'None'});});}
  return product;
}
test('opening a listing and asking its price executes both through checked local facts',async t=>{
  const f=await fixture(t),before=f.requests.length,r=await f.w.BritesConcierge.sendShopperCommand('Open Daisy Necklace then tell me its price');
  assert.equal(r.ok,true,r.reply||r.error);assert.equal(f.store.snapshot().currentHandle,'daisy-necklace');assert.match(r.reply,/Daisy Necklace/);assert.match(r.reply,/USD 50\.00/);assert.deepEqual(clone(r.completedActions).map(a=>a.type),['open','question']);assert.equal(f.requests.length,before);assert.equal(f.errors.length,0);
});
test('four-step control and fact request quotes the final exact selection and subtotal',async t=>{
  const f=await fixture(t);await f.command('Open Daisy Necklace');await f.command('Select 16 inch');const before=f.requests.length,r=await f.command('Open the metal menu then select Gold Filled then set quantity to 2 then tell me its price');
  assert.equal(r.ok,true,r.reply||r.error);assert.equal(f.store.snapshot().productControls.quantity,2);assert.match(r.reply,/USD 70\.00/);assert.match(r.reply,/USD 140\.00/);assert.deepEqual(clone(r.completedActions).map(a=>a.type),['options','select-option','product-quantity','question']);assert.equal(f.requests.length,before);assert.equal(f.errors.length,0);
});
async function fixture(t,{engraving=false}={}){
  const errors=[],virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(source['concierge-sandbox.html'],{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole}),w=dom.window,d=w.document,rows=[piece(0,engraving),piece(1,false)],requests=[];
  t.after(()=>{w.BritesConcierge?.close();w.close();});delete d.body.dataset.catalogueSeed;
  w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};
  w.fetch=async(raw,init={})=>{
    const url=new URL(raw,w.location.href);requests.push({url,init});let body;
    if(url.pathname==='/api/growth/catalogue')body={live:true,checkedAt:Date.now(),products:rows,pageInfo:{hasNextPage:false,endCursor:null}};
    else if(url.pathname==='/api/growth/inventory')body={live:true,checkedAt:Date.now(),products:rows,inventory:{schema:1,total:2,offset:0,limit:24,loaded:2,detailsLoaded:2,ready:true,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:false,nextOffset:null}};
    else if(url.pathname==='/api/growth/product')body={live:true,checkedAt:Date.now(),product:rows.find(p=>p.handle===url.searchParams.get('handle'))};
    else if(url.pathname==='/api/growth/storefront-services')body={schema:1,guidance:{},conflicts:[],offers:{items:[]}};
    else if(['/api/growth/events','/api/concierge-voice'].includes(url.pathname))body={ok:true,enabled:true,nativeAudio:true,publicDemo:true};
    else if(url.pathname==='/api/concierge')body={live:true,reply:'Welcome to the test boutique.',products:[],meanings:[],preferences:{},preserveSelection:true};
    else throw Error('Unexpected route for a checked immediate request: '+url.pathname);
    return {ok:true,json:async()=>clone(body)};
  };
  w.eval(source['brites-storefront-bridge.js']);w.eval(source['concierge-sandbox.js']);await settle();await w.BritesSandboxStorefront.preloadInventory();
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},retry(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){},destroy(){}};}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(source['brites-concierge.js']);await settle();w.BritesConcierge.open({focus:false});await settle();
  const root=d.querySelector('brites-concierge').shadowRoot;
  return {w,d,root,rows,requests,errors,store:w.BritesSandboxStorefront,command:text=>w.BritesConcierge.sendShopperCommand(text),saved:()=>Array.from({length:w.sessionStorage.length},(_,i)=>w.sessionStorage.getItem(w.sessionStorage.key(i))).join('\n')};
}
for(const tail of ['set gift note to','fill my engraving with','write my engraved message to','save engraving text as'])test('a rejected compound private field never falls through to inference or drops its tail: '+tail,async t=>{
  const f=await fixture(t,{engraving:true});await f.command('Open Daisy Necklace');
  const before=clone(f.store.snapshot()),requests=f.requests.length,literal='PRIVATE39 Eva & Paul',result=await f.command('Open my cart then '+tail+' '+literal);
  assert.equal(result.handled,true);assert.equal(result.ok,false);assert.match(result.reply,/own request|exact content stays private/i);
  assert.deepEqual(clone(f.store.snapshot()),before,'No earlier command should execute when a compound private setter is refused');assert.equal(f.requests.length,requests);
  assert.equal(f.saved().includes(literal),false);assert.equal(JSON.stringify(result).includes(literal),false);assert.equal(f.errors.length,0);
});
for(const question of ['What materials are available then fill my engraving with','How much is this piece then write my engraving text to','Tell me about Daisy Necklace then save gift note to'])test('question-first private compounds also remain private before the factual answer path: '+question,async t=>{
  const f=await fixture(t,{engraving:true});await f.command('Open Daisy Necklace');const before=clone(f.store.snapshot()),requests=f.requests.length,literal='PRIVATE39 Exact shopper wording',result=await f.command(question+' '+literal);
  assert.equal(result.handled,true);assert.equal(result.ok,false);assert.deepEqual(clone(f.store.snapshot()),before);assert.equal(f.requests.length,requests);assert.equal(f.saved().includes(literal),false);assert.equal(JSON.stringify(result).includes(literal),false);assert.equal(f.root.textContent.includes(literal),false);
});
test('typed engraving fills only the exact enabled preview and keeps literal command words out of chat, snapshots and receipts',async t=>{
  const f=await fixture(t,{engraving:true});await f.command('Open Daisy Necklace');const before=clone(f.store.snapshot()),requests=f.requests.length,literal='PRIVATE39 Eva & Paul and open my cart then select Gold Filled';
  const result=await f.command('Set engraving text to '+literal),field=f.d.querySelector('[name=test-engraving-preview]');
  assert.equal(result.ok,true,result.reply);assert.equal(field.value,literal);assert.equal(result.snapshot.pageKind,'product');assert.equal(result.snapshot.currentHandle,'daisy-necklace');
  assert.deepEqual(clone(result.snapshot.productControls.selectedOptions),before.productControls.selectedOptions);assert.equal(result.snapshot.engravingControls.hasText,true);assert.equal(result.snapshot.productControls.reviewReady,false);assert.equal(result.snapshot.bagControls.itemCount,0);
  // Test gift preferences have their own local session key; engraving is kept
  // solely in its page draft. Neither may enter the conversational envelope.
  assert.equal(f.saved().includes(literal),false);assert.equal(JSON.stringify(result).includes(literal),false);assert.equal(f.root.textContent.includes(literal),false);assert.equal(f.requests.length,requests);
  const undo=await f.command('Undo that');assert.equal(undo.ok,true,undo.reply);assert.equal(field.value,'');assert.equal(undo.snapshot.engravingControls.hasText,false);assert.equal(f.requests.length,requests);
});
test('unavailable engraving remains a handled private-field refusal without a service or provider read',async t=>{
  const f=await fixture(t);await f.command('Open Daisy Necklace');const requests=f.requests.length,literal='PRIVATE39 Unavailable',result=await f.command('Fill my engraving with '+literal);
  assert.equal(result.handled,true);assert.equal(result.ok,false);assert.match(result.reply,/enabled published engraving field/i);assert.equal(result.snapshot.currentHandle,'daisy-necklace');assert.equal(f.saved().includes(literal),false);assert.equal(f.requests.length,requests);
});
test('literal gift notes stay in their visible local field and never enter subsequent model conversation history',async t=>{
  const f=await fixture(t);await f.command('Open Daisy Necklace');const requests=f.requests.length,literal='PRIVATE39 Eva & Paul then open my cart',result=await f.command('Set gift note to '+literal);
  assert.equal(result.ok,true,result.reply);assert.equal(f.d.querySelector('[name=note]').value,literal);assert.equal(result.snapshot.pageKind,'product');assert.equal(result.snapshot.currentHandle,'daisy-necklace');assert.equal(JSON.stringify(result).includes(literal),false);assert.equal(f.requests.length,requests);
  assert.equal(JSON.parse(f.w.sessionStorage.getItem('brites-sandbox-gift-preferences')).giftNote,literal);
  const chatSaved=Array.from({length:f.w.sessionStorage.length},(_,i)=>f.w.sessionStorage.key(i)).filter(key=>key!=='brites-sandbox-gift-preferences').map(key=>f.w.sessionStorage.getItem(key)).join('\n');assert.equal(chatSaved.includes(literal),false);assert.equal(f.root.textContent.includes(literal),false);
  await f.command('Hello');const followup=f.requests.slice(requests).find(request=>request.url.pathname==='/api/concierge');assert(followup,'The follow-up exercises the real core conversation boundary');assert.equal(followup.init.body.includes(literal),false);assert.equal(f.errors.length,0);
});
test('typed partial completion exposes exact public receipts and the next question follows the actual opened product',async t=>{
  const f=await fixture(t);const requests=f.requests.length,result=await f.command('Open Daisy Necklace then select Platinum then show my cart');
  assert.equal(result.ok,false);assert.equal(result.partial,true);assert.deepEqual(clone(result.completedActions),[{type:'open',handle:'daisy-necklace'}]);assert.equal(result.stepResults.length,1);assert.match(result.reply,/Completed 1 of 3/);assert.equal(result.snapshot.pageKind,'product');assert.equal(result.snapshot.currentHandle,'daisy-necklace');assert.equal(result.snapshot.bagControls.itemCount,0);
  assert.deepEqual([...f.root.querySelectorAll('.card')].map(card=>card.dataset.productId),[f.rows[0].id]);
  const question=await f.command('What is the price of this piece?');assert.equal(question.productFacts.handle,'daisy-necklace');assert.match(question.reply,/USD 50\.00 to USD 75\.00/);assert.equal(f.requests.length,requests);
});
test('two-piece bag review returns a separate visible confirmation receipt and never adds automatically',async t=>{
  const f=await fixture(t);await f.command('Open Daisy Necklace then select Sterling Silver then select 18 inch');const requests=f.requests.length,result=await f.command('Add two of this to my cart');
  assert.equal(result.ok,true,result.reply);assert.equal(result.partial,false);assert.equal(result.prepared,true);assert.match(result.requiredCustomerClick,/Confirm add to bag/i);assert.deepEqual(clone(result.completedActions).map(action=>action.type),['product-quantity','review-add']);assert.equal(result.snapshot.productControls.quantity,2);assert.equal(result.snapshot.productControls.itemTotalPrice,110);assert.equal(result.snapshot.bagControls.itemCount,0);assert.equal(result.cartChanged,false);
  assert.equal(f.d.querySelector('.product-review .primary').textContent,'Confirm add to test bag');assert.equal(f.requests.length,requests);
});
test('factual material questions and unknown names cannot become selections or borrow a known product identity',async t=>{
  const f=await fixture(t);await f.command('Open Daisy Necklace then select Sterling Silver then select 18 inch');const before=clone(f.store.snapshot()),requests=f.requests.length;
  for(const text of ['What does Gold Filled cost compared with Sterling Silver?','Can I get this in Gold Filled?','Which material would you recommend for everyday wear?']){const result=await f.command(text);assert.equal(result.ok,true,result.reply);assert.equal(result.productFacts.handle,'daisy-necklace');assert.deepEqual(clone(f.store.snapshot()),before);}
  for(const text of ['Open Aurora Crown Necklace','What materials does Aurora Crown Necklace have?']){const result=await f.command(text);assert.equal(result.ok,false,text);assert.equal(result.productFacts,undefined);assert.deepEqual(clone(f.store.snapshot()),before);}
  assert.equal(f.requests.length,requests);
});
test('checked material and budget discovery displays real matches while keeping complete menus available',async t=>{
  const f=await fixture(t),requests=f.requests.length;
  const empty=await f.command('Show me gold filled necklaces under $65');assert.equal(empty.ok,true,empty.reply);assert.equal(empty.snapshot.visiblePieces.length,0,'The cheaper silver variant does not satisfy a gold filled budget');assert.equal(f.d.querySelectorAll('#demo-products [data-product-handle]').length,0);
  const match=await f.command('Recommend gold filled necklaces under $75');assert.equal(match.ok,true,match.reply);assert.deepEqual(clone(match.snapshot.visiblePieces).map(p=>p.handle),['daisy-necklace']);assert.deepEqual([...f.d.querySelectorAll('#demo-products [data-product-handle]')].map(card=>card.dataset.productHandle),['daisy-necklace']);
  const open=await f.command('Open Daisy Necklace then open the Chain Length menu');assert.equal(open.ok,true,open.reply);assert.deepEqual(clone(open.snapshot.productControls.optionGroups).map(group=>group.name),['Metal Choice','Chain Length']);assert.equal(open.snapshot.productControls.optionsOpen,true);assert.equal(f.requests.length,requests);
});
test('a pending exact factual read cannot answer for a product that the shopper has left',async t=>{
  const f=await fixture(t);await f.command('Open Daisy Necklace');let release,entered;const gate=new Promise(resolve=>{release=resolve;}),started=new Promise(resolve=>{entered=resolve;});
  f.w.BritesSandboxStorefront={...f.store,async readProduct(handle){const checked=await f.store.readProduct(handle);entered();await gate;return checked;}};
  const pending=f.command('What is the price of this piece?');await started;await f.store.execute({type:'open',handle:'maple-earrings'});release();const result=await pending;
  assert.equal(result.ok,false);assert.match(result.error,/cancelled/i);assert.equal(result.snapshot.currentHandle,'maple-earrings');assert.equal(result.productFacts,undefined);assert.equal(f.errors.length,0);
});
