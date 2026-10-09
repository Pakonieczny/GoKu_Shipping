'use strict';
// Real typed widget, host and bridge with synthetic HTTP/catalogue data.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const clone=value=>JSON.parse(JSON.stringify(value)),settle=async()=>{for(let i=0;i<10;i++)await new Promise(setImmediate);};
async function fixture(t){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM(fs.readFileSync(require.resolve('../../concierge-sandbox.html'),'utf8'),{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window,d=w.document;
  const products=Array.from({length:120},(_,i)=>{const title=i<8?'Cat Stud Earrings '+i:'Circle Necklace '+i,handle=title.toLowerCase().replace(/ /g,'-');return {id:'gid://shopify/Product/'+(48000+i),handle,title,type:i<8?'Earrings':'Necklace',url:'https://britesjewelry.com/products/'+handle,currency:'USD',image:'https://cdn.shopify.com/'+handle+'.jpg',description:'Synthetic acceptance listing.',options:[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled']}],variants:['Sterling Silver','14k Gold Filled'].map((metal,n)=>({id:'gid://shopify/ProductVariant/'+(480000+i*10+n),numericId:String(480000+i*10+n),title:metal,price:20+n,available:true,options:[{name:'Metal Choice',value:metal}]})),variantsComplete:true,checkedAt:Date.now(),detailState:'checked',storeCategories:[i<8?'stud-earrings':'regular-necklaces']};});
  let chatCalls=0,policy=null;const actions=[],presentations=[];
  w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};w.HTMLMediaElement.prototype.play=async function(){};w.HTMLMediaElement.prototype.pause=function(){};
  w.fetch=async(raw,init={})=>{const url=new URL(raw,w.location.href),body=init.body?JSON.parse(init.body):null;let value;
    if(url.pathname==='/api/growth/catalogue')value={live:true,checkedAt:Date.now(),products:products.map(p=>({...p,detailState:'unconfirmed',variants:p.variants.map(v=>({...v,available:false,availabilityKnown:false}))})),pageInfo:{hasNextPage:false,endCursor:null},seed:{schema:1,target:120,minimum:100,loaded:120,complete:true,partial:false,sourcePages:1,categoryCounts:{'stud-earrings':8,'regular-necklaces':112},unfilledCategories:[]}};
    else if(url.pathname==='/api/growth/inventory'){const offset=Number(url.searchParams.get('offset')),rows=products.slice(offset,offset+24);value={live:true,checkedAt:Date.now(),products:rows,inventory:{schema:1,total:120,offset,limit:24,loaded:rows.length,detailsLoaded:rows.length,ready:false,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:offset+24<120,nextOffset:offset+24<120?offset+24:null}};}
    else if(url.pathname==='/api/growth/product')value={live:true,checkedAt:Date.now(),product:products.find(p=>p.handle===url.searchParams.get('handle'))};
    else if(url.pathname==='/api/growth/events'||url.pathname==='/api/concierge'&&body?.event)value={ok:true};
    else if(url.pathname==='/api/concierge-voice')value={enabled:false,nativeAudio:false};
    else if(url.pathname==='/api/concierge'){chatCalls++;throw Error('Unchecked provider fallback is forbidden for this typed refresh regression');}
    else throw Error('Unexpected fixture route '+url.pathname);
    return {ok:true,status:200,json:async()=>clone(value)};
  };
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){}};}};
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js'])w.eval(fs.readFileSync(require.resolve('../../'+name),'utf8'));await settle();const store=w.BritesSandboxStorefront;await store.preloadInventory();
  const read=store.readProduct,execute=store.execute,present=store.presentProducts;w.BritesSandboxStorefront={...store,async readProduct(handle){const out=await read(handle);return policy?policy(handle,clone(out)):out;},async execute(action,options){actions.push(clone(action));return execute(action,options);},presentProducts(rows,options){presentations.push(rows.map(p=>p.handle));return present(rows,options);}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(fs.readFileSync(require.resolve('../../brites-concierge.js'),'utf8'));w.BritesConcierge.open({focus:false});
  t.after(()=>{w.BritesConcierge.close();w.close();assert.deepEqual(errors,[]);});
  return {w,d,store,actions,presentations,say:text=>w.BritesConcierge.sendShopperCommand(text),setPolicy(value){policy=value;},get chatCalls(){return chatCalls;}};
}
for(const stage of ['initial checked read','presentation revalidation'])test('typed refinement stays on the collection when '+stage+' expires during refresh',async t=>{
  const f=await fixture(t),initial=await f.say('Show animal earrings silver under USD50');assert.equal(initial.ok,true);const before=clone(f.store.snapshot()),cards=[...f.d.querySelectorAll('#demo-products .piece-card')].map(e=>e.dataset.productHandle),actions=f.actions.length,presentations=f.presentations.length;
  let reads=0;f.setPolicy((handle,out)=>{reads++;if(stage==='presentation revalidation'&&reads<=6)return out;const checkedAt=out.checkedAt-300001;return {...out,checkedAt,product:{...out.product,checkedAt}};});
  const failed=await f.say('Any material');assert.equal(f.chatCalls,0,'An expired local result must not fall through to provider shopping');assert.equal(failed.ok,false);assert.equal(failed.handled,true);assert.match(failed.reply,/confirm|try again/i);
  assert.equal(f.store.snapshot().pageKind,'collection');assert.equal(f.store.snapshot().search,before.search);assert.deepEqual([...f.d.querySelectorAll('#demo-products .piece-card')].map(e=>e.dataset.productHandle),cards);assert.equal(f.actions.length,actions);assert.equal(f.presentations.length,presentations);
  f.setPolicy(null);const recovered=await f.say('Any material');assert.equal(recovered.ok,true);assert.equal(f.chatCalls,0);assert.equal(f.store.snapshot().search,'animal earrings under 50 USD');assert.equal(f.store.snapshot().pageKind,'collection');
});
