'use strict';
// Real widget, host, bridge and native client; only the network/media/provider
// are synthetic. No typed command is invoked or permitted by this fixture.
const assert=require('node:assert/strict'),fs=require('node:fs');
const {JSDOM,VirtualConsole}=require('jsdom');
const Voice=require('../../brites-concierge-voice.js');
const names=['concierge-sandbox.html','brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js','brites-concierge.js'];
const source=Object.fromEntries(names.map(name=>[name,fs.readFileSync(require.resolve('../../'+name),'utf8')]));
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{for(let n=0;n<10;n++)await new Promise(setImmediate);};
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const categories=['regular-necklaces','beady-necklaces','stud-earrings','hoop-earrings','charm-only'];
// Every listing and price below is synthetic acceptance data, not live stock.
// Literal expected cohorts are defined independently from production matching.
const ANIMAL_ROWS=[
  ['Butterfly Stud Earrings','stud-earrings',20,60],
  ['Cat Stud Earrings','stud-earrings',25,55],
  ['Bird Stud Earrings','stud-earrings',30,65],
  ['Owl Hoop Earrings','hoop-earrings',35,65],
  ['Dog Stud Earrings','stud-earrings',40,70],
  ['Fox Hoop Earrings','hoop-earrings',45,75],
  ['Bunny Stud Earrings','stud-earrings',55,35],
  ['Hummingbird Hoop Earrings','hoop-earrings',60,30],
  ['Dragonfly Stud Earrings','stud-earrings',65,35],
  ['Dolphin Hoop Earrings','hoop-earrings',70,40],
  ['Elephant Necklace','regular-necklaces',22,60],
  ['Turtle Necklace','regular-necklaces',27,60],
  ['Whale Necklace','regular-necklaces',32,60],
  ['Bee Necklace','regular-necklaces',37,60],
  ['Bird Bracelet','bracelets',42,60],
  ['Cat Bracelet','bracelets',47,60],
  ['Fox Charm','charm-only',52,60],
  ['Owl Ring','rings',57,60]
];
const FLOWER_ROWS=[['Rose Stud Earrings','stud-earrings',18,70],['Daisy Necklace','regular-necklaces',28,80],['Lotus Hoop Earrings','hoop-earrings',85,60]];
const GUARD_ROWS=[['Rabbit Necklace','regular-necklaces',12,55,'held'],['Wolf Stud Earrings','stud-earrings',13,56,'unknown'],['Tiger Hoop Earrings','hoop-earrings',14,57,'unavailable']];
const EXTRA_ANIMAL_ROWS=Array.from({length:24},(_,i)=>['Dog Necklace '+(i+1),'regular-necklaces',90+i,150+i]);
const slug=title=>title.toLowerCase().replace(/[^a-z0-9]+/g,'-');
const ANIMALS=ANIMAL_ROWS.map(row=>slug(row[0])),FLOWERS=FLOWER_ROWS.map(row=>slug(row[0]));
const EARRINGS=ANIMALS.slice(0,10),SILVER_BUDGET=EARRINGS.slice(0,6),NO_BIRDS=[EARRINGS[0],EARRINGS[1],EARRINGS[4],EARRINGS[5]],ANY_MATERIAL_NO_BIRDS=NO_BIRDS.concat(EARRINGS[6],EARRINGS[8],EARRINGS[9]);
const LARGE_ANIMALS=ANIMALS.concat(EXTRA_ANIMAL_ROWS.map(row=>slug(row[0])));
function publishedProduct(index,includeGuardListings=false,largeAnimalListings=false){
  const row=ANIMAL_ROWS[index]||FLOWER_ROWS[index-18]||(includeGuardListings?GUARD_ROWS[index-21]:null)||(largeAnimalListings?EXTRA_ANIMAL_ROWS[index-(includeGuardListings?24:21)]:null),category=row?.[1]||categories[index%5];
  const title=row?.[0]||'Circle '+index+' '+(category.includes('earrings')?'Earrings':category==='charm-only'?'Charm':'Necklace'),handle=slug(title);
  const type=category.includes('earrings')?'Earrings':category==='bracelets'?'Bracelet':category==='rings'?'Ring':category==='charm-only'?'Charm':'Necklace';
  const options=[{name:'Metal Choice',values:['Sterling Silver','14k Gold Filled']}];
  const variants=options[0].values.map((metal,n)=>({id:'gid://shopify/ProductVariant/'+(470000+index*10+n),numericId:String(470000+index*10+n),title:metal,price:row?row[n+2]:90+index+n*10,available:row?.[4]!=='unavailable',...(row?.[4]==='unknown'?{availabilityKnown:false}:{}),options:[{name:'Metal Choice',value:metal}]}));
  return {id:'gid://shopify/Product/'+(47000+index),handle,title,type,url:'https://britesjewelry.com/products/'+handle,currency:'USD',image:'https://cdn.shopify.com/'+handle+'.jpg',description:'This synthetic listing has a 12mm charm. Published materials are Sterling Silver and 14k Gold Filled.',options,variants,variantsComplete:true,checkedAt:Date.now(),detailState:'checked',...(row?.[4]==='held'?{cartHold:true,recommendationHold:true}:{}),storeCategories:[category]};
}
async function fixture(t,{providerFallback=false,query='',savedCart=null,holdInventory=false,includeGuardListings=false,largeAnimalListings=false}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(source['concierge-sandbox.html'],{url:'https://preview.example/concierge-sandbox.html'+query,runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,products=Array.from({length:120},(_,i)=>publishedProduct(i,includeGuardListings,largeAnimalListings)),requests=[],packets=[],controls=[],presentations=[],reads=[],toolCalls=[],finals=[];
  if(savedCart){w.sessionStorage.setItem('brites-sandbox-cart',JSON.stringify(savedCart(products)));w.sessionStorage.setItem('brites-sandbox-product-identities',JSON.stringify(products.map(p=>[p.id,p.handle])));}
  let releaseInventory;const inventoryGate=holdInventory?new Promise(resolve=>releaseInventory=resolve):Promise.resolve();
  let channel,client,turn=0,providerSerial=0,typedAttempts=0,chatAttempts=0,lastReceiptStart=0,clockOffset=0,readPolicy=null,providerMode=providerFallback;
  const clock=()=>Date.now()+clockOffset;w.Date.now=clock;
  t.after(async()=>{w.BritesConcierge?.close();await client?.dispose();w.close();assert.deepEqual(errors,[]);assert.equal(typedAttempts,0);assert.equal(chatAttempts,0);});
  w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};w.HTMLMediaElement.prototype.play=async function(){};w.HTMLMediaElement.prototype.pause=function(){};
  const fetch=async(raw,init={})=>{
    const url=new URL(raw,w.location.href),body=init.body?JSON.parse(init.body):null;requests.push({url,init,body});let value;
    if(url.pathname==='/api/growth/catalogue')value={live:true,checkedAt:Date.now(),products:products.map(p=>({...p,detailState:'unconfirmed',variants:p.variants.map(v=>({...v,available:false,availabilityKnown:false}))})),pageInfo:{hasNextPage:false,endCursor:null},seed:{schema:1,target:120,minimum:100,loaded:120,complete:true,partial:false,sourcePages:1,categoryCounts:Object.fromEntries(categories.map(c=>[c,products.filter(p=>p.storeCategories.includes(c)).length])),unfilledCategories:[]}};
    else if(url.pathname==='/api/growth/inventory'){
      await inventoryGate;
      const offset=Number(url.searchParams.get('offset')),rows=products.slice(offset,offset+24);value={live:true,checkedAt:Date.now(),products:rows,inventory:{schema:1,total:120,offset,limit:24,loaded:rows.length,detailsLoaded:rows.length,ready:false,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:offset+24<120,nextOffset:offset+24<120?offset+24:null}};
    }else if(url.pathname==='/api/growth/product')value={live:true,checkedAt:Date.now(),product:products.find(p=>p.handle===url.searchParams.get('handle'))};
    else if(url.pathname==='/api/concierge-voice')value=body.action==='capabilities'?{enabled:true,nativeAudio:true,publicDemo:true}:body.action==='start'?{sdp:SDP,stopToken:'native-search47-synthetic-only',maxDurationMs:120000}:{stopped:true};
    else if(url.pathname==='/api/growth/events')value={ok:true};
    else if(url.pathname==='/api/concierge'&&body?.event)value={ok:true};
    else if(url.pathname==='/api/concierge'){chatAttempts++;throw Error('Typed or ordinary chat route is disabled in native acceptance47');}
    else throw Error('Unexpected native-only fixture route '+url.pathname);
    return {ok:true,status:200,json:async()=>clone(value)};
  };w.fetch=fetch;
  class Peer{
    constructor(){this.iceGatheringState='complete';}addTrack(){}close(){}
    createDataChannel(){channel={readyState:'connecting',send:value=>packets.push(JSON.parse(value)),close(){}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){channel.readyState='open';channel.onopen();}
  }
  const runtime={document:d,location:w.location,navigator:{mediaDevices:{getUserMedia:async()=>{const track={readyState:'live',stop(){this.readyState='ended';},addEventListener(){},removeEventListener(){}};return {getTracks:()=>[track],getAudioTracks:()=>[track]};}}},RTCPeerConnection:Peer,AbortController:w.AbortController,fetch,setTimeout:w.setTimeout.bind(w),clearTimeout:w.clearTimeout.bind(w),addEventListener:w.addEventListener.bind(w),removeEventListener:w.removeEventListener.bind(w)};
  w.BritesConciergeVoice={publicContext:Voice.publicContext,create(options){
    client=Voice.create({...options,runtime,greeting:false,
      onFinalizedTurn:async input=>{if(providerMode)return {handled:false};const result=await options.onFinalizedTurn(input);finals.push({input,result});return result;},
      onTool:async(args,context)=>{const call={args:clone(args),context};toolCalls.push(call);call.result=await options.onTool(args,context);return call.result;}
    });return client;
  }};
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){}};}};
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js'])w.eval(source[name]);await settle();const store=w.BritesSandboxStorefront;if(!holdInventory)await store.preloadInventory();
  const execute=store.execute,present=store.presentProducts,read=store.readProduct;w.BritesSandboxStorefront={...store,async execute(action,options){const call={action:clone(action)};controls.push(call);call.result=await execute(action,options);return call.result;},presentProducts(products,options){const call={products:clone(products),options:clone(options||{})};presentations.push(call);call.result=present(products,options);return call.result;},async readProduct(handle){const result=await read(handle);reads.push({handle,result:clone(result)});return readPolicy?readPolicy(handle,clone(result)):result;}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(source['brites-concierge.js']);const root=d.querySelector('brites-concierge').shadowRoot;
  w.BritesConcierge.sendShopperCommand=()=>{typedAttempts++;throw Error('Typed shopper handler is forbidden in native acceptance47');};root.querySelector('.composer form').onsubmit=()=>{typedAttempts++;throw Error('Typed composer is forbidden in native acceptance47');};
  w.BritesConcierge.open({focus:false});[...root.querySelectorAll('button')].find(b=>b.textContent==='Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();assert.equal(client.state,'listening');
  const emit=event=>channel.onmessage({data:JSON.stringify(event)}),responses=()=>packets.filter(p=>p.type==='response.create'),receipts=()=>packets.filter(p=>p.item?.content?.[0]?.text?.startsWith('Customer reply for this finalized shopper request.'));
  function begin(){const itemId='native47-input-'+(++turn);emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});return {itemId,version:turn+1};}
  const commit=input=>emit({type:'input_audio_buffer.committed',item_id:input.itemId}),final=(input,text)=>emit({type:'conversation.item.input_audio_transcription.completed',item_id:input.itemId,transcript:text});
  async function say(text){lastReceiptStart=receipts().length;const input=begin();commit(input);final(input,text);await settle();return {input,result:finals.find(f=>f.input.inputItemId===input.itemId)?.result};}
  async function tool(name,args){const request=responses().at(-1);assert.ok(request);const id='native47-provider-'+(++providerSerial),callId=id+'-tool';emit({type:'response.created',response:{id,metadata:request.response.metadata}});emit({type:'response.function_call_arguments.done',response_id:id,call_id:callId,name,arguments:JSON.stringify(args)});await settle();const packet=packets.find(p=>p.item?.type==='function_call_output'&&p.item.call_id===callId);assert.ok(packet);const host=toolCalls.find(c=>c.context.callId===callId)?.result,spoken=JSON.parse(packet.item.output);if(['control_storefront','prepare_jewellery_action'].includes(name)||host?.cartChanged===true){assert.ok(Object.keys(spoken).length>0);assert.ok(Object.keys(spoken).every(key=>['reply','customerMessage'].includes(key)),'The native provider must receive only the shopper reply for completed controls: '+Object.keys(spoken).join(', '));}return {host,spoken};}
  const choices=()=>Object.fromEntries((store.snapshot().productControls?.selectedOptions||[]).map(o=>[o.name,o.value])),cart=()=>JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart')||'[]');
  function lastSpoken(){const current=receipts().slice(lastReceiptStart);assert.equal(current.length,1,'this finalized native input must emit exactly one new shopper-only receipt');const text=current[0].item.content[0].text;const value=JSON.parse(text.slice(text.indexOf('{')));assert.deepEqual(Object.keys(value),['reply']);return value.reply;}
  return {w,d,root,store,products,requests,packets,controls,presentations,reads,toolCalls,finals,emit,responses,receipts,begin,commit,final,say,tool,choices,cart,lastSpoken,setReadPolicy(value){readPolicy=value;},setProviderFallback(value){providerMode=value===true;},advanceTime(ms){clockOffset+=ms;},async refreshInventory(){for(const p of products)p.checkedAt=clock();await store.preloadInventory({retry:true});await settle();},async releaseInventory(){releaseInventory?.();await store.preloadInventory();await settle();},assertNativeOnly(){assert.equal(typedAttempts,0);assert.equal(chatAttempts,0);},get client(){return client;}};
}
module.exports={fixture,clone,settle,ANIMALS,LARGE_ANIMALS,FLOWERS,EARRINGS,SILVER_BUDGET,NO_BIRDS,ANY_MATERIAL_NO_BIRDS};
