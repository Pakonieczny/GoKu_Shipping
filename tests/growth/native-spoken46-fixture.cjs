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
const INITIAL='lowercase-initial',LEAF='leaf-necklace-dainty';
// Exact published DOM description, including spaced headers and encoding.
const INITIAL_DESCRIPTION="Elevate your personal style with our Lowercase Letter Necklace, a subtle yet elegant accessory that allows you to carry the essence of individuality wherever you go. Meticulously handcrafted in your choice of high-quality materials‚ gold filled, sterling silver, or rose gold filled‚ this necklace features a delicately designed lowercase letter charm, making it the perfect piece for those who appreciate the beauty of simplicity and personalized adornment. - We use the Highest Quality materials from the US and Italy. - Your purchase will come packaged in a lovely Jewelry Box ----------------------------------- O R D E R ‚Ä¢ D E T A I L S 6 - 9mm lowercase letter pendant (depending on letter) Available necklace lengths are 14 - 18 inches ------------------------------------ P E R S O N A L I Z E Y O U R O R D E R Custom Back Engraving available, please leave us a Personalization note Not sure exactly what size to order? Fine tune the fit with an extender! ------------------------------------ P A C K A G I N G Your purchase will come beautifully packaged. If you are ordering for a gift and would like each piece to be packaged separately please let me know. If this purchase is a gift, and you would like us to include a handwritten message, leave a note in the \"gift message\" box at checkout. ------------------------------------ E X P E D I T E D S H I P P I N G You will be able to choose faster shipping options in the drop down menu when you check out. Ship times do NOT include production times (1-3 business days). However, if you select expedited shipping, we will try to get your order done faster. ------------------------------------ E X P L O R E O U R S H O P Don't forget to check out the rest of our shop! We specialize in making handmade custom jewelry for every occasion. We take pride in making sure each order is made exactly to the customers specifications. We love collaborating with our customers to create special and unique pieces for themselves and their loved ones. Please don't hesitate to contact us with any questions you have. Happy Shopping :)";
const metals=['Sterling Silver','14k Gold Filled','14k Rose Gold Filled','14k Solid Gold'];
function publishedProduct(index){
  const initial=index===0,leaf=index===1,handle=initial?INITIAL:leaf?LEAF:'native-spoken46-'+index,title=initial?'Lowercase Initial Necklace':leaf?'Leaf Pendant Cable Necklace':'Native Spoken '+index;
  const lengths=leaf?['14 Inch','16 Inch','18 Inch','20 Inch']:['14 inch','16 inch','18 inch','20 inch'];
  const options=[{name:'Metal Choice',values:metals},{name:'Necklace Length',values:lengths},{name:'Engraving',values:['None','Engraved']}];
  const variants=[];let serial=0;
  for(const [m,metal] of metals.entries())for(const [l,length] of lengths.entries())for(const engraving of ['None','Engraved']){
    const id=460000+index*100+(++serial),opts=[{name:'Metal Choice',value:metal},{name:'Necklace Length',value:length},{name:'Engraving',value:engraving}];
    // USD59 Initial Gold Filled/16 inch/None and USD316 Leaf Solid Gold/
    // 16 Inch/Engraved are the reported literal samples. Other amounts are
    // synthetic fixture prices and are not claims about live inventory.
    let price=(initial?[49,59,59,249]:leaf?[70,80,80,300]:[40,50,50,200])[m]+(l>1?(l-1)*5:0)+(engraving==='Engraved'?16:0);
    if(leaf&&metal==='14k Solid Gold'&&length==='16 Inch'&&engraving==='Engraved')price=316;
    variants.push({id:'gid://shopify/ProductVariant/'+id,numericId:String(id),title:opts.map(o=>o.value).join(' / '),price,available:true,options:opts});
  }
  const description=initial?INITIAL_DESCRIPTION:'The published leaf pendant is sold as a necklace with your selected material and chain length. PACKAGING Your purchase will come beautifully packaged.';
  return {id:'gid://shopify/Product/'+(46000+index),handle,title,type:initial||leaf?'Necklace':index%5===4?'Charm':index%5<2?'Necklace':'Earrings',url:'https://britesjewelry.com/products/'+handle,currency:'USD',image:'https://cdn.shopify.com/'+handle+'.jpg',description,options,variants,variantsComplete:true,checkedAt:Date.now(),detailState:'checked',storeCategories:[categories[index%5]]};
}
async function fixture(t,{providerFallback=false,query='',savedCart=null,holdInventory=false}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error));
  const dom=new JSDOM(source['concierge-sandbox.html'],{url:'https://preview.example/concierge-sandbox.html'+query,runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,products=Array.from({length:120},(_,i)=>publishedProduct(i)),requests=[],packets=[],controls=[],toolCalls=[],finals=[];
  if(savedCart){w.sessionStorage.setItem('brites-sandbox-cart',JSON.stringify(savedCart(products)));w.sessionStorage.setItem('brites-sandbox-product-identities',JSON.stringify(products.map(p=>[p.id,p.handle])));}
  let releaseInventory;const inventoryGate=holdInventory?new Promise(resolve=>releaseInventory=resolve):Promise.resolve();
  let channel,client,turn=0,providerSerial=0,typedAttempts=0,chatAttempts=0,lastReceiptStart=0;
  t.after(async()=>{w.BritesConcierge?.close();await client?.dispose();w.close();assert.deepEqual(errors,[]);assert.equal(typedAttempts,0);assert.equal(chatAttempts,0);});
  w.matchMedia=()=>({matches:false,addEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};w.HTMLMediaElement.prototype.play=async function(){};w.HTMLMediaElement.prototype.pause=function(){};
  const fetch=async(raw,init={})=>{
    const url=new URL(raw,w.location.href),body=init.body?JSON.parse(init.body):null;requests.push({url,init,body});let value;
    if(url.pathname==='/api/growth/catalogue')value={live:true,checkedAt:Date.now(),products:products.map(p=>({...p,detailState:'unconfirmed',variants:p.variants.map(v=>({...v,available:false,availabilityKnown:false}))})),pageInfo:{hasNextPage:false,endCursor:null},seed:{schema:1,target:120,minimum:100,loaded:120,complete:true,partial:false,sourcePages:1,categoryCounts:Object.fromEntries(categories.map(c=>[c,24])),unfilledCategories:[]}};
    else if(url.pathname==='/api/growth/inventory'){
      await inventoryGate;
      const offset=Number(url.searchParams.get('offset')),rows=products.slice(offset,offset+24);value={live:true,checkedAt:Date.now(),products:rows,inventory:{schema:1,total:120,offset,limit:24,loaded:rows.length,detailsLoaded:rows.length,ready:false,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:offset+24<120,nextOffset:offset+24<120?offset+24:null}};
    }else if(url.pathname==='/api/growth/product')value={live:true,checkedAt:Date.now(),product:products.find(p=>p.handle===url.searchParams.get('handle'))};
    else if(url.pathname==='/api/concierge-voice')value=body.action==='capabilities'?{enabled:true,nativeAudio:true,publicDemo:true}:body.action==='start'?{sdp:SDP,stopToken:'native-spoken46-synthetic-only',maxDurationMs:120000}:{stopped:true};
    else if(url.pathname==='/api/growth/events')value={ok:true};
    else if(url.pathname==='/api/concierge'&&body?.event)value={ok:true};
    else if(url.pathname==='/api/concierge'){chatAttempts++;throw Error('Typed or ordinary chat route is disabled in native acceptance46');}
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
      onFinalizedTurn:providerFallback?async()=>({handled:false}):async input=>{const result=await options.onFinalizedTurn(input);finals.push({input,result});return result;},
      onTool:async(args,context)=>{const call={args:clone(args),context};toolCalls.push(call);call.result=await options.onTool(args,context);return call.result;}
    });return client;
  }};
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){}};}};
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js'])w.eval(source[name]);await settle();const store=w.BritesSandboxStorefront;if(!holdInventory)await store.preloadInventory();
  const execute=store.execute;w.BritesSandboxStorefront={...store,async execute(action,options){controls.push(clone(action));return execute(action,options);}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(source['brites-concierge.js']);const root=d.querySelector('brites-concierge').shadowRoot;
  w.BritesConcierge.sendShopperCommand=()=>{typedAttempts++;throw Error('Typed shopper handler is forbidden in native acceptance46');};root.querySelector('.composer form').onsubmit=()=>{typedAttempts++;throw Error('Typed composer is forbidden in native acceptance46');};
  w.BritesConcierge.open({focus:false});[...root.querySelectorAll('button')].find(b=>b.textContent==='Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();assert.equal(client.state,'listening');
  const emit=event=>channel.onmessage({data:JSON.stringify(event)}),responses=()=>packets.filter(p=>p.type==='response.create'),receipts=()=>packets.filter(p=>p.item?.content?.[0]?.text?.startsWith('Customer reply for this finalized shopper request.'));
  function begin(){const itemId='native46-input-'+(++turn);emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});return {itemId,version:turn+1};}
  const commit=input=>emit({type:'input_audio_buffer.committed',item_id:input.itemId}),final=(input,text)=>emit({type:'conversation.item.input_audio_transcription.completed',item_id:input.itemId,transcript:text});
  async function say(text){lastReceiptStart=receipts().length;const input=begin();commit(input);final(input,text);await settle();return {input,result:finals.find(f=>f.input.inputItemId===input.itemId)?.result};}
  async function tool(name,args){const request=responses().at(-1);assert.ok(request);const id='native46-provider-'+(++providerSerial),callId=id+'-tool';emit({type:'response.created',response:{id,metadata:request.response.metadata}});emit({type:'response.function_call_arguments.done',response_id:id,call_id:callId,name,arguments:JSON.stringify(args)});await settle();const packet=packets.find(p=>p.item?.type==='function_call_output'&&p.item.call_id===callId);assert.ok(packet);const host=toolCalls.find(c=>c.context.callId===callId)?.result,spoken=JSON.parse(packet.item.output);if(['control_storefront','prepare_jewellery_action'].includes(name)||host?.cartChanged===true){assert.ok(Object.keys(spoken).length>0);assert.ok(Object.keys(spoken).every(key=>['reply','customerMessage'].includes(key)),'The native provider must receive only the shopper reply for completed controls: '+Object.keys(spoken).join(', '));}return {host,spoken};}
  const choices=()=>Object.fromEntries((store.snapshot().productControls?.selectedOptions||[]).map(o=>[o.name,o.value])),cart=()=>JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart')||'[]');
  function lastSpoken(){const current=receipts().slice(lastReceiptStart);assert.equal(current.length,1,'this finalized native input must emit exactly one new shopper-only receipt');const text=current[0].item.content[0].text;const value=JSON.parse(text.slice(text.indexOf('{')));assert.deepEqual(Object.keys(value),['reply']);return value.reply;}
  return {w,d,root,store,products,requests,packets,controls,toolCalls,finals,emit,responses,receipts,begin,commit,final,say,tool,choices,cart,lastSpoken,async releaseInventory(){releaseInventory?.();await store.preloadInventory();await settle();},assertNativeOnly(){assert.equal(typedAttempts,0);assert.equal(chatAttempts,0);},get client(){return client;}};
}
module.exports={fixture,clone,settle,INITIAL,LEAF};
