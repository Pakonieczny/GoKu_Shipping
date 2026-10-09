'use strict';
// Independent native search continuation checks. Production client, widget,
// bridge, catalogue grammar and host are used unchanged. Catalogue/HTTP,
// media hardware and provider events are literal synthetic fixtures; these
// checks cannot certify physical ASR, live narration, or complete shop coverage.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {JSDOM,VirtualConsole}=require('jsdom');
const project=process.env.BRITES_ADVERSARIAL47_PROJECT||path.resolve(__dirname,'../..');
const Voice=require(path.join(project,'brites-concierge-voice.js'));
const Catalogue=require(path.join(project,'brites-catalogue-intents.js'));
const Bridge=require(path.join(project,'brites-storefront-bridge.js'));
const read=name=>fs.readFileSync(path.join(project,name),'utf8');
const clone=value=>JSON.parse(JSON.stringify(value));
const settle=async()=>{for(let n=0;n<8;n++)await new Promise(setImmediate);};
const deferred=()=>{let resolve;return {promise:new Promise(done=>resolve=done),resolve};};
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n';
const MOTIFS=['Bunny','Fox','Butterfly','Owl','Turtle','Cat'];
const METALS=['Sterling Silver','14k Gold Filled','14k Gold Plated','14k Solid Gold'];
function literalProduct(index){
  const animal=index<36,type=index<24||index>=36?'Earrings':'Necklace';
  const handle='independent-search47-'+index,title=(animal?MOTIFS[index%6]:'Daisy')+' '+(type==='Earrings'?'Stud Earrings':'Pendant Necklace')+' '+(index+1),id=47000+index;
  const options=[{name:'Metal Choice',values:METALS},{name:'Engraving',values:['None','Engraved']}];
  const variants=METALS.flatMap((metal,m)=>['None','Engraved'].map((engraving,e)=>{
    const number=470000+index*10+m*2+e+1,price=(animal?[40+index%6*5,60+index%6*5,20,260]:[10,20,8,210])[m]+e*7;
    return {id:'gid://shopify/ProductVariant/'+number,numericId:String(number),title:metal+' / '+engraving,price,available:true,options:[{name:'Metal Choice',value:metal},{name:'Engraving',value:engraving}]};
  }));
  return {id:'gid://shopify/Product/'+id,handle,title,type,url:'https://britesjewelry.com/products/'+handle,currency:'USD',image:'https://cdn.shopify.com/'+handle+'.jpg',images:[{url:'https://cdn.shopify.com/'+handle+'.jpg',alt:title}],description:'The published '+title+' is this listing. Wear it with a butterfly necklace; this cross-sell does not identify the listed design.',options,variants,variantsComplete:true,checkedAt:Date.now(),detailState:'checked',storeCategories:[type==='Earrings'?'stud-earrings':'regular-necklaces']};
}
async function fixture(t,{partialInventory=false}={}){
  const errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));
  const dom=new JSDOM(read('concierge-sandbox.html'),{url:'https://preview.example/concierge-sandbox.html',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document,products=Array.from({length:120},(_,i)=>literalProduct(i)),requests=[],packets=[],controls=[],presentations=[],finals=[],toolCalls=[];
  let client,channel,turn=0,responseSerial=0,typedAttempts=0,chatAttempts=0,readGate=null,presentFailure=false;
  let hidden=false;Object.defineProperty(d,'hidden',{get:()=>hidden});
  w.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});w.HTMLElement.prototype.scrollIntoView=function(){};w.scrollTo=()=>{};w.HTMLMediaElement.prototype.play=async function(){};w.HTMLMediaElement.prototype.pause=function(){};
  w.fetch=async(raw,init={})=>{
    const url=new URL(String(raw),w.location.href),body=init.body?JSON.parse(init.body):null;requests.push({url,body});let value;
    if(url.pathname==='/api/growth/catalogue')value={live:true,checkedAt:Date.now(),products:products.map(p=>({...p,detailState:'unconfirmed',variants:p.variants.map(v=>({...v,available:false,availabilityKnown:false}))})),pageInfo:{hasNextPage:false,endCursor:null},seed:{schema:1,target:120,minimum:100,loaded:120,complete:true,partial:false,sourcePages:1,categoryCounts:{'regular-necklaces':12,'beady-necklaces':0,'stud-earrings':108,'hoop-earrings':0,'charm-only':0},unfilledCategories:[]}};
    else if(url.pathname==='/api/growth/inventory'){
      const offset=Number(url.searchParams.get('offset'));
      if(partialInventory&&offset>0)return {ok:false,status:503,json:async()=>({error:'Synthetic unavailable catalogue segment'})};
      const rows=products.slice(offset,offset+24);value={live:true,checkedAt:Date.now(),products:rows,inventory:{schema:1,total:120,offset,limit:24,loaded:rows.length,detailsLoaded:rows.length,ready:false,partial:false,expiresAt:Date.now()+300000},pageInfo:{hasNextPage:offset+24<120,nextOffset:offset+24<120?offset+24:null}};
    }else if(url.pathname==='/api/growth/product')value={live:true,checkedAt:Date.now(),product:products.find(p=>p.handle===url.searchParams.get('handle'))};
    else if(url.pathname==='/api/concierge-voice')value=body.action==='capabilities'?{enabled:true,nativeAudio:true,publicDemo:true}:body.action==='start'?{sdp:SDP,stopToken:'independent-search47-synthetic',maxDurationMs:120000}:{stopped:true};
    else if(url.pathname==='/api/growth/events'||url.pathname==='/api/concierge'&&body?.event)value={ok:true};
    else if(url.pathname==='/api/concierge'){chatAttempts++;throw Error('Ordinary chat/typed route forbidden in independent native47');}
    else throw Error('Unexpected independent native47 route '+url.pathname);
    return {ok:true,status:200,json:async()=>clone(value)};
  };
  class Peer{
    constructor(){this.iceGatheringState='complete';}addTrack(){}close(){}
    createDataChannel(){channel={readyState:'connecting',send:value=>packets.push(JSON.parse(value)),close(){}};return channel;}
    async createOffer(){return {type:'offer',sdp:SDP};}async setLocalDescription(value){this.localDescription=value;}async setRemoteDescription(){channel.readyState='open';channel.onopen();}
  }
  const runtime={document:d,location:w.location,navigator:{mediaDevices:{getUserMedia:async()=>{const track={readyState:'live',stop(){this.readyState='ended';},addEventListener(){},removeEventListener(){}};return {getTracks:()=>[track],getAudioTracks:()=>[track]};}}},RTCPeerConnection:Peer,AbortController:w.AbortController,fetch:w.fetch,setTimeout:w.setTimeout.bind(w),clearTimeout:w.clearTimeout.bind(w),addEventListener:w.addEventListener.bind(w),removeEventListener:w.removeEventListener.bind(w)};
  w.BritesConciergeVoice={publicContext:Voice.publicContext,create(options){client=Voice.create({...options,runtime,greeting:false,onFinalizedTurn:async input=>{const record={input};finals.push(record);record.result=await options.onFinalizedTurn(input);return record.result;},onTool:async(args,context)=>{const record={args:clone(args),context};toolCalls.push(record);record.result=await options.onTool(args,context);return record.result;}});return client;}};
  w.BritesConciergeAvatar={create(){return {setState(){},setEmotion(){},setVisible(){},setPaused(){},triggerGreeting(){},clearFocus(){},focusProduct(){},setLevel(){},setSpeechSignal(){},clearProduct(){},showProduct(){},cancelPerformance(){},setFloating(){},cue(){}};}};
  for(const name of ['brites-catalogue-intents.js','brites-concierge-shopping-guide.js','brites-storefront-bridge.js','concierge-sandbox.js'])w.eval(read(name));await settle();const store=w.BritesSandboxStorefront;await store.preloadInventory();await settle();
  const execute=store.execute,present=store.presentProducts,readProduct=store.readProduct;
  w.BritesSandboxStorefront={...store,execute(action,options){controls.push(clone(action));return execute(action,options);},presentProducts(rows,options){presentations.push({handles:rows.map(p=>p.handle),options});if(presentFailure){presentFailure=false;return {ok:false,message:'The checked selection could not be shown. Please try again.'};}return present(rows,options);},async readProduct(handle){if(readGate){readGate.entered++;await readGate.promise;}return readProduct(handle);}};
  const script=d.createElement('script');script.src='/brites-concierge.js';script.dataset.sandbox='true';Object.defineProperty(d,'currentScript',{get:()=>script});w.eval(read('brites-concierge.js'));const root=d.querySelector('brites-concierge').shadowRoot;
  w.BritesConcierge.sendShopperCommand=()=>{typedAttempts++;throw Error('Typed command forbidden in independent native47');};root.querySelector('.composer form').onsubmit=()=>{typedAttempts++;throw Error('Typed composer forbidden in independent native47');};
  w.BritesConcierge.open({focus:false});[...root.querySelectorAll('button')].find(b=>b.textContent==='Talk to me').click();await settle();root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));await settle();assert.equal(client.state,'listening');
  t.after(async()=>{readGate?.resolve();w.BritesConcierge?.close();await client?.dispose();w.close();assert.deepEqual(errors,[]);assert.equal(typedAttempts,0);assert.equal(chatAttempts,0);});
  const emit=event=>channel.onmessage({data:JSON.stringify(event)}),responses=()=>packets.filter(p=>p.type==='response.create');
  function begin(){const itemId='independent47-input-'+(++turn);emit({type:'input_audio_buffer.speech_started',item_id:itemId});emit({type:'input_audio_buffer.speech_stopped',item_id:itemId});return {itemId,version:turn+1};}
  const commit=input=>emit({type:'input_audio_buffer.committed',item_id:input.itemId}),final=(input,text)=>emit({type:'conversation.item.input_audio_transcription.completed',item_id:input.itemId,transcript:text});
  async function say(text){const input=begin();commit(input);final(input,text);await settle();return {input,result:finals.find(f=>f.input.inputItemId===input.itemId)?.result};}
  async function tool(name,args){const request=responses().at(-1);assert.ok(request);const id='independent47-provider-'+(++responseSerial),callId=id+'-tool';emit({type:'response.created',response:{id,metadata:request.response.metadata}});emit({type:'response.function_call_arguments.done',response_id:id,call_id:callId,name,arguments:JSON.stringify(args)});await settle();return toolCalls.find(r=>r.context.callId===callId)?.result;}
  const cards=()=>[...d.querySelectorAll('#demo-products [data-product-handle]')].map(e=>e.dataset.productHandle),loaded=()=>Array.from(store.snapshot().loadedPieces,p=>p.handle),cart=()=>JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart')||'[]');
  return {w,d,root,store,products,requests,packets,controls,presentations,finals,toolCalls,client,emit,responses,begin,commit,final,say,tool,cards,loaded,cart,failNextPresentation(){presentFailure=true;},gateReads(){readGate={...deferred(),entered:0};return readGate;},setHidden(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));}};
}
function assertScope(text,{theme='animals',categories=[],excludedTerms=[],excludedCategories=[],material=null,max=null,currency=null}={}){
  const p=Catalogue.plan(text);assert.deepEqual(p.themes,theme?[theme]:[]);assert.deepEqual(p.categories,categories);assert.deepEqual(p.excludedTerms,excludedTerms);assert.deepEqual(p.excludedCategories,excludedCategories);assert.equal(p.material?.kind||null,material);assert.equal(p.max,max);assert.equal(p.currency,currency);return p;
}
function animalHandles(h,earrings=false){return h.products.slice(0,earrings?24:36).map(p=>p.handle);}
function noControlNavigation(h){assert.ok(h.controls.every(a=>['search','filter'].includes(a.type)),JSON.stringify(h.controls));assert.deepEqual(h.cart(),[]);}

for(const phrase of ['Show the rest','Show more','Show the remaining listings'])test('continuation grammar preserves every completed prior criterion: '+phrase,()=>{
  const previous='animal earrings 14k gold filled under USD 75 without butterfly without necklaces',before=Catalogue.plan(previous),r=Catalogue.discovery(phrase,previous);assert.equal(r.recognized,true);assert.equal(r.denied,false);assert.deepEqual(r.plan.categories,before.categories);assert.deepEqual(r.plan.themes,before.themes);assert.deepEqual(r.plan.terms,before.terms);assert.deepEqual(r.plan.excludedTerms,before.excludedTerms);assert.deepEqual(r.plan.excludedCategories,before.excludedCategories);assert.deepEqual(r.plan.material,before.material);assert.equal(r.plan.max,75);assert.equal(r.plan.currency,'USD');
});
test('explicit only-category wording retains the animal and exclusion scope',()=>{
  for(const phrase of ['Show only earrings','Earrings only','Only earrings','Please show only earrings']){const r=Catalogue.discovery(phrase,'animal gold filled under USD 75 without butterfly');assert.equal(r.recognized,true,phrase);assert.deepEqual(r.plan.themes,['animals'],phrase);assert.deepEqual(r.plan.categories,['earrings'],phrase);assert.deepEqual(r.plan.excludedTerms,['butterfly'],phrase);assert.equal(r.plan.material.kind,'filled');assert.equal(r.plan.max,75);}
});
test('ordinary fresh categories and full browsing still leave the old animal scope deliberately',()=>{
  const prior='animal gold filled under USD 75 without butterfly';const fresh=Catalogue.discovery('Show earrings',prior);assert.deepEqual(fresh.plan.themes,[]);assert.deepEqual(fresh.plan.categories,['earrings']);const all=Catalogue.discovery('Show all pieces',prior);assert.equal(all.browseAll,true);assert.deepEqual(all.plan.themes,[]);assert.deepEqual(all.plan.categories,[]);assert.equal(all.plan.material,null);assert.equal(all.plan.max,null);
});
function assertResults(result,total,shown){
  assert.equal(result?.ok,true);const info=result.searchResults;assert.ok(info,'A checked search must report its complete loaded-match count and presentation cursor');assert.equal(info.schema,1);assert.equal(info.scope,'checked-loaded-inventory');assert.equal(info.total,total);assert.equal(info.shown,shown);assert.equal(info.remaining,total-shown);assert.equal(info.hasMore,shown<total);assert.equal(typeof info.query,'string');assert.ok(result.products.length<=6,'Voice product facts stay bounded while the actual host presents the requested batch');assert.equal(result.inventory.catalogueComplete,false);
}
test('a short animal search honestly counts all loaded matches while presenting six, then next six and remaining',async t=>{
  const h=await fixture(t),first=await h.say('Show animal jewelry');assertResults(first.result,36,6);assert.equal(h.loaded().length,6);const seen=new Set(h.loaded());assert.match(first.result.reply,/36/);assert.doesNotMatch(first.result.reply,/whole (?:shop|catalogue)|entire (?:shop|catalogue)|all 120/);
  const more=await h.say('Show more');assertResults(more.result,36,12);assert.equal(h.loaded().length,6);assert.ok(h.loaded().every(handle=>!seen.has(handle)));h.loaded().forEach(handle=>seen.add(handle));
  const rest=await h.say('Show the rest');assertResults(rest.result,36,36);assert.equal(h.loaded().length,24);assert.ok(h.loaded().every(handle=>!seen.has(handle)));h.loaded().forEach(handle=>seen.add(handle));assert.deepEqual(seen,new Set(animalHandles(h)));noControlNavigation(h);
});
for(const [phrase,batch,shown] of [['Show the rest',30,36],['Show more',6,12]])test('native '+phrase+' presents unseen animal cards under the original query with a bounded spoken projection',async t=>{
  const h=await fixture(t);await h.say('Show animal jewelry');const first=h.loaded(),said=await h.say(phrase);assertResults(said.result,36,shown);assertScope(h.store.snapshot().search);assert.equal(h.loaded().length,batch);assert.ok(h.loaded().every(handle=>!first.includes(handle)));assert.ok(h.loaded().every(handle=>animalHandles(h).includes(handle)));assert.equal(new Set(h.loaded()).size,h.loaded().length);assert.equal(h.presentations.at(-1).handles.length,batch);if(h.cards().length<batch)assert.ok([...h.d.querySelectorAll('button')].some(b=>b.textContent==='Explore more pieces'),'Every card of the native all-unseen batch must be reachable');const revision=h.store.snapshot().contextRevision;h.final(said.input,phrase);await settle();assert.equal(h.store.snapshot().contextRevision,revision,'Repeated final ASR cannot perform the continuation again');noControlNavigation(h);
});
test('native category, material and item-budget narrowing retain the animal query and exact qualifying prices',async t=>{
  const h=await fixture(t);await h.say('Show animal jewelry');let said=await h.say('Show only earrings');assertScope(h.store.snapshot().search,{categories:['earrings']});assertResults(said.result,24,6);assert.ok(h.loaded().every(id=>animalHandles(h,true).includes(id)));await h.say('In gold filled');assertScope(h.store.snapshot().search,{categories:['earrings'],material:'filled'});said=await h.say('Under USD 65');assertScope(h.store.snapshot().search,{categories:['earrings'],material:'filled',max:65,currency:'USD'});const expected=h.products.slice(0,24).filter((p,i)=>i%6<2).map(p=>p.handle);assertResults(said.result,8,6);const seen=new Set(h.loaded());assert.ok(h.loaded().every(id=>expected.includes(id)));assert.equal(said.result.searchCriteria.maxPrice,65);assert.equal(said.result.searchCriteria.currency,'USD');for(const p of said.result.products){assert.equal(p.matchingPriceRange.currency,'USD');assert.ok(p.matchingPriceRange.max<=65);assert.ok(p.matchingVariantIds.every(id=>h.products.find(q=>q.id===p.id).variants.find(v=>v.id===id).options.find(o=>o.name==='Metal Choice').value==='14k Gold Filled'));}said=await h.say('Show the rest');assertResults(said.result,8,8);assert.equal(h.loaded().length,2);h.loaded().forEach(id=>seen.add(id));assert.deepEqual(seen,new Set(expected));noControlNavigation(h);
});
test('native trailing-only narrowing retains explicit motif and negative category exclusions',async t=>{
  const h=await fixture(t);await h.say('Show animal jewelry without butterfly without necklaces');await h.say('Earrings only');let said=await h.say('In gold filled under USD 75');assertScope(h.store.snapshot().search,{categories:['earrings'],excludedTerms:['butterfly'],excludedCategories:['necklaces'],material:'filled',max:75,currency:'USD'});const expected=h.products.slice(0,24).filter((p,i)=>i%6<4&&i%6!==2).map(p=>p.handle);assertResults(said.result,12,6);const seen=new Set(h.loaded());assert.ok(h.loaded().every(id=>expected.includes(id)));said=await h.say('Show the rest');assertResults(said.result,12,12);h.loaded().forEach(id=>seen.add(id));assert.deepEqual(seen,new Set(expected));noControlNavigation(h);
});
test('continuing a foreign-currency empty scope cannot spend the old USD budget',async t=>{
  const h=await fixture(t);await h.say('Show animal earrings 14k gold filled under USD 65');let said=await h.say('Under CAD 65');assertScope(h.store.snapshot().search,{categories:['earrings'],material:'filled',max:65,currency:'CAD'});assert.deepEqual(h.loaded(),[]);assertResults(said.result,0,0);assert.equal(said.result.currencyMismatch,true);said=await h.say('Show the rest');assertScope(h.store.snapshot().search,{categories:['earrings'],material:'filled',max:65,currency:'CAD'});assert.deepEqual(h.loaded(),[]);assertResults(said.result,0,0);assert.match(said.result.reply,/CAD|no (?:more|additional|remaining|checked|available)/i);noControlNavigation(h);
});
test('no-prior and conditional continuation cannot invent a new collection request',async t=>{
  const h=await fixture(t),before=h.store.snapshot();for(const phrase of ['Show the rest','If I say show the rest, what happens?','I said show the rest yesterday','Do not show more','Can you show the rest?','Could you show me more?','Show all the remaining results']){await h.say(phrase);assert.equal(h.store.snapshot().search,before.search,phrase);assert.deepEqual(h.loaded(),Array.from(before.loadedPieces,p=>p.handle),phrase);}assert.equal(h.presentations.length,0);assert.equal(h.controls.length,0);
});
test('native provider arguments cannot substitute a foreign search after the finalized continuation',async t=>{
  const h=await fixture(t);await h.say('Show animal jewelry');await h.say('Show the rest');const page=h.store.snapshot(),controls=h.controls.length,presentations=h.presentations.length;await h.tool('control_storefront',{type:'search',query:'daisy earrings',filter:'earrings'});assert.equal(h.store.snapshot().contextRevision,page.contextRevision);assert.equal(h.store.snapshot().search,page.search);assert.deepEqual(h.loaded(),Array.from(page.loadedPieces,p=>p.handle));assert.equal(h.controls.length,controls);assert.equal(h.presentations.length,presentations);noControlNavigation(h);
});
test('current product inquiry and literal private wording cannot activate catalogue continuation',async t=>{
  const h=await fixture(t);await h.say('Open '+h.products[0].title);await h.say('Select Sterling Silver');await h.say('Select Engraved');const shown=h.presentations.length;await h.say('Set engraving wording to show the rest PRIVATE47');assert.equal(h.store.snapshot().pageKind,'product');assert.equal(h.store.snapshot().currentHandle,h.products[0].handle);assert.equal(h.d.querySelector('textarea[name="test-engraving-preview"]')?.value,'show the rest PRIVATE47');await h.say('Do you have this in silver?');assert.equal(h.store.snapshot().pageKind,'product');assert.equal(h.presentations.length,shown);assert.doesNotMatch(JSON.stringify(h.packets),/PRIVATE47/);assert.doesNotMatch(h.w.sessionStorage.getItem('brites-concierge-v1')||'',/PRIVATE47/);assert.deepEqual(h.cart(),[]);
});
test('partial checked inventory never becomes a complete-catalogue claim when continued',async t=>{
  const h=await fixture(t,{partialInventory:true});let said=await h.say('Show animal earrings');assert.equal(said.result?.ok,true);assert.equal(said.result.inventory.ready,false);assert.equal(said.result.inventory.partial,true);assert.equal(said.result.inventory.catalogueComplete,false);said=await h.say('Show the rest');assert.equal(said.result?.ok,true);assert.ok(h.loaded().every(handle=>animalHandles(h,true).includes(handle)));assert.equal(said.result.inventory.catalogueComplete,false);assert.doesNotMatch(said.result.reply,/whole (?:shop|catalogue)|entire (?:shop|catalogue)|all 120|complete catalogue/);noControlNavigation(h);
});
test('a newer native input retires pending continuation without showing stale results',async t=>{
  const h=await fixture(t);await h.say('Show animal jewelry');const before=h.store.snapshot(),gate=h.gateReads(),input=h.begin();h.commit(input);h.final(input,'Show the rest');await settle();assert.ok(gate.entered>0,'Continuation must validate the actual checked rows before presentation');const newer=h.begin();h.commit(newer);h.final(newer,'No thanks');await settle();gate.resolve();await settle();assert.equal(h.store.snapshot().search,before.search);assert.deepEqual(h.loaded(),Array.from(before.loadedPieces,p=>p.handle));assert.equal(h.store.snapshot().contextRevision,before.contextRevision);assert.deepEqual(h.cart(),[]);
});

test('native detail and back navigation preserve the same committed search cursor',async t=>{
  const h=await fixture(t);await h.say('Show animal jewelry');const first=h.loaded(),name=h.products.find(p=>p.handle===first[0]).title;await h.say('Open '+name);assert.equal(h.store.snapshot().pageKind,'product');await h.say('Go back');assert.equal(h.store.snapshot().pageKind,'collection');assertScope(h.store.snapshot().search);const said=await h.say('Show more');assertResults(said.result,36,12);assert.equal(h.loaded().length,6);assert.ok(h.loaded().every(id=>!first.includes(id)));assert.ok(h.loaded().every(id=>animalHandles(h).includes(id)));assert.deepEqual(h.cart(),[]);
});
test('any material and any price each clear only their own current search constraint',async t=>{
  const h=await fixture(t);await h.say('Show animal earrings 14k gold filled under USD 65');let said=await h.say('Any material');assertScope(h.store.snapshot().search,{categories:['earrings'],max:65,currency:'USD'});assertResults(said.result,24,6);await h.say('In gold filled');said=await h.say('Any price');assertScope(h.store.snapshot().search,{categories:['earrings'],material:'filled'});assert.equal(said.result.searchCriteria.material,'gold filled');assert.equal(said.result.searchCriteria.maxPrice,null);assertResults(said.result,24,6);noControlNavigation(h);
});
test('an explicit new flower topic clears the prior animal material and budget search',async t=>{
  const h=await fixture(t);await h.say('Show animal earrings sterling silver under USD 45');const said=await h.say('Show flowers');assertScope(h.store.snapshot().search,{theme:'flowers'});assert.equal(said.result.searchCriteria.material,null);assert.equal(said.result.searchCriteria.maxPrice,null);assertResults(said.result,84,6);assert.ok(h.loaded().every(id=>h.products.slice(36).some(p=>p.handle===id)));noControlNavigation(h);
});
test('freshly changed unseen prices are recomputed before continuing the exact item-budget scope',async t=>{
  const h=await fixture(t);let said=await h.say('Show animal earrings 14k gold filled under USD 75');assertResults(said.result,16,6);const first=h.loaded(),changed=h.products.slice(0,24).find((p,i)=>i%6<4&&!first.includes(p.handle));assert.ok(changed);changed.variants=changed.variants.map(v=>({...v,price:v.price+200}));changed.checkedAt=Date.now();await h.store.preloadInventory({retry:true});await settle();said=await h.say('Show the rest');assertScope(h.store.snapshot().search,{categories:['earrings'],material:'filled',max:75,currency:'USD'});assertResults(said.result,15,15);assert.equal(h.loaded().length,9);assert.ok(h.loaded().every(id=>!first.includes(id)&&id!==changed.handle));assert.ok(said.result.products.every(p=>p.matchingPriceRange.max<=75));noControlNavigation(h);
});
test('a failed host presentation does not consume unseen matches or commit a continuation cursor',async t=>{
  const h=await fixture(t);await h.say('Show animal jewelry');const before=h.store.snapshot(),first=h.loaded();h.failNextPresentation();let said=await h.say('Show more');assert.equal(said.result?.ok,false);assert.equal(h.store.snapshot().contextRevision,before.contextRevision);assert.deepEqual(h.loaded(),first);said=await h.say('Show more');assertResults(said.result,36,12);assert.equal(h.loaded().length,6);assert.ok(h.loaded().every(id=>!first.includes(id)));noControlNavigation(h);
});

test('one scoped category refinement honors its newly requested material and item cap',async t=>{
  const h=await fixture(t);await h.say('Show animal necklaces sterling silver under USD 45 without butterfly');const said=await h.say('Only earrings gold filled under USD 75');assertScope(h.store.snapshot().search,{categories:['earrings'],excludedTerms:['butterfly'],material:'filled',max:75,currency:'USD'});assertResults(said.result,12,6);assert.equal(said.result.searchCriteria.material,'gold filled');assert.equal(said.result.searchCriteria.maxPrice,75);assert.ok(h.loaded().every(id=>h.products.slice(0,24).some((p,i)=>i%6<4&&i%6!==2&&p.handle===id)));noControlNavigation(h);
});
