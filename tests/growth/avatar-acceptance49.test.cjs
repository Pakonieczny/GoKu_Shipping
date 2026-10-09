'use strict';

// Independent behavior checks against the production host, widget, resolver,
// and native voice client. Network, speech recognition, microphone, scrolling
// and avatar rendering are synthetic. These are not hardware certification.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {fixture,publishedProduct,settle}=require('./native-continuity48-fixture.cjs');
const copy=value=>JSON.parse(JSON.stringify(value));
// Native-only fixtures deliberately prohibit typed entry. This separate local
// harness derivative permits genuine typed widget input for the mixed-mode
// races below. Production sources, media/transport stubs and assertions are
// unchanged; an ordinary provider/chat HTTP route is still forbidden.
let mixedFixture;
function typedFixture(t,options){if(!mixedFixture){const Module=require('node:module'),path=require('node:path'),name=require.resolve('./native-continuity48-fixture.cjs'),text=fs.readFileSync(name,'utf8'),guard="  w.BritesConcierge.sendShopperCommand=()=>{typedAttempts++;throw Error('Typed shopper handler is forbidden in native acceptance47');};root.querySelector('.composer form').onsubmit=()=>{typedAttempts++;throw Error('Typed composer is forbidden in native acceptance47');};",signature="permissionState='prompt'}={})",mount="w.eval(source['brites-concierge.js']);const root";for(const anchor of [guard,signature,mount])assert.equal(text.split(anchor).length,2,'Only the known synthetic fixture entry points are changed');const helper=new Module(name,module);helper.filename=name;helper.paths=Module._nodeModulePaths(path.dirname(name));helper._compile(text.replace(guard,'').replace(signature,"permissionState='prompt',beforeWidget=null}={})").replace(mount,"beforeWidget?.({w,d});w.eval(source['brites-concierge.js']);const root"),name);mixedFixture=helper.exports.fixture;}return mixedFixture(t,options);}
const stored=f=>Object.fromEntries(Array.from({length:f.w.sessionStorage.length},(_,i)=>{const key=f.w.sessionStorage.key(i);return [key,f.w.sessionStorage.getItem(key)];}));
const button=(root,label)=>{const found=[...root.querySelectorAll('button')].find(node=>node.textContent===label);assert.ok(found,'Visible control exists: '+label);return found;};
const starts=f=>f.requests.filter(r=>r.body?.action==='start').length;
const stops=f=>f.requests.filter(r=>r.body?.action==='stop').length;
const greetings=f=>f.responses().filter(p=>/Greet the shopper/.test(p.response.instructions||''));
const latestPage=f=>{const packet=f.packets.filter(p=>p.item?.content?.[0]?.text?.startsWith('Public website UI')).at(-1);assert.ok(packet);return packet.item.content[0].text;};
const touch=(f,target,type='pointerdown')=>{assert.ok(target,'Actual touched control exists');const event=new f.w.Event(type,{bubbles:true,composed:true});Object.defineProperty(event,'pointerType',{value:'touch'});target.dispatchEvent(event);};
function necklaceCatalogue(){const rows=Array.from({length:120},(_,i)=>publishedProduct(i)),p=rows[10];p.options=[{name:'Metal Choice',values:['Sterling Silver','14k Solid Gold']},{name:'Length',values:['16 inches','18 inches']}];p.variants=p.options[0].values.flatMap((metal,i)=>p.options[1].values.map((length,j)=>({id:'gid://shopify/ProductVariant/'+(490000+i*10+j),numericId:String(490000+i*10+j),title:metal+' / '+length,price:113+i*207+j*9,available:true,options:[{name:'Metal Choice',value:metal},{name:'Length',value:length}]})));return rows;}
async function waitFor(predicate,reason){for(let n=0;n<60;n++){if(predicate())return;await settle();await new Promise(resolve=>setTimeout(resolve,1));}assert.ok(predicate(),reason);}

test('opening the real typing control keeps the microphone connected and later native followups work',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'}),client=f.client;
 button(f.root,'Type instead').click();await settle();
 assert.equal(f.client,client);assert.notEqual(client.state,'idle');assert.equal(stops(f),0);assert.equal(f.microphoneCalls,1);
 await f.say('What quantity have I selected?');assert.equal(f.lastSpoken(),'Quantity 1.');assert.equal(starts(f),1);f.assertNativeOnly();
});

test('real page-help typing handler answers current options while preserving native conversation',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'}),before=f.controls.length;
 button(f.root,'Help with this page').click();await settle();
 assert.equal(stops(f),0);assert.notEqual(f.client.state,'idle');assert.match(f.root.querySelector('.captions').textContent,/Sterling Silver/);assert.equal(f.controls.length,before);
 await f.say('What have I selected?');assert.equal(f.lastSpoken(),'No options are selected yet. Quantity 1.');assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('actual typed option suggestion chip changes the current piece without ending native voice',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'});
 button(f.root,'Help me choose').click();await settle();
 const choices=f.root.querySelector('.shopping-help-choices');button(choices,'Sterling Silver').click();await settle();
 assert.equal(f.choices()['Metal Choice'],'Sterling Silver');assert.equal(stops(f),0);assert.equal(starts(f),1);assert.notEqual(f.client.state,'idle');
 await f.say('Set quantity to 3');await f.say('What have I selected?');assert.match(f.lastSpoken(),/Metal Choice: Sterling Silver\. Quantity 3/);assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('a real typed option change interrupts an old native reply without ending the mic or authorizing its delayed add',async t=>{
 const f=await typedFixture(t,{query:'?product=cat-stud-earrings'});assert.equal((await f.w.BritesConcierge.sendShopperCommand('Select Sterling Silver')).ok,true);f.setProviderFallback(true);await f.say('Add this piece to my bag');const issued=f.responses().at(-1);assert.ok(issued);
 const responseId='acceptance49-interrupted-provider';f.emit({type:'response.created',response:{id:responseId,status:'in_progress',metadata:issued.response.metadata}});f.emit({type:'output_audio_buffer.started',response_id:responseId});await settle();const clearBefore=f.packets.filter(p=>p.type==='output_audio_buffer.clear').length;
 const changed=await f.w.BritesConcierge.sendShopperCommand('Select 14k Gold Filled');assert.equal(changed.ok,true);assert.equal(f.choices()['Metal Choice'],'14k Gold Filled');assert.ok(f.packets.filter(p=>p.type==='output_audio_buffer.clear').length>clearBefore);const controls=f.controls.length;
 const item={id:'acceptance49-delayed-add-item',type:'function_call',status:'completed',name:'control_storefront',call_id:'acceptance49-delayed-add',arguments:JSON.stringify({type:'add',handle:'cat-stud-earrings'})};f.emit({type:'response.function_call_arguments.done',response_id:responseId,call_id:item.call_id,item_id:item.id,name:item.name,arguments:item.arguments});f.emit({type:'response.output_item.done',response_id:responseId,output_index:0,item});f.emit({type:'response.done',response:{id:responseId,status:'completed',output:[item]}});await settle();assert.equal(f.controls.length,controls);assert.equal(f.cart().length,0);assert.equal(stops(f),0);assert.equal(starts(f),1);assert.equal(f.microphoneCalls,1);
 f.setProviderFallback(false);await f.say('What have I selected?');assert.match(f.lastSpoken(),/14k Gold Filled/);assert.match(f.lastSpoken(),/Quantity 1/);assert.equal(f.cart().length,0);
});

test('synthetic mobile touch selects only the actual latest touched option group',async t=>{
 const f=await fixture(t,{customProducts:necklaceCatalogue(),query:'?product=elephant-necklace'});
 await f.store.execute({type:'options',handle:'elephant-necklace'});
 touch(f,f.d.querySelector('[data-option-name="Metal Choice"]'));touch(f,f.d.querySelector('[data-option-name="Length"]'));
 await f.say('sixteen');assert.deepEqual(copy(f.choices()),{Length:'16 inches'});
 touch(f,f.d.querySelector('[data-option-name="Metal Choice"]'));await f.say('14k Solid Gold');assert.deepEqual(copy(f.choices()),{Length:'16 inches','Metal Choice':'14k Solid Gold'});assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('synthetic touch quantity follows the actual control after a menu was touched',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'});await f.store.execute({type:'options',handle:'cat-stud-earrings'});
 touch(f,f.d.querySelector('[data-option-name="Metal Choice"]'));touch(f,f.d.querySelector('input[aria-label="Quantity of this exact piece"]'));
 await f.say('three');assert.equal(f.store.snapshot().productControls.quantity,3);assert.deepEqual(copy(f.choices()),{});
 await f.say('What quantity have I selected?');assert.equal(f.lastSpoken(),'Quantity 3.');assert.ok(f.lastSpoken().split(/\s+/).length<10);f.assertNativeOnly();
});

test('touched product section survives entering concierge and cannot change a retired product menu',async t=>{
 const f=await fixture(t,{customProducts:necklaceCatalogue(),query:'?product=elephant-necklace'});await f.store.execute({type:'options',handle:'elephant-necklace'});
 touch(f,f.d.querySelector('[data-option-name="Length"]'));touch(f,f.d.querySelector('[data-store-section="description"]'));touch(f,f.root.host);await f.say('Go to this section');assert.equal(f.controls.at(-1).action.section,'description');
 const before=f.controls.length;await f.say('sixteen');assert.equal(f.controls.length,before);assert.deepEqual(copy(f.choices()),{});f.assertNativeOnly();
});

test('old touched option and old section do not survive manual navigation to a different product',async t=>{
 const f=await fixture(t,{customProducts:necklaceCatalogue(),query:'?product=elephant-necklace'});await f.store.execute({type:'options',handle:'elephant-necklace'});
 touch(f,f.d.querySelector('[data-option-name="Length"]'));touch(f,f.d.querySelector('[data-store-section="description"]'));
 await f.store.execute({type:'open',handle:'cat-stud-earrings'});await settle();assert.equal(f.store.snapshot().activeSection,'details');
 const before=f.controls.length;await f.say('sixteen');assert.equal(f.controls.length,before);assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.deepEqual(copy(f.choices()),{});
 await f.say('What size is this piece?');assert.match(f.lastSpoken(),/12\s?mm/);assert.doesNotMatch(f.lastSpoken(),/Elephant|16 inches/);assert.match(latestPage(f),/cat-stud-earrings/);f.assertNativeOnly();
});

test('twenty manual product navigations keep the same voice client and current page answers',async t=>{
 const f=await fixture(t),client=f.client;
 for(let n=0;n<20;n++){await f.store.execute({type:'open',handle:f.products[n].handle});await settle();assert.equal(f.client,client);assert.notEqual(client.state,'idle');}
 await f.say('What am I looking at?');assert.match(f.lastSpoken(),new RegExp(f.products[19].title));assert.match(latestPage(f),new RegExp(f.products[19].handle));assert.equal(starts(f),1);assert.equal(stops(f),0);f.assertNativeOnly();
});

test('actual pagehide stores resumable intent; same-tab reload rechecks page choices and never greets again',async t=>{
 const a=await fixture(t,{customProducts:necklaceCatalogue(),query:'?product=elephant-necklace',greetingEnabled:true,permissionState:'granted'});
 await a.say('Select 14k Solid Gold then select 18 inches then set quantity to 2');assert.deepEqual(copy(a.choices()),{'Metal Choice':'14k Solid Gold',Length:'18 inches'});
 a.w.dispatchEvent(new a.w.Event('pagehide'));await settle();const session=stored(a);assert.ok(Number(session['brites-concierge-v1-voice-resume'])>0);assert.equal(a.client.state,'idle');
 const b=await fixture(t,{customProducts:necklaceCatalogue(),query:'?product=elephant-necklace',savedSession:session,greetingEnabled:true,autoResume:true,permissionState:'granted'});
 assert.equal(b.microphoneCalls,1);assert.equal(greetings(b).length,0);assert.equal(b.greetings.length,0);assert.deepEqual(copy(b.choices()),{'Metal Choice':'14k Solid Gold',Length:'18 inches'});
 await b.say('What have I selected?');assert.match(b.lastSpoken(),/14k Solid Gold/);assert.match(b.lastSpoken(),/18 inches/);assert.match(b.lastSpoken(),/Quantity 2/);assert.equal(b.cart().length,0);b.assertNativeOnly();
});

test('hidden then visible same-tab voice reconnects once after permission was already granted',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings',greetingEnabled:true,permissionState:'granted'});let hidden=false;Object.defineProperty(f.d,'hidden',{get:()=>hidden,configurable:true});
 hidden=true;f.d.dispatchEvent(new f.w.Event('visibilitychange'));await settle();assert.equal(f.client.state,'idle');assert.equal(stops(f),1);
 hidden=false;f.d.dispatchEvent(new f.w.Event('visibilitychange'));await waitFor(()=>starts(f)===2&&f.client.state==='listening','Already allowed voice reconnects when the tab returns');
 assert.equal(f.microphoneCalls,2);assert.equal(greetings(f).length,1);await f.say('What quantity have I selected?');assert.equal(f.lastSpoken(),'Quantity 1.');f.assertNativeOnly();
});

test('explicit End voice clears resume intent and pageshow cannot restart the microphone',async t=>{
 const f=await fixture(t,{permissionState:'granted'});button(f.root,'End voice').click();await settle();f.w.dispatchEvent(new f.w.Event('pageshow'));await settle();
 assert.equal(f.client.state,'idle');assert.equal(starts(f),1);assert.equal(f.w.sessionStorage.getItem('brites-concierge-v1-voice-resume'),null);assert.equal(f.microphoneCalls,1);f.assertNativeOnly();
});

test('provider call boundary renews an opted-in native conversation without another greeting or action replay',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings',greetingEnabled:true});await f.say('Select Sterling Silver');const before=f.controls.length;
 await f.client.stop('limit');await waitFor(()=>starts(f)===2&&f.client.state!=='idle','The opted-in conversation renews after its provider call boundary');
 assert.equal(greetings(f).length,1);assert.equal(f.controls.length,before);assert.equal(f.cart().length,0);assert.equal(f.microphoneCalls,2);
 await f.say('Set quantity to 2');await f.say('Add this piece to my bag');assert.equal(f.cart().length,1);assert.equal(f.cart()[0].quantity,2);assert.equal(f.cart()[0].variant,'Sterling Silver');assert.equal(f.cart()[0].price,25);f.assertNativeOnly();
});

test('checked search cursor and exact bag survive same-tab voice restart and current-page reload',async t=>{
 const a=await fixture(t);await a.say('Show me a short list of animal earrings');const first=a.store.snapshot().visiblePieces.map(p=>p.handle);
 await a.say('Open Cat Stud Earrings');await a.say('Select Sterling Silver');await a.say('Set quantity to 3');await a.say('Add this piece to my bag');
 const session=stored(a);assert.ok(JSON.parse(session['brites-concierge-v1']).catalogueSearch,'The search is committed before the product reload');const b=await fixture(t,{query:'?product=cat-stud-earrings',savedSession:session});assert.equal(b.cart().length,1);assert.equal(b.cart()[0].quantity,3);assert.equal(b.cart()[0].price,25);
 await b.say('Go back to the results');const backReply=b.lastSpoken();await b.say('Show more');const next=b.store.snapshot().visiblePieces.map(p=>p.handle);assert.equal(next.length,4,JSON.stringify({backReply,moreReply:b.lastSpoken(),page:b.store.snapshot().pageKind,search:b.store.snapshot().search,next}));assert.ok(next.every(handle=>!first.includes(handle)));assert.equal(b.cart().length,1);assert.equal(b.cart()[0].quantity,3);b.assertNativeOnly();
});

async function reloadSearch(t){const a=await fixture(t);await a.say('Show me a short list of animal earrings');await a.say('Open Cat Stud Earrings');return fixture(t,{query:'?product=cat-stud-earrings',savedSession:stored(a)});}

test('restoring a saved collection fails closed if current inventory refresh fails',async t=>{
 const f=await reloadSearch(t),controls=copy(f.store.snapshot().productControls);f.advanceTime(300001);const fetch=f.w.fetch;
 f.w.fetch=async(raw,init)=>{if(new URL(raw,f.w.location.href).pathname==='/api/growth/inventory')throw Error('Synthetic inventory read failure');return fetch(raw,init);};
 const out=await f.say('Go back to the results');await waitFor(()=>f.finals.some(row=>row.input.inputItemId===out.input.itemId),'The failed inventory restore finishes');
 assert.equal(f.store.snapshot().pageKind,'product');assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.deepEqual(copy(f.store.snapshot().productControls),controls);assert.equal(f.finals.find(row=>row.input.inputItemId===out.input.itemId).result.ok,false);assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('a new spoken request cancels delayed saved-result restoration before it can replace the current page',async t=>{
 const f=await reloadSearch(t);f.advanceTime(300001);let release,held=0;const gate=new Promise(resolve=>release=resolve),fetch=f.w.fetch;
 f.w.fetch=async(raw,init)=>{if(new URL(raw,f.w.location.href).pathname==='/api/growth/inventory'){held++;await gate;}return fetch(raw,init);};
 const old=await f.say('Go back to the results');assert.ok(held>0,'The actual return-to-results has reached the deferred inventory read');await f.say('What quantity have I selected?');assert.equal(f.lastSpoken(),'Quantity 1.');
 for(const p of f.products)p.checkedAt=f.w.Date.now();release();await waitFor(()=>f.finals.some(row=>row.input.inputItemId===old.input.itemId),'The interrupted restore retires');
 assert.equal(f.store.snapshot().pageKind,'product');assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('saved collection handles cannot manufacture an unchecked earlier product',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings',savedSession:{'brites-sandbox-view-v1-collection':JSON.stringify({schema:1,kind:'catalogue',handle:'',search:'animal earrings',filter:'all',sort:'featured',handles:['not-a-published-product'],selection:null})}});
 await f.say('Go back to the results');assert.equal(f.store.snapshot().pageKind,'product');assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.match(f.lastSpoken(),/couldn.t check|try again/i);assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('the host cannot commit delayed saved-result restoration after its explicit action signal is aborted',async t=>{
 const f=await reloadSearch(t);f.advanceTime(300001);let release,held=0;const gate=new Promise(resolve=>release=resolve),fetch=f.w.fetch;
 f.w.fetch=async(raw,init)=>{if(new URL(raw,f.w.location.href).pathname==='/api/growth/inventory'){held++;await gate;}return fetch(raw,init);};
 const controller=new f.w.AbortController(),pending=f.store.execute({type:'back'},{signal:controller.signal});await waitFor(()=>held>0,'The actual host back action waits for fresh inventory');controller.abort();
 for(const p of f.products)p.checkedAt=f.w.Date.now();release();const result=await pending;assert.equal(result.ok,false);assert.equal(f.store.snapshot().pageKind,'product');assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('returning to saved results uses current prices and never revives a saved available variant',async t=>{
 const a=await fixture(t);await a.say('Show me a short list of animal earrings in Sterling Silver under USD50');const saved=stored(a),rows=Array.from({length:120},(_,i)=>publishedProduct(i));
 rows[1].variants[0].price=99;rows[2].variants[0].available=false;await a.say('Open Cat Stud Earrings');Object.assign(saved,stored(a));
 const f=await fixture(t,{query:'?product=cat-stud-earrings',savedSession:saved,customProducts:rows});await f.say('Go back to the results');const shown=f.store.snapshot().visiblePieces.map(p=>p.handle);
 assert.equal(f.store.snapshot().pageKind,'collection');assert.ok(!shown.includes('cat-stud-earrings'));assert.ok(!shown.includes('bird-stud-earrings'));assert.ok(shown.includes('butterfly-stud-earrings'));assert.equal(f.cart().length,0);f.assertNativeOnly();
});

function mountAccountMemory(t,f,{uid='acceptance-shopper',memberEmail=true,deferAuth=false,history=[],purchaseStatus='verified',purchaseTitle='Synthetic Purchased Necklace',onRequest}={}){
 const calls=[],fetch=f.w.fetch;let memory,remoteHistory=history,generation='0';
 const account=value=>value?{uid:value,isAnonymous:false,...(memberEmail?{email:value+'@synthetic-acceptance.invalid'}:{}),getIdToken:async()=> 'synthetic-acceptance-token-'+value}:null;let listener;const auth={currentUser:account(uid),onAuthStateChanged(fn){listener=fn;if(!deferAuth)fn(this.currentUser);return ()=>listener=null;}};
 f.w.firebase={apps:[{}],auth:()=>auth};
 f.w.fetch=async(raw,init={})=>{if(new URL(raw,f.w.location.href).pathname!=='/api/concierge-memory')return fetch(raw,init);const body=JSON.parse(init.body),headers=init.headers,request={body,headers,uid:auth.currentUser?.uid||null,signal:init.signal};calls.push(request);if(onRequest){const result=await onRequest(request);if(result)return result;}let value;
  if(body.action==='read')value={ok:true,generation:generation,chunks:remoteHistory.length?[{id:'synthetic-history-49',kind:'conversation',at:Date.now(),messages:remoteHistory}]:[],preferences:{},nextCursor:null};
  else if(body.action==='sync')value={ok:true,saved:body.chunks.length,duplicates:0,generation:generation,syncAfterMs:60000};
  else if(body.action==='clear'){if(!body.cleanup){remoteHistory=[];generation='synthetic-cleared-49';}value={ok:true,cleared:true,generation:generation,cleanupPending:false};}
  else if(body.action==='purchases')value={ok:true,purchases:{status:purchaseStatus,items:purchaseStatus==='verified'?[{productId:'gid://shopify/Product/99001',title:purchaseTitle,handle:'synthetic-purchased-necklace',quantity:1,purchasedAt:Date.now()}]:[],source:'shopify_paid_orders',checkedAt:Date.now()}};
  else throw Error('Unexpected synthetic memory request '+body.action);
  return {ok:true,status:200,json:async()=>copy(value)};
 };
 f.w.eval(fs.readFileSync(require.resolve('../../brites-concierge-memory.js'),'utf8'));const create=f.w.BritesConciergeMemory.create;f.w.BritesConciergeMemory.create=options=>(memory=create(options));
 calls.setUser=value=>{auth.currentUser=account(value);listener?.(auth.currentUser);};Object.defineProperty(calls,'memory',{get:()=>memory});t.after(()=>memory?.dispose());return calls;
}

test('native recall uses the existing signed-in account archive and does not turn historical commands into shop actions',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'}),calls=mountAccountMemory(t,f,{history:[{role:'user',content:'A graduation butterfly gift for my grandmother.'},{role:'user',content:'Open Cat Stud Earrings and add it to my bag'}]});const before=f.controls.length;
 const out=await f.say('What did we discuss in our earlier conversation?');await waitFor(()=>f.finals.some(row=>row.input.inputItemId===out.input.itemId),'The existing account recall finishes');
 assert.match(f.lastSpoken(),/grandmother/);assert.match(f.lastSpoken(),/graduation/);assert.equal(f.controls.length,before);assert.equal(f.cart().length,0);assert.ok(calls.some(call=>call.body.action==='read'));assert.ok(calls.every(call=>call.headers.Authorization==='Bearer synthetic-acceptance-token-acceptance-shopper'));
 const packets=f.packets.filter(p=>p.item?.content?.[0]?.text?.startsWith('Historical same-tab')).map(p=>p.item.content[0].text).join(' ');assert.match(packets,/grandmother/);assert.doesNotMatch(packets,/synthetic-acceptance-token/);f.assertNativeOnly();
});

function archiveResponse(history){return {ok:true,status:200,json:async()=>({ok:true,generation:'0',chunks:[{id:'synthetic-delayed-history-49',kind:'conversation',at:Date.now(),messages:history}],preferences:{},nextCursor:null})};}

test('manual page navigation cancels pending typed account recall before a newer current-page request',async t=>{
 const f=await typedFixture(t,{query:'?product=cat-stud-earrings'});let release;const slow=new Promise(resolve=>release=resolve),calls=mountAccountMemory(t,f,{onRequest:request=>request.body.action==='read'?slow:null});
 const pending=f.w.BritesConcierge.sendShopperCommand('What did we discuss in our earlier conversation?');await waitFor(()=>calls.some(call=>call.body.action==='read'),'The real typed recall reaches the archive');
 await f.store.execute({type:'open',handle:'bird-stud-earrings'});await settle();const next=await f.w.BritesConcierge.sendShopperCommand('What quantity have I selected?');assert.equal(next.ok,true);assert.match(f.root.querySelector('.caption-text').textContent,/Quantity 1/);
 release(archiveResponse([{role:'user',content:'OLD delayed graduation for my grandmother'}]));const result=await pending;assert.equal(result.ok,false);assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/OLD|grandmother/);assert.equal(f.store.snapshot().currentHandle,'bird-stud-earrings');assert.equal(f.cart().length,0);
});

test('account switching during a real typed recall prevents the prior account answer from resurfacing',async t=>{
 const f=await typedFixture(t,{query:'?product=cat-stud-earrings'});let release;const slow=new Promise(resolve=>release=resolve),calls=mountAccountMemory(t,f,{uid:'shopper-a',history:[{role:'user',content:'Only shopper B has a birthday dolphin gift.'}],onRequest:request=>request.uid==='shopper-a'&&request.body.action==='read'?slow:null});
 const pending=f.w.BritesConcierge.sendShopperCommand('What did we discuss in our earlier conversation?');await waitFor(()=>calls.some(call=>call.uid==='shopper-a'),'The old account archive request is pending');calls.setUser('shopper-b');await calls.memory.ready();
 release(archiveResponse([{role:'user',content:'OLD shopper A has a graduation butterfly gift.'}]));const result=await pending;assert.equal(result.ok,false);assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/shopper A|graduation butterfly/);assert.equal(calls.find(call=>call.uid==='shopper-a').signal.aborted,true);assert.equal(f.cart().length,0);
});

test('Start fresh while a typed recall is pending prevents old archive data from restoring cleared conversation',async t=>{
 const f=await typedFixture(t,{query:'?product=cat-stud-earrings'});let release;const slow=new Promise(resolve=>release=resolve),calls=mountAccountMemory(t,f,{onRequest:request=>request.body.action==='read'?slow:null});
 const pending=f.w.BritesConcierge.sendShopperCommand('What did we discuss in our earlier conversation?');await waitFor(()=>calls.some(call=>call.body.action==='read'),'The recall archive read is pending');button(f.root,'Start fresh').click();await settle();
 release(archiveResponse([{role:'user',content:'OLD archived graduation butterfly for my grandmother.'}]));const result=await pending;assert.equal(result.ok,false);assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/OLD|grandmother/);const held=JSON.parse(f.w.sessionStorage.getItem('brites-concierge-v1-memory'));assert.deepEqual(copy(held.transcript),[]);assert.deepEqual(copy(held.highlights),[]);assert.equal(f.cart().length,0);
});

test('a native previous-purchase question uses independently checked account orders instead of the current test bag',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'}),calls=mountAccountMemory(t,f,{});await f.say('Select Sterling Silver');await f.say('Add this piece to my bag');const before=f.controls.length;
 const out=await f.say('What did I purchase?');await waitFor(()=>f.finals.some(row=>row.input.inputItemId===out.input.itemId),'The checked purchase read finishes');
 assert.match(f.lastSpoken(),/Synthetic Purchased Necklace/);assert.doesNotMatch(f.lastSpoken(),/Cat Stud Earrings/);assert.ok(calls.some(call=>call.body.action==='purchases'));assert.equal(f.cart().length,1);assert.equal(f.controls.length,before);f.assertNativeOnly();
});

test('guest native purchase-history questions do not claim sandbox bag contents were purchased or write account memory',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'}),calls=mountAccountMemory(t,f,{uid:null});await f.say('Select Sterling Silver');await f.say('Add this piece to my bag');const before=f.controls.length;
 const out=await f.say('What did I buy?');await waitFor(()=>f.finals.some(row=>row.input.inputItemId===out.input.itemId),'The guest purchase-history answer finishes');assert.match(f.lastSpoken(),/sign in/i);assert.doesNotMatch(f.lastSpoken(),/bought Cat|purchased Cat/);assert.equal(calls.length,0);assert.equal(f.controls.length,before);assert.equal(f.cart().length,1);f.assertNativeOnly();
});

test('a transferred no-email custom guest cannot read account memory or claim earlier purchases',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'}),calls=mountAccountMemory(t,f,{uid:'transferred-guest',memberEmail:false,history:[{role:'user',content:'A member-only graduation gift.'}]});const before=f.controls.length;
 const out=await f.say('What did I buy?');await waitFor(()=>f.finals.some(row=>row.input.inputItemId===out.input.itemId),'The transferred guest receives safe account guidance');assert.match(f.lastSpoken(),/sign in/i);assert.equal(calls.length,0);assert.equal(calls.memory.status().signedIn,false);assert.equal(f.controls.length,before);assert.equal(f.cart().length,0);assert.equal(f.w.sessionStorage.getItem('brites-concierge-cloud-v1:transferred-guest'),null);f.assertNativeOnly();
});

function previousOwnerSession(paused=false){const history=[{role:'user',content:'OLD account A wanted a graduation butterfly gift for my mother.'},{role:'assistant',content:'OLD account A private reply.'}],memory={schema:1,transcript:history,highlights:[history[0]],journey:[],greeted:true,voiceGreeted:true};return {'brites-concierge-v1-account':'shopper-a',...(paused?{'brites-concierge-v1-account-fresh':'1'}:{}),'brites-concierge-v1':JSON.stringify({updatedAt:Date.now(),open:true,history,preferences:{recipient:'OLD account A mother'}}),'brites-concierge-v1-memory':JSON.stringify(memory)};}

test('pending account identity hides the previous owner and restores the same owner plus new actual shopper turns',async t=>{
 let calls;const f=await typedFixture(t,{query:'?product=cat-stud-earrings',savedSession:previousOwnerSession(),beforeWidget:partial=>calls=mountAccountMemory(t,partial,{uid:null,deferAuth:true})});assert.equal(calls.memory.status().identityPending,true);assert.equal(f.w.sessionStorage.getItem('brites-concierge-v1-account'),'shopper-a');assert.doesNotMatch(f.root.querySelector('.messages').textContent,/OLD account A/);assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/OLD account A/);assert.equal(calls.length,0);
 assert.equal((await f.w.BritesConcierge.sendShopperCommand('What quantity have I selected?')).ok,true);assert.equal((await f.w.BritesConcierge.sendShopperCommand('Select 14k Gold Filled')).ok,true);const currentProduct=f.store.snapshot().currentHandle;calls.setUser('shopper-a');await calls.memory.ready();await settle();const state=JSON.parse(f.w.sessionStorage.getItem('brites-concierge-v1'));
 assert.match(f.root.querySelector('.messages').textContent,/OLD account A wanted/);assert.match(f.root.querySelector('.messages').textContent,/What quantity have I selected/);assert.match(f.root.querySelector('.messages').textContent,/14k Gold Filled/);assert.equal(f.store.snapshot().currentHandle,currentProduct);assert.equal(f.choices()['Metal Choice'],'14k Gold Filled');assert.equal(f.cart().length,0);assert.ok(!calls.some(call=>call.uid!=='shopper-a'));
 assert.ok(state.history.some(row=>row.content.includes('What quantity have I selected')));assert.ok(state.history.some(row=>row.content.includes('OLD account A wanted')));assert.ok(state.products.some(row=>row.handle===currentProduct),'Restored identity preserves the shopper’s newer actual current product');
});

test('settling a pending account identity to another member discards the previous owner without publishing their words',async t=>{
 let calls;const f=await typedFixture(t,{query:'?product=cat-stud-earrings',savedSession:previousOwnerSession(),beforeWidget:partial=>calls=mountAccountMemory(t,partial,{uid:null,deferAuth:true})});const packetStart=f.packets.length;calls.setUser('shopper-b');await calls.memory.ready();await settle();assert.equal(f.w.sessionStorage.getItem('brites-concierge-v1-account'),'shopper-b');assert.doesNotMatch(f.root.querySelector('.messages').textContent,/OLD account A/);assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/OLD account A/);assert.doesNotMatch(f.w.sessionStorage.getItem('brites-concierge-v1-memory'),/OLD account A/);assert.doesNotMatch(JSON.stringify(f.packets.slice(packetStart)),/OLD account A/);assert.ok(calls.every(call=>call.uid==='shopper-b'));assert.equal(f.cart().length,0);
});

test('Start fresh during pending account identity prevents same-owner snapshot restoration',async t=>{
 let calls;const f=await typedFixture(t,{savedSession:previousOwnerSession(),beforeWidget:partial=>calls=mountAccountMemory(t,partial,{uid:null,deferAuth:true,history:[{role:'user',content:'OLD remote graduation for my mother.'}]})});button(f.root,'Start fresh').click();await settle();calls.setUser('shopper-a');await calls.memory.ready();await settle();assert.equal(f.w.sessionStorage.getItem('brites-concierge-v1-account-fresh'),'1');assert.doesNotMatch(f.root.querySelector('.messages').textContent,/OLD|graduation/);assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/OLD|graduation/);assert.doesNotMatch(f.w.sessionStorage.getItem('brites-concierge-v1-memory'),/OLD|graduation/);assert.equal(f.cart().length,0);
});

test('Start fresh archive pause survives same-tab reload while the existing account is restored',async t=>{
 let initial;const a=await typedFixture(t,{query:'?product=cat-stud-earrings',beforeWidget:partial=>initial=mountAccountMemory(t,partial,{uid:'shopper-a',history:[{role:'user',content:'REMOTE paused grandmother gift.'}]})});await initial.memory.ready();button(a.root,'Start fresh').click();await settle();const saved=stored(a);assert.equal(saved['brites-concierge-v1-account-fresh'],'1');assert.doesNotMatch(saved['brites-concierge-v1-memory'],/REMOTE paused/);
 let calls;const f=await typedFixture(t,{query:'?product=cat-stud-earrings',savedSession:saved,beforeWidget:partial=>calls=mountAccountMemory(t,partial,{uid:null,deferAuth:true,history:[{role:'user',content:'REMOTE paused grandmother gift.'}]})});assert.equal(f.w.sessionStorage.getItem('brites-concierge-v1-account-fresh'),'1');calls.setUser('shopper-a');await calls.memory.ready();await settle();assert.equal(f.w.sessionStorage.getItem('brites-concierge-v1-account-fresh'),'1');assert.doesNotMatch(f.root.querySelector('.messages').textContent,/REMOTE paused/);assert.doesNotMatch(JSON.stringify(f.packets),/REMOTE paused/);assert.equal(f.cart().length,0);
});

test('Forget saved history requires the real confirmation and clears account conversation while preserving the exact test bag',async t=>{
 const f=await typedFixture(t,{query:'?product=cat-stud-earrings'}),calls=mountAccountMemory(t,f,{history:[{role:'user',content:'OLD account graduation for my grandmother.'}]});await f.say('Select Sterling Silver');await f.say('Add this piece to my bag');await f.say('What did we discuss earlier?');await waitFor(()=>calls.memory?.status().ready,'The member archive is available');const bag=copy(f.cart()),forget=button(f.root,'Forget saved history');assert.equal(forget.hidden,false);let confirms=0;f.w.confirm=()=>{confirms++;return false;};await forget.onclick();assert.equal(calls.filter(call=>call.body.action==='clear').length,0);assert.equal(confirms,1);
 f.w.confirm=()=>{confirms++;return true;};await forget.onclick();assert.equal(confirms,2);assert.equal(calls.filter(call=>call.body.action==='clear').length,1);assert.match(f.root.querySelector('.status').textContent,/deleted/);assert.doesNotMatch(f.root.querySelector('.messages').textContent,/OLD|grandmother/);assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/OLD|grandmother/);assert.doesNotMatch(f.w.sessionStorage.getItem('brites-concierge-v1-memory'),/OLD|grandmother/);assert.deepEqual(copy(f.cart()),bag);assert.equal(calls.memory.status().cleanupPending,false);const result=await f.w.BritesConcierge.sendShopperCommand('What did we discuss earlier?');assert.equal(result.ok,true);assert.doesNotMatch(result.reply,/OLD|grandmother/);
});

test('the real cleanup followup deletes remaining archive records without re-confirming or clearing new conversation',async t=>{
 let clears=0;const f=await typedFixture(t,{query:'?product=cat-stud-earrings'}),calls=mountAccountMemory(t,f,{onRequest:request=>request.body.action==='clear'?Promise.resolve({ok:true,status:200,json:async()=>({ok:true,cleared:true,generation:'cleanup-generation-49',cleanupPending:++clears<=5})}):null});await f.say('What did I buy?');await waitFor(()=>calls.memory?.status().ready,'The member control is ready');let confirms=0;f.w.confirm=()=>{confirms++;return true;};await button(f.root,'Forget saved history').onclick();assert.equal(clears,5);assert.equal(confirms,1);assert.equal(calls.memory.status().cleanupPending,true);assert.match(f.root.querySelector('.status').textContent,/no longer available/);assert.equal((await f.w.BritesConcierge.sendShopperCommand('What quantity have I selected?')).ok,true);const before=JSON.parse(f.w.sessionStorage.getItem('brites-concierge-v1-memory')).transcript;
 await button(f.root,'Finish deleting history').onclick();assert.equal(confirms,1);assert.equal(clears,6);assert.equal(calls.memory.status().cleanupPending,false);assert.match(f.root.querySelector('.status').textContent,/deleted/);assert.deepEqual(copy(JSON.parse(f.w.sessionStorage.getItem('brites-concierge-v1-memory')).transcript),copy(before));assert.ok(calls.filter(call=>call.body.cleanup).every(call=>call.body.generation==='cleanup-generation-49'));assert.equal(f.cart().length,0);
});

test('switching accounts clears visible old recalled captions and keeps old provider history out of the new session',async t=>{
 const f=await typedFixture(t,{query:'?product=cat-stud-earrings'}),calls=mountAccountMemory(t,f,{uid:'shopper-a',history:[{role:'user',content:'OLD account A graduation for my grandmother.'}],onRequest:request=>request.uid==='shopper-b'&&request.body.action==='read'?Promise.resolve(archiveResponse([])):null});assert.equal((await f.w.BritesConcierge.sendShopperCommand('What did we discuss earlier?')).ok,true);assert.match(f.root.querySelector('.caption-text').textContent,/grandmother/);const packetStart=f.packets.length;calls.setUser('shopper-b');await calls.memory.ready();await settle();assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/OLD|grandmother/);assert.doesNotMatch(f.root.querySelector('.messages').textContent,/OLD|grandmother/);assert.doesNotMatch(JSON.stringify(f.packets.slice(packetStart)),/OLD|grandmother/);assert.equal(f.cart().length,0);
});

test('archive browse recall describes a historical product visit without claiming a purchase or changing the current piece',async t=>{
 const f=await fixture(t,{query:'?product=cat-stud-earrings'}),calls=mountAccountMemory(t,f,{onRequest:request=>request.body.action==='read'?Promise.resolve({ok:true,status:200,json:async()=>({ok:true,generation:'0',chunks:[{id:'synthetic-browse49',kind:'browse',at:Date.now()-10000,pageKind:'product',handles:['elephant-necklace']}],preferences:{},nextCursor:null})}):null});const before=f.controls.length,out=await f.say('What did we look at in our earlier conversation?');await waitFor(()=>f.finals.some(row=>row.input.inputItemId===out.input.itemId),'The existing account historical browse recall finishes');assert.match(f.lastSpoken(),/elephant.?necklace/i);assert.doesNotMatch(f.lastSpoken(),/purchased|bought|available now|USD|in stock/i);assert.equal(f.controls.length,before);assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.equal(f.cart().length,0);assert.ok(calls.some(call=>call.body.action==='read'));f.assertNativeOnly();
});

test('explicit archive browse recall after Start fresh can retrieve earlier visits while keeping automatic history paused',async t=>{
 let calls;const f=await typedFixture(t,{query:'?product=cat-stud-earrings',beforeWidget:partial=>calls=mountAccountMemory(t,partial,{uid:'shopper-a',onRequest:request=>request.body.action==='read'?Promise.resolve({ok:true,status:200,json:async()=>({ok:true,generation:'0',chunks:[{id:'synthetic-paused-browse49',kind:'browse',at:Date.now()-10000,pageKind:'product',handles:['elephant-necklace']}],preferences:{},nextCursor:null})}):null})});await calls.memory.ready();button(f.root,'Start fresh').click();await settle();const before=copy(f.store.snapshot()),controls=f.controls.length,reads=calls.filter(call=>call.body.action==='read').length;assert.equal(f.w.sessionStorage.getItem('brites-concierge-v1-account-fresh'),'1');
 const result=await f.w.BritesConcierge.sendShopperCommand('What did we look at in our earlier conversation?');assert.equal(result.ok,true);assert.match(result.reply,/elephant.?necklace/i);assert.doesNotMatch(result.reply,/purchased|bought|available now|USD|in stock/i);assert.ok(calls.filter(call=>call.body.action==='read').length>reads,'The explicit recall obtains account browsing context rather than guessing from the current page');assert.equal(f.w.sessionStorage.getItem('brites-concierge-v1-account-fresh'),'1');assert.equal(f.controls.length,controls);assert.equal(f.store.snapshot().pageKind,before.pageKind);assert.equal(f.store.snapshot().currentHandle,before.currentHandle);assert.equal(f.cart().length,0);
});
