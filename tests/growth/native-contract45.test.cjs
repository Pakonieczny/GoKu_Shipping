'use strict';
// Server and browser contract checks with synthetic provider/storage fixtures.
// These do not establish microphone recognition or audible speech playback.
const test=require('node:test'),assert=require('node:assert/strict');
const server=require('../../netlify/functions/_britesConciergeVoice.js');
const client=require('../../brites-concierge-voice.js');
const {fixture,AT,KEY}=require('./voice-fixture36.cjs');
const lineId='line:45-1',handle='solid-gold-necklace';
const validators=[server,client];

test('a native provider session actually receives declared homepage and exact cart edit controls',async()=>{
  const f=fixture(),before=f.protectedBytes(),answer=await f.start();
  assert.equal(answer.response.status,200);
  const config=JSON.parse(f.provider.find(p=>p.url.endsWith('/calls')).init.body.get('session'));
  const tool=config.tools.find(t=>t.name==='control_storefront');
  assert.ok(tool);
  for(const type of ['home','select-option','product-quantity','add','bag','bag-quantity','bag-remove','bag-select-option','bag-set-engraving'])assert.ok(tool.parameters.properties.type.enum.includes(type),type);
  assert.deepEqual(tool.parameters.required,['type']);
  assert.equal(tool.parameters.additionalProperties,false);
  assert.equal(config.max_output_tokens,1200);
  assert.equal(config.tools.length,6);
  assert.equal(answer.value.maxDurationMs,120000);
  assert.doesNotMatch(JSON.stringify(answer.value),new RegExp(KEY));
  assert.equal(f.protectedBytes(),before,'new capabilities do not change money or control records');
  assert.equal(f.journal()[0].stage,'call_verified');
  assert.deepEqual(f.deadlines,[{callId:f.journal()[0].callId,expiresAt:AT+120000}]);
});

test('home is an exact argument-free control, never arbitrary navigation or hidden selectors',()=>{
  for(const implementation of validators){
    assert.deepEqual(implementation.validateToolArguments({type:'home'},'control_storefront'),{type:'home'});
    for(const value of [{type:'home',handle},{type:'home',url:'https://example.invalid'},{type:'home',query:'all'},{type:'home',section:'catalogue'},{type:'home',actions:[{type:'add',handle}]},{type:'home',confirmed:true}])assert.equal(implementation.validateToolArguments(value,'control_storefront'),null,JSON.stringify(value));
  }
});

test('cart option edits require one exact stable line and one published string choice',()=>{
  const valid={type:'bag-select-option',lineId,optionName:'Chain Length',optionValue:'16 inches'};
  for(const implementation of validators){
    assert.deepEqual(implementation.validateToolArguments(valid,'control_storefront'),valid);
    for(const change of [{lineId:undefined},{lineId:'../cart'},{lineId:'line'.repeat(51)},{optionName:undefined},{optionValue:undefined},{optionName:' '},{optionValue:''},{optionValue:16},{optionValue:'16\u0000 inches'},{optionName:'x'.repeat(121)},{optionValue:'x'.repeat(301)},{handle},{variantId:'gid://shopify/ProductVariant/451'},{quantity:2},{selector:'#private'}])assert.equal(implementation.validateToolArguments({...valid,...change},'control_storefront'),null,JSON.stringify(change));
  }
});

test('cart engraving accepts exact bounded literal text or explicit clearing with no extra target authority',()=>{
  for(const implementation of validators){
    for(const text of ['', 'Exact shopper words', 'x'.repeat(300)]){
      const value={type:'bag-set-engraving',lineId,text};
      assert.deepEqual(implementation.validateToolArguments(value,'control_storefront'),value);
    }
    for(const text of [undefined,null,3,new String('word'),'x'.repeat(301),'private\u0000text','private\u0085text','hidden\u202econtent'])assert.equal(implementation.validateToolArguments({type:'bag-set-engraving',lineId,text},'control_storefront'),null);
    for(const value of [{type:'bag-set-engraving',text:'missing line'},{type:'bag-set-engraving',lineId:'invalid line',text:''},{type:'bag-set-engraving',lineId,text:'words',handle},{type:'bag-set-engraving',lineId,text:'words',giftNote:'extra'},{type:'bag-set-engraving',lineId,text:'words',variantId:'gid://shopify/ProductVariant/451'}])assert.equal(implementation.validateToolArguments(value,'control_storefront'),null);
  }
});

test('new controls retain selected-product, quantity and purchase authority boundaries',()=>{
  for(const implementation of validators){
    assert.deepEqual(implementation.validateToolArguments({type:'select-option',handle,optionName:'Metal Choice',optionValue:'14k Solid Gold'},'control_storefront'),{type:'select-option',handle,optionName:'Metal Choice',optionValue:'14k Solid Gold'});
    for(const quantity of [1,20])assert.ok(implementation.validateToolArguments({type:'bag-quantity',lineId,quantity},'control_storefront'));
    for(const quantity of [0,21,1.5,'2'])assert.equal(implementation.validateToolArguments({type:'bag-quantity',lineId,quantity},'control_storefront'),null);
    for(const type of ['purchase','payment','order','bag-replace-all','execute-plan'])assert.equal(implementation.validateToolArguments({type},'control_storefront'),null);
    assert.equal(implementation.validateToolArguments({type:'add',handle,quantity:3,confirmation:true},'control_storefront'),null);
  }
});

test('rate-limited reconnects return the next real minute boundary without provider or money changes',async()=>{
  for(const [at,reject,expectedMs,expectedSeconds] of [[AT,'site',60000,'60'],[AT+42500,'caller',17500,'18'],[AT+59999,'site',1,'1']]){
    const f=fixture({at,rateLimit:key=>reject==='site'?key!=='realtime-preview-start':!key.startsWith('realtime-preview-caller-')}),before=f.protectedBytes();
    const answer=await f.start();
    assert.equal(answer.response.status,429);
    assert.equal(answer.value.code,'VOICE_RATE_LIMITED');
    assert.equal(answer.value.retryAfterMs,expectedMs);
    assert.equal(answer.response.headers.get('Retry-After'),expectedSeconds);
    assert.equal(answer.response.headers.get('Cache-Control'),'no-store');
    assert.equal(f.counts.starts,0);
    assert.equal(f.journal().length,0);
    assert.equal(f.protectedBytes(),before);
    assert.equal(f.rates[0].limit,3);
    if(reject==='caller')assert.equal(f.rates[1].limit,2);
  }
});

test('safe renewal can depend only on explicit remote stop confirmation, preserving exact deadlines and money',async()=>{
  for(const [status,throws,confirmed] of [[200,false,true],[204,false,true],[404,false,true],[500,false,false],[200,true,false]]){
    const f=fixture({hangupStatus:status,hangupThrows:throws}),before=f.protectedBytes(),started=await f.start();
    const row=f.journal()[0],answer=await f.call({action:'stop',stopToken:started.value.stopToken});
    assert.equal(answer.value.stopped,confirmed);
    const deadline=f.rows.get('VoiceDeadlines/'+row.callId);
    assert.equal(deadline.state,confirmed?'closed':'pending');
    assert.equal(deadline.expiresAt,AT+120000);
    assert.equal(f.protectedBytes(),before);
    assert.equal(f.counts.starts,1,'stop confirmation never creates another provider call');
  }
});

test('declared reconnect duration remains clipped to the existing hard call limit and sandbox stop',async()=>{
  assert.equal(server.MAX_DURATION_MS,120000);
  for(const remaining of [1,30000,119999,120000,900000]){
    const f=fixture({control:{stopAt:AT+remaining}}),answer=await f.start();
    assert.equal(answer.response.status,200);
    assert.equal(answer.value.maxDurationMs,Math.min(remaining,120000));
    assert.equal(answer.value.expiresAt,AT+Math.min(remaining,120000));
    assert.equal(f.deadlines[0].expiresAt,answer.value.expiresAt);
  }
});

test('native guide directions distinguish current choices from discovery and describe only shopper-facing receipts',()=>{
  const config=server.sessionConfig();
  assert.match(config.instructions,/take me home or to the homepage uses control_storefront home/);
  assert.match(config.instructions,/These are shopping controls, never a motif, meaning or new product search/);
  assert.match(config.instructions,/One control_storefront call lets the host resolve every explicit choice/);
  assert.match(config.instructions,/Never guess a karat, missing option or unavailable combination/);
  assert.match(config.instructions,/host-provided reply or customerMessage/);
  assert.match(config.instructions,/Never read implementation details, backend terms, tool names, raw status values/);
  assert.match(config.instructions,/Never buy, submit orders, enter payment or contact details/);
  assert.match(config.instructions,/Never echo engraving or gift-note text into summaries or context/);
});
