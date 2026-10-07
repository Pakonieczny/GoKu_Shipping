'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const demo=require('../../netlify/functions/_britesConciergeDemoTurn.js'),native=require('../../netlify/functions/_britesConciergeVoice.js');
const performance={mood:'curious',gesture:'acknowledge',intensity:.45,durationMs:1200};
const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'};
const request=body=>new Request('https://preview.test/api/concierge-demo-turn',{method:'POST',headers:{Origin:'https://preview.test','Content-Type':'application/json'},body:JSON.stringify({action:'conversation',message:'Can we chat?',...body})});
function fixture(result){const calls=[];const handler=demo.createHandler({env,rateLimit:async()=>true,reserveConversation:async()=>({allocatedUsd:.05}),completeTone:async args=>{calls.push(args);return result;}});return {handler,calls};}
for(const mood of demo.AVATAR_MOODS)test('mood is a bounded presentation choice shared by native and typed validators: '+mood,()=>{
  const value={...performance,mood};assert.deepEqual(demo.validateAvatarPerformance(value),value);assert.deepEqual(native.validateToolArguments(value,'set_avatar_performance'),value);
});
for(const gesture of demo.AVATAR_GESTURES)test('gesture is restricted to the supported robot performance vocabulary: '+gesture,()=>{
  const value={...performance,gesture};assert.deepEqual(demo.validateAvatarPerformance(value),value);assert.deepEqual(native.validateToolArguments(value,'set_avatar_performance'),value);
});
for(const value of [{...performance,intensity:0,durationMs:400},{...performance,intensity:1,durationMs:2500}])test('valid intensity/duration edge is preserved without coercion: '+JSON.stringify(value),()=>assert.deepEqual(demo.validateAvatarPerformance(value),value));
for(const value of [null,[],{}, {...performance,mood:'sentient'}, {...performance,gesture:'navigate'}, {...performance,intensity:'0.5'}, {...performance,intensity:-.1}, {...performance,intensity:1.1}, {...performance,intensity:Infinity}, {...performance,intensity:NaN}, {...performance,durationMs:'1200'}, {...performance,durationMs:399}, {...performance,durationMs:2501}, {...performance,durationMs:500.1}, {...performance,url:'https://outside.example'}, {...performance,productId:'gid://shopify/Product/1'}, {...performance,action:'cart'}, {...performance,thoughts:'private feelings'}, {mood:'warm',gesture:'greet',intensity:.3}])test('malformed or overpowered avatar envelope is rejected: '+JSON.stringify(value),()=>{
  assert.equal(demo.validateAvatarPerformance(value),null);assert.equal(native.validateToolArguments(value,'set_avatar_performance'),null);
});
test('native session describes a closed presentation-only function while preserving existing shop schemas and media bounds',()=>{
  const config=native.sessionConfig(),tool=config.tools.find(tool=>tool.name==='set_avatar_performance');assert.equal(config.tools.length,6);assert.equal(tool.parameters.additionalProperties,false);assert.deepEqual(tool.parameters.required,['mood','gesture','intensity','durationMs']);assert.deepEqual(Object.keys(tool.parameters.properties),['mood','gesture','intensity','durationMs']);assert.equal(tool.parameters.properties.durationMs.maximum,2500);
  assert.equal(config.max_output_tokens,1200);assert.equal(native.MAX_DURATION_MS,120000);assert.equal(config.audio.output.voice,'marin');assert.match(config.instructions,/simulated communication/);assert.match(config.instructions,/once for the current shopper turn/);assert.match(config.instructions,/never the website, products, shopping authority or voice audio/);assert.match(config.instructions,/three shopping tools and one optional expression tool/);assert.match(config.instructions,/Real confirmed cart feedback has host priority/);
  assert.deepEqual(config.tools[2].parameters.required,['handle','action']);assert.match(config.tools[2].parameters.properties.variantId.description,/Omit for view or options/);
});
test('validated genuine model-selected performance accompanies typed prose without adding action authority',async()=>{
  const f=fixture({reply:'We can take this at your pace.',tone:'warm',avatarPerformance:performance}),answer=await(await f.handler(request())).json();assert.equal(answer.aiUsed,true);assert.deepEqual(answer.avatarPerformance,performance);assert.equal(answer.conversationOnly,true);assert.equal(answer.actions,undefined);assert.equal(answer.products,undefined);
  assert.match(f.calls[0].system,/choose a tasteful short avatarPerformance/);assert.match(f.calls[0].system,/not inner thoughts, actual feelings/);assert.equal(f.calls[0].maxTokens,180);
});
test('invalid expression data is dropped while a separately valid safe reply survives',async()=>{
  const f=fixture({reply:'We can take this at your pace.',tone:'gentle',avatarPerformance:{...performance,gesture:'navigate',url:'https://outside.example'}}),answer=await(await f.handler(request())).json();assert.equal(answer.aiUsed,true);assert.equal(answer.reply,'We can take this at your pace.');assert.equal(answer.avatarPerformance,undefined);assert.doesNotMatch(JSON.stringify(answer),/outside\.example|navigate/);
});
test('valid expression data never rescues unsafe prose, links, facts or fabricated performed actions',async()=>{
  const f=fixture({reply:'I added the necklace to your bag.',tone:'warm',avatarPerformance:performance}),answer=await(await f.handler(request())).json();assert.equal(answer.aiUsed,false);assert.equal(answer.providerUnavailable,true);assert.equal(answer.avatarPerformance,undefined);
});
for(const message of ['I am grieving','My mother died','I’m frustrated today','This is not helpful'])test('explicit grief/frustration vetoes triumphant model performance: '+message,()=>{
  const chosen={...performance,mood:'celebrate',gesture:'confirm',intensity:.8,durationMs:2200};assert.deepEqual(demo.guardAvatarPerformance(chosen,message),{mood:'reassuring',gesture:'reassure',intensity:.35,durationMs:1500});
});
test('recent explicit grief context retains quiet presentation without constructing a private shopper profile',()=>{
  const chosen={...performance,mood:'celebrate',gesture:'confirm'};assert.equal(demo.guardAvatarPerformance(chosen,'Can we chat?',[{role:'user',content:'My friend passed away.'}]).mood,'reassuring');assert.deepEqual(demo.guardAvatarPerformance(chosen,'Not a memorial, I am celebrating a graduation'),chosen);assert.deepEqual(demo.guardAvatarPerformance(performance,'Thank you'),performance);
});
test('actual bounded progress is a model hint, while unknown/private progress and extra context are rejected',async()=>{
  for(const progress of demo.PUBLIC_PROGRESS){const f=fixture({reply:'We can take this at your pace.',tone:'warm',avatarPerformance:performance}),response=await f.handler(request({publicContext:{pageKind:'product',hasSelection:true,progress}}));assert.equal(response.status,200);assert.equal(JSON.parse(f.calls[0].prompt).publicContext.progress,progress);}
  for(const context of [{progress:'payment-complete'}, {progress:'inferred-sadness'}, {progress:'none',customerProfile:'private'}, {progress:{action:'cart'}}]){const f=fixture({reply:'Hello',tone:'warm'});assert.equal((await f.handler(request({publicContext:context}))).status,400);assert.equal(f.calls.length,0);}
});
