'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const server=require('../../netlify/functions/_britesConciergeDemoTurn.js');
const client=require('../../brites-concierge-voice.js');

function req(body,origin='https://preview.test'){return new Request('https://preview.test/api/concierge-demo-turn',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify(body)});}
const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'};
const checked={reply:'Checked.',question:'Which feels closest?',products:[{title:'Christmas Tree Charm',currency:'USD',minPrice:35,why:'A seasonal family tradition.'}],meanings:[{text:'This can be a personal reminder of gathering together.',context:'Christmas'}],internal:'never expose'};

test('demo route is sandbox-only, same-origin and explicitly capability gated',async()=>{
  let completions=0;const enabled=server.createHandler({env,completeTone:async()=>{completions++;return '{"tone":"warm"}';}});
  assert.deepEqual(await (await enabled(req({action:'capabilities'}))).json(),{enabled:true,aiAvailable:true,mode:'browser-speech-bridge',maxDurationMs:120000,maxTurns:12});
  assert.equal((await enabled(req({action:'turn',message:'hi',catalogue:checked},'https://evil.test'))).status,403);
  const disabled=server.createHandler({env:{...env,BRITES_GROWTH_SANDBOX:'0'},completeTone:async()=>'{"tone":"warm"}'});
  assert.equal((await disabled(req({action:'turn',message:'hi',catalogue:checked}))).status,503);assert.equal(completions,0);
});

test('model may choose only tone; spoken product facts come from checked projection',async()=>{
  let input;const handler=server.createHandler({env,rateLimit:async()=>true,completeTone:async value=>{input=value;return '{"tone":"celebratory","speech":"Invented Diamond Crown for $1"}';}});
  const response=await handler(req({action:'turn',message:'A Christmas gift',history:[{role:'user',content:'for mom'}],catalogue:checked}),{ip:'203.0.113.2'}),body=await response.json();
  assert.equal(response.status,200);assert.equal(body.tone,'celebratory');assert.equal(body.aiUsed,true);assert.match(body.speech,/Christmas Tree Charm/);assert.match(body.speech,/\$35\.00/);assert.doesNotMatch(body.speech,/Diamond Crown|internal|https?:/);assert.equal(input.model,'gpt-4o-mini');assert.equal(input.maxTokens,40);assert.ok(input.system.includes('Do not return prose'));
});

test('invalid or oversized data and rate limits fail before inference',async()=>{
  let calls=0;const handler=server.createHandler({env,completeTone:async()=>{calls++;return '{}';},rateLimit:async()=>false});
  assert.equal((await handler(req({action:'turn',message:'x',catalogue:checked,actionUrl:'/cart'}))).status,400);
  assert.equal((await handler(req({action:'turn',message:'x'.repeat(601),catalogue:checked}))).status,400);
  assert.equal((await handler(req({action:'turn',message:'hello',catalogue:checked}))).status,429);assert.equal(calls,0);
});

test('provider failure is a marked deterministic fallback and never invents a product',async()=>{
  const handler=server.createHandler({env,completeTone:async()=>{throw Error('provider secret');}});
  const result=await (await handler(req({action:'turn',message:'surprise me',catalogue:{products:[],question:'Necklace or earrings?'}}))).json();
  assert.equal(result.aiUsed,false);assert.equal(result.tone,'warm');assert.equal(result.speech,'Of course. Necklace or earrings?');assert.doesNotMatch(JSON.stringify(result),/secret|product/i);
});

test('missing provider still leaves the bounded browser voice demo usable and marked non-AI',async()=>{
  const handler=server.createHandler({env,rateLimit:async()=>true});
  assert.deepEqual(await (await handler(req({action:'capabilities'}))).json(),{enabled:true,aiAvailable:false,mode:'browser-speech-bridge',maxDurationMs:120000,maxTurns:12});
  const result=await (await handler(req({action:'turn',message:'A Christmas gift',catalogue:checked}))).json();
  assert.equal(result.enabled,true);assert.equal(result.aiUsed,false);assert.match(result.speech,/Christmas Tree Charm/);
});

test('catalogue sanitizer drops actions, URLs and malformed products',()=>{
  const value=server.sanitizeCatalogue({products:[{title:'Checked Piece',currency:'CAD',minPrice:42,why:'A thought.',url:'https://competitor.test'},{title:'Bad',minPrice:'none'}],meanings:[{text:'Personal interpretation',sources:[{url:'https://private.test'}]}],question:'One or two?',action:'checkout'});
  assert.deepEqual(value,{products:[{title:'Checked Piece',currency:'CAD',minPrice:42,why:'A thought.'}],meanings:[{text:'Personal interpretation',context:''}],question:'One or two?'});
});

test('Netlify AI Gateway bridge uses the injected key and canonical OpenAI v1 endpoint',async()=>{
  let observed;
  const complete=server.createGatewayTone({base:'https://gateway.test',key:'sandbox-gateway-key',fetchImpl:async(url,init)=>{
    observed={url,init};return Response.json({choices:[{message:{content:'{"tone":"gentle"}'}}]});
  }});
  assert.equal(await complete({model:'gpt-4o-mini',maxTokens:40,system:'fixed',prompt:'checked'}),'{"tone":"gentle"}');
  assert.equal(observed.url,'https://gateway.test/v1/chat/completions');
  assert.equal(observed.init.headers.Authorization,'Bearer sandbox-gateway-key');
  assert.equal(JSON.parse(observed.init.body).model,'gpt-4o-mini');
  assert.equal(server.gatewayEndpoint('https://gateway.test/v1/'),'https://gateway.test/v1/chat/completions');
  assert.equal(server.createGatewayTone({base:'http://unsafe.test',key:'x'}),null);
});

// The former browser-speech bridge remains tested above for compatibility,
// but the shopper adapter must never silently fall back to that synthetic voice.
function unavailableNativeFixture({native=false,denied=false}={}){
  let recognition=0,synthesis=0,microphones=0;const requests=[],errors=[];
  const runtime={location:{origin:'https://preview.test'},document:{hidden:false,addEventListener(){},removeEventListener(){}},navigator:{mediaDevices:{getUserMedia:async()=>{microphones++;throw Error('must not request mic');}}},SpeechRecognition:class{constructor(){recognition++;}},SpeechSynthesisUtterance:class{},speechSynthesis:{speak(){synthesis++;},cancel(){synthesis++;}},AbortController,setTimeout,clearTimeout,addEventListener(){},removeEventListener(){},fetch:async(url,init)=>{requests.push({url,body:JSON.parse(init.body)});return Response.json(denied?{enabled:false,message:'Preview allocation is paused.'}:{enabled:true});}};
  if(native)runtime.RTCPeerConnection=class{};
  const voice=client.create({runtime,onError:value=>errors.push(value)});
  return {voice,requests,errors,counts:()=>({recognition,synthesis,microphones})};
}
test('synthetic browser voice cannot masquerade as native OpenAI conversation',async()=>{
  const f=unavailableNativeFixture();assert.equal(await f.voice.start(),false);assert.equal(f.voice.state,'idle');assert.equal(f.requests.length,0);assert.deepEqual(f.counts(),{recognition:0,synthesis:0,microphones:0});assert.match(f.errors[0],/OpenAI voice/);await f.voice.dispose();
});
test('native preview allocation denial preserves typing without launching old speech bridge',async()=>{
  const f=unavailableNativeFixture({native:true,denied:true});assert.equal(await f.voice.start(),false);assert.equal(f.requests.length,1);assert.equal(f.requests[0].url,'/api/concierge-voice');assert.deepEqual(f.counts(),{recognition:0,synthesis:0,microphones:0});assert.equal(f.voice.state,'idle');assert.match(f.errors[0],/allocation/);await f.voice.dispose();
});
