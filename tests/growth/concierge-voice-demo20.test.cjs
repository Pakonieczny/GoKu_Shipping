'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const server=require('../../netlify/functions/_britesConciergeDemoTurn.js');
const client=require('../../brites-concierge-voice.js');

function req(body,origin='https://preview.test'){return new Request('https://preview.test/api/concierge-demo-turn',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify(body)});}
const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'};
const checked={reply:'Checked.',question:'Which feels closest?',products:[{title:'Christmas Tree Charm',currency:'USD',minPrice:35,why:'A seasonal family tradition.'}],meanings:[{text:'This can be a personal reminder of gathering together.',context:'Christmas'}],internal:'never expose'};

test('demo route is sandbox-only, same-origin and explicitly capability gated',async()=>{
  let completions=0;const enabled=server.createHandler({env,completeTone:async()=>{completions++;return '{"tone":"warm"}';}});
  assert.deepEqual(await (await enabled(req({action:'capabilities'}))).json(),{enabled:true,mode:'browser-speech-bridge',maxDurationMs:120000,maxTurns:12});
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

test('catalogue sanitizer drops actions, URLs and malformed products',()=>{
  const value=server.sanitizeCatalogue({products:[{title:'Checked Piece',currency:'CAD',minPrice:42,why:'A thought.',url:'https://competitor.test'},{title:'Bad',minPrice:'none'}],meanings:[{text:'Personal interpretation',sources:[{url:'https://private.test'}]}],question:'One or two?',action:'checkout'});
  assert.deepEqual(value,{products:[{title:'Checked Piece',currency:'CAD',minPrice:42,why:'A thought.'}],meanings:[{text:'Personal interpretation',context:''}],question:'One or two?'});
});

function browserFixture(){
  const states=[],transcripts=[],levels=[],errors=[],requests=[],recognitions=[],spoken=[],timers=new Set();let utterance;
  class Recognition{constructor(){recognitions.push(this);this.started=0;this.aborted=0;}start(){this.started++;}abort(){this.aborted++;}emit(text){this.onresult?.({resultIndex:0,results:Object.assign([[{transcript:text}]],{0:Object.assign([{transcript:text}],{isFinal:true}),length:1})});}}
  class Utterance{constructor(text){this.text=text;utterance=this;}}
  const runtime={location:{origin:'https://preview.test'},document:{documentElement:{lang:'en-US'},hidden:false,addEventListener(){},removeEventListener(){}},navigator:{},SpeechRecognition:Recognition,SpeechSynthesisUtterance:Utterance,speechSynthesis:{cancelled:0,cancel(){this.cancelled++;},speak(value){spoken.push(value.text);value.onstart?.();}},AbortController,fetch:async(url,init)=>{const body=JSON.parse(init.body);requests.push({url,body});if(body.action==='capabilities')return Response.json({enabled:true,mode:'browser-speech-bridge',maxDurationMs:120000,maxTurns:12});return Response.json({enabled:true,speech:'Of course. Christmas Tree Charm from $35.00.',tone:'warm',aiUsed:true});},setTimeout(fn,ms){const id=setTimeout(fn,ms);timers.add(id);return id;},clearTimeout(id){clearTimeout(id);timers.delete(id);},addEventListener(){},removeEventListener(){}};
  const voice=client.create({runtime,onState:value=>states.push(value),onTranscript:value=>transcripts.push(value),onLevel:value=>levels.push(value),onError:value=>errors.push(value),onTool:async()=>checked});
  return {runtime,voice,states,transcripts,levels,errors,requests,recognitions,spoken,get utterance(){return utterance;}};
}

test('browser fallback is opt-in, listens, checks catalogue, speaks and animates bounded states',async()=>{
  const f=browserFixture();assert.equal(f.recognitions.length,0);assert.equal(f.requests.length,0);assert.equal(await f.voice.start(),true);assert.equal(f.recognitions.length,1);assert.equal(f.states.at(-1),'listening');
  f.recognitions[0].emit('A Christmas gift');await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));
  assert.ok(f.states.includes('thinking'));assert.equal(f.states.at(-1),'speaking');assert.deepEqual(f.spoken,['Of course. Christmas Tree Charm from $35.00.']);assert.equal(f.requests.filter(x=>x.body.action==='turn').length,1);assert.equal(f.requests.find(x=>x.body.action==='turn').body.catalogue.internal,'never expose');
  assert.equal(f.transcripts[0].spokenOnly,true);assert.equal(f.transcripts.at(-1).spokenOnly,true);const starts=f.recognitions[0].started,cancels=f.runtime.speechSynthesis.cancelled;f.voice.interrupt();assert.ok(f.runtime.speechSynthesis.cancelled>cancels);assert.ok(f.recognitions[0].started>starts);assert.equal(f.states.at(-1),'listening');await f.voice.stop();assert.equal(f.voice.state,'idle');assert.ok(f.recognitions[0].aborted>0);
});

test('foreign fallback endpoint is rejected before microphone or fetch',()=>{assert.throws(()=>client.create({runtime:{location:{origin:'https://preview.test'}},demoEndpoint:'https://evil.test/demo'}),/must be on this website/);});
