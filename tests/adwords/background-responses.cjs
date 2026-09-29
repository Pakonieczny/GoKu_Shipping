const assert=require('node:assert/strict'),crypto=require('node:crypto'),{Readable}=require('node:stream');
const {createAdDesignAdapters,TEXT_MODEL,LEGACY_TEXT_MODEL}=require('../../netlify/functions/googleAdsAdDesignAdapters');
const {parseResponse}=require('../../netlify/functions/googleAdsAdDesignResearch');
// Durable Sonnet receipts: a marker is saved before sending and the compact answer after it.
// Everything is offline: a fake Anthropic stream and an in-memory Firestore.
const CLAUDE_URL='https://api.anthropic.com/v1/messages',sse=events=>Readable.from(events.map(e=>'event: '+e.type+'\ndata: '+JSON.stringify(e)+'\n\n'));
const stream=(text,{stop='end_turn',cut=false}={})=>({ok:true,status:200,headers:{get:()=>null},body:sse([{type:'message_start',message:{id:'msg_fixture',type:'message',role:'assistant',model:TEXT_MODEL,content:[],usage:{input_tokens:1200,output_tokens:1}}},{type:'content_block_start',index:0,content_block:{type:'text',text:''}},{type:'content_block_delta',index:0,delta:{type:'text_delta',text}},...(cut?[]:[{type:'content_block_stop',index:0},{type:'message_delta',delta:{stop_reason:stop},usage:{output_tokens:300}},{type:'message_stop'}])])});
(async()=>{
 const docs=new Map(),calls=[],markersAtSend=[],key=id=>'brites_creative_responses/'+crypto.createHash('sha256').update(id).digest('hex');let reply=()=>stream('{"saved":"output"}'),legacyStatus='in_progress';
 const db={collection:name=>({doc:id=>({get:async()=>({exists:docs.has(name+'/'+id),data:()=>docs.get(name+'/'+id)}),set:async value=>docs.set(name+'/'+id,value)})})};
 const deps={fb:()=>({db}),env:{ANTHROPIC_API_KEY:'fixture',OPENAI_API_KEY:'fixture'},responsePollWindowMs:0,sleep:async()=>{},log:()=>{},fetch:async(url,options)=>{calls.push({url,options});if(url===CLAUDE_URL){markersAtSend.push([...docs.values()].map(v=>v.requestId+(v.response?':answer':':marker')));return reply();}
  return {ok:true,json:async()=>legacyStatus==='completed'?{id:'resp_fixture',status:'completed',model:LEGACY_TEXT_MODEL,output_text:'saved output',usage:{input_tokens:1000,output_tokens:100}}:{id:'resp_fixture',status:'in_progress'}};}};
 const request={model:TEXT_MODEL,background:true,input:[{role:'user',content:[{type:'input_text',text:'Design the ad.'}]}],text:{format:{type:'json_schema',name:'fixture',strict:true,schema:{type:'object',additionalProperties:false,properties:{saved:{type:'string'}},required:['saved']}}}};
 const sent=()=>calls.filter(c=>c.url===CLAUDE_URL).length;

 // A completed answer is saved compactly and reused by a replacement worker without paying again.
 let a=createAdDesignAdapters(deps);const first=await a.responses(request,'paid-request');
 assert.equal(first.output_text,'{"saved":"output"}');assert.equal(first.status,'completed');assert.equal(first.costEstimated,false);assert.equal(first.estimatedUsd,(1200*2+300*10)/1e6);
 assert.deepEqual(markersAtSend[0],['paid-request:marker'],'the submitted marker is saved before the request is sent');
 const saved=docs.get(key('paid-request'));assert(saved.submittedAt&&saved.completedAt&&saved.response.output_text===first.output_text&&saved.response.output.length===0&&saved.response.claudeUsage.output_tokens===300,'compact receipt keeps the answer, usage and model once');
 a=createAdDesignAdapters(deps);assert(await a.hasResponse('paid-request'));
 assert.deepEqual(await a.retrieveResponse('paid-request'),first);assert.deepEqual(await a.responses(request,'paid-request'),first);assert.equal(sent(),1,'a later worker reuses the paid answer without another request');

 // An unconfirmed marker inside its window may still be streaming elsewhere: wait, never replace it.
 docs.set(key('in-flight'),{requestId:'in-flight',provider:'anthropic',model:TEXT_MODEL,submittedAt:Date.now()});
 await assert.rejects(()=>a.retrieveResponse('in-flight'),e=>e.providerPending===true);await assert.rejects(()=>a.responses(request,'in-flight'),e=>e.providerPending===true);
 assert(await a.hasResponse('in-flight'),'an explicit resume may wait on a streaming marker');assert.equal(sent(),1,'an unconfirmed marker never sends a replacement');
 const waiting=createAdDesignAdapters({...deps,responsePollWindowMs:60000,sleep:async()=>{docs.set(key('in-flight'),{...docs.get(key('in-flight')),completedAt:Date.now(),response:saved.response});}});
 assert.equal((await waiting.retrieveResponse('in-flight')).output_text,first.output_text,'an answer the other worker saves while waiting is collected');assert.equal(sent(),1);
 // After the window no worker can still be streaming it: it has definitely ended, so the operator's Retry can proceed.
 docs.set(key('stale'),{requestId:'stale',provider:'anthropic',model:TEXT_MODEL,submittedAt:Date.now()-17*60000});
 const ended=await a.retrieveResponse('stale');assert.equal(ended.status,'failed');assert.equal(ended.error.code,'worker_stopped');assert.equal(ended.costEstimated,true,'the reservation stands in for an unknown charge');
 assert.throws(()=>parseResponse(ended),e=>e.code==='AI_RESPONSE_STOPPED'&&e.definiteResponse===true);assert.equal(sent(),1,'reading an ended marker sends nothing');

 // A stream that stops mid-answer ends its marker at once; the shared client does not replay it.
 reply=()=>stream('{"sav',{cut:true});const stopped=await a.responses(request,'cut-request');
 assert.equal(stopped.status,'failed');assert.equal(stopped.error.code,'stream_interrupted');assert(docs.get(key('cut-request')).interruptedAt);assert.equal(sent(),2,'a started answer is never re-sent automatically');
 assert.equal((await createAdDesignAdapters(deps).retrieveResponse('cut-request')).status,'failed');assert.equal(sent(),2);
 // A rejection that never started an answer is definite and uncharged.
 reply=()=>({ok:false,status:400,headers:{get:()=>null},text:async()=>JSON.stringify({type:'error',error:{type:'invalid_request_error',message:'Fixture invalid'}})});
 await assert.rejects(()=>a.responses(request,'rejected'),e=>e.definiteResponse===true&&e.notDispatched===true&&/rejected the request \(HTTP 400\)/.test(e.message));assert(docs.get(key('rejected')).failedAt);assert.equal(sent(),3);
 // max_tokens and refusals are receipts too, and read back the same way.
 reply=()=>stream('{"saved":"out',{stop:'max_tokens'});const truncated=await a.responses(request,'max-tokens');
 assert.equal(truncated.status,'incomplete');assert.equal(truncated.incomplete_details.reason,'max_output_tokens');assert.throws(()=>parseResponse(truncated),e=>e.code==='AI_OUTPUT_INCOMPLETE');
 assert.deepEqual(await createAdDesignAdapters(deps).retrieveResponse('max-tokens'),truncated);
 reply=()=>stream('I cannot help with that.',{stop:'refusal'});await a.responses(request,'refusal');
 assert.throws(()=>parseResponse(docs.get(key('refusal')).response),e=>e.code==='AI_REFUSAL','the saved refusal keeps its refusal part');assert.equal(sent(),5);
 // No key: refused before a marker or a request exists.
 await assert.rejects(()=>createAdDesignAdapters({...deps,env:{OPENAI_API_KEY:'fixture'}}).responses(request,'no-key'),e=>e.notDispatched===true&&/ANTHROPIC_API_KEY/.test(e.message));assert(!docs.has(key('no-key')));assert.equal(sent(),5);

 // A response bought from the former Astra model keeps opening, at its original rates, without a new request.
 docs.set(key('astra-request'),{requestId:'astra-request',responseId:'resp_fixture',submittedAt:Date.now()});
 await assert.rejects(()=>a.retrieveResponse('astra-request'),e=>e.providerPending===true);legacyStatus='completed';
 const legacy=await createAdDesignAdapters(deps).responses(request,'astra-request');assert.equal(legacy.output_text,'saved output');assert.equal(legacy.model,LEGACY_TEXT_MODEL);assert(Math.abs(legacy.estimatedUsd-.015)<1e-12);
 assert.equal(calls.filter(c=>c.options.method==='POST').length,sent(),'only Sonnet requests were ever posted');
 assert.equal(await a.retrieveResponse('unknown'),null);
 console.log('PASS durable Sonnet receipts: marker before sending, reuse without paying again, bounded unconfirmed window, no automatic replay, saved Astra answers still open');
})().catch(e=>{console.error(e);process.exitCode=1;});
