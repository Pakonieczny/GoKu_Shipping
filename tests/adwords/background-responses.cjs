const assert=require('node:assert/strict');
const {createAdDesignAdapters,TEXT_MODEL}=require('../../netlify/functions/googleAdsAdDesignAdapters');
(async()=>{
 const docs=new Map(),calls=[];let complete=false;
 const db={collection:name=>({doc:id=>({get:async()=>({exists:docs.has(name+'/'+id),data:()=>docs.get(name+'/'+id)}),set:async value=>docs.set(name+'/'+id,value)})})};
 const deps={fb:()=>({db}),env:{OPENAI_API_KEY:'fixture'},responsePollWindowMs:0,fetch:async(url,options)=>{calls.push({url,options});return {ok:true,json:async()=>options.method==='POST'?{id:'resp_fixture',status:'queued'}:complete?{id:'resp_fixture',status:'completed',model:TEXT_MODEL,output_text:'saved output',usage:{input_tokens:1,output_tokens:1}}:{id:'resp_fixture',status:'in_progress'}};}};
 let a=createAdDesignAdapters(deps);await assert.rejects(()=>a.responses({model:TEXT_MODEL,background:true},'same-paid-request'),e=>e.providerPending===true);assert.equal(calls.filter(c=>c.options.method==='POST').length,1);assert(await a.hasResponse('same-paid-request'));
 complete=true;a=createAdDesignAdapters(deps);const result=await a.retrieveResponse('same-paid-request');assert.equal(result.output_text,'saved output');assert.equal(calls.filter(c=>c.options.method==='POST').length,1);await a.responses({model:TEXT_MODEL,background:true},'same-paid-request');assert.equal(calls.filter(c=>c.options.method==='POST').length,1);assert.equal(JSON.parse(calls[0].options.body).store,true);
 assert.equal(await a.retrieveResponse('unknown'),null);
 console.log('PASS background receipt survives worker replacement and retrieves the same paid response without another POST');
})().catch(e=>{console.error(e);process.exitCode=1;});
