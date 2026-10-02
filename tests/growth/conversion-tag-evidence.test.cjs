'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url');
const helper=require('../../netlify/functions/_britesGrowthConversionTagEvidence');
let checks=0;const eq=(a,b,m)=>{assert.deepEqual(a,b,m);checks++;},ok=(a,m)=>{assert.ok(a,m);checks++;};
const action=(id='12345',extra={})=>({conversionAction:{resourceName:'customers/99999/conversionActions/'+id,id,name:'Purchase',status:'ENABLED',type:'WEBPAGE',category:'PURCHASE',...extra}});
const snippet=destination=>({eventSnippet:`gtag('event','conversion',{'send_to':'${destination}'});`,globalSiteTag:'PRIVATE_GLOBAL_HTML'});
async function endpoint(){const root=path.resolve(__dirname,'../..');let source=fs.readFileSync(path.join(root,'netlify/functions/britesGrowthAds.js'),'utf8');for(const name of ['_britesGrowth.js','_britesGrowthDemand.js','_britesGrowthConversionTagEvidence.js'])source=source.replaceAll("'./"+name+"'",JSON.stringify(pathToFileURL(path.join(root,'netlify/functions',name)).href));return import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));}
(async()=>{
  let queries=[];
  const result=await helper.read({gaql:async query=>{queries.push(query);return [action('12345',{tagSnippets:[snippet('AW-123456789/Exact_label-1'),snippet('AW-123456789/Exact_label-1')]}),action('12346',{status:'REMOVED',tagSnippets:[snippet('AW-123456789/Removed')]}),action('12347',{type:'UPLOAD_CLICKS'})];},now:()=>777});
  eq(queries,[helper.QUERY]);ok(helper.QUERY.includes("status IN ('ENABLED', 'REMOVED')"));ok(helper.QUERY.includes("category = 'PURCHASE'"));ok(helper.QUERY.endsWith('LIMIT 201'));
  eq(result.complete,true);eq(result.checkedAt,777);eq(result.actions.length,3);eq(result.actions[0].destinations,[{destination:'AW-123456789/Exact_label-1',conversionId:'AW-123456789',label:'Exact_label-1'}]);eq(result.actions[1].status,'REMOVED');eq(result.actions[2].destinationState,'no_destination_returned');
  for(const key of ['providerWrites','runtimeDispatchVerified','transactionIdentityVerified','duplicateCountingVerified'])eq(result[key],false);eq(result.conversionUploads,0);
  ok(!JSON.stringify(result).includes('PRIVATE_GLOBAL_HTML'));ok(!JSON.stringify(result).includes('gtag('));
  const falseMatches=helper.project([action('1',{tagSnippets:[{globalSiteTag:"{'send_to':'AW-123456789/GlobalOnly'}",eventSnippet:"{'other':'AW-123456789/Other','send_to':'AW-1234/Short','send_to':'AW-123456789/too$long','send_to':'AW-123456789/"+'x'.repeat(101)+"'}"}]})]);eq(falseMatches.actions[0].destinations,[]);
  eq(falseMatches.complete,false,'malformed destinations do not produce complete mapping evidence');
  const snake=helper.project([{conversion_action:{resource_name:'customers/99/conversionActions/7',id:'7',name:'X',type:'WEBPAGE',status:'ENABLED',category:'PURCHASE',tag_snippets:[{event_snippet:'{"send_to":"AW-12345/Valid"}'}]}}]);eq(snake.actions[0].destinations[0].label,'Valid');
  for(const malformed of [null,{},action('1',{resourceName:'customers/99/conversionActions/2'}),action('1',{id:9007199254740992}),action('1',{category:'LEAD'}),action('1',{status:'HIDDEN'}),action('1',{type:'<script>'})]){const projected=helper.project([malformed]);eq(projected.complete,false);eq(projected.actions,[]);}
  eq(helper.project([action('1'),action('1')]).complete,false);eq(helper.project([action('1'),action('1')]).actions.length,1);
  const over=helper.project(Array.from({length:201},(_,i)=>action(String(i+1))));eq(over.complete,false);eq(over.truncated,true);eq(over.actions.length,200);
  for(const snippets of [{},[null],Array.from({length:13},()=>snippet('AW-12345/Valid')),[{eventSnippet:'x'.repeat(100001)}]]){const projected=helper.project([action('1',{tagSnippets:snippets})]);eq(projected.complete,false);eq(projected.actions[0].complete,false);}
  eq(helper.project({results:[]}).complete,false);eq(helper.project([]).complete,true);
  const fail=await helper.read({gaql:async()=>{throw Error('SECRET_ACCOUNT RAW_SNIPPET');}});eq(fail.state,'unavailable');eq(fail.complete,false);ok(!JSON.stringify(fail).includes('SECRET_ACCOUNT'));
  const {createHandler,READ_ACTIONS}=await endpoint();ok(READ_ACTIONS.includes('conversionActionTagEvidence'));
  const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_GROWTH_ADMIN_KEY:'fixture-only-key'};let loads=0,calls=0;
  const handler=createHandler({environment:()=>env,loadEngine:async()=>{loads++;return{gaql:async query=>{calls++;eq(query,helper.QUERY);return[action('1',{tagSnippets:[snippet('AW-12345/Test')]})];}};}});
  const call=(body,headers={})=>handler(new Request('https://sandbox.example/api/growth-ads',{method:'POST',headers:{'X-Growth-Key':'fixture-only-key',...headers},body:JSON.stringify(body)}));
  eq((await call({action:'conversionActionTagEvidence'},{'X-Growth-Key':''})).status,401);eq((await call({action:'conversionActionTagEvidence'},{Origin:'https://other.example'})).status,403);eq(loads,0);
  for(const key of ['query','labels','statuses','force','apply','limit','conversionId','customerId'])eq((await call({action:'conversionActionTagEvidence',[key]:true})).status,400);eq(loads,0);
  const response=await call({action:'conversionActionTagEvidence'});eq(response.status,200);eq(calls,1);eq((await response.json()).sandboxReadOnly,true);
  env.BRITES_GROWTH_SANDBOX='0';eq((await call({action:'conversionActionTagEvidence'})).status,503);eq(loads,1);env.BRITES_GROWTH_SANDBOX='1';env.BRITES_GROWTH_NAMESPACE='live';eq((await call({action:'conversionActionTagEvidence'})).status,503);eq(loads,1);
  const failHandler=createHandler({environment:()=>({...env,BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox'}),loadEngine:async()=>{throw Error('SECRET_ENGINE_ERROR');}});const engineFailure=await failHandler(new Request('https://sandbox.example/api/growth-ads',{method:'POST',headers:{'X-Growth-Key':'fixture-only-key'},body:JSON.stringify({action:'conversionActionTagEvidence'})}));eq(engineFailure.status,503);ok(!(await engineFailure.text()).includes('SECRET_ENGINE_ERROR'));
  console.log(`${checks} conversion tag evidence checks passed`);
})().catch(error=>{console.error(error);process.exitCode=1;});
