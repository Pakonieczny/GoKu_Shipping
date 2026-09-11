// Verify the official preview request and its scope without contacting Google.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict');
const file=path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js'),realRequire=require('node:module').createRequire(file);
const groupRef='customers/123/assetGroups/7',workspace={settings:{productId:'11',groupRef},context:{campaignId:'42',groups:[{ref:groupRef,channel:'pmax',name:'Necklaces'}]}};
let calls=[],response={result:{previews:[{assetGroup:groupRef,expirationDateTime:'2026-10-01T12:00:00Z',uiPreviewResult:{shareablePreviewUrl:'https://ads.google.com/aw/preview/example'}}]}},ok=true;
const fetch=async(url,input)=>{calls.push({url,...input,body:JSON.parse(input.body)});return {ok,json:async()=>response};};
const context=vm.createContext({module:{exports:{}},exports:{},require:n=>n==='node-fetch'?fetch:realRequire(n),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,URLSearchParams,setTimeout,clearTimeout});vm.runInContext(fs.readFileSync(file,'utf8'),context);
context.fixture={ref:{get:async()=>({exists:true,data:()=>workspace})},gaql:async query=>{assert(query.includes("asset_group.resource_name = '"+groupRef+"'")&&query.includes('campaign.id = 42'));return [{assetGroup:{resourceName:groupRef},campaign:{id:'42'}}];}};
vm.runInContext('_adDesignWorkspaceRef=()=>fixture.ref;gaql=fixture.gaql;mintToken=async()=>"fixture"',context);
const preview=context.module.exports.adDesignGooglePreview,input={workspaceId:'work',productId:'11',groupRef};let checks=0;
(async()=>{
 const result=await preview(input);assert(result.url===response.result.previews[0].uiPreviewResult.shareablePreviewUrl&&result.includesEditorArtwork===false&&result.scope==='current_google_assets');checks++;
 assert(calls.length===1&&calls[0].url==='https://googleads.googleapis.com/v24/customers/123:generateShareablePreviews'&&calls[0].body.operation.shareablePreviews[0].assetGroup===groupRef&&calls[0].body.operation.shareablePreviews[0].previewType==='UI_PREVIEW');checks++;
 assert(!calls.some(c=>/mutate/.test(c.url))&&result.message.includes('Unsent Creative Studio'));checks++;
 await assert.rejects(()=>preview({...input,productId:'22'}),/product or ad group changed/);checks++;
 workspace.context.groups[0].channel='search';assert((await preview(input)).supported===false&&calls.length===1);checks++;workspace.context.groups[0].channel='pmax';
 workspace.settings.groupRef='customers/999/assetGroups/7';workspace.context.groups[0].ref=workspace.settings.groupRef;await assert.rejects(()=>preview({...input,groupRef:workspace.settings.groupRef}),/connected Google/);checks++;workspace.settings.groupRef=groupRef;workspace.context.groups[0].ref=groupRef;
 response={result:{previews:[{assetGroup:'customers/123/assetGroups/8',uiPreviewResult:{shareablePreviewUrl:'https://ads.google.com/other'}}]}};await assert.rejects(()=>preview(input),/no usable preview/);checks++;
 response={result:{previews:[{assetGroup:groupRef,uiPreviewResult:{shareablePreviewUrl:'https://google.com.example.test/unsafe'}}]}};await assert.rejects(()=>preview(input),/no usable preview/);checks++;
 ok=false;response={error:{message:'Preview unavailable',status:'FAILED_PRECONDITION'}};await assert.rejects(()=>preview(input),/Google could not generate/);checks++;
 console.log('PASS '+checks+' official Google preview request, account/group isolation and error checks');
})().catch(e=>{console.error(e);process.exitCode=1;});
