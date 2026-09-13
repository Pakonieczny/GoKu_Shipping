const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('netlify/functions/googleAdsAutopilot.js','utf8'),start=source.indexOf('async function conversionHealth('),end=source.indexOf('\n}',start)+2;
async function health(mode){
 const context=vm.createContext({Date,ENV:{GADS_CONVERSION_ACTION:'customers/123/conversionActions/456',GADS_CONVERSION_UPLOAD_API:'legacy'},fb:()=>null,_accountTz:async()=> 'UTC',_acctDateYmd:()=> '2026-09-13',gaql:async q=>{
  if(mode==='quota')throw Error('Google request quota exhausted');
  if(q.includes('FROM conversion_action'))return mode==='missing'?[]:[{conversionAction:{id:'456',name:'offline (Upload)',status:'ENABLED',type:'UPLOAD_CLICKS',category:'PURCHASE'}}];
  if(q.includes('conversion_tracking_setting'))return [{customer:{conversionTrackingSetting:{conversionTrackingStatus:'CONVERSION_TRACKING_MANAGED_BY_SELF'}}}];
  return [{metrics:{conversions:1}}];
 }});vm.runInContext(source.slice(start,end),context);return context.conversionHealth({force:true});
}
(async()=>{
 const unavailable=await health('quota');assert.equal(unavailable.actionsChecked,false);assert.equal(unavailable.healthy,false);assert.equal(unavailable.validated,false);assert(!unavailable.reasons.some(s=>/not found|not enabled/.test(s)));
 const missing=await health('missing');assert.equal(missing.actionsChecked,true);assert(missing.reasons.some(s=>/not found/.test(s)));
 const enabled=await health('enabled');assert.equal(enabled.configuredAction.status,'ENABLED');assert.equal(enabled.healthy,true);assert.equal(enabled.validated,false);
 console.log('PASS conversion health separates failed lookup, missing action and enabled-but-unverified uploads');
})().catch(e=>{console.error(e);process.exitCode=1});
