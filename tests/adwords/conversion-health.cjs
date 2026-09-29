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
// Data Manager submissions past Google's 24 hours, with Google Ads' own import diagnostics for the action (synthetic rows).
async function diagnosed(summary,{stuck=15}={}){
 const seen=[],context=vm.createContext({Date,ENV:{GADS_CONVERSION_ACTION:'customers/123/conversionActions/456'},fb:()=>null,_accountTz:async()=> 'UTC',_acctDateYmd:()=> '2026-09-13',
  dataManagerService:()=>({health:async()=>({configured:true,processing:stuck,staleProcessing:stuck,confirmed:0,unknown:0,staleReasons:stuck?{'Google still answers PROCESSING':stuck}:{},oldestProcessingAt:Date.parse('2026-09-17T12:00:00Z')})}),
  gaql:async q=>{seen.push(q);
   if(q.includes('FROM offline_conversion_upload_conversion_action_summary')){if(summary instanceof Error)throw summary;return summary;}
   if(q.includes('FROM conversion_action'))return [{conversionAction:{id:'456',name:'offline (Upload)',status:'ENABLED',type:'UPLOAD_CLICKS',category:'PURCHASE'}}];
   if(q.includes('conversion_tracking_setting'))return [{customer:{conversionTrackingSetting:{conversionTrackingStatus:'CONVERSION_TRACKING_MANAGED_BY_SELF'}}}];
   return [{metrics:{conversions:1}}];
 }});vm.runInContext(source.slice(start,end),context);const out=await context.conversionHealth({force:true});out.seen=seen;return out;
}
const summaryRow=(x={})=>({offlineConversionUploadConversionActionSummary:{client:'GOOGLE_ADS_API',status:'EXCELLENT',totalEventCount:'15',successfulEventCount:'15',pendingEventCount:'0',lastUploadDateTime:'2026-09-17 10:00:00',dailySummaries:[],alerts:[],...x}});
(async()=>{
 const unavailable=await health('quota');assert.equal(unavailable.actionsChecked,false);assert.equal(unavailable.healthy,false);assert.equal(unavailable.validated,false);assert(!unavailable.reasons.some(s=>/not found|not enabled/.test(s)));
 const missing=await health('missing');assert.equal(missing.actionsChecked,true);assert(missing.reasons.some(s=>/not found/.test(s)));
 const enabled=await health('enabled');assert.equal(enabled.configuredAction.status,'ENABLED');assert.equal(enabled.healthy,true);assert.equal(enabled.validated,false);
 console.log('PASS conversion health separates failed lookup, missing action and enabled-but-unverified uploads');
 // Stuck submissions: Google Ads' import diagnostics say whether Google never received them, recorded them, or refused them.
 let h=await diagnosed([]);const q=h.seen.find(s=>s.includes('offline_conversion_upload_conversion_action_summary'));
 assert(/^SELECT\s/.test(q)&&/\.alerts\b/.test(q)&&/\.daily_summaries\b/.test(q)&&/WHERE offline_conversion_upload_conversion_action_summary\.conversion_action_id = 456$/.test(q),'one read-only summary query for the configured action');
 assert(h.reasons.some(s=>/show no recent imports, so Google has no record of the 15 stuck submission\(s\) in this action/.test(s)),'no imports on Google\'s side is said as such');
 assert.equal(h.googleUploads.received,0);
 h=await diagnosed([summaryRow({dailySummaries:[{uploadDate:'2026-09-24',successfulCount:'2',failedCount:'1',pendingCount:'0'}]})]);
 assert(h.reasons.some(s=>/\(EXCELLENT\): latest import day \(2026-09-17\) 15 received, 15 recorded, 0 pending; last 7 days 2 recorded, 1 failed, 0 pending\./.test(s)&&/probably recorded, and it is our status check/.test(s)),'recorded imports point at our status check');
 assert.deepEqual(JSON.parse(JSON.stringify(h.googleUploads.clients[0].week)),{recorded:2,failed:1,pending:0});
 h=await diagnosed([summaryRow({status:'NEEDS_ATTENTION',successfulEventCount:'9',alerts:[{error:{conversionUploadError:'CLICK_NOT_FOUND'},errorPercentage:0.4},{error:{}}]})]);
 assert(h.reasons.some(s=>/import errors for the configured action on its latest import day \(2026-09-17\): CLICK_NOT_FOUND \(40%\)\. Those conversions are refused, not processing\./.test(s)),'Google\'s refusal code and share are named');
 assert.equal(h.googleUploads.alerts.length,1,'an alert without an error code is dropped');
 h=await diagnosed([summaryRow()],{stuck:0});
 assert(!h.reasons.some(s=>/import diagnostics/.test(s)),'nothing is said about diagnostics while no submission is stuck');
 h=await diagnosed(Error('queryError=PROHIBITED_RESOURCE_TYPE_IN_FROM_CLAUSE'));
 assert(/PROHIBITED_RESOURCE_TYPE/.test(h.googleUploads.error)&&h.configuredAction.status==='ENABLED'&&h.reasons.some(s=>/waited more than 24 hours/.test(s)),'a failed diagnostics read leaves the rest of the health intact');
 console.log('PASS stuck Data Manager submissions are explained with Google Ads\' own import diagnostics');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exitCode=1});
