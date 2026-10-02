const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('netlify/functions/googleAdsAutopilot.js','utf8'),start=source.indexOf('async function conversionHealth('),end=source.indexOf('\n}',start)+2;
const goalEvidence=async()=>({readOnly:true,status:'unavailable',receiptConfirmationChecked:false,orderOverlapChecked:false,duplicateCountingVerified:false,campaignsComplete:false,campaigns:[],actions:[]});
async function health(mode){
 const context=vm.createContext({Date,ENV:{GADS_CONVERSION_ACTION:'customers/123/conversionActions/456',GADS_CONVERSION_UPLOAD_API:'legacy'},fb:()=>null,campaignGoalEvidence:goalEvidence,_accountTz:async()=> 'UTC',_acctDateYmd:()=> '2026-09-13',gaql:async q=>{
  if(mode==='quota')throw Error('Google request quota exhausted');
  if(q.includes('FROM conversion_action'))return mode==='missing'?[]:[{conversionAction:{id:'456',name:'offline (Upload)',status:'ENABLED',type:'UPLOAD_CLICKS',category:'PURCHASE'}}];
  if(q.includes('conversion_tracking_setting'))return [{customer:{conversionTrackingSetting:{conversionTrackingStatus:'CONVERSION_TRACKING_MANAGED_BY_SELF'}}}];
  return [{metrics:{conversions:1}}];
 }});vm.runInContext(source.slice(start,end),context);return context.conversionHealth({force:true});
}
// Data Manager submissions past Google's 24 hours, with Google Ads' own import diagnostics for the action (synthetic rows).
async function diagnosed(summary,{stuck=15,goalAudit=null,actions=null}={}){
 const seen=[],context=vm.createContext({Date,ENV:{GADS_CONVERSION_ACTION:'customers/123/conversionActions/456',BRITES_GROWTH_SANDBOX:'1'},fb:()=>null,campaignGoalEvidence:async()=>goalAudit||goalEvidence(),_accountTz:async()=> 'UTC',_acctDateYmd:()=> '2026-09-13',
  dataManagerService:()=>({health:async()=>({configured:true,processing:stuck,staleProcessing:stuck,confirmed:0,unknown:0,staleReasons:stuck?{'Google still answers PROCESSING':stuck}:{},oldestProcessingAt:Date.parse('2026-09-17T12:00:00Z')})}),
  gaql:async q=>{seen.push(q);
   if(q.includes('FROM offline_conversion_upload_conversion_action_summary')){if(summary instanceof Error)throw summary;return summary;}
   if(q.includes('FROM conversion_action'))return actions||[{conversionAction:{id:'456',name:'offline (Upload)',status:'ENABLED',type:'UPLOAD_CLICKS',category:'PURCHASE'}}];
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
 // Aggregate import diagnostics describe the action. They cannot identify
 // individual saved receipts, confirm purchase attribution, or prove absence.
 let h=await diagnosed([]);const q=h.seen.find(s=>s.includes('offline_conversion_upload_conversion_action_summary'));
 assert(/^SELECT\s/.test(q)&&/\.alerts\b/.test(q)&&/\.daily_summaries\b/.test(q)&&/WHERE offline_conversion_upload_conversion_action_summary\.conversion_action_id = 456$/.test(q),'one read-only summary query for the configured action');
 assert(h.reasons.some(s=>/show no recent imports\. These aggregate summaries do not confirm the 15 stored receipt\(s\)/.test(s)),'absent recent aggregate imports do not resolve individual saved receipts');
 assert.equal(h.dataManager.confirmed,0);assert.equal(h.validated,false);assert.equal(h.orderOverlapVerified,false);
 assert(!h.reasons.some(s=>/Google has no record of the|probably recorded/.test(s)),'absence of aggregate evidence is not individual rejection');
 assert.equal(h.googleUploads.received,0);
 h=await diagnosed([summaryRow({dailySummaries:[{uploadDate:'2026-09-24',successfulCount:'2',failedCount:'1',pendingCount:'0'}]})]);
 assert(h.reasons.some(s=>/\(EXCELLENT\): latest import day \(2026-09-17\) 15 received, 15 recorded, 0 pending; last 7 days 2 recorded, 1 failed, 0 pending\./.test(s)&&/this aggregate does not identify these stored receipts; verify their individual status and destination/.test(s)),'recorded aggregate imports are distinct from saved receipt confirmation');
 assert.equal(h.googleUploads.recorded,15);assert.equal(h.dataManager.confirmed,0);assert.equal(h.validated,false);
 assert(!h.reasons.some(s=>/probably recorded|count the same sales/.test(s)),'aggregate success does not identify these purchases');
 assert.deepEqual(JSON.parse(JSON.stringify(h.googleUploads.clients[0].week)),{recorded:2,failed:1,pending:0});
 h=await diagnosed([summaryRow({status:'NEEDS_ATTENTION',successfulEventCount:'9',alerts:[{error:{conversionUploadError:'CLICK_NOT_FOUND'},errorPercentage:0.4},{error:{}}]})]);
 assert(h.reasons.some(s=>/import errors for the configured action on its latest import day \(2026-09-17\): CLICK_NOT_FOUND \(40%\)\. Those conversions are refused, not processing\./.test(s)),'Google\'s refusal code and share are named');
 assert.equal(h.googleUploads.alerts.length,1,'an alert without an error code is dropped');
 assert.equal(h.dataManager.confirmed,0);assert.equal(h.dataManager.staleProcessing,15,'action-level error shares cannot assign failure to individual receipts');
 h=await diagnosed([summaryRow()],{stuck:0});
 assert(!h.reasons.some(s=>/import diagnostics/.test(s)),'nothing is said about diagnostics while no submission is stuck');
 h=await diagnosed(Error('queryError=PROHIBITED_RESOURCE_TYPE_IN_FROM_CLAUSE'));
 assert(/PROHIBITED_RESOURCE_TYPE/.test(h.googleUploads.error)&&h.configuredAction.status==='ENABLED'&&h.reasons.some(s=>/waited more than 24 hours/.test(s)),'a failed diagnostics read leaves the rest of the health intact');
 console.log('PASS overdue receipt outcomes remain unconfirmed regardless of aggregate import totals or error shares');
 const upload={id:'456',name:'offline (Upload)',status:'ENABLED',type:'UPLOAD_CLICKS',category:'PURCHASE',primaryForGoal:false};
 const webpage={id:'789',name:'Purchase',status:'ENABLED',type:'WEBPAGE',category:'PURCHASE',primaryForGoal:true};
 const verified=extra=>({readOnly:true,status:'verified',receiptConfirmationChecked:false,orderOverlapChecked:false,duplicateCountingVerified:false,campaignsComplete:true,campaigns:[],actions:[],...extra});
 for(const optimizationStatus of ['included','excluded','no_active_campaigns']){
  const audit=verified({actions:[{resourceName:'customers/123/conversionActions/456',optimizationStatus}]});
  h=await diagnosed([],{stuck:0,goalAudit:audit,actions:[{conversionAction:upload},{conversionAction:webpage}]});
  assert.equal(h.goalMembershipVerified,true);assert.equal(h.orderOverlapVerified,false);
  assert.equal(h.goalEvidence.actions[0].optimizationStatus,optimizationStatus);
  assert(h.reasons.some(s=>/campaign goal membership has been checked in the read-only goal audit/.test(s)&&/Order overlap remains unverified/.test(s)));
  assert(!h.reasons.some(s=>/campaign goal membership has not been verified|campaign goals have not been verified|bidding optimizes toward|make .*Primary|set .*Secondary/.test(s)));
  assert.equal(h.dataManager.confirmed,0);assert.equal(h.validated,false,'goal settings never prove provider processing');
 }
 for(const primaryForGoal of [true,undefined]){
  h=await diagnosed([],{stuck:0,goalAudit:verified(),actions:[{conversionAction:{...upload,primaryForGoal}},{conversionAction:webpage}]});
  assert.equal(h.goalMembershipVerified,true);assert.equal(h.orderOverlapVerified,false);
  assert(h.reasons.some(s=>/Campaign goal membership has been checked/.test(s)&&/Order overlap remains unverified/.test(s)));
  assert(!h.reasons.some(s=>/campaign goals have not been verified|count the same sales|make .*Primary|set .*Secondary/.test(s)));
  assert.equal(h.validated,false);
 }
 for(const status of ['partial','unavailable']){
  h=await diagnosed([],{stuck:0,goalAudit:{...verified(),status},actions:[{conversionAction:upload},{conversionAction:webpage}]});
  assert.equal(h.goalMembershipVerified,false);assert.equal(h.orderOverlapVerified,false);
  assert(h.reasons.some(s=>/campaign goal membership has not been verified/.test(s)));
  assert(!h.reasons.some(s=>/goal membership has been checked/.test(s)));
 }
 console.log('PASS goal-health explanations respect checked campaign membership while retaining order-overlap uncertainty');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exitCode=1});
