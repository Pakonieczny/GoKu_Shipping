// Money safety: the console fails closed without EDIT_PASSCODE, only the server can drive the
// worker, and no enable / budget / end-date / approval / automatic change can pass Paul's daily
// ceiling or monthly stop. No network: Google Ads, Firestore and the worker are local fakes.
const fs=require('fs'),vm=require('vm'),assert=require('assert/strict'),path=require('path');
const dir=path.resolve(__dirname,'../../netlify/functions')+'/',realRequire=require('module').createRequire(dir+'googleAdsAutopilot.js');
const clone=x=>x==null?x:JSON.parse(JSON.stringify(x));
let passed=0;const check=(v,msg)=>{assert(v,msg);passed++;};
const UNSET='Set EDIT_PASSCODE in Netlify to enable changes';
const SECRETS={GADS_REFRESH_TOKEN:'refresh',GADS_CLIENT_SECRET:'secret',GADS_DEVELOPER_TOKEN:'dev'};

/* ---------- router + worker (fake engine, fake dispatch) ---------- */
function load(file,env,E,calls){const mod={exports:{}},saved=[];const admin={firestore:()=>({collection:c=>({doc:d=>({get:async()=>({exists:false}),set:async(v,o)=>{saved.push([c+'/'+d,v]);}})})})};admin.firestore.FieldValue={serverTimestamp:()=>Date.now()};
  const ctx={process:{env:{URL:'https://example.invalid',...env}},console,Date,Set,JSON,module:mod,exports:mod.exports,require:n=>n==='node-fetch'?async(url,opts)=>{calls.push(['dispatch',JSON.parse(opts.body)]);return {ok:true,status:202};}:n==='./googleAdsAutopilot'?E:n==='./firebaseAdmin'?admin:require(n)};
  vm.createContext(ctx);vm.runInContext(fs.readFileSync(dir+file,'utf8'),ctx);return {api:mod.exports,ctx,saved};}
function fakeEngine(calls,ctrl){return {COL:{state:'state',control:'control',approvals:'approvals'},control:async()=>{calls.push(['control']);return ctrl();},dashboard:async()=>({ok:true}),opportunitiesWithStatus:async()=>({opportunities:[]}),
  monthlySpendGuard:async()=>{calls.push(['monthly']);return {ok:true,tripped:false};},mineSearchTerms:async()=>{calls.push(['mine']);return {};},applyApproval:async(id,c,o)=>{calls.push(['apply',id,o]);return {ok:true};}};}
const post=(body,headers={})=>({httpMethod:'POST',headers,body:JSON.stringify(body)});

(async()=>{
 // 1. EDIT_PASSCODE unset: reads stay open, every change is refused with one clear sentence.
 let calls=[],ctrl=()=>({enabled:true,dryRun:false}),E=fakeEngine(calls,()=>ctrl()),K=load('googleAdsAutopilotKick.js',{...SECRETS},E,calls);
 let r=await K.api.httpHandler(post({action:'dashboard'}));check(r.statusCode===200&&JSON.parse(r.body).editPasscodeSet===false,'dashboard readable and reports that no passcode is set');
 r=await K.api.httpHandler(post({action:'opportunities'}));check(r.statusCode===200,'cached opportunities readable');
 const src=fs.readFileSync(dir+'googleAdsAutopilotKick.js','utf8'),listed=[...src.matchAll(/if \(\[([^\]]+)\]\.includes\(a\)\)/g)].flatMap(m=>[...m[1].matchAll(/"([A-Za-z0-9_]+)"/g)].map(x=>x[1]));
 const routed=[...new Set([...src.matchAll(/\ba\s*===\s*["']([A-Za-z0-9_]+)["']/g)].map(m=>m[1]).concat(listed))],reads=vm.runInContext('READ_ACTIONS',K.ctx);
 check(routed.length>100&&[...reads].every(a=>routed.includes(a)),'every read-only action exists in the router');
 const changes=routed.filter(a=>!K.ctx.isReadAction(a,{})).map(a=>({action:a,id:'1'})).concat([{action:'opportunities',force:true},{action:'someFutureAction'},{}]);
 for(const body of changes)for(const headers of [{},{'x-edit-passcode':'guess'}]){calls.length=0;r=await K.api.httpHandler(post({...body,passcode:'guess'},headers));const j=JSON.parse(r.body);
   assert(r.statusCode===403&&j.error===UNSET&&j.code==='EDIT_PASSCODE_NOT_SET'&&calls.length===0,'refused without running: '+(body.action||'(none)'));}
 check(changes.length>60,'all '+changes.length+' changing, paid, deleting, control and unknown actions refused before any work');
 // The worker: anonymous or guessed tokens refused; only the server-derived token works.
 const B=load('googleAdsAutopilot-background.js',{...SECRETS},E,calls),token=K.ctx.workerToken();
 check(/^internal-[a-f0-9]{64}$/.test(token)&&token===B.ctx.workerToken(),'router and worker derive the same server-only token');
 for(const t of [undefined,'','internal-'+'0'.repeat(64),'guess']){calls.length=0;r=await B.api.handler(post({tasks:['publishApproval'],id:'d1',token:t}));assert(r.statusCode===403&&JSON.parse(r.body).code==='EDIT_PASSCODE_NOT_SET'&&calls.length===0);}
 check(true,'worker refuses anonymous and guessed tokens with no passcode set');
 calls.length=0;r=await B.api.handler(post({tasks:['publishApproval'],id:'d1',token}));check(r.statusCode===200&&calls.some(c=>c[0]==='apply'&&c[1]==='d1'&&c[2].waitForLeaseMs>0),'server token publishes, waiting its turn behind a running publication');
 const bare=load('googleAdsAutopilot-background.js',{},E,calls);check(bare.ctx.workerToken()===undefined,'no passcode and no Google secrets: no token exists');
 calls.length=0;r=await bare.api.handler(post({tasks:['monthly'],token:'internal-'}));check(r.statusCode===403&&calls.length===0,'and the worker accepts nothing');
 // Scheduled kick with automation off: only the monthly stop keeps checking (it can only pause).
 ctrl=()=>({enabled:false,maxMonthlySpend:500});calls.length=0;r=await K.api.kick();const d=calls.find(c=>c[0]==='dispatch');
 check(d&&JSON.stringify(d[1].tasks)==='["monthly"]'&&d[1].token===token,'automation off: hourly kick still sends the monthly stop check');
 ctrl=()=>({enabled:false});calls.length=0;await K.api.kick();check(!calls.some(c=>c[0]==='dispatch'),'automation off, no monthly stop: nothing dispatched');
 calls.length=0;r=JSON.parse((await B.api.handler(post({tasks:['monthly'],token}))).body);check(calls.some(c=>c[0]==='monthly')&&r.status!=='dormant','worker runs the monthly stop with automation off');
 calls.length=0;r=JSON.parse((await B.api.handler(post({tasks:['monthly','mine'],token}))).body);check(r.status==='dormant'&&!calls.some(c=>c[0]==='mine'||c[0]==='monthly'),'any optimisation task keeps the whole run dormant');

 // 2. EDIT_PASSCODE set (pasted with quotes and a trailing space): exact passcode only.
 calls=[];ctrl=()=>({enabled:true});E=fakeEngine(calls,()=>ctrl());K=load('googleAdsAutopilotKick.js',{EDIT_PASSCODE:'"s3cret-pass" ',...SECRETS},E,calls);
 r=await K.api.httpHandler(post({action:'dashboard'},{'x-edit-passcode':'s3cret-pass'}));check(r.statusCode===200&&JSON.parse(r.body).editPasscodeSet===true,'trimmed, unquoted passcode accepted');
 for(const h of [{},{'x-edit-passcode':'s3cret'},{'x-edit-passcode':'s3cret-pass-x'}]){r=await K.api.httpHandler(post({action:'setStatus',id:'1',status:'ENABLED'},h));assert.equal(r.statusCode,401);}
 check(true,'missing or wrong passcode refused (401)');
 const B2=load('googleAdsAutopilot-background.js',{EDIT_PASSCODE:'"s3cret-pass" ',...SECRETS},E,calls);check(K.ctx.workerToken()==='s3cret-pass','worker token is the passcode once set');
 r=await B2.api.handler(post({tasks:['publishApproval'],id:'d1',token:'wrong'}));check(r.statusCode===401,'worker refuses a wrong passcode');
 // Control limits are validated: an empty or invalid ceiling can no longer switch the checks off.
 const H={'x-edit-passcode':'s3cret-pass'};
 for(const patch of [{maxDailyBudgetTotal:''},{maxDailyBudgetTotal:'abc'},{maxDailyBudgetTotal:0},{maxDailyBudgetTotal:null},{maxMonthlySpend:-5},{anomalySpendMultiple:1}]){K.saved.length=0;r=await K.api.httpHandler(post({action:'setControl',patch},H));assert(r.statusCode===400&&/Nothing was saved/.test(JSON.parse(r.body).error)&&!K.saved.length,JSON.stringify(patch));}
 check(true,'invalid money limits refused and nothing saved');
 K.saved.length=0;r=await K.api.httpHandler(post({action:'setControl',patch:{maxDailyBudgetTotal:'150',maxMonthlySpend:900}},H));check(r.statusCode===200&&K.saved[0][1].maxDailyBudgetTotal===150&&K.saved[0][1].maxMonthlySpend===900,'valid limits saved as numbers');

 /* ---------- engine: spend limits against a fake account (CAD, ceiling 100) ---------- */
 await engineChecks();
 console.log('PASS '+passed+' money-safety checks (passcode fail-closed, worker token, daily ceiling, monthly stop, approvals, automatic changes)');
})().catch(e=>{console.error(e);process.exit(1);});

function memory(){const docs=new Map();let n=0;
 const doc=p=>({path:p,id:p.split('/').pop(),get:async()=>({exists:docs.has(p),id:p.split('/').pop(),data:()=>clone(docs.get(p))}),set:async(v,o)=>{docs.set(p,o&&o.merge?{...docs.get(p),...clone(v)}:clone(v));},update:async v=>{if(!docs.has(p))throw Error('missing '+p);docs.set(p,{...docs.get(p),...clone(v)});},collection:c=>col(p+'/'+c)});
 const col=(p,fs=[])=>({doc:id=>doc(p+'/'+id),add:async v=>{const r=doc(p+'/auto'+(++n));await r.set(v);return r;},where:(k,op,v)=>col(p,fs.concat([[k,v]])),limit:()=>col(p,fs),
   get:async()=>{const list=[...docs].filter(([k,v])=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')&&fs.every(([fk,fv])=>v[fk]===fv)).map(([k,v])=>({id:k.split('/').pop(),data:()=>clone(v)}));return {docs:list,empty:!list.length,forEach:fn=>list.forEach(fn)};}});
 return {docs,db:{collection:c=>col(c),runTransaction:async fn=>fn({get:r=>r.get(),set:(r,v,o)=>r.set(v,o),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})},FV:{serverTimestamp:()=>Date.now()}};}

async function engineChecks(){
 const env={GADS_CUSTOMER_ID:'123'};let onWait=null;
 const cx=vm.createContext({module:{exports:{}},exports:{},require:n=>n==='node-fetch'?async()=>{throw Error('Live network forbidden');}:realRequire(n),process:{env},console,Buffer,Date,Intl,Map,Set,URL,setTimeout:fn=>{if(onWait)onWait();return setImmediate(fn);},clearTimeout});
 vm.runInContext(fs.readFileSync(dir+'googleAdsAutopilot.js','utf8'),cx);const E=cx.module.exports,get=n=>vm.runInContext(n,cx);
 const bind=v=>{cx.__m=v;vm.runInContext(Object.keys(v).map(k=>k+'=__m.'+k).join('\n'),cx);};
 const ymd=get('_acctDateYmd'),tz='America/Toronto',today=ymd(tz,0),past=ymd(tz,-10*86400000),later=ymd(tz,30*86400000);
 const B=n=>'customers/123/campaignBudgets/'+n;
 let W,f,muts,ledgers;
 const reset=()=>{W={rate:0.7,mtdNative:0,yesterday:0,baseline:[],terms:[],keywords:[],camps:{
   1:{name:'A',status:'ENABLED',serving:'SERVING',res:B(11)},2:{name:'B',status:'ENABLED',serving:'ENDED',res:B(12),end:past},3:{name:'C',status:'PAUSED',serving:'NONE',res:B(13)},
   4:{name:'D',status:'PAUSED',serving:'NONE',res:B(11)},5:{name:'E',status:'ENABLED',serving:'SERVING',res:B(15)}},budgets:{[B(11)]:60,[B(12)]:28,[B(13)]:35,[B(15)]:30},pauseFails:false};
   f=memory();muts=[];ledgers=[];};
 const row=id=>{const c=W.camps[id];return {campaign:{id:String(id),name:c.name,status:c.status,servingStatus:c.serving,resourceName:'customers/123/campaigns/'+id,startDateTime:'2026-07-14 00:00:00',endDateTime:c.end?c.end+' 23:59:59':null},campaignBudget:{resourceName:c.res,amountMicros:String(W.budgets[c.res]*1e6)}};};
 const gaql=async q=>{
  if(q.includes("FROM campaign WHERE campaign.status = 'ENABLED'"))return Object.keys(W.camps).filter(id=>W.camps[id].status==='ENABLED').map(row);
  let m=q.match(/FROM campaign WHERE campaign\.id = (\d+)/);if(m)return W.camps[m[1]]?[row(m[1])]:[];
  if(q.includes('FROM campaign_budget WHERE'))return [...q.matchAll(/'([^']+)'/g)].map(x=>x[1]).filter(r=>r in W.budgets).map(r=>({campaignBudget:{resourceName:r,amountMicros:String(W.budgets[r]*1e6)}}));
  if(q.includes("campaign.status != 'REMOVED'"))return [];
  if(q.includes('DURING YESTERDAY'))return [{metrics:{costMicros:String(W.yesterday*1e6)}}];
  if(q.includes('segments.date, metrics.cost_micros'))return W.baseline.map((v,i)=>({segments:{date:'d'+i},metrics:{costMicros:String(v*1e6)}}));
  if(q.includes('FROM customer WHERE segments.date BETWEEN'))return [{metrics:{costMicros:String(W.mtdNative*1e6)}}];
  if(q.includes('FROM search_term_view'))return W.terms;
  if(q.includes('FROM ad_group_criterion'))return W.keywords;
  if(q.includes('FROM ad_group_ad_asset_view'))return [{adGroupAdAssetView:{fieldType:'HEADLINE'},asset:{resourceName:'customers/123/assets/9',textAsset:{text:'Weak line'}},campaign:{name:'A'},metrics:{impressions:900}}];
  throw Error('unexpected query '+q);};
 const mutate=async(service,ops,o={})=>{if(service==='campaigns'&&W.pauseFails)throw Error('Google refused the pause');muts.push({service,ops:clone(ops),validateOnly:!!(o.validateOnly||(o.ctrl&&o.ctrl.dryRun))});return {results:ops.map(()=>({resourceName:'x'}))};};
 bind({fb:()=>f,gaql,mutate,mutateAll:async(ops,o)=>mutate('all',ops,o),_accountCurrency:async()=>'CAD',_accountTz:async()=>tz,_fxRateToUsd:async()=>W.rate,ledger:async e=>{ledgers.push(clone(e));return 'L'+ledgers.length;},_verifyLedger:async()=>{},_assertCampaignNotDeleted:async()=>{},_deletedCampaignIds:async()=>new Set()});
 const C=(x={})=>({enabled:true,dryRun:false,maxDailyBudgetTotal:100,budgetCurrency:'CAD',...x});
 const refuses=async(fn,re,msg)=>{const before=muts.length;await assert.rejects(fn,re);check(muts.length===before,msg);};

 // control(): a missing or invalid stored ceiling falls back; the environment ceiling always caps.
 reset();for(const [stored,envCeil,want] of [['',undefined,100],['abc','150',150],[500,'200',200],[80,'150',80],[null,undefined,100]]){if(envCeil)env.GADS_MAX_DAILY_BUDGET_TOTAL=envCeil;else delete env.GADS_MAX_DAILY_BUDGET_TOTAL;f.docs.set('Brites_GAds_Control/control',{maxDailyBudgetTotal:stored});assert.equal((await E.control()).maxDailyBudgetTotal,want,JSON.stringify([stored,envCeil]));}
 delete env.GADS_MAX_DAILY_BUDGET_TOTAL;check(true,'ceiling never reads as "no ceiling"');

 // Enabling: spendable budgets are 60 (A) + 30 (E); the ENABLED-but-ENDED B does not spend.
 reset();await refuses(()=>E.setCampaignStatus('3','ENABLED',{ctrl:C()}),/CAD 125\.00, over your daily ceiling of CAD 100\.00/,'enable refused over the ceiling, in the account currency');
 await refuses(()=>E.setCampaignStatus('3','ENABLED',{ctrl:C({dryRun:true})}),/over your daily ceiling/,'dry-run validation refuses the same way');
 await E.setCampaignStatus('4','ENABLED',{ctrl:C()});check(muts.length===1&&muts[0].ops[0].update.status==='ENABLED','campaign on an already-spending shared budget adds nothing and may enable');
 W.budgets[B(13)]=10;muts.length=0;await E.setCampaignStatus('3','ENABLED',{ctrl:C()});check(muts.length===1,'enable fitting exactly under the ceiling allowed');
 await E.setCampaignStatus('1','PAUSED',{ctrl:C({maxDailyBudgetTotal:1})});check(muts.length===2,'pausing is never blocked');
 reset();W.mtdNative=800;await refuses(()=>E.setCampaignStatus('3','ENABLED',{ctrl:C({maxDailyBudgetTotal:500,maxMonthlySpend:500})}),/USD 560\.00\) has reached your monthly stop threshold of USD 500/,'enable refused once month-to-date spend reached the monthly stop');
 W.rate=null;await refuses(()=>E.setCampaignStatus('3','ENABLED',{ctrl:C({maxDailyBudgetTotal:500,maxMonthlySpend:5000})}),/without an exchange rate/,'no exchange rate: monthly stop cannot be verified, nothing enabled');
 f.docs.set('Brites_GAds_State/fxRates',{['CAD:'+ymd(tz,-3*86400000)]:0.7,['CAD:2020-01-01']:0.1,['EUR:'+today]:2});await refuses(()=>E.setCampaignStatus('3','ENABLED',{ctrl:C({maxDailyBudgetTotal:500,maxMonthlySpend:500})}),/USD 560\.00/,'recent saved rate used when the rate service is down');
 // Final daily rates are saved in fxRatesFinal (the reporting's store); the retired fxRates document is no longer written.
 f.docs.delete('Brites_GAds_State/fxRates');f.docs.set('Brites_GAds_State/fxRatesFinal',{['CAD:'+ymd(tz,-2*86400000)]:0.7,['CAD:2020-01-01']:0.1,['EUR:'+today]:2});
 await refuses(()=>E.setCampaignStatus('3','ENABLED',{ctrl:C({maxDailyBudgetTotal:500,maxMonthlySpend:500})}),/USD 560\.00/,'rates the reporting saves (fxRatesFinal) back the monthly stop when the rate service is down');
 {const g=await E.monthlySpendGuard({ctrl:C({maxMonthlySpend:500})});check(g.tripped&&g.mtd===560&&muts.some(m=>m.ops.every(o=>o.update.status==='PAUSED')),'monthly stop still pauses with the rate service down, on the saved final rate');muts.length=0;}
 // Budgets: checked as a total, resolved server-side, lowering always allowed.
 reset();await refuses(()=>E.setCampaignBudget('1',75,{ctrl:C()}),/CAD 105\.00, over your daily ceiling/,'budget raise refused when the total would pass the ceiling');
 await E.setCampaignBudget('1',65,{ctrl:C(),budgetRes:B(999)});check(muts[0].ops[0].update.resourceName===B(11)&&muts[0].ops[0].update.amountMicros===65e6,'budget written to the live resource, never a stale browser one');
 await E.setCampaignBudget('5',20,{ctrl:C({maxDailyBudgetTotal:50})});check(muts.length===2,'lowering a budget allowed even above the ceiling');
 await E.setCampaignBudget('3',90,{ctrl:C()});check(muts.length===3,'paused campaign budget may change (enabling it is checked)');
 await refuses(()=>E.setCampaignBudget('3',150,{ctrl:C()}),/exceeds your account ceiling CAD 100/,'single budget over the ceiling refused, labelled CAD');
 // End date: moving an ENABLED+ENDED campaign's end date out revives its spend.
 reset();await refuses(()=>E.setCampaignEndDate('2',{endDate:later,ctrl:C()}),/CAD 118\.00, over your daily ceiling/,'reviving an ended campaign is checked like enabling it');
 await E.setCampaignEndDate('2',{endDate:later,ctrl:C({maxDailyBudgetTotal:120})});check(muts.length===1,'revival within the ceiling allowed');

 // Ceiling trim: one update per budget, ENDED ignored, rounded down under the ceiling.
 reset();W.camps[4].status='ENABLED';W.budgets[B(15)]=60;const t=await E.enforceBudgetCeiling({ctrl:C()});
 check(t.total===120&&muts.length===1&&muts[0].ops.length===2&&new Set(muts[0].ops.map(o=>o.update.resourceName)).size===2,'shared budget counted and trimmed once; ended campaign ignored');
 check(muts[0].ops.reduce((n,o)=>n+o.update.amountMicros/1e6,0)<=100,'trimmed total at or under the ceiling');

 // Anomaly breaker: no false trip on a launch; trips on a real spike or on spend beyond 2x the ceiling.
 reset();W.baseline=[10,10,10];W.yesterday=60;let a=await E.anomalyCheck({ctrl:C()});check(!a.tripped&&!f.docs.has('Brites_GAds_Control/control'),'launch after a quiet spell does not trip');
 W.baseline=Array(8).fill(10);W.yesterday=30;a=await E.anomalyCheck({ctrl:C()});const tc=f.docs.get('Brites_GAds_Control/control');check(a.tripped&&tc.enabled===false&&/CAD 30\.00 > 2\.5× the recent daily average CAD 10\.00/.test(tc.tripReason),'spike over the mature baseline trips, reason in CAD');
 reset();W.yesterday=250;a=await E.anomalyCheck({ctrl:C()});check(a.tripped,'spend beyond twice the daily ceiling trips with no baseline');

 // Monthly stop: a failed pause is recorded and retried, never reported as done.
 reset();W.mtdNative=800;W.pauseFails=true;let g=await E.monthlySpendGuard({ctrl:C({maxMonthlySpend:500})});const gc=f.docs.get('Brites_GAds_Control/control');
 check(g.tripped&&g.ok===false&&/pausing campaigns failed/.test(g.error)&&/FAILED/.test(gc.tripReason)&&ledgers.some(l=>l.kind==='monthlySpendGuard'&&l.error),'failed pause visible in result, control and activity');
 W.pauseFails=false;g=await E.monthlySpendGuard({ctrl:C({maxMonthlySpend:500})});check(g.ok&&g.paused===3&&muts[0].ops.every(o=>o.update.status==='PAUSED'),'retry pauses every enabled campaign');
 Object.values(W.camps).forEach(c=>c.status='PAUSED');f=memory();g=await E.monthlySpendGuard({ctrl:C({maxMonthlySpend:500})});check(g.tripped&&g.alreadyPaused&&!f.docs.size,'nothing left to pause: no repeat writes');

 // Approvals: budget moves must still match what was reviewed and fit the ceiling.
 const budgetDraft=to=>({type:'budget',status:'APPROVED',payload:{service:'campaignBudgets',operations:[{update:{resourceName:B(11),amountMicros:to*1e6},updateMask:'amount_micros'}],meta:{budgetCurrency:'CAD',baseline:[{budgetRes:B(11),from:60,to}]}}});
 const draft=()=>f.docs.get('Brites_GAds_Approvals/d1');
 reset();f.docs.set('Brites_GAds_Approvals/d1',budgetDraft(72));W.budgets[B(11)]=50;await refuses(()=>E.applyApproval('d1',C({maxDailyBudgetTotal:500})),/changed after this proposal/,'budget edited since review: approval refused');check(draft().status==='APPROVED'&&/changed after/.test(draft().lastError),'refusal shown on the draft');
 reset();f.docs.set('Brites_GAds_Approvals/d1',budgetDraft(72));await refuses(()=>E.applyApproval('d1',C()),/exceed the account ceiling/,'approved raise refused when the spendable total would pass the ceiling');
 reset();f.docs.set('Brites_GAds_Approvals/d1',budgetDraft(66));await E.applyApproval('d1',C());check(muts.length===1&&draft().status==='APPLIED','matching, in-limit budget move publishes once');
 await assert.rejects(()=>E.applyApproval('d1',C()),/already published/);check(muts.length===1,'cannot publish twice');
 reset();f.docs.set('Brites_GAds_Approvals/d1',{type:'search',status:'APPROVED',payload:{mutateOperations:[{campaignBudgetOperation:{create:{resourceName:B(-1),amountMicros:5e6}}},{campaignOperation:{create:{resourceName:'customers/123/campaigns/-2',name:'BA · x',status:'ENABLED',campaignBudget:B(-1)}}}]}});
 await refuses(()=>E.applyApproval('d1',C()),/Drafts publish campaigns paused/,'a draft can never switch a campaign on');
 // Publication lease: a second draft waits its turn, or stays APPROVED with the reason shown.
 reset();f.docs.set('Brites_GAds_Approvals/d1',budgetDraft(66));f.docs.set('Brites_GAds_State/publicationLease',{owner:'other',until:Date.now()+60000});
 await refuses(()=>E.applyApproval('d1',C()),/Another publication/,'busy lease: nothing sent');check(draft().status==='APPROVED'&&/Another publication/.test(draft().lastError),'busy lease reason visible, draft retryable');
 onWait=()=>f.docs.delete('Brites_GAds_State/publicationLease');await E.applyApproval('d1',C(),{waitForLeaseMs:60000});onWait=null;check(muts.length===1&&draft().status==='APPLIED'&&!draft().lastError,'waiting worker publishes once the lease is released');

 // Search-term mining: correct operation shapes; never exclude a converting or targeted term, or on thin data.
 reset();const T=(term,ag,conv,cost,clicks,status='NONE')=>({searchTermView:{searchTerm:term,status},campaign:{id:'1',name:'A'},adGroup:{resourceName:'customers/123/adGroups/'+ag},metrics:{conversions:conv,costMicros:String(cost*1e6),clicks:String(clicks)}});
 W.terms=[T('gold ring',101,2,20,25),T('cheap junk',101,0,10,20),T('cheap junk',102,0,6,15),T('silver chain',101,0,30,40),T('silver chain',102,1,5,5),T('brites',101,0,40,60),T('thin term',101,0,25,10),T('waiting term',101,0,30,50),T('excluded term',101,0,30,50,'EXCLUDED')];
 W.keywords=[{campaign:{id:'1'},adGroupCriterion:{keyword:{text:'Brites'}}}];
 f.docs.set('Brites_GAds_Approvals/old',{type:'negatives',status:'PENDING',payload:{service:'campaignCriteria',operations:[{create:{campaign:'customers/123/campaigns/1',negative:true,keyword:{text:'waiting term',matchType:'EXACT'}}}]}});
 const mined=await E.mineSearchTerms({ctrl:C()}),drafts=[...f.docs].filter(([k,v])=>k.includes('/auto')).map(([k,v])=>v),kw=drafts.find(d=>d.type==='keywords'),neg=drafts.find(d=>d.type==='negatives');
 check(mined.negatives===1&&JSON.stringify(neg.payload.operations)===JSON.stringify([{create:{campaign:'customers/123/campaigns/1',negative:true,keyword:{text:'cheap junk',matchType:'EXACT'}}}]),'only the campaign-wide, well-evidenced waste becomes a negative, in the API create shape');
 check(kw.status==='PENDING'&&kw.payload.operations.map(o=>o.create.keyword.text).join()==='gold ring,silver chain'&&kw.payload.operations[0].create.adGroup==='customers/123/adGroups/101','converting terms proposed as keywords in the API create shape, for review');
 f.docs.set('Brites_GAds_Approvals/note',{type:'creative',status:'PENDING',payload:{note:'operator-review'}});const pr=await E.pruneAssets({ctrl:C()});check(pr.existing&&pr.queued===0,'asset review note not duplicated daily (and removes nothing)');
}
