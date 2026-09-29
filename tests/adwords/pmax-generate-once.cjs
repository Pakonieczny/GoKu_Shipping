// A Performance Max draft from the console is one paid generation per request, even from two tabs or after a
// network blip: the request names its generation, a second request joins it or gets its finished draft back, only
// the worker that takes the claim pays, and one draft reaches Approvals. A different product or destination is its
// own generation; a failed one can be retried. Offline: the Kick, the worker and the engine run together with
// in-memory Firestore, a recording fetch and stand-ins for Merchant Center and Google; the paid copy call only counts.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const dir=path.resolve(__dirname,'../../netlify/functions')+'/',engineFile=dir+'googleAdsAutopilot.js',realRequire=require('node:module').createRequire(engineFile);
const clone=v=>v==null?v:JSON.parse(JSON.stringify(v)),J=JSON.stringify,MIN=60000;
let n=0;const check=(v,m)=>{assert.ok(v,m);n++;console.log('PASS '+m);};

// One clock for every part, moved by the test rather than by waiting.
let now=Date.parse('2026-09-29T12:00:00Z');
class Clock extends Date{constructor(...a){if(a.length)super(...a);else super(now);}static now(){return now;}}

// Firestore: documents, the queries the engine uses, and transactions that run one at a time (Firestore retries a
// conflicting transaction, so two claims at the same moment still settle one after the other).
function memory(){
  const docs=new Map();let seq=0,queue=Promise.resolve();
  const read=(o,k)=>k.split('.').reduce((v,p)=>v==null?v:v[p],o);
  const snap=p=>({id:p.split('/').pop(),exists:docs.has(p),ref:doc(p),data:()=>clone(docs.get(p))});
  const doc=p=>({id:p.split('/').pop(),path:p,get:async()=>snap(p),collection:x=>collection(p+'/'+x),delete:async()=>{docs.delete(p);},
    set:async(v,o)=>{docs.set(p,o&&o.merge?{...(docs.get(p)||{}),...clone(v)}:clone(v));},
    update:async v=>{if(!docs.has(p))throw Error('No document '+p);docs.set(p,{...docs.get(p),...clone(v)});}});
  const collection=(p,filters=[])=>({path:p,where:(f,op,v)=>collection(p,[...filters,[f,op,v]]),limit:()=>collection(p,filters),orderBy(){return this;},select(){return this;},
    doc:x=>doc(p+'/'+(x==null?'auto'+(++seq):x)),add:async v=>{const id='auto'+(++seq);docs.set(p+'/'+id,clone(v));return doc(p+'/'+id);},
    get:async()=>{const list=[...docs.keys()].filter(k=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).filter(k=>filters.every(([f,op,v])=>{const x=read(docs.get(k),f);return op==='=='?x===v:op==='in'?v.includes(x):true;})).map(snap);
      return {docs:list,size:list.length,empty:!list.length,forEach:fn=>list.forEach(fn)};}});
  const runTransaction=fn=>{const run=queue.then(()=>fn({get:r=>r.get(),set:(r,v,o)=>{r.set(v,o);},update:(r,v)=>{r.update(v);},delete:r=>{r.delete();}}));queue=run.catch(()=>{});return run;};
  return {docs,db:{collection,runTransaction},FV:{serverTimestamp:()=>Clock.now()}};
}

const OFFERS=[['shopify_US_11_101','Corgi necklace',11,'corgi-necklace'],['shopify_US_22_201','Fox necklace',22,'fox-necklace'],['shopify_US_33_301','Owl necklace',33,'owl-necklace']]
  .map(([itemId,title,id,handle])=>({itemId,title,id,handle,feedLabel:'US',link:'https://britesjewelry.com/products/'+handle,targetCountries:['US']}));
const [CORGI,FOX,OWL]=OFFERS;
// What the console sends for one product of the "Animal necklaces" opportunity (pmaxOpportunityPayload).
const request=(offer=CORGI,o={})=>({action:'generatePmax',handle:'animal-necklaces',titles:[offer.title],productTitles:[offer.title],collectionTitle:'Animal necklaces',itemIds:[offer.itemId],feedLabel:'US',
  dailyBudget:12,searchThemes:[offer.title.toLowerCase()],countries:['2840'],offerDetails:[{itemId:offer.itemId,title:offer.title}],targetRoas:0,days:30,...o});

function harness(){
  const mem=memory(),T={mem,dispatched:[],paid:[],upstream:202,gate:null,ineligible:0};
  // ---- the engine, offline ----
  const cx=vm.createContext({module:{exports:{}},exports:{},require:x=>x==='node-fetch'?async u=>{throw Error('Live network forbidden: '+u);}:realRequire(x),
    process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date:Clock,Intl,Map,Set,URL,URLSearchParams,setTimeout,clearTimeout});
  vm.runInContext(fs.readFileSync(engineFile,'utf8'),cx);
  const stubs={fb:()=>mem,control:async()=>({maxDailyBudgetTotal:100,budgetCurrency:'CAD',defaultCountries:['2840']}),mintToken:async()=>'offline-token',_accountCurrency:async()=>'CAD',
    merchantCenterId:async()=>'555',listCountries:async()=>[{id:'2840',name:'United States',code:'US'}],_accountTz:async()=>'America/Toronto',
    _pmaxResearchCandidate:()=>({}),getCollections:async()=>[{handle:'animal-necklaces',title:'Animal necklaces'}],collectionProfiles:async()=>({list:[]}),_pmaxIsEligible:()=>true,
    // Merchant Center: an offer can be reported ineligible (T.ineligible counts how many times).
    merchantProducts:async({itemIds=[]}={})=>{if(T.ineligible>0){T.ineligible--;return [];}return OFFERS.filter(o=>itemIds.some(x=>String(x).toLowerCase()===o.itemId.toLowerCase()));},
    shopifyGql:async()=>({nodes:OFFERS.map(o=>({id:'gid://shopify/Product/'+o.id,handle:o.handle,status:'ACTIVE'}))}),
    discoverPmaxAudienceResource:async()=>null,validatePmaxAudienceResource:async()=>({resource:null}),
    // An existing Performance Max campaign that can take the product (its checks are covered in pmax-structure.cjs).
    _pmaxExistingDraft:async({campaignId})=>({target:{id:String(campaignId),name:'BA · Necklaces PMax',status:'ENABLED',brandGuidelinesEnabled:false,assetGroupNames:['AG · Charms'],budget:20,sharedBudget:false,assetGroupCount:1,countryNames:['United States'],bidding:'Maximize conversion value'},
      add:0,after:20,spendable:true,enabledTotal:20,raise:null,guard:{campaignId:String(campaignId)}}),
    // The paid ad copy: counted, held while T.gate is pending, never bought (the deterministic copy is used).
    _pmaxAdCopy:async(...a)=>{T.paid.push(clone(a));if(T.gate)await T.gate;return null;},
    openaiJSON:async()=>{throw Error('Paid AI call forbidden');}};
  cx.__m=stubs;vm.runInContext(Object.keys(stubs).map(k=>k+'=__m.'+k).join('\n'),cx);
  const E=cx.module.exports;
  // ---- the Kick and the background worker, with this engine and a recording fetch ----
  const admin={firestore:()=>mem.db};admin.firestore.FieldValue=mem.FV;
  const load=file=>{const env={EDIT_PASSCODE:'test-pass',URL:'https://example.invalid'},m={exports:{}};
    const ep={exports:{}},epx={process:{env},console,Date:Clock,module:ep,exports:ep.exports,require:x=>x==='./firebaseAdmin'?admin:require(x)};vm.createContext(epx);vm.runInContext(fs.readFileSync(dir+'_editPasscode.js','utf8'),epx);
    // The request to the worker: recorded, then answered with T.upstream (T.onDispatch can act first, or fail it).
    const ctx={process:{env},console,Date:Clock,Set,JSON,module:m,exports:m.exports,require:x=>x==='node-fetch'?async(url,opts)=>{const b=JSON.parse(opts.body);T.dispatched.push(b);if(T.onDispatch)await T.onDispatch(b);return {ok:T.upstream<400,status:T.upstream};}
      :x==='./googleAdsAutopilot'?E:x==='./firebaseAdmin'?admin:x==='./_editPasscode'?ep.exports:require(x)};
    vm.createContext(ctx);vm.runInContext(fs.readFileSync(dir+file,'utf8'),ctx);return m.exports;};
  const kick=load('googleAdsAutopilotKick.js'),worker=load('googleAdsAutopilot-background.js');
  Object.assign(T,{E,
    // A console request with the passcode; sent is how many worker dispatches it made.
    ask:async body=>{const k=T.dispatched.length,r=await kick.httpHandler({httpMethod:'POST',headers:{'x-edit-passcode':'test-pass'},body:J(body)});assert.equal(r.statusCode,200);return {...JSON.parse(r.body),sent:T.dispatched.length-k};},
    last:()=>T.dispatched.length-1,
    poll:async genId=>clone(await kick.handleAction({action:'genStatus',genId})),
    // Netlify delivering dispatch i to the background worker (again, when a test repeats it).
    deliver:async i=>JSON.parse((await worker.handler({httpMethod:'POST',headers:{},body:J(T.dispatched[i])})).body),
    drafts:()=>[...mem.docs.entries()].filter(([k])=>/^Brites_GAds_Approvals\/[^/]+$/.test(k)).map(([k,v])=>({id:k.split('/').pop(),...v}))});
  return T;
}
const until=async cond=>{for(let i=0;i<2000&&!cond();i++)await new Promise(r=>setImmediate(r));assert.ok(cond(),'timed out waiting');};
// A step that never settles would let Node exit quietly with status 0: that is a failure too.
let settled=false;process.on('exit',()=>{if(!settled){console.error('FAIL: the checks stopped before the end (a step never settled)');process.exitCode=1;}});

(async()=>{
  const T=harness(),{E}=T;
  // ===== The request names its generation =====
  const id=o=>E.pmaxGenerationId({handle:'animal-necklaces',feedLabel:'US',itemIds:[CORGI.itemId,FOX.itemId],...o});
  check(/^pmax-[0-9a-f]{32}$/.test(id())&&id()===id({feedLabel:'us',itemIds:[FOX.itemId.toUpperCase(),CORGI.itemId,FOX.itemId],dailyBudget:15,genId:'tab-2'}),
    'the same product, feed, destination and offers name the same generation (offer order and case, the budget and a client id do not matter)');
  check(new Set([id(),id({itemIds:[CORGI.itemId]}),id({handle:'charms'}),id({feedLabel:'CA'}),id({existingCampaignId:'101'}),id({existingCampaignId:'102'})]).size===6,
    'a different offer, collection, feed or destination (a new campaign, or which campaign it joins) is a different generation');

  // ===== Two identical requests: one paid call, one draft =====
  const a1=await T.ask(request());
  check(a1.queued&&a1.genId===E.pmaxGenerationId({handle:'animal-necklaces',feedLabel:'US',itemIds:[CORGI.itemId]})&&!a1.joined&&a1.sent===1,'the first request claims the generation its request names and dispatches it once');
  const d0=T.dispatched[0];
  check(J(d0.tasks)==='["pmaxGenerate"]'&&d0.genId===a1.genId&&d0.runId&&d0.handle==='animal-necklaces'&&J(d0.itemIds)===J([CORGI.itemId])&&d0.existingCampaignId===null&&d0.token==='test-pass','the worker gets the request, the generation and the run it may take');
  let s=await T.poll(a1.genId);check(s.phase==='queued'&&s.ok===undefined&&s.kind==='pmax','a poll meanwhile reads queued, so every tab keeps waiting');
  now+=5000;const a2=await T.ask(request());
  check(a2.genId===a1.genId&&a2.joined&&a2.sent===0,'the same tab asking again (a retry after a network blip) joins it; nothing more is dispatched');
  let release;T.gate=new Promise(r=>{release=r;});const working=T.deliver(0);await until(()=>T.paid.length===1);
  check((await T.poll(a1.genId)).phase==='running','the worker takes the claim and writes the copy (the paid call, counted and never made)');
  const b1=await T.ask(request(CORGI,{genId:'1790000000000-pmx',itemIds:[CORGI.itemId.toUpperCase()]}));
  check(b1.genId===a1.genId&&b1.joined&&b1.sent===0,'a second tab joins the running generation');
  let dupDone=false;const dupRun=T.deliver(0).then(r=>{dupDone=true;return r;});await until(()=>dupDone||T.paid.length>1);
  check(dupDone&&T.paid.length===1&&(await dupRun).result.pmax.skipped===true,'the same dispatch delivered again while it runs pays nothing');
  release();T.gate=null;const w=await working;
  check(w.result.pmax.approvalId&&T.paid.length===1&&T.drafts().length===1,'the generation finishes with one draft');
  const tabA=await T.poll(a1.genId),tabB=await T.poll(b1.genId);await T.poll(a1.genId);
  check(tabA.ok===true&&tabA.phase==='done'&&tabA.approvalId===w.result.pmax.approvalId&&tabB.approvalId===tabA.approvalId&&T.drafts().length===1,'both tabs polling read the same finished draft; polling creates nothing');
  now+=3*MIN;const c1=await T.ask(request());
  check(c1.genId===a1.genId&&c1.joined&&c1.finished&&c1.sent===0&&(await T.poll(c1.genId)).approvalId===tabA.approvalId,'a tab asking after it finished gets that draft back instead of paying for another');
  const again=await T.deliver(0);
  check(again.result.pmax.skipped===true&&T.paid.length===1&&T.drafts().length===1,'the dispatch delivered again after it finished pays nothing and drafts nothing');
  const d=T.drafts()[0];check(d.type==='pmax'&&d.status==='PENDING'&&J(d.payload.meta.itemIds)===J([CORGI.itemId]),'the one draft is the requested product, waiting in Approvals');

  // ===== Two tabs at the same moment =====
  const before=T.dispatched.length,[x,y]=await Promise.all([T.ask(request(FOX)),T.ask(request(FOX))]),xi=T.last();
  check(x.genId===y.genId&&x.genId!==a1.genId&&[x,y].filter(r=>r.joined).length===1&&T.dispatched.length===before+1,'two tabs asking at the same moment: one claims and dispatches, the other joins; a different product is its own generation');
  await Promise.all([T.deliver(xi),T.deliver(xi)]);
  check(T.paid.length===2&&T.drafts().length===2&&(await T.poll(x.genId)).ok===true,'delivered twice at once, it still pays once and drafts once');

  // ===== A different destination =====
  const j1=await T.ask(request(CORGI,{existingCampaignId:'101',addBudget:0}));
  check(j1.genId!==a1.genId&&!j1.joined&&j1.sent===1&&T.dispatched[T.last()].existingCampaignId==='101','the same product into an existing campaign is its own generation');
  await T.deliver(T.last());s=await T.poll(j1.genId);
  check(s.ok===true&&s.joined&&s.joined.campaignId==='101'&&T.paid.length===3&&T.drafts().length===3&&T.drafts().find(r=>r.id===s.approvalId).payload.meta.kind==='pmaxAddToCampaign','it writes its own draft, which adds the product to that campaign');

  // ===== A failed generation can be retried =====
  T.ineligible=1;const f1=await T.ask(request(OWL)),fi=T.last();await T.deliver(fi);s=await T.poll(f1.genId);
  check(s.ok===false&&s.phase==='done'&&/no longer eligible/.test(s.error)&&T.paid.length===3&&T.drafts().length===3,'a failed generation reports its error to the tabs polling it; nothing was paid for or drafted');
  const f2=await T.ask(request(OWL));
  check(f2.genId===f1.genId&&!f2.joined&&f2.sent===1&&T.dispatched[T.last()].runId!==T.dispatched[fi].runId,'asking again retries it as a new run');
  await T.deliver(T.last());s=await T.poll(f2.genId);
  check(s.ok===true&&s.approvalId&&T.paid.length===4&&T.drafts().length===4,'the retry writes its copy and its draft');
  T.upstream=502;const e1=await T.ask(request(OWL,{existingCampaignId:'102'}));s=await T.poll(E.pmaxGenerationId({handle:'animal-necklaces',feedLabel:'US',itemIds:[OWL.itemId],existingCampaignId:'102'}));
  check(/Background dispatch failed: HTTP 502/.test(e1.error)&&s.ok===false&&/HTTP 502/.test(s.error),'a dispatch the worker never received is reported, and closes the generation for every tab');
  T.upstream=202;const e2=await T.ask(request(OWL,{existingCampaignId:'102'}));
  check(e2.queued&&!e2.joined&&e2.sent===1,'asking again dispatches it again');
  await T.deliver(T.last());check((await T.poll(e2.genId)).ok===true&&T.paid.length===5&&T.drafts().length===5,'and that run drafts once');
  // The worker took the run although the console's request to it then failed (a timeout): the console follows that run.
  let go;T.gate=new Promise(r=>{go=r;});let slow=null;
  T.onDispatch=async()=>{T.onDispatch=null;slow=T.deliver(T.last());await until(()=>T.paid.length===6);throw Error('network timeout');};
  const t1=await T.ask(request(FOX,{existingCampaignId:'102'}));
  check(t1.queued&&!t1.error&&t1.sent===1&&(await T.poll(t1.genId)).phase==='running','a request to the worker that fails after the worker took the run is followed, not shown as failed (a retry would pay again)');
  go();T.gate=null;await slow;s=await T.poll(t1.genId);
  check(s.ok===true&&T.paid.length===6&&T.drafts().length===6&&(await T.ask(request(FOX,{existingCampaignId:'102'}))).joined,'that run ends with its one draft, which the next request gets back');

  // ===== After the window, a lost dispatch, a deleted draft, force =====
  now+=11*MIN;const n1=await T.ask(request()),lostAt=T.last();
  check(n1.genId===a1.genId&&!n1.joined&&n1.sent===1,'ten minutes after it finished, the same request is a new generation');
  now+=2*MIN;s=await T.poll(n1.genId);
  check(s.ok===false&&/did not start, so nothing was paid for/.test(s.error),'a claim the worker has not taken within two minutes reads as not started');
  const n2=await T.ask(request());
  check(!n2.joined&&n2.sent===1,'the next request starts a new run');
  const lost=await T.deliver(lostAt);
  check(lost.result.pmax.skipped===true&&T.paid.length===6,'the lost dispatch arriving late pays nothing');
  await T.deliver(T.last());s=await T.poll(n2.genId);check(s.ok===true&&T.paid.length===7&&T.drafts().length===7,'the new run drafts once');
  await E.deleteProposedAd({id:s.approvalId});now+=MIN;
  const n3=await T.ask(request()),waiting=T.last();
  check(!n3.joined&&n3.sent===1,'a draft deleted from Approvals is not handed back: asking again starts a new generation');
  const n4=await T.ask({...request(),force:true});
  check(!n4.joined&&n4.sent===1&&T.dispatched[T.last()].runId!==T.dispatched[waiting].runId,'force starts a new run even while one waits');
  const superseded=await T.deliver(waiting);
  check(superseded.result.pmax.skipped===true&&T.paid.length===7,'the run force replaced pays nothing');
  check(await E.finishPmaxGeneration(n4.genId,T.dispatched[waiting].runId,{ok:true,approvalId:'stale'})===false&&(await T.poll(n4.genId)).phase==='queued','an older run can never overwrite the status of the run that replaced it');
  await T.deliver(T.last());check((await T.poll(n4.genId)).ok===true&&T.paid.length===8&&T.drafts().length===8,'the forced run drafts once');
  // A worker that took its run and stopped: past Netlify's fifteen minutes the poll says so and a new request runs again.
  const r1=await T.ask(request(FOX,{existingCampaignId:'101'})),run=T.dispatched[T.last()].runId;
  check(await E.takePmaxGeneration(r1.genId,run)===true,'a worker takes its run once');
  check(await E.takePmaxGeneration(r1.genId,run)===false,'and never twice');
  now+=14*MIN;check(!(await T.poll(r1.genId)).ok&&(await T.ask(request(FOX,{existingCampaignId:'101'}))).joined,'a run under fifteen minutes old is still joined');
  now+=MIN;s=await T.poll(r1.genId);
  check(s.ok===false&&/stopped before it finished\. Check Approvals/.test(s.error)&&(await T.ask(request(FOX,{existingCampaignId:'101'}))).sent===1,'past fifteen minutes it reads as stopped, and asking again starts a new run');
  check(T.paid.length===8&&T.drafts().length===8,'eight paid copies for eight generations, and one draft each');
  settled=true;console.log('PASS '+n+' one-paid-generation-per-PMax-request checks');
})().catch(e=>{settled=true;console.error(e);process.exit(1);});
