// An approved draft whose publication result is unknown is resolved by what the operator saw in
// Google Ads, never by re-sending it. A worker that stopped mid-publication must not leave its
// draft "Publishing…" forever, and the queue marker must never erase an unconfirmed result.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js'),'utf8');
const clone=x=>x==null?x:JSON.parse(JSON.stringify(x));
function setup(){
  const docs=new Map(),COL={state:'state',approvals:'approvals'};
  const document=p=>({path:p,get:async()=>({exists:docs.has(p),data:()=>clone(docs.get(p))}),update:async v=>{if(!docs.has(p))throw Error('Missing document');docs.set(p,{...docs.get(p),...clone(v)});}});
  const fb={FV:{serverTimestamp:()=>'server-time'},db:{collection:n=>({doc:id=>document(n+'/'+id)}),runTransaction:async work=>{const pending=[],tx={get:r=>r.get(),update:(r,v)=>pending.push(()=>r.update(v))};const out=await work(tx);for(const fn of pending)await fn();return out;}}};
  const sent=[],ctx={Date,COL,fb:()=>fb,mutate:async()=>{sent.push('mutate');},mutateAll:async()=>{sent.push('mutateAll');},applyApproval:async()=>{sent.push('apply');}};
  vm.createContext(ctx);
  const start=source.indexOf('// A worker that stops mid-publication'),end=source.indexOf('/* ============================ Helpers',start);
  assert(start>0&&end>start,'reconciliation block present');vm.runInContext(source.slice(start,end),ctx);
  return {ctx,docs,sent};
}
const MIN=60000;let count=0;async function test(name,fn){await fn();count++;console.log('PASS '+name);}
(async()=>{
  await test('only an APPLYING draft past 15 minutes without its own live lease reads as unconfirmed',()=>{const {ctx}=setup(),now=Date.now(),old={status:'APPLYING',applyStartedAt:now-16*MIN,applyAttempt:'a1'};
    assert.equal(ctx._staleApplying(old,null,now),true);
    assert.equal(ctx._staleApplying({...old,applyStartedAt:now-5*MIN},null,now),false);
    assert.equal(ctx._staleApplying(old,{owner:'a1',until:now+MIN},now),false);
    assert.equal(ctx._staleApplying(old,{owner:'someone-else',until:now+MIN},now),true);
    assert.equal(ctx._staleApplying(old,{owner:'a1',until:now-MIN},now),true);
    assert.equal(ctx._staleApplying({...old,status:'APPROVED'},null,now),false);});
  await test('"It was published" records the change as applied without contacting Google',async()=>{const e=setup();e.docs.set('approvals/d1',{status:'APPLY_UNKNOWN',applyAttempt:'a1',needsReconciliation:true,lastError:'Network lost'});
    const r=await e.ctx.reconcileApproval({id:'d1',outcome:'published'});assert.equal(r.status,'APPLIED');const d=e.docs.get('approvals/d1');
    assert.equal(d.status,'APPLIED');assert.equal(d.needsReconciliation,false);assert.equal(d.lastError,null);assert.equal(d.applyAttempt,null);assert.equal(d.reconciliation.outcome,'published');assert.equal(d.reconciliation.priorStatus,'APPLY_UNKNOWN');assert.deepEqual(e.sent,[]);});
  await test('"It was not published" returns the draft to Approved with the reason, still unsent',async()=>{const e=setup();e.docs.set('approvals/d1',{status:'APPLY_UNKNOWN',applyAttempt:'a1',needsReconciliation:true});
    await e.ctx.reconcileApproval({id:'d1',outcome:'not_published'});const d=e.docs.get('approvals/d1');assert.equal(d.status,'APPROVED');assert.match(d.lastError,/not published/);assert.equal(d.needsReconciliation,false);assert.deepEqual(e.sent,[]);});
  await test('a stale APPLYING draft can be reconciled; a running or ordinary draft cannot',async()=>{const e=setup(),now=Date.now();
    e.docs.set('approvals/stale',{status:'APPLYING',applyStartedAt:now-20*MIN,applyAttempt:'gone'});await e.ctx.reconcileApproval({id:'stale',outcome:'not_published'});assert.equal(e.docs.get('approvals/stale').status,'APPROVED');
    e.docs.set('approvals/live',{status:'APPLYING',applyStartedAt:now-20*MIN,applyAttempt:'a2'});e.docs.set('state/publicationLease',{owner:'a2',until:now+MIN});await assert.rejects(()=>e.ctx.reconcileApproval({id:'live',outcome:'published'}),/still publishing/);assert.equal(e.docs.get('approvals/live').status,'APPLYING');
    e.docs.set('approvals/fresh',{status:'APPLYING',applyStartedAt:now-2*MIN,applyAttempt:'a3'});await assert.rejects(()=>e.ctx.reconcileApproval({id:'fresh',outcome:'published'}),/still publishing/);
    e.docs.set('approvals/pending',{status:'PENDING'});await assert.rejects(()=>e.ctx.reconcileApproval({id:'pending',outcome:'published'}),/no unconfirmed publication/);
    await assert.rejects(()=>e.ctx.reconcileApproval({id:'stale',outcome:'maybe'}),/Choose/);await assert.rejects(()=>e.ctx.reconcileApproval({id:'../x',outcome:'published'}),/Invalid/);});
  await test('the queue marker is written for approved drafts only and never hides an unconfirmed result',async()=>{const e=setup();
    e.docs.set('approvals/ok',{status:'APPROVED',lastError:'Another publication is still running.'});await e.ctx.markPublishRequested('ok');assert(e.docs.get('approvals/ok').publishRequestedAt>0);assert.equal(e.docs.get('approvals/ok').lastError,null);
    e.docs.set('approvals/unknown',{status:'APPLY_UNKNOWN',lastError:'Network lost'});await e.ctx.markPublishRequested('unknown');assert.equal(e.docs.get('approvals/unknown').lastError,'Network lost');assert.equal(e.docs.get('approvals/unknown').publishRequestedAt,undefined);});
  // The console states every approved draft in plain words, from the same fields the worker writes.
  const page=fs.readFileSync(path.resolve(__dirname,'../../brites-adwords.html'),'utf8'),line=name=>{const i=page.search(new RegExp('\\n(async )?function '+name+'\\('));assert(i>0,name+' present');const rest=page.slice(i+1),j=rest.slice(1).search(/\n(async function |function |\/\/|var |\/\*)/);return rest.slice(0,j+1);};
  const ui={};vm.createContext(ui);vm.runInContext(['isAdVersionApproval','requiresCreative','approvalStuckState','approvalStuckSummary'].map(line).join('\n'),ui);
  await test('an approved draft reads as unconfirmed, publishing, queued, not published, checked in dry run or not sent',()=>{const now=Date.now(),kw={type:'keywords',payload:{}},key=a=>ui.approvalStuckState({...kw,status:'APPROVED',...a}).key;
    assert.equal(key({status:'APPLY_UNKNOWN'}),'unknown');assert.equal(key({status:'APPLYING'}),'busy');
    assert.equal(key({publishRequestedAt:now-MIN}),'queued');
    assert.equal(key({publishRequestedAt:now-10*MIN}),'ready');
    assert.equal(key({publishRequestedAt:now-MIN,lastError:'Another publication is still running.'}),'failed');
    assert.equal(key({publishRequestedAt:now-2*MIN,validatedAt:now-MIN}),'checked');
    assert.equal(key({validatedAt:now-MIN}),'checked');
    assert.equal(ui.approvalStuckState({type:'creative',status:'APPROVED',payload:{}}).key,'needs');
    assert.match(ui.approvalStuckState({...kw,status:'APPROVED',validatedAt:now}).me,/dry run.*not published/);
    assert.equal(ui.approvalStuckSummary([{...kw,status:'APPLY_UNKNOWN'},{...kw,status:'APPROVED',publishRequestedAt:now}]),'Approved drafts · 1 to check in Google Ads · 1 queued');});
  await test('every card action passes one gate; the answers are writes and the improvement draft lands in Approvals',()=>{
    assert.match(line('approvalActionGate'),/closest\("\[data-rj\],\[data-reconcile\],\[data-ap\],\[data-retry\]"\)/);
    assert.match(line('approvalActionGate'),/APPROVAL_BUSY\[id\]\)\{e\.preventDefault\(\);e\.stopImmediatePropagation\(\);return;\}/);
    assert(!/querySelectorAll\("\[data-reconcile\]"\)/.test(page),'no second wiring path for the Google Ads answers');
    assert.match(page,/list\.innerHTML="";approvalActionGate\(list\);/);
    assert.match(page,/var API_MUTATING=new Set\(\[[^\]]*"reconcileApproval"/);
    assert.match(line('approvalLand'),/closePerformanceDialog\(\);go\("approvals"\);await reload\(\);/);
    const improve=line('runCampaignImprovementAction');assert.equal((improve.match(/if\(!await approvalLand\(/g)||[]).length,2);
    assert.match(line('approvalSwap'),/approvalFocusBack\(keep\.focus\);\}$/);assert.match(line('approvalKeep'),/focus:approvalFocusMark\(list\)/);});
  console.log(count+' approval reconciliation and state checks passed.');
})().catch(e=>{console.error(e);process.exitCode=1;});
