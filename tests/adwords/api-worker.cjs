const fs=require('fs'),vm=require('vm'),assert=require('assert/strict');
const dir=require('path').resolve(__dirname,'../../netlify/functions')+'/';
const calls=[];let rejectMark=false,upstream=202;
const E={COL:{state:'state',control:'control',approvals:'approvals'},control:async()=>({enabled:false,dryRun:false}),markApprovalApproved:async id=>{calls.push(['reviewGate',id]);if(rejectMark)throw Error('not reviewed')},reviewCreativeApproval:async(id,hash)=>({ok:true,id,hash}),prepareCreativeApproval:async id=>{calls.push(['prepare',id]);return {ok:true,id}},applyApproval:async id=>{calls.push(['apply',id]);return {ok:true,id}},playbookVersions:async()=>({items:[]})};
const db={collection:()=>({doc:()=>({get:async()=>({exists:false})})})};const admin={firestore:()=>db};admin.firestore.FieldValue={};
function load(file){const mod={exports:{}};const ctx={process:{env:{EDIT_PASSCODE:'test-pass',URL:'https://example.invalid'}},console,Date,Set,JSON,module:mod,exports:mod.exports,require:n=>n==='node-fetch'?async(url,opts)=>{calls.push(['dispatch',JSON.parse(opts.body)]);return {ok:upstream<400,status:upstream}}:n==='./googleAdsAutopilot'?E:n==='./firebaseAdmin'?admin:require(n)};vm.createContext(ctx);vm.runInContext(fs.readFileSync(dir+file,'utf8'),ctx);return {api:mod.exports,ctx};}
(async()=>{
 let count=0;const {api,ctx}=load('googleAdsAutopilotKick.js');
 let r=await api.httpHandler({httpMethod:'POST',headers:{},body:JSON.stringify({action:'approve',id:'draft'})});assert.equal(r.statusCode,401);count++;
 E.dashboard=async()=>({ok:true});
 r=await api.httpHandler({httpMethod:'POST',headers:{'x-edit-passcode':'test-pass'},body:'{"action":"dashboard"}'});assert.equal(r.statusCode,200);assert.equal(JSON.parse(r.body).ok,true);count++;
 r=await api.httpHandler({httpMethod:'POST',headers:{},body:'{"action":"dashboard","passcode":"test-pass"}'});assert.equal(r.statusCode,200);count++;
 r=await api.httpHandler({httpMethod:'POST',headers:{'x-edit-passcode':'wrong'},body:'{"action":"dashboard","passcode":"wrong"}'});assert.equal(r.statusCode,401);count++;
 calls.length=0;rejectMark=true;await assert.rejects(()=>api.handleAction({action:'approve',id:'draft'}));assert.equal(calls.filter(x=>x[0]==='dispatch').length,0);rejectMark=false;count++;
 calls.length=0;r=await api.handleAction({action:'approve',id:'draft'});assert(r.queued);assert.equal(calls[0][0],'reviewGate');assert.equal(calls[1][1].tasks[0],'publishApproval');assert.equal(calls[1][1].id,'draft');count++;
 calls.length=0;r=await api.handleAction({action:'creativePrepare',id:'draft',retry:true});assert(r.queued);assert.equal(calls[0][1].tasks[0],'creativePrepare');assert(calls[0][1].retry);count++;
 calls.length=0;r=await api.handleAction({action:'distill',genId:'learning-123'});assert.equal(r.genId,'learning-123');assert.equal(calls[0][1].genId,'learning-123');count++;
 upstream=500;await assert.rejects(()=>api.handleAction({action:'creativePrepare',id:'draft'}),/dispatch failed/);upstream=202;count++;
 const bg=load('googleAdsAutopilot-background.js').api;calls.length=0;r=await bg.handler({httpMethod:'POST',body:JSON.stringify({tasks:['budgets'],token:'test-pass'})});assert.equal(JSON.parse(r.body).status,'dormant');assert.equal(calls.length,0);count++;
 calls.length=0;await bg.handler({httpMethod:'POST',body:JSON.stringify({tasks:['publishApproval'],id:'draft',token:'test-pass'})});assert.equal(calls[0][0],'apply');count++;
 calls.length=0;await bg.handler({httpMethod:'POST',body:JSON.stringify({tasks:['creativePrepare'],id:'draft',token:'test-pass'})});assert.equal(calls[0][0],'prepare');count++;

 const designEvent=(action,data={})=>({httpMethod:'POST',headers:{'x-edit-passcode':'test-pass'},body:JSON.stringify({action,...data})});
 for(const action of ['adDesignWorkspace','saveAdDesign','uploadAdDesignReference','adDesignProductImages','adDesignStatus']){
   E[action]=async data=>{calls.push([action,data]);return {ok:true,workspaceId:data.workspaceId}};
   calls.length=0;r=await api.httpHandler(designEvent(action,{workspaceId:'workspace-1',productId:'gid://shopify/Product/1',after:'cursor-2'}));assert.equal(JSON.parse(r.body).workspaceId,'workspace-1');assert.equal(calls[0][0],action);assert.equal(calls[0][1].after,'cursor-2');assert(!calls.some(x=>x[0]==='dispatch'));count++;
   calls.length=0;r=await api.httpHandler({...designEvent(action),headers:{'x-edit-passcode':'wrong'}});assert.equal(r.statusCode,401);assert.equal(calls.length,0);count++;
 }
 E.startAdDesign=async data=>({ok:true,workspaceId:data.workspaceId,jobId:'job-1',queued:true});
 calls.length=0;r=await api.httpHandler(designEvent('startAdDesign',{workspaceId:'workspace-1'}));assert.equal(JSON.parse(r.body).jobId,'job-1');assert.equal(calls[0][1].tasks[0],'adDesign');assert.equal(calls[0][1].jobId,'job-1');assert.equal(calls[0][1].token,'test-pass');count++;
 E.startAdDesign=async data=>({ok:true,workspaceId:data.workspaceId,jobId:'job-1',queued:false,cached:true});calls.length=0;r=await api.httpHandler(designEvent('startAdDesign',{workspaceId:'workspace-1'}));assert.equal(JSON.parse(r.body).cached,true);assert.equal(calls.length,0);count++;
 E.startAdDesign=async data=>({ok:true,workspaceId:data.workspaceId,jobId:'job-1',queued:true});upstream=503;r=await api.httpHandler(designEvent('startAdDesign',{workspaceId:'workspace-1'}));assert.equal(JSON.parse(r.body).ok,false);assert.match(JSON.parse(r.body).error,/dispatch failed/);upstream=202;count++;
 E.runAdDesign=async data=>{calls.push(['designRun',data]);return {ok:true,workspaceId:data.workspaceId,jobId:data.jobId}};
 calls.length=0;await bg.handler({httpMethod:'POST',body:JSON.stringify({tasks:['adDesign'],workspaceId:'workspace-1',jobId:'job-1',token:'test-pass'})});assert.equal(calls[0][0],'designRun');assert.equal(calls.length,1);count++;
 calls.length=0;r=await bg.handler({httpMethod:'POST',body:JSON.stringify({tasks:['adDesign'],workspaceId:'workspace-1',jobId:'job-1',token:'wrong'})});assert.equal(r.statusCode,401);assert.equal(calls.length,0);count++;
 E.runAdDesign=async data=>{calls.push(['designRun',data]);return {ok:true,paused:true,dispatch:true,...data}};
 calls.length=0;await bg.handler({httpMethod:'POST',body:JSON.stringify({tasks:['adDesign'],workspaceId:'workspace-1',jobId:'job-1',token:'test-pass'})});assert.deepEqual(calls.map(x=>x[0]),['designRun','dispatch']);assert.equal(calls[1][1].jobId,'job-1');assert.equal(calls[1][1].tasks[0],'adDesign');count++;
 upstream=503;calls.length=0;r=await bg.handler({httpMethod:'POST',body:JSON.stringify({tasks:['adDesign'],workspaceId:'workspace-1',jobId:'job-1',token:'test-pass'})});assert.match(JSON.parse(r.body).log.join(' '),/Completed work is saved/);assert.equal(calls.filter(x=>x[0]==='dispatch').length,1);count++;
 console.log('PASS',count,'API / worker checks (authentication, approval gate, dispatch, status IDs, automation-off behavior)');
})();
