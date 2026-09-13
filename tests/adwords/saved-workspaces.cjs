const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {savedWorkspaces}=require('../../netlify/functions/googleAdsGroups');
const root=path.resolve(__dirname,'../..');
const group='customers/123/assetGroups/7';
const workspace=i=>({id:'design_'+String(i).padStart(3,'0'),data:()=>({context:{campaignId:'42',groups:[{ref:group,name:'Peach charm'}]},settings:{groupRef:group,productId:'gid://shopify/Product/10'},sourceSetId:'sources',updatedAt:i,sourceVersion:3})});
const docs=Array.from({length:53},(_,i)=>workspace(i));
docs[1]={id:'design_001',data:()=>({...workspace(1).data(),archivedAt:1})};
docs[2]={id:'design_002',data:()=>({...workspace(2).data(),context:{...workspace(2).data().context,campaignId:'99'}})};
let reads=0;
const db={collection(name){assert.equal(name,'State');return {doc(name){assert.equal(name,'adDesign');return {collection(name){assert.equal(name,'workspaces');let after='',limit=0;return {select(){return this;},orderBy(field){assert.equal(field,'__name__');return this;},limit(n){limit=n;return this;},startAfter(v){after=v;return this;},async get(){reads++;return {docs:docs.filter(d=>d.id>after).slice(0,limit)};}};}};}};}};
(async()=>{
 const input={db,stateCollection:'State',deletedCampaignIds:new Set(['99'])};
 const first=await savedWorkspaces(input);assert.equal(first.workspaces.length,48);assert.equal(first.nextCursor,'design_049');
 assert.ok(!first.workspaces.some(w=>['design_001','design_002'].includes(w.workspaceId)));
 const second=await savedWorkspaces({...input,after:first.nextCursor});assert.equal(second.workspaces.length,3);assert.equal(second.nextCursor,null);
 assert.equal(new Set([...first.workspaces,...second.workspaces].map(w=>w.workspaceId)).size,51);
 await assert.rejects(()=>savedWorkspaces({...input,after:'../other'}),/Invalid/);assert.equal(reads,2);
 const html=fs.readFileSync(path.join(root,'brites-adwords.html'),'utf8');
 const fn=html.slice(html.indexOf('function openApprovalDesign('),html.indexOf('\nfunction',html.indexOf('function openApprovalDesign(')));
 let opened;const ctx={DASH:{pending:[{id:'peach',payload:{adDesign:{workspaceId:'shared',productId:'peach',settings:{groupRef:group}}}}]},openAdDesign:(context,options)=>{opened={context,options};}};
 vm.runInNewContext(fn+';openApprovalDesign("peach");',ctx);
 assert.equal(opened.context.workspaceId,'shared');assert.equal(opened.context.productId,'peach');assert.equal(opened.context.groupRef,group);assert.equal(opened.options.legacy,true);
 if(process.env.BRITES_EDITOR_DOM_RUNTIME){
   const {JSDOM}=require(path.join(process.env.BRITES_EDITOR_DOM_RUNTIME,'jsdom'));
   const dom=new JSDOM('<div id="groupContext"></div><section id="v-groups"></section>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
   w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};w.HTMLDialogElement.prototype.close=function(){this.open=false;};
   w.eval(fs.readFileSync(path.join(root,'brites-groups.js'),'utf8'));
   const calls=[];let selected;w.BritesGroups.init({api:async(action,input)=>{calls.push(action);if(action==='adGroups')throw Error('Google quota exhausted');if(action==='adDesignSavedWorkspaces')return {ok:true,workspaces:[first.workspaces[0]],nextCursor:null};throw Error('Unexpected live Google dependency');},currentView:()=> 'groups',go:()=>{},changed:()=>{},design:async input=>{selected=input;}});
   w.BritesGroups.show();await new Promise(r=>setTimeout(r,10));
   assert.ok(w.document.querySelector('[role=alert]').textContent.includes('quota'));
   w.document.querySelector('[data-bg-action=saved]').click();await new Promise(r=>setTimeout(r,10));
   assert.ok(w.document.querySelector('dialog').textContent.includes('Peach charm'));
   w.document.querySelector('[data-bg-saved]').click();await new Promise(r=>setTimeout(r,10));
   assert.equal(selected.workspaceId,'design_000');assert.equal(Object.keys(selected).length,1);assert.deepEqual(calls,['adGroups','adDesignSavedWorkspaces']);
   assert.equal(w.document.querySelector('dialog'),null);dom.window.close();
 }
 console.log('PASS saved-workspace pagination, archive filtering, quota recovery, and exact approval scope');
})().catch(e=>{console.error(e);process.exit(1);});
