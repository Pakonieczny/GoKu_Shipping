'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {JSDOM}=require('jsdom');
const source=fs.readFileSync(require.resolve('../../brites-growth.js'),'utf8');
const valid=()=>({readOnly:true,receiptOnly:true,dryRun:true,queueUpdated:false,individualOrdersUpdated:0,individualOrderAttributionConfirmed:false,providerAggregateUsedForConfirmation:false,productionApplyAvailable:false,scannedRows:2,selectedReceipts:2,providerReceiptsConfirmed:1,proposedRepairs:1,blockedRows:1});
function fixture(){const dom=new JSDOM('<main id="fixture"></main>',{url:'https://sandbox.invalid/'});vm.runInNewContext(source,{window:dom.window,document:dom.window.document,URL});return {dom,ui:dom.window.BritesGrowth,root:dom.window.document.getElementById('fixture')};}
test('status repair projection never contains raw server identities or unexpected private data',()=>{
  const f=fixture();const summary=f.ui.receiptPreviewFor({...valid(),rows:[{orderId:'PRIVATE_ORDER',requestId:'PRIVATE_REQUEST'}],secret:'PRIVATE_SECRET'});
  assert.deepEqual(JSON.parse(JSON.stringify(summary)),{scannedRows:2,selectedReceipts:2,providerReceiptsConfirmed:1,proposedRepairs:1,blockedRows:1});assert.ok(!JSON.stringify(summary).includes('PRIVATE'));f.dom.window.close();
});
test('write results, aggregate confirmation and purchase-attribution claims fail closed',()=>{
  const f=fixture();for(const patch of [{readOnly:false},{receiptOnly:false},{dryRun:false},{queueUpdated:true},{individualOrdersUpdated:1},{individualOrderAttributionConfirmed:true},{providerAggregateUsedForConfirmation:true},{productionApplyAvailable:true}])assert.throws(()=>f.ui.receiptPreviewFor({...valid(),...patch}),/could not be verified/);f.dom.window.close();
});
test('unavailable and invalid counts remain unavailable rather than becoming zero',()=>{
  const f=fixture();for(const value of [undefined,null,'3',-1,Infinity,0.5])assert.equal(f.ui.receiptPreviewFor({...valid(),proposedRepairs:value}).proposedRepairs,null);assert.equal(f.ui.receiptPreviewFor({...valid(),proposedRepairs:0}).proposedRepairs,0);f.dom.window.close();
});
test('actual mounted preview states no writes and requires rechecking before applying',async()=>{
  const f=fixture();let calls=0;await f.ui.mount(f.root,{request:async()=>({at:Date.now(),counts:{},queue:[],blockers:[],control:{}}),receiptPreviewReader:async()=>{calls++;return valid();}});
  const button=[...f.root.querySelectorAll('button')].find(b=>b.textContent==='Preview receipt status repairs');assert.ok(button);await button.onclick();assert.equal(calls,1);assert.match(f.root.textContent,/Preview only\. No conversion or order records were changed/);assert.match(f.root.textContent,/Proposed status repairs: 1/);assert.match(f.root.textContent,/Rows requiring further evidence: 1/);assert.match(f.root.textContent,/must be checked again/);assert.equal(button.disabled,false);f.dom.window.close();
});
test('preview failure preserves workspace and never displays private exception text',async()=>{
  const f=fixture();await f.ui.mount(f.root,{request:async()=>({at:Date.now(),counts:{},queue:[],blockers:[],control:{}}),receiptPreviewReader:async()=>{throw Error('PRIVATE_CONTACT PRIVATE_TOKEN');}});
  const button=[...f.root.querySelectorAll('button')].find(b=>b.textContent==='Preview receipt status repairs');await button.onclick();assert.match(f.root.textContent,/safe status repair preview is unavailable/);assert.ok(!f.root.textContent.includes('PRIVATE'));assert.ok(f.root.querySelector('input[aria-label="Filter research queue"]'));assert.equal(button.disabled,false);f.dom.window.close();
});
