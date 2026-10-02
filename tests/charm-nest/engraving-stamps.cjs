const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const E=require('../../charm-nest-engraving-seals.js'),A=require('../../charm-nest-activity.js');
const old={key:'old',state:'written',approvedBy:'Paul',approvedAt:1000,row:{engrave:{}}};
E.keep(old);old.approvedAt=null;old.state='words';assert.equal(E.list(old).length,1);E.add(old,'engraveApproved','Seth',2000);assert.deepEqual(E.list(old).map(s=>s.by),['Paul','Seth']);assert.equal(E.list(JSON.parse(JSON.stringify(old))).length,2,'history survives checkpoint serialization');
const waived=E.list(E.record({kind:'skipped',at:2000,by:'Seth'}));
assert.deepEqual(waived.map(s=>[s.how,s.at,s.by]),[['engravePlain',2000,'Seth']],'a cut-plain decision cannot invent an approval seal');
const priorThenWaived=E.list(E.record({kind:'skipped',at:3000,by:'Seth',saved:{engravingSeals:[{how:'engraveApproved',at:1000,by:'Paul'}]}}));
assert.deepEqual(priorThenWaived.map(s=>[s.how,s.at,s.by]),[['engraveApproved',1000,'Paul'],['engravePlain',3000,'Seth']],'waiving engraving preserves the earlier approval without inventing a new one');
assert.equal(E.list(E.record({kind:'preparing',at:4000,by:'Seth'})).length,0,'preparing a preview has not approved it');
const timelineRow={key:'order:line',order:{receiptId:'3701000'},line:{transactionId:'tx-1'},poolIds:['copy-1']};
const recovered=E.fromEvents([
 {id:'approval',type:'engraveApproved',orderId:'3701000',lineKey:'order:line',at:1000,by:'Paul'},
 {id:'plain',type:'engraveChanged',orderId:'3701000',lineKey:'order:line',at:2500,by:'Seth',data:{how:'skipped',decidedAt:2000}},
 {id:'legacy-plain',type:'engraveChanged',orderId:'3701000',transactionId:'tx-1',at:3000,by:'Alex',data:{how:'skipped'}},
 {id:'wrong-order',type:'engraveChanged',orderId:'9999',lineKey:'order:line',at:4000,by:'Wrong',data:{how:'skipped'}},
 {id:'wrong-line',type:'engraveChanged',orderId:'3701000',lineKey:'other-line',transactionId:'tx-2',at:4000,by:'Wrong',data:{how:'skipped',poolId:'other-copy'}},
 {id:'no-engraving',type:'engraveChanged',lineKey:'order:line',at:4000,by:'Wrong',data:{how:'none'}},
 {id:'edited-words',type:'engraveChanged',lineKey:'order:line',at:4000,by:'Wrong',data:{how:'words'}},
 {id:'wrong-event',type:'held',lineKey:'order:line',at:4000,by:'Wrong',data:{how:'skipped'}}
],timelineRow);
assert.deepEqual(recovered.map(s=>[s.how,s.at,s.by]),[['engraveApproved',1000,'Paul'],['engravePlain',2000,'Seth'],['engravePlain',3000,'Alex']],'legacy decisions recover only their exact order/line and recorded signers/times');
const source=fs.readFileSync('charm-nest-bridge.js','utf8');
const recordedWords=[],wordsContext={Date,TL:{line(row,type,event){recordedWords.push({row,type,event});}}};vm.createContext(wordsContext);
const wordsStart=source.indexOf('  function wordsEvent('),wordsEnd=source.indexOf('  async function classifyOnce(',wordsStart);vm.runInContext(source.slice(wordsStart,wordsEnd),wordsContext);
wordsContext.wordsEvent({row:timelineRow,key:'order:line',text:'Original words',decidedAt:2000,decision:{at:1800}},'Seth','Original words','skipped');
assert.equal(recordedWords[0].event.at,2000,'the timeline shares the actual cut-plain seal timestamp');assert.equal(recordedWords[0].event.data.decidedAt,2000);assert.equal(recordedWords[0].event.by,'Seth');
let finish,finishCheck,pressed=false,moved=0,saved=0,removed=0,checks=0,verification={ok:true};
const wait=new Promise(r=>finish=r),checkWait=new Promise(r=>finishCheck=r);
const job={key:'order',copies:['piece'],row:{order:{receiptId:'3701000'},spec:{designSku:'FROG'},engrave:{}},fit:{size:1,capMm:1,glyphs:[]},view:{cx:0,cy:0,cutMembers:[]},verify:{geometry:{ok:true}},text:'A',state:'review'};
const approvalJobs=new Map([[job.key,job]]);
const ctx={Date,Promise,PT:72/25.4,P:{buildBackFile:async()=>({bytes:new Uint8Array([1])})},S:{settings:{}},B:{pool:{rows:new Map()}},charmFor:()=>({sourceId:'source'}),sheetFor:()=>({}),sourceOf:()=>({parsed:{}}),fitOpts:()=>({lineGap:.18}),verifyBackFile:async()=>{checks++;await checkWait;return verification;},EG:{cardKey:null,card:null},CNListActivity:A,CNEngravingSeals:{...E,press:async()=>{pressed=true;await wait;}},employeeName:()=> 'Paul',Review:{remove(){removed++;}},goes(){moved++;},EG_TAB:()=>'',agent(){},render(){},saveBacks:async()=>saved++,toast(){},askEmployee:()=> 'Paul'};
ctx.items=()=>approvalJobs;ctx.fitTasks=new WeakMap();
vm.createContext(ctx);const start=source.indexOf('  async function prepareApproval('),end=source.indexOf('  /** An approval',start);vm.runInContext(source.slice(start,end),ctx);
const button=()=>({textContent:'Approved',disabled:false,attrs:new Map(),setAttribute(k,v){this.attrs.set(k,v);},getAttribute(k){return this.attrs.get(k) ?? null;},removeAttribute(k){this.attrs.delete(k);}}),approvedButton=button();
async function checkEditedHistory(copies,state='written'){
 const old={how:'engraveApproved',at:1000,by:'Paul'},latest={how:state==='skipped'?'engravePlain':'engraveApproved',at:2000,by:'Seth'};
 const row={key:'order',poolIds:copies,engrave:{state:'written',approved:true,approvedAt:1000,approvedBy:'Paul',seals:[old]}};
 const edited={key:'edit',state,copies:['edited'],text:'New words',approvedAt:state==='skipped'?null:2000,approvedBy:state==='skipped'?null:'Seth',decidedAt:state==='skipped'?2000:undefined,decidedBy:state==='skipped'?'Seth':undefined,engravingSeals:[old,latest],editSheet:{sheetId:'sheet'}};
 const loaded={key:'order',copies:copies.slice(),backs:[{poolId:'edited'}],row};
 const context={CNEngravingSeals:E,api:async()=>({sheet:{id:'sheet',backPool:[{poolId:'edited',engravingSeals:[old,latest]}]}}),allSheets:()=>[],S:{library:{rows:[]},mode:'engrave'},items:()=>new Map([['edit',edited],['order',loaded]]),Orders:{rows:()=>[row],render(){}},Review:{remove(){},render(){}},refreshBacks(){},Session:{schedule(){}}};
 vm.createContext(context);const start=source.indexOf('  async function syncEditedBack('),end=source.indexOf('\n  const jobOf',start);vm.runInContext(source.slice(start,end),context);await context.syncEditedBack(edited);
 assert.deepEqual(E.list(row.engrave).map(s=>[s.how,s.at,s.by]),[[old.how,1000,'Paul'],[latest.how,2000,'Seth']],'already-open order rows immediately receive old and new seal history');assert.deepEqual(E.list(loaded).map(s=>s.by),['Paul','Seth'],'the loaded placement sees both signers without a timeline poll');
 if(copies.length===1){assert.equal(row.engrave.approvedAt,edited.approvedAt);assert.equal(row.engrave.approvedBy,edited.approvedBy);assert.equal(row.engrave.text,'New words');if(state==='skipped')assert.equal(row.engrave.decidedBy,'Seth');}
 else{assert.equal(row.engrave.approvedAt,1000,'editing one of several copies preserves the others’ current approval');assert.equal(row.engrave.approvedBy,'Paul');}
}
(async()=>{
 const p=ctx.approve(job,null,approvedButton);
 for(let n=0;n<10 && !checks;n++)await Promise.resolve();
 assert.equal(checks,1);assert(!pressed,'the exported file is checked before a stamp is added');assert(job.approvalPreparing);assert.equal(E.list(job).length,0);assert.equal(job.state,'review');
 assert.equal(approvedButton.textContent,'Approved','verification keeps the same compact button label');assert.equal(approvedButton.getAttribute('aria-busy'),'true');assert(approvedButton.disabled);
 await ctx.approve(job,'Paul',{});assert.equal(checks,1,'double click during file verification does not start another approval');
 finishCheck();for(let n=0;n<10 && !pressed;n++)await Promise.resolve();
 assert(pressed);assert(job.stamping);assert.equal(job.state,'review','background pollers cannot observe an approval before its stamp finishes');assert(!job.row.engrave.approved);assert.equal(moved,0);assert.equal(saved,0);assert.equal(removed,0);
 assert.equal(approvedButton.textContent,'Approved','stamping never replaces Approved with extra text');assert.equal(approvedButton.getAttribute('aria-busy'),'true');ctx.employeeName=()=> 'Seth';assert.equal(E.list(job)[0].by,'Paul','the seal keeps the signed-in operator captured at approval, even if the viewer changes');
 await ctx.approve(job,'Paul',{});assert.equal(E.list(job).length,1,'double click during stamping cannot record another approval');
 finish();await p;assert(!job.stamping);assert(!job.approvalPreparing);assert.equal(job.state,'approved');assert.equal(moved,1);assert.equal(saved,1);assert.equal(removed,1);
 assert.equal(approvedButton.getAttribute('aria-busy'),null);assert.equal(approvedButton.textContent,'Approved');assert.equal(E.list(job)[0].by,'Paul');
 await ctx.approve(job,'Paul',{});assert.equal(E.list(job).length,1,'an already-decided card cannot add another stamp');assert.equal(saved,1);
 verification={ok:false,why:'engraving intersects a cut-out or cut-edge clearance'};
 const failed={...job,key:'failed',state:'review',approvedAt:null,approvedBy:null,engravingSeals:[{how:'engraveApproved',at:1000,by:'Seth'}],row:{...job.row,engrave:{}}};
 approvalJobs.set(failed.key,failed);
 const retryButton=button();await ctx.approve(failed,'Paul',retryButton);await ctx.approve(failed,'Paul',retryButton);
 assert.equal(failed.state,'review','a failed export remains an editable placement');assert(!failed.row.engrave.approved);assert(!failed.approvalPreparing);assert.equal(E.list(failed).length,1,'failed exports never accumulate approval seals');assert.equal(E.list(failed)[0].by,'Seth','existing historical seals remain');assert.equal(moved,1);assert.equal(saved,1);assert.equal(removed,1);
 assert.equal(retryButton.textContent,'Approved','failed verification keeps only Approved');assert.equal(retryButton.getAttribute('aria-busy'),null);assert.equal(retryButton.disabled,false,'failed verification enables a corrected retry');
 failed.state='blocked';const before=checks;await ctx.approve(failed,'Paul',{});assert.equal(checks,before,'the keyboard cannot approve a blocked card');
 await checkEditedHistory(['edited']);await checkEditedHistory(['edited','other']);await checkEditedHistory(['edited'],'skipped');
 console.log('PASS: verification before stamping; full stamp before departure; constant Approved button; no false/repeated approval; captured signer and historical approvals retained; edited history appears immediately in loaded orders');
})().catch(e=>{console.error(e);process.exitCode=1;});
