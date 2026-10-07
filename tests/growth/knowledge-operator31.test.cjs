'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {JSDOM}=require('jsdom');
const NOW=Date.now(),ID='gid://shopify/Product/3101',VERSION='a'.repeat(64),NEXT='c'.repeat(64),HANDLE='fixture-reviewed-symbol';
const clone=value=>JSON.parse(JSON.stringify(value));
function data({id=ID,handle=HANDLE,version=VERSION,status='approved'}={}){
  const row={id:'fixture-row-'+id,rank:1,title:'Synthetic reviewed symbol',productId:id,handle,dossierVersion:version,status:'complete'};
  const dossier={productId:id,handle,status,version,savedAt:NOW,sources:[{id:'museum',url:'https://museum.example.edu/symbols',title:'Synthetic neutral source',excerpt:'Synthetic source evidence.',reviewed:true,checkedAt:NOW}],meanings:[{kind:'interpretation',text:'This symbol may be a personal reminder of a milestone.',context:'Synthetic qualified interpretation.',sourceIds:['museum']}],facts:[],competitors:[],buyerIntents:[],recommendations:[{channel:'keywords',basis:'hypothesis',action:'Review exact symbol queries.',measure:'Qualified interest after review.',sourceIds:['museum'],keywords:['symbol pendant']}]};
  const live={id,handle,checkedAt:NOW,meaningHold:false,recommendationHold:false,cartHold:false};
  const supplement={productId:id,handle,status:'approved',version:'b'.repeat(64),baseDossierVersion:version,savedAt:NOW,productCheckedAt:NOW,sources:[{id:'supplement',checkedAt:NOW,reviewed:true}],meanings:[{text:'Synthetic supplement interpretation.',sourceIds:['supplement']}]};
  const knowledge={productId:id,kind:'interpretation',text:dossier.meanings[0].text,context:dossier.meanings[0].context,sources:[{title:'Synthetic neutral source',url:dossier.sources[0].url,checkedAt:NOW}]};
  return {row,dossier,live,supplement,knowledge};
}
const fulfilled=value=>({status:'fulfilled',value});
function reads(f){return {research:fulfilled({dossiers:[f.dossier],productIssues:[],productIssueState:'available'}),product:fulfilled({live:true,product:f.live}),supplements:fulfilled({supplements:[f.supplement]}),knowledge:fulfilled({products:[f.knowledge]})};}
function plainUi(){const dom=new JSDOM('<div id="fixture-root"></div>',{url:'https://sandbox.invalid'});vm.runInContext(fs.readFileSync(path.resolve(__dirname,'../../brites-growth.js'),'utf8'),vm.createContext(dom.window));return {dom,window:dom.window,api:dom.window.BritesGrowth,root:dom.window.document.querySelector('#fixture-root')};}
async function until(check){for(let i=0;i<100;i++){if(check())return;await new Promise(setImmediate);}assert.fail('Operator fixture did not settle.');}
function button(f,label){const found=[...f.root.querySelectorAll('button')].find(value=>value.textContent===label);assert.ok(found,'Missing operator button '+label);return found;}
async function mounted({extra=false,operatorTools=true,status='approved',hook=null}={}){
  const f=plainUi(),primary=data({status}),secondary=data({id:'gid://shopify/Product/3102',handle:'fixture-second-symbol'}),requests=[];
  secondary.row.rank=2;secondary.row.title='Second synthetic symbol';
  const request=async(op,payload)=>{
    requests.push({op,payload:payload&&clone(payload)});
    if(hook){const intercepted=await hook(op,payload,{primary,secondary,requests});if(intercepted!==undefined)return intercepted;}
    if(op==='status')return {at:NOW,counts:{ranked:extra?2:1,matched:extra?2:1,complete:extra?2:1,approvedDossiers:extra?2:1},control:{},queue:[primary.row,...(extra?[secondary.row]:[])]};
    const current=op.includes(encodeURIComponent(secondary.row.productId))||op.includes(secondary.row.handle)?secondary:primary;
    if(op.startsWith('research?'))return clone({dossiers:[current.dossier],productIssues:[]});
    if(op.startsWith('product?'))return clone({product:current.live,live:true});
    if(op.startsWith('story-supplements?'))return clone({supplements:[current.supplement]});
    if(op.startsWith('knowledge?'))return clone({products:[current.knowledge]});
    if(op.startsWith('demand?'))return {products:[]};
    throw Error('Unexpected synthetic read '+op);
  };
  await f.api.mount(f.root,{request,operatorTools});await f.root.querySelector('.row').onclick();
  return {...f,primary,secondary,requests};
}
function proposal(f,changes={}){return {productId:ID,baseDossierVersion:VERSION,recommendationIndex:0,sourceIds:['museum'],keywords:['reviewed symbol gift'],reviewed:true,...changes};}
async function loadFile(f,value,{size=null,name='reviewed-keywords.json',text=null}={}){
  const input=f.root.querySelector('input[type="file"]'),content=JSON.stringify(value);
  assert.ok(input);Object.defineProperty(input,'files',{value:[{name,size:size??Buffer.byteLength(content),text:text||(()=>Promise.resolve(content))}],configurable:true});await input.onchange();return input;
}
function pasteJson(f,text){const input=f.root.querySelector('[aria-label="Paste reviewed keyword revision JSON"]');assert.ok(input);input.value=typeof text==='string'?text:JSON.stringify(text);input.oninput();button(f,'Preview pasted keyword revision').onclick();return input;}
test('operator diagnostics retain exact versions, source dates, holds and keyword index bindings',()=>{
  const f=plainUi(),fixture=data();try{
    fixture.live.meaningHold=true;fixture.supplement.baseDossierVersion='d'.repeat(64);
    const report=f.api.shopperKnowledgeDiagnosticsFor(fixture.row,reads(fixture),NOW);
    assert.equal(report.selection.productId,ID);assert.equal(report.product.identityVerified,true);assert.equal(report.dossier.version,VERSION);
    assert.equal(report.dossier.sources[0].checkedAt,NOW);assert.equal(report.dossier.sources[0].ageState,'fresh');assert.equal(report.issues.meaningHold,true);
    assert.equal(report.supplements.records[0].currentBaseVersionMatches,false);assert.equal(report.publicKnowledge.meaningCount,1);
    assert.equal(report.dossier.keywordRecommendations[0].recommendationIndex,0);assert.deepEqual([...report.dossier.keywordRecommendations[0].sourceIds],['museum']);
    assert.equal(report.readOnly,true);assert.equal(report.authoringWrites,false);assert.equal(report.approvals,false);
  }finally{f.dom.window.close();}
});
test('empty public projection never turns an unavailable or foreign supplement read into absence',()=>{
  const f=plainUi(),fixture=data();try{
    const current=reads(fixture);current.knowledge=fulfilled({products:[]});current.supplements={status:'rejected',reason:Error('secret-token-never-display')};
    let report=f.api.shopperKnowledgeDiagnosticsFor(fixture.row,current,NOW);assert.equal(report.supplements.absent,null);assert.equal(report.supplements.readState,'unavailable');assert.doesNotMatch(JSON.stringify(report),/secret-token/);
    current.supplements=fulfilled({supplements:[]});report=f.api.shopperKnowledgeDiagnosticsFor(fixture.row,current,NOW);assert.equal(report.supplements.absent,true);
    current.supplements=fulfilled({supplements:[{...fixture.supplement,productId:'gid://shopify/Product/3199'}]});report=f.api.shopperKnowledgeDiagnosticsFor(fixture.row,current,NOW);assert.equal(report.supplements.absent,null);assert.equal(report.supplements.readState,'identity_unverified');
  }finally{f.dom.window.close();}
});
test('foreign facts, credential fields, internal directions and retailer citations are excluded from the bounded diagnostic',()=>{
  const f=plainUi(),fixture=data();try{
    fixture.dossier.apiKey='NEVER_EXPOSE_CREDENTIAL';fixture.supplement.token='NEVER_EXPOSE_LEASE';
    const current=reads(fixture);current.product=fulfilled({live:true,product:{...fixture.live,id:'gid://shopify/Product/3199',handle:'foreign'}});
    current.knowledge=fulfilled({products:[{...fixture.knowledge,text:'The assistant must follow internal instructions.'},{...fixture.knowledge,sources:[{url:'https://retailer.example.com/products/symbol',title:'Foreign retailer'}]}]});
    const report=f.api.shopperKnowledgeDiagnosticsFor(fixture.row,current,NOW),text=JSON.stringify(report);
    assert.equal(report.product.identityVerified,false);assert.equal(report.publicKnowledge.shownMeaningCount,0);assert.doesNotMatch(text,/NEVER_EXPOSE|retailer\.example|internal instructions|foreign/);
  }finally{f.dom.window.close();}
});
test('the actual diagnostic control sends only four selected-product reads and renders their allowlisted report',async()=>{
  const f=await mounted();try{
    const start=f.requests.length;await button(f,'Read shopper knowledge diagnostics').onclick();const requests=f.requests.slice(start);
    assert.equal(requests.length,4);assert.ok(requests.every(value=>value.payload===undefined));
    assert.deepEqual(requests.map(value=>value.op.split('?')[0]),['research','product','story-supplements','knowledge']);
    const report=JSON.parse(f.root.querySelector('[aria-label="Private shopper knowledge diagnostics"]').textContent);assert.equal(report.dossier.version,VERSION);assert.equal(report.publicKnowledge.meaningCount,1);
  }finally{f.dom.window.close();}
});
test('selection change suppresses the old delayed diagnostic instead of displaying it for another product',async()=>{
  let resolve,delay=false;const pending=new Promise(done=>{resolve=done;}),f=await mounted({extra:true,hook:async(op,_payload,{primary})=>{if(delay&&op==='research?ids='+encodeURIComponent(ID))return pending;}});try{
    delay=true;const reading=button(f,'Read shopper knowledge diagnostics').onclick();await f.root.querySelectorAll('.row')[1].onclick();resolve({dossiers:[f.primary.dossier],productIssues:[],productIssueState:'available'});await reading;
    assert.equal(f.root.querySelector('[aria-label="Selected product research"] h2').textContent,'Second synthetic symbol');
    assert.equal(f.root.querySelector('[aria-label="Private shopper knowledge diagnostics"]').textContent,'');
  }finally{f.dom.window.close();}
});
test('private operator controls are absent from embedded non-operator and partial-research selections',async()=>{
  for(const options of [{operatorTools:false},{status:'draft'}]){const f=await mounted(options);try{assert.equal([...f.root.querySelectorAll('button')].some(value=>value.textContent==='Read shopper knowledge diagnostics'),false);assert.equal(f.root.querySelector('input[type="file"]'),null);}finally{f.dom.window.close();}}
});
test('reviewed JSON is previewed without dispatch and rejects unknown, foreign, stale or oversized proposals',async()=>{
  const f=await mounted();try{
    await loadFile(f,proposal(f));assert.equal(button(f,'Apply reviewed keyword revision').disabled,false);assert.match(f.root.querySelector('[aria-label="Reviewed keyword revision preview"]').textContent,/reviewed symbol gift/);assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
    for(const value of [proposal(f,{productId:'gid://shopify/Product/3199'}),proposal(f,{baseDossierVersion:'d'.repeat(64)}),proposal(f,{reviewed:false}),proposal(f,{sourceIds:['foreign']}),proposal(f,{keywords:Array.from({length:9},(_,i)=>'symbol gift '+i)}),{...proposal(f),apiKey:'NEVER_EXPOSE_CREDENTIAL'}]){await loadFile(f,value);assert.equal(button(f,'Apply reviewed keyword revision').disabled,true);assert.equal(f.root.querySelector('[aria-label="Reviewed keyword revision preview"]').textContent,'');}
    await loadFile(f,proposal(f),{size:32768});assert.equal(button(f,'Apply reviewed keyword revision').disabled,true);assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
  }finally{f.dom.window.close();}
});
test('pasted JSON uses the exact review preview without sending a revision and editing invalidates it',async()=>{
  const f=await mounted();try{
    const input=pasteJson(f,proposal(f));assert.equal(button(f,'Apply reviewed keyword revision').disabled,false);
    const preview=f.root.querySelector('[aria-label="Reviewed keyword revision preview"]');assert.deepEqual(JSON.parse(preview.textContent),proposal(f));
    assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
    input.value='{';input.oninput();assert.equal(button(f,'Apply reviewed keyword revision').disabled,true);assert.equal(preview.textContent,'');
    assert.match(f.root.textContent,/Pasted JSON has changed/);assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
  }finally{f.dom.window.close();}
});
test('paste rejects malformed, oversized UTF-8, unknown, stale and foreign proposals without dispatch',async()=>{
  const f=await mounted();try{
    for(const text of ['{',' '.repeat(32768),JSON.stringify(proposal(f))+' '.repeat(32768),JSON.stringify(proposal(f))+'\u00e9'.repeat(16400),proposal(f,{productId:'gid://shopify/Product/3199'}),proposal(f,{baseDossierVersion:NEXT}),proposal(f,{sourceIds:['foreign']}),{...proposal(f),apiKey:'NEVER_EXPOSE_PASTE_CREDENTIAL'}]){
      pasteJson(f,text);assert.equal(button(f,'Apply reviewed keyword revision').disabled,true);assert.equal(f.root.querySelector('[aria-label="Reviewed keyword revision preview"]').textContent,'');
      assert.match(f.root.textContent,/pasted JSON is invalid, too large or bound to different research/);assert.doesNotMatch(f.root.textContent,/NEVER_EXPOSE_PASTE_CREDENTIAL/);
    }
    assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
  }finally{f.dom.window.close();}
});
test('valid pasted revision still requires separate Apply, exact current research and approved readback',async()=>{
  const f=await mounted({hook:async(op,payload,{primary})=>{
    if(op!=='keyword-revision')return;primary.dossier.version=NEXT;primary.dossier.recommendations[0].keywords.push(...payload.keywords);
    return {ok:true,changed:true,productId:ID,baseDossierVersion:VERSION,version:NEXT,recommendationIndex:0,keywordCount:2,sandboxOnly:true,providerCalls:0,inferenceCalls:0,campaignWrites:0,budgetWrites:0};
  }});try{
    pasteJson(f,proposal(f));assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
    const before=f.requests.length;await button(f,'Apply reviewed keyword revision').onclick();
    const operations=f.requests.slice(before);assert.deepEqual(operations.slice(0,3).map(value=>value.op),['research?ids='+encodeURIComponent(ID),'keyword-revision','research?ids='+encodeURIComponent(ID)]);
    assert.ok(operations.slice(3).every(value=>value.payload===undefined&&/^(?:research|demand)\?ids=/.test(value.op)));assert.equal(operations.filter(value=>value.op==='keyword-revision').length,1);
    assert.deepEqual(f.requests.find(value=>value.op==='keyword-revision').payload,proposal(f));assert.match(f.root.textContent,/Reviewed keyword revision saved and read back/);
  }finally{f.dom.window.close();}
});
test('late old file completion cannot replace a newer pasted preview',async()=>{
  let resolve;const pending=new Promise(done=>{resolve=done;}),f=await mounted();try{
    const loading=loadFile(f,proposal(f,{keywords:['old file symbol gift']}),{text:()=>pending});
    await until(()=>/Reading the reviewed JSON file/.test(f.root.textContent));pasteJson(f,proposal(f,{keywords:['current pasted symbol gift']}));
    await loading;resolve(JSON.stringify(proposal(f,{keywords:['old file symbol gift']})));await new Promise(setImmediate);
    const preview=JSON.parse(f.root.querySelector('[aria-label="Reviewed keyword revision preview"]').textContent);assert.deepEqual(preview.keywords,['current pasted symbol gift']);
    assert.equal(button(f,'Apply reviewed keyword revision').disabled,false);assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
  }finally{f.dom.window.close();}
});
test('late old file completion cannot replace a newer selected file preview',async()=>{
  let resolve;const pending=new Promise(done=>{resolve=done;}),f=await mounted();try{
    const loading=loadFile(f,proposal(f,{keywords:['old file symbol gift']}),{text:()=>pending});await until(()=>/Reading the reviewed JSON file/.test(f.root.textContent));
    await loadFile(f,proposal(f,{keywords:['current file symbol gift']}));await loading;resolve(JSON.stringify(proposal(f,{keywords:['old file symbol gift']})));await new Promise(setImmediate);
    assert.deepEqual(JSON.parse(f.root.querySelector('[aria-label="Reviewed keyword revision preview"]').textContent).keywords,['current file symbol gift']);assert.equal(button(f,'Apply reviewed keyword revision').disabled,false);
  }finally{f.dom.window.close();}
});
test('a stalled file read times out with paste recovery feedback and no write',async()=>{
  const f=await mounted();let expire;const originalSet=f.window.setTimeout.bind(f.window),originalClear=f.window.clearTimeout.bind(f.window),sentinel=987654321;
  f.window.setTimeout=(callback,ms,...args)=>{if(ms===8000){expire=callback;return sentinel;}return originalSet(callback,ms,...args);};f.window.clearTimeout=id=>{if(id!==sentinel)originalClear(id);};
  try{
    const loading=loadFile(f,proposal(f),{text:()=>new Promise(()=>{})});await until(()=>typeof expire==='function');assert.equal(button(f,'Apply reviewed keyword revision').disabled,true);
    expire();await loading;assert.match(f.root.textContent,/file did not finish loading/);assert.match(f.root.textContent,/Paste its JSON below and preview it instead/);
    assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);pasteJson(f,proposal(f));assert.equal(button(f,'Apply reviewed keyword revision').disabled,false);
  }finally{f.dom.window.close();}
});
test('unreadable file uses safe paste recovery without exposing the file exception',async()=>{
  const f=await mounted();try{
    await loadFile(f,proposal(f),{text:()=>Promise.reject(Error('NEVER_EXPOSE_FILE_CREDENTIAL'))});assert.equal(button(f,'Apply reviewed keyword revision').disabled,true);
    assert.match(f.root.textContent,/unreadable or bound to different research/);assert.match(f.root.textContent,/Paste its JSON below/);assert.doesNotMatch(f.root.textContent,/NEVER_EXPOSE_FILE_CREDENTIAL/);
    assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
  }finally{f.dom.window.close();}
});
test('selection change cancels a pending file read immediately without altering the new product',async()=>{
  const f=await mounted({extra:true});try{
    const loading=loadFile(f,proposal(f),{text:()=>new Promise(()=>{})});await until(()=>/Reading the reviewed JSON file/.test(f.root.textContent));
    await f.root.querySelectorAll('.row')[1].onclick();await loading;
    assert.equal(f.root.querySelector('[aria-label="Selected product research"] h2').textContent,'Second synthetic symbol');assert.equal(f.root.querySelector('[aria-label="Reviewed keyword revision preview"]').textContent,'');
    assert.equal(button(f,'Apply reviewed keyword revision').disabled,true);assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);
  }finally{f.dom.window.close();}
});
test('explicit Apply accepts the real research response without a state field and verifies approved readback',async()=>{
  const f=await mounted({hook:async(op,payload,{primary})=>{
    if(op!=='keyword-revision')return;primary.dossier.version=NEXT;primary.dossier.recommendations[0].keywords.push(...payload.keywords);
    return {ok:true,changed:true,productId:ID,baseDossierVersion:VERSION,version:NEXT,recommendationIndex:0,keywordCount:2,sandboxOnly:true,providerCalls:0,inferenceCalls:0,campaignWrites:0,budgetWrites:0};
  }});try{
    await loadFile(f,proposal(f));const before=f.requests.length;await button(f,'Apply reviewed keyword revision').onclick();const operations=f.requests.slice(before);
    assert.equal(operations[0].op,'research?ids='+encodeURIComponent(ID));assert.equal(operations[1].op,'keyword-revision');assert.deepEqual(operations[1].payload,proposal(f));assert.equal(operations[2].op,'research?ids='+encodeURIComponent(ID));
    assert.match(f.root.textContent,/Reviewed keyword revision saved and read back/);assert.ok(f.root.textContent.includes(NEXT));assert.match(f.root.textContent,/Existing Demand retains its previous version/);
  }finally{f.dom.window.close();}
});
test('fresh version drift and unavailable authentication block the revision before mutation dispatch',async()=>{
  for(const mode of ['version','authentication']){let checking=false;const f=await mounted({hook:async(op,_payload,{primary})=>{if(checking&&op==='research?ids='+encodeURIComponent(ID)){if(mode==='authentication')throw Error('NEVER_EXPOSE_AUTH_SECRET');return {dossiers:[{...primary.dossier,version:NEXT}],productIssues:[],productIssueState:'available'};}}});try{
    await loadFile(f,proposal(f));checking=true;await button(f,'Apply reviewed keyword revision').onclick();assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);assert.doesNotMatch(f.root.textContent,/NEVER_EXPOSE_AUTH_SECRET/);assert.match(f.root.textContent,/could not be verified/);
  }finally{f.dom.window.close();}}
});
test('changing selection during the fresh pre-write read cancels mutation dispatch',async()=>{
  let resolve,checking=false;const pending=new Promise(done=>{resolve=done;}),f=await mounted({extra:true,hook:async(op)=>{if(checking&&op==='research?ids='+encodeURIComponent(ID))return pending;}});try{
    await loadFile(f,proposal(f));checking=true;const applying=button(f,'Apply reviewed keyword revision').onclick();await f.root.querySelectorAll('.row')[1].onclick();resolve({dossiers:[f.primary.dossier],productIssues:[],productIssueState:'available'});await applying;
    assert.equal(f.requests.some(value=>value.op==='keyword-revision'),false);assert.equal(f.root.querySelector('[aria-label="Selected product research"] h2').textContent,'Second synthetic symbol');
  }finally{f.dom.window.close();}
});
test('known pre-write refusals remain distinct from uncertain or unverified storage outcomes',async()=>{
  for(const result of [{ok:false,changed:false,code:'STORY_REBIND_REQUIRED',recheckRequired:true},{ok:false,changed:false,code:'RESEARCH_VERSION_CHANGED',recheckRequired:true},{ok:false,changed:false,code:'PRODUCT_HOLD',recheckRequired:true},{ok:false,changed:null,recheckRequired:true,researchWriteAttempted:true},{ok:false,recheckRequired:true}]){const f=await mounted({hook:async(op)=>op==='keyword-revision'?result:undefined});try{
    await loadFile(f,proposal(f));await button(f,'Apply reviewed keyword revision').onclick();assert.doesNotMatch(f.root.textContent,/Reviewed keyword revision saved and read back/);
    const expected=result.changed===null?/storage outcome is uncertain/:result.changed!==false?/revision outcome could not be verified/:result.code==='STORY_REBIND_REQUIRED'?/approved story is bound to this research version/:result.code==='PRODUCT_HOLD'?/current product hold blocked/:/approved research version changed/;
    assert.match(f.root.textContent,expected);if(result.changed===false)assert.doesNotMatch(f.root.textContent,/storage outcome is uncertain/);
  }finally{f.dom.window.close();}}
});
test('confirmed save metadata still requires exact retained terms, appended order and source bindings in readback',async()=>{
  for(const mode of ['terms','order','source','count']){const f=await mounted({hook:async(op,payload,{primary})=>{
    if(op!=='keyword-revision')return;
    primary.dossier.version=NEXT;primary.dossier.recommendations[0].keywords.push(...payload.keywords);
    if(mode==='terms')primary.dossier.recommendations[0].keywords=['foreign seed',...payload.keywords];
    if(mode==='order')primary.dossier.recommendations[0].keywords.reverse();
    if(mode==='source')primary.dossier.recommendations[0].sourceIds=['foreign'];
    return {ok:true,changed:true,productId:ID,baseDossierVersion:VERSION,version:NEXT,recommendationIndex:0,keywordCount:mode==='count'?3:2,sandboxOnly:true,providerCalls:0,inferenceCalls:0,campaignWrites:0,budgetWrites:0};
  }});try{
    await loadFile(f,proposal(f));await button(f,'Apply reviewed keyword revision').onclick();
    assert.doesNotMatch(f.root.textContent,/Reviewed keyword revision saved and read back/);assert.match(f.root.textContent,/could not be verified/);
    assert.equal(f.requests.filter(value=>value.op==='keyword-revision').length,1);
  }finally{f.dom.window.close();}}
});
test('late post-dispatch revision response never appears as feedback for a newly selected product',async()=>{
  let resolve;const pending=new Promise(done=>{resolve=done;}),f=await mounted({extra:true,hook:async(op)=>op==='keyword-revision'?pending:undefined});try{
    await loadFile(f,proposal(f));const applying=button(f,'Apply reviewed keyword revision').onclick();
    await until(()=>f.requests.some(value=>value.op==='keyword-revision'));await f.root.querySelectorAll('.row')[1].onclick();
    resolve({ok:true,changed:true,productId:ID,baseDossierVersion:VERSION,version:NEXT,recommendationIndex:0,keywordCount:2,sandboxOnly:true,providerCalls:0,inferenceCalls:0,campaignWrites:0,budgetWrites:0});await applying;
    assert.equal(f.root.querySelector('[aria-label="Selected product research"] h2').textContent,'Second synthetic symbol');
    assert.doesNotMatch(f.root.textContent,/Reviewed keyword revision saved and read back/);
  }finally{f.dom.window.close();}
});
const receiptDiagnostics=()=>({readOnly:true,receiptOnly:true,queueUpdated:false,individualOrdersUpdated:0,savedReceiptRows:2,uniqueReceipts:2,confirmed:1,rejected:0,processing:0,unconfirmed:1,unavailable:0,receipts:[{receiptKey:'synthetic-receipt',outcome:'confirmed',code:'EXACT_DESTINATION_ONE_RECORD_SUCCESS'}]});
const receiptPreview=()=>({readOnly:true,receiptOnly:true,dryRun:true,queueUpdated:false,individualOrdersUpdated:0,individualOrderAttributionConfirmed:false,providerAggregateUsedForConfirmation:false,productionApplyAvailable:false,scannedRows:2,selectedReceipts:2,providerReceiptsConfirmed:1,proposedRepairs:1,blockedRows:1});
async function standaloneReceipt(hook){
  const f=plainUi(),requests=[];f.window.sessionStorage.setItem('brites-growth-key','synthetic-owner-only');
  f.window.fetch=async(url,options)=>{
    requests.push({url,options,body:options.body?JSON.parse(options.body):null});
    if(url==='/api/growth/status')return {ok:true,json:async()=>({at:NOW,counts:{},queue:[],control:{}})};
    return hook(requests.at(-1));
  };
  await f.api.mount(f.root);return {...f,requests};
}
test('standalone authenticated receipt controls dispatch only bounded existing read actions',async()=>{
  const f=await standaloneReceipt(async request=>({ok:true,json:async()=>request.body.action==='receiptDiagnostics'?receiptDiagnostics():receiptPreview()}));try{
    await button(f,'Check saved conversion receipts').onclick();await button(f,'Preview receipt status repairs').onclick();
    const reads=f.requests.filter(value=>value.url==='/api/growth-ads');assert.equal(reads.length,2);
    assert.deepEqual(reads.map(value=>value.body),[{action:'receiptDiagnostics',limit:20,maxMs:12000},{action:'receiptReconciliationPreview',limit:20,maxMs:12000}]);
    for(const read of reads){assert.equal(read.options.method,'POST');assert.equal(read.options.credentials,'same-origin');assert.equal(read.options.headers['X-Growth-Key'],'synthetic-owner-only');}
    assert.match(f.root.textContent,/Provider receipts confirmed: 1/);assert.match(f.root.textContent,/Proposed status repairs: 1/);
    assert.match(f.root.textContent,/must be checked again/);assert.doesNotMatch(f.root.textContent,/synthetic-owner-only/);
    assert.equal([...f.root.querySelectorAll('button')].some(value=>/upload|replay|apply.*receipt/i.test(value.textContent)),false);
  }finally{f.dom.window.close();}
});
test('standalone failed or non-read-only receipt responses preserve workspace and withhold exception details',async()=>{
  for(const mode of ['authentication','transport','invalid']){const f=await standaloneReceipt(async()=>{
    if(mode==='transport')throw Error('NEVER_EXPOSE_RECEIPT_CREDENTIAL');
    return {ok:mode!=='authentication',json:async()=>mode==='authentication'?{error:'NEVER_EXPOSE_RECEIPT_CREDENTIAL'}:{...receiptDiagnostics(),queueUpdated:true}};
  });try{
    await button(f,'Check saved conversion receipts').onclick();await button(f,'Preview receipt status repairs').onclick();
    assert.match(f.root.textContent,/read-only receipt result could not be verified/);assert.match(f.root.textContent,/safe status repair preview is unavailable/);
    assert.doesNotMatch(f.root.textContent,/NEVER_EXPOSE|Proposed status repairs:/);assert.ok(f.root.querySelector('[aria-label="Filter research queue"]'));
    assert.equal(button(f,'Check saved conversion receipts').disabled,false);assert.equal(button(f,'Preview receipt status repairs').disabled,false);
  }finally{f.dom.window.close();}}
});
test('embedded synthetic request mount has no implicit receipt bridge and retains explicit receipt callbacks',async()=>{
  const f=await mounted();try{
    assert.equal([...f.root.querySelectorAll('button')].some(value=>['Check saved conversion receipts','Preview receipt status repairs'].includes(value.textContent)),false);
    let diagnosticsCalls=0,previewCalls=0;f.window.fetch=async()=>{assert.fail('Explicit receipt callbacks must not use a network bridge.');};
    await f.api.mount(f.root,{request:async()=>({at:NOW,counts:{},queue:[],control:{}}),receiptReader:async()=>{diagnosticsCalls++;return receiptDiagnostics();},receiptPreviewReader:async()=>{previewCalls++;return receiptPreview();}});
    await button(f,'Check saved conversion receipts').onclick();await button(f,'Preview receipt status repairs').onclick();assert.equal(diagnosticsCalls,1);assert.equal(previewCalls,1);
  }finally{f.dom.window.close();}
});
