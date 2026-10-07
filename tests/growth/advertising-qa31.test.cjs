'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {webcrypto}=require('node:crypto'),{JSDOM}=require('jsdom');
const readOnly=require('../../netlify/functions/_britesGrowthAdsReadOnly');
const ROOT=path.resolve(__dirname,'../..'),NOW=Date.parse('2026-10-07T14:30:00Z');
async function until(check){for(let index=0;index<200;index++){if(check())return;await new Promise(setImmediate);}assert.fail('Synthetic QA did not settle.');}
async function ui({writeText=async()=>{},crypto=webcrypto}={}){
  const dom=new JSDOM(fs.readFileSync(path.join(ROOT,'ads-export-qa.html'),'utf8'),{url:'https://sandbox.invalid/ads-export-qa.html',runScripts:'outside-only'}),window=dom.window,mounts=[];
  window.Date.now=()=>NOW;window.TextEncoder=TextEncoder;
  Object.defineProperty(window,'crypto',{value:crypto,configurable:true});
  Object.defineProperty(window.navigator,'clipboard',{value:{writeText},configurable:true});
  window.setTimeout=callback=>setImmediate(callback);
  const context=dom.getInternalVMContext();
  vm.runInContext(fs.readFileSync(path.join(ROOT,'brites-growth.js'),'utf8'),context);
  const mount=window.BritesGrowth.mount;
  window.BritesGrowth.mount=async(root,options)=>{mounts.push(options);return mount(root,options);};
  vm.runInContext(fs.readFileSync(path.join(ROOT,'ads-export-qa.js'),'utf8'),context);
  await until(()=>mounts.length>0||window.document.querySelector('#qa-status').textContent.includes('could not'));
  if(mounts.length)await until(()=>window.document.querySelector('.row'));
  return {dom,window,mounts,expected:()=>JSON.parse(window.document.querySelector('#qa-expected').textContent)};
}
function button(f,label){const found=[...f.window.document.querySelectorAll('button')].find(button=>button.textContent===label);assert.ok(found,'Missing actual QA button '+label);return found;}
const ownId='gid://shopify/Product/2301';
async function research(f,index=f.mounts.length-1){return f.mounts[index].request('research?ids='+encodeURIComponent(ownId));}
async function demand(f,index=f.mounts.length-1){return (await f.mounts[index].request('demand?ids='+encodeURIComponent(ownId))).products[0];}
async function selectPrimary(f){f.window.document.querySelector('.row').click();await until(()=>[...f.window.document.querySelectorAll('button')].some(button=>button.textContent==='Copy research and measured demand'));}
test('current-time synthetic fixtures retain real source and packet hashes after the archived dates expire',async()=>{
  const f=await ui();try{
    const result=await research(f),dossier=result.dossiers[0],packet=result.operatorReviews[0].operatorReviewPacket,entry=await demand(f),expected=f.expected();
    assert.equal(dossier.savedAt,NOW);assert.equal(dossier.sources[0].checkedAt,NOW);assert.equal(entry.evidence.at,NOW);assert.equal(expected.checkedAt,NOW);
    assert.equal(readOnly.operatorPacketHasExactSourceBindings(packet,dossier),true);
    const product={productId:dossier.productId,handle:dossier.handle,dossierVersion:dossier.version};
    const text=f.window.BritesGrowth.demandBriefFor(dossier,expected.title,entry,null,'available',NOW,{result,product});
    assert.ok(text);assert.doesNotMatch(text,/CORRECTIVE RESEARCH ONLY/);
    for(const value of [expected.dossierVersion,expected.packetVersion,expected.sourceVersion,expected.citation,...expected.candidateIds,...expected.positiveTerms,...expected.negativeTerms])assert.ok(text.includes(value));
    assert.match(text,/reported spend 2.5 CAD/);assert.match(text,/reported conversions \(unvalidated\)/);assert.match(text,/not actual competitor spend or predicted returns/);
  }finally{f.dom.window.close();}
});
test('reset rehashes only the synthetic copies and keeps unchanged candidate identities',async()=>{
  const f=await ui();try{
    const first=await research(f),oldExpected=f.expected(),later=NOW+2*86400000;
    f.window.Date.now=()=>later;button(f,'Reset synthetic workspace').click();await until(()=>f.mounts.length===2);
    const fresh=await research(f),newExpected=f.expected();
    assert.equal(first.dossiers[0].sources[0].checkedAt,NOW);assert.equal(fresh.dossiers[0].sources[0].checkedAt,later);
    assert.notEqual(oldExpected.sourceVersion,newExpected.sourceVersion);assert.notEqual(oldExpected.packetVersion,newExpected.packetVersion);
    assert.deepEqual(oldExpected.candidateIds,newExpected.candidateIds);assert.equal(oldExpected.dossierVersion,newExpected.dossierVersion);
    assert.equal(readOnly.operatorPacketHasExactSourceBindings(fresh.operatorReviews[0].operatorReviewPacket,fresh.dossiers[0]),true);
  }finally{f.dom.window.close();}
});
test('genuine Demand age and exact-version guards still reject stale or mismatched evidence',async()=>{
  const f=await ui();try{
    const result=await research(f),dossier=result.dossiers[0],entry=await demand(f),product={productId:ownId,handle:dossier.handle,dossierVersion:dossier.version},context={result,product};
    assert.equal(f.window.BritesGrowth.demandBriefFor(dossier,'Fixture',entry,null,'available',NOW+86400001,context),null);
    entry.evidence.dossierVersion='c'.repeat(64);
    assert.equal(f.window.BritesGrowth.demandBriefFor(dossier,'Fixture',entry,null,'available',NOW,context),null);
  }finally{f.dom.window.close();}
});
test('dynamic changed-version fixtures have valid hashes but remain corrective for the old selected queue version',async()=>{
  const f=await ui();try{
    const scenario=f.window.document.querySelector('#qa-scenario');scenario.value='version-drift';scenario.dispatchEvent(new f.window.Event('change'));await until(()=>f.mounts.length===2);
    const before=await research(f),changed=await research(f),dossier=changed.dossiers[0],packet=changed.operatorReviews[0].operatorReviewPacket;
    assert.equal(readOnly.operatorPacketHasExactSourceBindings(packet,dossier),true);assert.equal(dossier.sources[0].checkedAt,NOW);assert.notEqual(dossier.version,before.dossiers[0].version);
    const product={productId:ownId,handle:dossier.handle,dossierVersion:before.dossiers[0].version};
    const text=f.window.BritesGrowth.operatorBriefFor(changed,product,dossier,'Fixture',NOW);
    assert.match(text,/CORRECTIVE RESEARCH ONLY/);assert.doesNotMatch(text,/Product advertising test brief/);
  }finally{f.dom.window.close();}
});
test('actual research and measured copy controls report writeText rejection without claiming copied contents',async()=>{
  let attempts=0;const f=await ui({writeText:async()=>{attempts++;throw Error('Synthetic clipboard refusal.');}});try{
    await selectPrimary(f);await button(f,'Copy product test brief').onclick();
    assert.equal(attempts,1);assert.ok(button(f,'Current evidence or clipboard unavailable · refresh first'));
    await button(f,'Copy research and measured demand').onclick();assert.equal(attempts,2);
    assert.equal([...f.window.document.querySelectorAll('button')].filter(button=>button.textContent==='Current evidence or clipboard unavailable · refresh first').length,2);
    assert.equal([...f.window.document.querySelectorAll('button')].some(button=>/copied/i.test(button.textContent)),false);
  }finally{f.dom.window.close();}
});
test('unavailable WebCrypto leaves the fixture unmounted and reports preparation failure',async()=>{
  const f=await ui({crypto:{subtle:{digest:async()=>{throw Error('Synthetic hashing unavailable.');}}}});try{
    assert.equal(f.mounts.length,0);assert.equal(f.window.document.querySelector('#qa-expected').textContent,'');
    assert.match(f.window.document.querySelector('#qa-status').textContent,/Synthetic source hashes could not be prepared/);
  }finally{f.dom.window.close();}
});
