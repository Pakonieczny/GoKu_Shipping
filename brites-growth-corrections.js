(function(global){'use strict';
  const id=value=>/^gid:\/\/shopify\/Product\/[1-9]\d*$/.test(String(value||''))?String(value):/^[1-9]\d*$/.test(String(value||''))?'gid://shopify/Product/'+value:null;
  const node=(tag,text,cls)=>{const e=document.createElement(tag);if(text!=null)e.textContent=text;if(cls)e.className=cls;if(tag==='pre'){e.style.whiteSpace='pre-wrap';e.style.overflowWrap='anywhere';e.style.maxHeight='320px';e.style.overflow='auto';}return e;};
  function packetToProposal(packet,productId,handle){
    if(!packet||typeof packet!=='object'||Array.isArray(packet)||id(packet.productId)!==id(productId)||packet.handle!==handle)throw Error('Import a proposal for this exact product and handle.');
    if(packet.proposal){const allowed=['dossierVersion','expectedTitle','expectedDescriptionHtmlSha256','expectedProductType','sourceIds','reviewedIssueIds','issueRecordUpdatedAt','issueRecordHash','snippetPatches','draftProductType','expectedVariantIdSetSha256','expectedOptionLabels'];if(Object.keys(packet.proposal).some(k=>!allowed.includes(k)))throw Error('Only a typed review proposal can be imported.');return {productId:id(productId),handle,proposal:JSON.parse(JSON.stringify(packet.proposal))};}
    const draft=packet.listingRepairDraft,source=packet.sourceBinding,offer=packet.currentOfferEvidence;
    if(!draft||!source||!offer||id(draft.expectedProductId)!==id(productId)||draft.expectedHandle!==handle||id(source.productId)!==id(productId)||source.handle!==handle||source.dossierVersion!==packet.dossierVersion||offer.variantsComplete!==true)throw Error('The retained exact product/source/variant binding is incomplete.');
    return {productId:id(productId),handle,proposal:{dossierVersion:packet.dossierVersion,expectedTitle:packet.title,expectedDescriptionHtmlSha256:draft.expectedDescriptionHtmlSha256,expectedProductType:draft.expectedProductType,sourceIds:source.ownProductSourceIds,reviewedIssueIds:(packet.openIssues||[]).filter(i=>i.status!=='resolved').map(i=>i.id),issueRecordUpdatedAt:packet.reviewedIssueRecordUpdatedAt,snippetPatches:draft.snippetPatches,draftProductType:draft.draftProductType,expectedVariantIdSetSha256:offer.variantIdSetSha256,expectedOptionLabels:offer.optionLabels}};
  }
  function assertReviewResponse(value,productId,handle){
    if(!value||value.mode!=='sandbox_review'||value.readOnly!==true||value.canApply!==false||value.executionCompatibility!=='not_established'||value.productId!==id(productId)||value.handle!==handle)throw Error('A read-only exact-product review could not be verified.');
    return value;
  }
  function assertPacketResponse(value,productId,handle){
    if(!value||value.mode!=='sandbox_correction_packet'||value.readOnly!==true||value.canApply!==false||value.executionCompatibility!=='not_established'||value.productId!==id(productId)||value.handle!==handle||typeof value.packetVersion!=='string'||!/^[a-f0-9]{64}$/.test(value.packetVersion))throw Error('A private exact-product correction packet could not be verified.');
    return packetToProposal(value,productId,handle);
  }
  function mount(container,opts={}){
    const productId=id(opts.productId),handle=opts.handle;
    const section=node('section',null,'recommendation correction-review');container.appendChild(section);
    section.append(node('h3','Review product corrections'),node('p','Preview only. Current Shopify values, sources and issues must match before a proposal is usable. No Shopify write is available.','sub'));
    const summary=node('dl',null,'correction-summary');summary.setAttribute('aria-label','Correction review readiness');
    const summaryValue={};
    for(const [key,label] of [['readiness','Correction readiness'],['issues','Affected issues'],['recheck','Exact-current recheck'],['owner','Owner review']]){const card=node('div');card.append(node('dt',label));summaryValue[key]=node('dd',key==='owner'?'Required · no apply action':'Checking…');card.appendChild(summaryValue[key]);summary.appendChild(card);}
    const status=node('p',null,'status'),controls=node('div',null,'controls'),input=node('input'),preview=node('button','Review current baseline','btn'),recheck=node('button','Recheck baseline','btn'),copy=node('button','Copy reviewed correction packet','btn'),content=node('div');
    input.type='file';input.accept='.json,application/json';input.style.maxWidth='100%';input.setAttribute('aria-label','Import private exact-product correction proposal');
    for(const b of [preview,recheck,copy]){b.type='button';b.disabled=true;}
    controls.append(input,preview,recheck,copy);section.append(summary,controls,status,content);
    let proposal=null,result=null,seq=0,destroyed=false,controller=null;
    const request=opts.request;
    function validSelection(){return !!productId&&/^[a-z0-9_-]{1,180}$/.test(handle||'');}
    function resetButtons(){preview.disabled=destroyed||!proposal||typeof request!=='function';recheck.disabled=destroyed||!result?.binding||typeof request!=='function';copy.disabled=destroyed||!result;}
    function renderSummary(value,action){
      const issues=Array.isArray(value?.currentIssues)?value.currentIssues.length:Array.isArray(proposal?.reviewedIssueIds)?proposal.reviewedIssueIds.length:null,blocked=value?.state!=='ready_for_review';
      summaryValue.readiness.textContent=!value?'Saved correction found · baseline check pending':blocked?'Blocked · '+String(value.state||'unavailable').replace(/_/g,' '):'Ready for owner review';
      summaryValue.issues.textContent=issues==null?'Unavailable':String(issues);
      if(action==='verifyBaseline')summaryValue.recheck.textContent=blocked?'Recheck found changed or incomplete evidence':'Rechecked · no binding drift found';
      else summaryValue.recheck.textContent=value?.binding?'Baseline checked · recheck not yet run':'Pending exact-current baseline read';
      summaryValue.owner.textContent='Required · no apply action';
    }
    function render(value,action){
      renderSummary(value,action);
      content.replaceChildren();
      content.append(node('span',String(value.state||'unavailable').replace(/_/g,' '),'tag'),node('p','Selected product: '+handle,'sub'));
      const holds=value.holds||{};content.append(node('p',value.productIssueState==='unavailable'?'Current holds are unavailable. Promotion remains unavailable until issues can be read.':'Promotion '+(holds.recommendationHold?'held':'not held')+' · Cart '+(holds.cartHold?'held':'not held')+' · Meaning/history '+(holds.meaningHold?'held':'not held')+'. Review keeps all existing holds.','status'));
      for(const issue of value.currentIssues||[]){content.append(node('b',issue.kind+' · open'),node('p',issue.detail));}
      for(const conflict of value.conflicts||[])content.appendChild(node('p',conflict,'error'));
      if(value.baseline&&value.after){
        const title=node('p','Title baseline: '+value.baseline.title+' · unchanged in this review.','sub');content.appendChild(title);
        if(value.baseline.productType!==value.after.productType)content.append(node('p','Product type: '+value.baseline.productType+' → '+value.after.productType));
        if(value.baseline.descriptionHtml!==value.after.descriptionHtml){content.append(node('h4','Current description HTML'),node('pre',value.baseline.descriptionHtml),node('h4','Proposed description HTML'),node('pre',value.after.descriptionHtml));}
      }
      if(value.variants?.length){content.append(node('h4','Current option combinations'),node('p',value.variants.length+' current variants · '+(value.coverage?.variants||'coverage unavailable'),'sub'));const list=node('ul');for(const v of value.variants){list.appendChild(node('li',(v.selectedOptions||[]).map(o=>o.name+': '+o.value).join(' · ')));}content.appendChild(list);}
      const collection=value.collectionImpact;
      if(collection){content.append(node('h4','Collection consequences'),node('p',collection.state==='unknown'?'Type/title consequences require further collection verification.':collection.state==='not_required'?'Description-only preview; no collection change is proposed.':'Collection pages reviewed; no entry/exit claim is inferred.'),node('p',collection.note,'sub'));for(const rule of collection.affectedRules||[])content.appendChild(node('p',(rule.title||rule.collectionId)+': '+rule.beforeValue+' → '+rule.afterValue+' · '+rule.result,'sub'));for(const unknown of collection.unknownSources||[])content.appendChild(node('p',(unknown.collectionId?unknown.collectionId+' · ':'')+unknown.reason,'sub'));}
      if(value.binding)content.appendChild(node('p','Admin API requested '+(value.binding.requestedApiVersion||'unavailable')+' · served '+(value.binding.servedApiVersion||'unavailable')+' · checked '+new Date(value.binding.readAt).toLocaleString(),'status'));
      for(const receipt of value.sourceReceipts||[]){try{const u=new URL(receipt.url);if(u.protocol==='https:'&&['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)){const a=node('a','Reviewed own-product source');a.href=u.href;a.target='_blank';a.rel='noopener noreferrer';content.appendChild(a);}}catch{}}
      for(const warning of value.warnings||[])content.appendChild(node('p',warning,'sub'));
    }
    function load(packet){
      if(destroyed||!validSelection())throw Error('Choose an exact product before importing a correction.');
      const loaded=packetToProposal(packet,productId,handle);
      if(opts.dossier&&(opts.dossier.productId!==productId||opts.dossier.handle!==handle||opts.dossier.status!=='approved'||loaded.proposal.dossierVersion!==opts.dossier.version))throw Error('The imported proposal differs from the selected current approved research version.');
      ++seq;controller?.abort();proposal=loaded.proposal;result=null;content.replaceChildren();renderSummary(null);status.textContent='Private retained proposal loaded. Review reads the current baseline; request drafts are ignored.';resetButtons();return loaded;
    }
    async function run(action){
      if(destroyed||!proposal||typeof request!=='function')return null;
      const current=++seq;controller?.abort();controller=new AbortController();preview.disabled=recheck.disabled=copy.disabled=true;status.textContent='Reading exact current product, approved sources and issues…';
      const body={action,productId,handle,proposal,...(action==='verifyBaseline'?{previewBinding:result?.binding}:{})};
      try{
        const value=assertReviewResponse(await request(body,{signal:controller.signal}),productId,handle);
        if(destroyed||current!==seq)return null;result=value;render(value,action);status.textContent='Review complete. No listing, campaign or issue record was changed.';return value;
      }catch{if(!destroyed&&current===seq){result=null;content.replaceChildren();renderSummary(null);summaryValue.readiness.textContent='Unavailable · retained review data preserved';status.textContent='Current correction review is unavailable. Retained proposals remain available; no write was attempted.';}return null;}
      finally{if(!destroyed&&current===seq)resetButtons();}
    }
    async function loadStored(){
      if(destroyed||!validSelection()||typeof request!=='function')return null;
      const current=++seq;controller?.abort();controller=new AbortController();status.textContent='Checking for a saved private exact-product correction…';
      try{
        const value=await request({action:'loadPacket',productId,handle},{signal:controller.signal}),loaded=assertPacketResponse(value,productId,handle);
        if(destroyed||current!==seq)return null;load(value);status.textContent='Saved private correction loaded. Reading the exact current baseline…';return run('preview');
      }catch{if(!destroyed&&current===seq){summaryValue.readiness.textContent='No validated correction available';summaryValue.issues.textContent='Unavailable';summaryValue.recheck.textContent='Not available';status.textContent='No current saved correction is available for this exact product. A private packet can still be imported.';resetButtons();}return null;}
    }
    input.onchange=async()=>{try{const f=input.files?.[0];if(!f)return;if(f.size>300000)throw Error('large');load(JSON.parse(await f.text()));}catch{++seq;controller?.abort();status.textContent='Import a valid private proposal for this exact product, with its reviewed source/version/variant baseline.';proposal=null;result=null;content.replaceChildren();summaryValue.readiness.textContent='Invalid or mismatched correction';summaryValue.issues.textContent='Unavailable';summaryValue.recheck.textContent='Not available';resetButtons();}};
    preview.onclick=()=>run('preview');recheck.onclick=()=>run('verifyBaseline');
    copy.onclick=async()=>{if(destroyed||!result)return;try{assertReviewResponse(result,productId,handle);const packet={schemaVersion:1,mode:'sandbox_review',canApply:false,executionCompatibility:'not_established',productId,handle,proposal,previewBinding:result.binding||null,state:result.state,holds:result.holds,productIssueState:result.productIssueState,after:result.after||null,coverage:result.coverage,collectionImpact:result.collectionImpact||null,conflicts:result.conflicts,warnings:result.warnings,sourceReceipts:result.sourceReceipts};await global.navigator.clipboard.writeText(JSON.stringify(packet,null,2));if(!destroyed)copy.textContent='Reviewed packet copied';}catch{if(!destroyed)copy.textContent='Clipboard unavailable';}};
    function destroy(){if(destroyed)return;destroyed=true;++seq;controller?.abort();opts.signal?.removeEventListener('abort',destroy);section.remove();proposal=result=null;}
    let ready=Promise.resolve(null);
    if(!validSelection()){input.disabled=true;status.textContent='An exact Product ID and handle are required for correction review.';}else if(typeof request==='function'&&opts.autoLoad!==false){status.textContent='Checking for a saved private correction for this selected product.';ready=loadStored();}else status.textContent=typeof request==='function'?'Import the private proposal for this selected product.':'An owner-authenticated correction reader is required. Retained import can still be reviewed.';
    if(opts.signal?.aborted)destroy();else opts.signal?.addEventListener('abort',destroy,{once:true});
    return {destroy,load,review:async packet=>{if(packet)load(packet);return run('preview');},recheck:()=>run('verifyBaseline'),ready};
  }
  global.BritesGrowthCorrections={mount,packetToProposal,assertReviewResponse,assertPacketResponse};
})(window);
