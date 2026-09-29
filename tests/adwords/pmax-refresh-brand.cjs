// A creative refresh of an existing Performance Max asset group respects the campaign's brand guidelines:
// with them on, Google keeps the business name and logo on the campaign and rejects them on an asset group,
// so the refresh neither creates nor links them, from the draft through review to the published operations.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const file=path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js'),realRequire=require('node:module').createRequire(file);
const clone=v=>v==null?v:JSON.parse(JSON.stringify(v));let n=0;const check=(v,m)=>{assert.ok(v,m);n++;};
function engine(){
  const context=vm.createContext({module:{exports:{}},exports:{},require:x=>x==='node-fetch'?async()=>{throw Error('Live network forbidden');}:realRequire(x),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,URLSearchParams,setTimeout,clearTimeout});
  vm.runInContext(fs.readFileSync(file,'utf8')+'\nmodule.exports.__refresh={putCopy:_putCreativeCopy,materialize:materializeReviewedCreative,guard:_guardRefreshBrandSetting};',context);
  return {E:context.module.exports,T:context.module.exports.__refresh,bind(values){context.__m=values;vm.runInContext(Object.keys(values).map(k=>k+'=__m.'+k).join('\n'),context);}};
}
function memory(){
  const docs=new Map();
  const doc=p=>({id:p.split('/').pop(),path:p,get:async()=>({exists:docs.has(p),data:()=>clone(docs.get(p))}),set:async v=>docs.set(p,clone(v)),update:async v=>docs.set(p,{...(docs.get(p)||{}),...clone(v)}),collection:x=>collection(p+'/'+x)});
  const collection=p=>({where(){return this;},limit(){return this;},get:async()=>({docs:[]}),add:async v=>{const id='auto'+docs.size;docs.set(p+'/'+id,clone(v));return {id};},doc:x=>doc(p+'/'+x)});
  return {docs,db:{collection,runTransaction:async fn=>fn({get:r=>r.get(),set:(r,v)=>r.set(v),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})},FV:{serverTimestamp:()=>Date.now()}};
}
const group='customers/123/assetGroups/7',campaign={id:'42',resourceName:'customers/123/campaigns/42',name:'Necklaces'};
const link=(field,i)=>({assetGroupAsset:{resourceName:`customers/123/assetGroupAssets/7~${900+i}~${field}`,fieldType:field},asset:field.includes('HEADLINE')||field==='DESCRIPTION'?{textAsset:{text:field.toLowerCase()+' '+i}}:{}});
const baseLinks=['HEADLINE','HEADLINE','HEADLINE','LONG_HEADLINE','DESCRIPTION','DESCRIPTION','MARKETING_IMAGE','SQUARE_MARKETING_IMAGE'];
const linksOf=ops=>field=>ops.filter(o=>o.assetGroupAssetOperation?.create?.fieldType===field);
const unlinked=ops=>{const used=JSON.stringify(ops.filter(o=>!o.assetOperation));return ops.filter(o=>o.assetOperation?.create&&!used.includes(JSON.stringify(o.assetOperation.create.resourceName)));};
const copy={headlines:['Duck necklace','Gift a duck','Brites duck'],longHeadlines:['A duck necklace made to order'],descriptions:['Handmade duck necklace.','See the listing for options.']};
const image=h=>({hash:h.padEnd(24,'0'),width:1200,height:1200,bytes:5});
const creative=()=>({schema:1,phase:'ready',logo:image('logo'),groups:[{key:'g0',ref:group,name:'Duck',channel:'pmax',assets:{square:image('sq'),landscape:image('la'),portrait:image('po')},review:{pass:true}}]});
// setting: true / false (Google omits false) / 'unreadable' (the field cannot be selected)
function refreshFixture({setting,groupLinks}){
  const e=engine(),queries=[];let queued=null;
  e.bind({fb:()=>memory(),_productShotsByIds:async ids=>ids.map(id=>({id:'gid://shopify/Product/'+id,title:'Duck necklace',handle:'duck',shots:[{url:'https://cdn.shopify.com/duck.jpg'}]})),enqueueApproval:async item=>{queued=clone(item);return 'refresh-1';},
    gaql:async q=>{queries.push(q);
      if(q.includes('FROM campaign ')){if(setting==='unreadable'&&q.includes('brand_guidelines_enabled'))throw Error('Unrecognized field campaign.brand_guidelines_enabled');return [{campaign:{...campaign,...(setting===true?{brandGuidelinesEnabled:true}:{})}}];}
      if(q.includes('FROM asset_group_asset'))return groupLinks.map(link);
      if(q.includes('FROM asset_group_signal'))return [];
      if(q.includes('FROM asset_group_listing_group_filter'))return [{assetGroupListingGroupFilter:{type:'SUBDIVISION',caseValue:{}}},{assetGroupListingGroupFilter:{type:'UNIT_INCLUDED',caseValue:{productItemId:{value:'shopify_US_10_100'}}}}];
      if(q.includes('FROM asset_group '))return [{assetGroup:{id:'7',resourceName:group,name:'Duck',finalUrls:['https://britesjewelry.com/products/duck']}}];
      throw Error('Unexpected query '+q);}});
  return {e,queries,queued:()=>queued};
}
(async()=>{
  // 1. Brand guidelines on: the draft records it and never removes or re-links a group business name or logo.
  let f=refreshFixture({setting:true,groupLinks:baseLinks});let out=await f.e.E.backfillPmaxCreative({campaignIds:['42']});let d=f.queued();
  check(out.queued===1&&d.payload.meta.brandGuidelinesEnabled===true,'brand guidelines on: the refresh draft records the campaign setting');
  check(f.queries.some(q=>/SELECT campaign\.id, campaign\.resource_name, campaign\.name, campaign\.brand_guidelines_enabled FROM campaign /.test(q)),'the refresh reads campaign.brand_guidelines_enabled with its campaigns');
  const brandPayload=clone(d.payload);f.e.T.putCopy(brandPayload,[{key:'g0',ref:group,name:'Duck',channel:'pmax',copy}]);let L=linksOf(brandPayload.mutateOperations);
  check(!L('BUSINESS_NAME').length&&L('HEADLINE').length===3&&L('DESCRIPTION').length===2&&!unlinked(brandPayload.mutateOperations).length,'reviewed copy replaces the text without a group business name or an unused business-name asset');
  f.e.bind({assertCreativeReviewed:()=>{},_loadCreativeAsset:async()=>Buffer.from('image')});
  let ops=await f.e.T.materialize({type:'creative',tag:d.tag,status:'APPROVED',payload:brandPayload,creative:creative()});L=linksOf(ops);
  check(!L('LOGO').length&&!L('BUSINESS_NAME').length&&L('SQUARE_MARKETING_IMAGE').length===1&&!unlinked(ops).length,'published operations carry the reviewed images but no logo asset or logo link');
  // 2. Brand guidelines off (Google omits the false boolean): unchanged business name and logo on the group.
  f=refreshFixture({setting:false,groupLinks:[...baseLinks,'BUSINESS_NAME','LOGO']});await f.e.E.backfillPmaxCreative({campaignIds:['42']});d=f.queued();
  const plainPayload=clone(d.payload);f.e.T.putCopy(plainPayload,[{key:'g0',ref:group,name:'Duck',channel:'pmax',copy}]);
  f.e.bind({assertCreativeReviewed:()=>{},_loadCreativeAsset:async()=>Buffer.from('image')});
  ops=await f.e.T.materialize({type:'creative',tag:d.tag,status:'APPROVED',payload:plainPayload,creative:creative()});L=linksOf(ops);
  check(d.payload.meta.brandGuidelinesEnabled===false&&L('BUSINESS_NAME').length===1&&L('LOGO').length===1&&ops.filter(o=>o.assetGroupAssetOperation?.remove?.includes('~BUSINESS_NAME')||o.assetGroupAssetOperation?.remove?.includes('~LOGO')).length===2,'brand guidelines off: the old business name and logo are replaced on the group as before');
  // 3. The setting cannot be read: a group that links its own business name proves it is off; otherwise nothing is prepared.
  f=refreshFixture({setting:'unreadable',groupLinks:[...baseLinks,'BUSINESS_NAME','LOGO']});out=await f.e.E.backfillPmaxCreative({campaignIds:['42']});
  check(out.queued===1&&f.queued().payload.meta.brandGuidelinesEnabled===false,'an unreadable setting with group-level business name and logo drafts the refresh with them');
  f=refreshFixture({setting:'unreadable',groupLinks:baseLinks});out=await f.e.E.backfillPmaxCreative({campaignIds:['42']});
  check(out.queued===0&&f.queued()===null&&/did not confirm whether this campaign uses brand guidelines/.test(out.results[0].error),'an unknown setting prepares no refresh rather than one Google would refuse');
  // 4. Publication (dry run validates through Google; live sends once): the guard compares the reviewed setting with Google's.
  const publish=async({payload,live,dryRun=true,tag='creative-refresh-7'})=>{
    const e=engine(),mem=memory(),sent=[];mem.docs.set('Brites_GAds_Approvals/r1',{type:'creative',tag,status:'APPROVED',payload,creative:creative()});
    e.bind({fb:()=>mem,assertCreativeReviewed:()=>{},_loadCreativeAsset:async()=>Buffer.from('image'),
      gaql:async q=>{if(q.includes('campaign.brand_guidelines_enabled FROM campaign WHERE campaign.id = 42'))return live==='unreadable'?(()=>{throw Error('Unrecognized field');})():[{campaign:{id:'42',...(live?{brandGuidelinesEnabled:true}:{})}}];throw Error('Unexpected query '+q);},
      mutateAll:async(o,opt)=>{sent.push({ops:clone(o),validateOnly:!!(opt.validateOnly||opt.ctrl.dryRun)});if(!opt.validateOnly&&!opt.ctrl.dryRun)opt.onDispatch();return {mutateOperationResponses:o.map(()=>({assetGroupAssetResult:{resourceName:'customers/123/assetGroupAssets/1'}}))};}});
    let result=null,error=null;try{result=await e.E.applyApproval('r1',{dryRun,maxDailyBudgetTotal:100});}catch(x){error=x;}
    return {result,error,sent,doc:mem.docs.get('Brites_GAds_Approvals/r1')};
  };
  let p=await publish({payload:brandPayload,live:true});
  check(p.result&&p.result.status==='VALIDATED'&&p.sent.length===1&&p.sent[0].validateOnly&&!linksOf(p.sent[0].ops)('BUSINESS_NAME').length&&!linksOf(p.sent[0].ops)('LOGO').length,'dry run: the brand-guidelines refresh is validated by Google without business name or logo links');
  p=await publish({payload:brandPayload,live:true,dryRun:false});
  check(p.result&&p.result.status==='APPLIED'&&p.sent.length===1&&!p.sent[0].validateOnly&&p.doc.status==='APPLIED','live: the reviewed refresh publishes once when the setting still matches');
  p=await publish({payload:plainPayload,live:true});
  check(p.error&&/different brand guidelines setting/.test(p.error.message)&&!p.sent.length&&p.doc.status==='APPROVED','a refresh prepared before the campaign moved to brand guidelines is refused before Google sees it');
  const legacy=clone(plainPayload);delete legacy.meta.brandGuidelinesEnabled;
  p=await publish({payload:legacy,live:true});
  check(p.error&&/different brand guidelines setting/.test(p.error.message)&&!p.sent.length,'an older refresh without the setting is refused when it would link a business name to a brand-guidelines campaign');
  p=await publish({payload:legacy,live:false});
  check(p.result&&p.sent.length===1,'an older refresh still publishes to a campaign without brand guidelines');
  p=await publish({payload:plainPayload,live:'unreadable'});
  check(p.result&&p.sent.length===1&&p.sent[0].validateOnly,'an unreadable live setting leaves the decision to Google validation');
  p=await publish({payload:{...clone(plainPayload),meta:{...plainPayload.meta,existingCampaignId:'42'}},live:true,tag:'something-else'});
  check(p.result&&p.sent.length===1,'the guard only applies to creative refresh drafts');
  console.log('PASS '+n+' brand-guidelines creative refresh checks');
  require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
