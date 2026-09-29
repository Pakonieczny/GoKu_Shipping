// Every new Performance Max campaign targets English, and every asset group carries its own logo and business
// name (brand guidelines are off, so Google requires both on each group and refuses later edits without them).
// The Improve job (creative refresh) adds a missing logo or business name first. Offline: no Google call.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const file=path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js'),realRequire=require('node:module').createRequire(file);
const sharp=realRequire('sharp'),structure=realRequire('./_googleAdsPmaxStructure'),{orderAssetGroupMutations}=realRequire('./googleAdsAdDesign');
const clone=v=>v==null?v:JSON.parse(JSON.stringify(v));let n=0;const check=(v,m)=>{assert.ok(v,m);n++;};
function engine(){
  const context=vm.createContext({module:{exports:{}},exports:{},require:x=>x==='node-fetch'?async()=>{throw Error('Live network forbidden');}:realRequire(x),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,URLSearchParams,setTimeout,clearTimeout});
  vm.runInContext(fs.readFileSync(file,'utf8')+'\nmodule.exports.__req={resolve:_resolveBrandLogo,materialize:materializeReviewedCreative,putCopy:_putCreativeCopy,svg:_brandWordmarkSvg,placeholder:BRAND_LOGO_DATA};',context);
  return {E:context.module.exports,T:context.module.exports.__req,bind(values){context.__m=values;vm.runInContext(Object.keys(values).map(k=>k+'=__m.'+k).join('\n'),context);}};
}
function memory(){
  const docs=new Map();
  const doc=p=>({id:p.split('/').pop(),path:p,get:async()=>({exists:docs.has(p),data:()=>clone(docs.get(p))}),set:async v=>docs.set(p,clone(v)),update:async v=>docs.set(p,{...(docs.get(p)||{}),...clone(v)}),collection:x=>collection(p+'/'+x)});
  const collection=p=>({where(){return this;},limit(){return this;},get:async()=>({docs:[]}),add:async v=>{const id='auto'+docs.size;docs.set(p+'/'+id,clone(v));return {id};},doc:x=>doc(p+'/'+x)});
  return {docs,db:{collection,runTransaction:async fn=>fn({get:r=>r.get(),set:(r,v)=>r.set(v),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})},FV:{serverTimestamp:()=>Date.now()}};
}
const creates=(ops,key)=>ops.filter(o=>o[key]&&o[key].create).map(o=>o[key].create);
const links=(ops,field,group)=>creates(ops,'assetGroupAssetOperation').filter(c=>c.fieldType===field&&(!group||c.assetGroup===group));
const groupsOf=ops=>creates(ops,'assetGroupOperation').map(g=>g.resourceName);
const imageCreates=ops=>creates(ops,'assetOperation').filter(c=>c.imageAsset);
const unlinked=ops=>{const used=JSON.stringify(ops.filter(o=>!o.assetOperation));return creates(ops,'assetOperation').filter(c=>!used.includes(JSON.stringify(c.resourceName)));};
const blockOf=(ops,group)=>ops.filter(o=>{const a=o.assetGroupAssetOperation;if(!a)return false;if(a.create)return a.create.assetGroup===group;return String(a.remove||'').startsWith(group.replace('/assetGroups/','/assetGroupAssets/')+'~');});
const contiguous=(ops,group)=>{const idx=ops.map((o,i)=>blockOf([o],group).length?i:-1).filter(i=>i>=0);return idx.length&&idx[idx.length-1]-idx[0]+1===idx.length;};
const coll={handle:'animal-necklaces',title:'Animal necklaces'};
const offers=[['shopify_US_11_101','Corgi necklace'],['shopify_US_22_201','Fox necklace'],['shopify_US_33_301','Owl necklace']];
const build=(e,extra={})=>e.E.buildPmaxCampaignOps(coll,{dailyBudget:10,merchantId:'555',feedLabel:'US',countries:['2840','2124'],itemIds:offers.map(o=>o[0]),offerDetails:offers.map(([itemId,title])=>({itemId,title})),...extra});
(async()=>{
  const e=engine(),P=e.T.placeholder;
  // 1. Language: one English criterion on the new campaign, after its locations.
  const one=build(e,{itemIds:[offers[0][0]]}),crit=creates(one.ops,'campaignCriterionOperation'),lang=crit.filter(c=>c.language);
  check(lang.length===1&&lang[0].language.languageConstant==='languageConstants/1000'&&lang[0].campaign==='customers/123/campaigns/-2'&&!lang[0].negative,'the new PMax campaign targets English (languageConstants/1000), the language of its copy');
  check(crit.findIndex(c=>c.language)>crit.map(c=>!!c.location).lastIndexOf(true)&&crit.filter(c=>c.location).length===2,'the English criterion follows the location criteria');
  check(JSON.stringify(one.languages)==='["English"]','the build reports its language for the draft');
  // 2. Logo and business name: each group links its own, one shared official-logo asset per campaign.
  const many=build(e),groups=groupsOf(many.ops);
  check(groups.length===3&&groups.every(g=>links(many.ops,'LOGO',g).length===1&&links(many.ops,'BUSINESS_NAME',g).length===1),'every asset group links exactly one logo and one business name');
  const logoAssets=imageCreates(many.ops);
  check(logoAssets.length===1&&logoAssets[0].imageAsset.data===P&&groups.every(g=>links(many.ops,'LOGO',g)[0].asset===logoAssets[0].resourceName),'one official Brites logo asset is created for the campaign and linked to each group');
  const temp=creates(many.ops,'assetOperation').map(c=>c.resourceName);
  check(new Set(temp).size===temp.length&&/\/assets\/-\d+$/.test(logoAssets[0].resourceName)&&Number(logoAssets[0].resourceName.split('/').pop())<=-10000,'the logo asset has its own temporary ID, clear of every other asset');
  check(!unlinked(many.ops).length,'no asset is created without a link');
  const supplied=build(e,{itemIds:[offers[0][0]],imageAssets:{logo:'customers/123/assets/-900001',square:[],landscape:[],portrait:[]}});
  check(links(supplied.ops,'LOGO')[0].asset==='customers/123/assets/-900001'&&!imageCreates(supplied.ops).length,'a supplied logo is linked instead, without a placeholder asset');
  // Joining an existing campaign keeps the group-level logo (brand guidelines off) and never adds campaign criteria.
  const joined=structure.intoExistingCampaign(one.ops,{customerId:'123',campaignId:'77',brandGuidelinesEnabled:false});
  check(!joined.some(o=>o.campaignCriterionOperation)&&links(joined,'LOGO').length===1&&links(joined,'BUSINESS_NAME').length===1&&!unlinked(joined).length,'a group joining an existing campaign keeps its logo and business name, and leaves the campaign language as it is');
  const joinedBrand=structure.intoExistingCampaign(one.ops,{customerId:'123',campaignId:'77',brandGuidelinesEnabled:true});
  check(!links(joinedBrand,'LOGO').length&&!links(joinedBrand,'BUSINESS_NAME').length&&!imageCreates(joinedBrand).length&&!unlinked(joinedBrand).length,'with brand guidelines the joining group links neither, and the logo asset is not created');
  // 3. Publication renders the placeholder into the official logo, exactly as a reviewed logo, once per request.
  const official=await sharp(Buffer.from(e.T.svg())).jpeg({quality:95}).toBuffer(),data=official.toString('base64'),name='Brites reviewed 1024x1024 '+e.E.creativeHash(data).slice(0,20);
  let ops=await e.T.resolve(clone(many.ops));const img=imageCreates(ops);
  check(img.length===1&&img[0].imageAsset.data===data&&img[0].name===name&&!JSON.stringify(ops).includes(P),'the placeholder becomes the official logo JPEG under the reviewed-logo name, so Google keeps one copy');
  const meta=await sharp(Buffer.from(img[0].imageAsset.data,'base64')).metadata();
  check(meta.width===1024&&meta.height===1024&&meta.format==='jpeg','the logo meets Google\'s square logo size');
  check(groups.every(g=>links(ops,'LOGO',g).length===1&&links(ops,'LOGO',g)[0].asset===img[0].resourceName)&&ops.length===many.ops.length,'each group still links the one logo; nothing else changes');
  const splitLike=[...clone(one.ops),...JSON.parse(JSON.stringify(one.ops).replace(/-(\d+)/g,(m,d)=>'-'+(Number(d)+100000)))].filter(o=>!o.campaignOperation&&!o.campaignBudgetOperation&&!o.campaignCriterionOperation);
  ops=await e.T.resolve(splitLike);
  check(imageCreates(ops).length===1&&links(ops,'LOGO').length===2&&new Set(links(ops,'LOGO').map(l=>l.asset)).size===1,'two placeholders in one request become one logo asset linked to both groups');
  const withReviewed=[{assetOperation:{create:{resourceName:'customers/123/assets/-950001',name,imageAsset:{data}}}},...clone(one.ops)];
  ops=await e.T.resolve(withReviewed);
  check(imageCreates(ops).length===1&&links(ops,'LOGO')[0].asset==='customers/123/assets/-950001','an identical reviewed logo already in the request is reused');
  const unused=clone(one.ops).filter(o=>!(o.assetGroupAssetOperation&&o.assetGroupAssetOperation.create.fieldType==='LOGO'));
  ops=await e.T.resolve(unused);
  check(!imageCreates(ops).length&&ops.length===unused.length-1,'a placeholder no link uses is not created');
  // The reviewed creative replaces it: a reviewed PMax draft publishes the reviewed logo on every group.
  e.bind({assertCreativeReviewed:()=>{},_loadCreativeAsset:async()=>Buffer.from('image')});
  const image=h=>({hash:h.padEnd(24,'0'),width:1200,height:1200,bytes:5});
  const reviewed={type:'pmax',status:'APPROVED',payload:{mutateOperations:clone(many.ops),meta:{}},creative:{schema:1,phase:'ready',logo:image('logo'),groups:groups.map((ref,i)=>({key:'g'+i,ref,channel:'pmax',assets:{square:image('sq'+i),landscape:image('la'+i),portrait:image('po'+i)},review:{pass:true}}))}};
  ops=await e.T.materialize(reviewed);
  check(groups.every(g=>links(ops,'LOGO',g).length===1&&links(ops,'BUSINESS_NAME',g).length===1)&&!JSON.stringify(ops).includes(P)&&!unlinked(ops).length&&imageCreates(ops).length===1+3*groups.length,'a reviewed draft publishes one reviewed logo per group and drops the placeholder');
  // A placeholder can never reach Google.
  let tokens=0;e.bind({mintToken:async()=>{tokens++;return 't';}});
  await assert.rejects(()=>e.E.mutateAll(clone(many.ops),{ctrl:{dryRun:true}}),/Brites logo for this draft was not prepared/);
  check(tokens===0,'mutate refuses an unrendered logo placeholder before any Google request');
  // 4. Ordering: a logo or business name the group lacks goes first in its contiguous block.
  const g='customers/123/assetGroups/7',rm=(id,f)=>({assetGroupAssetOperation:{remove:`customers/123/assetGroupAssets/7~${id}~${f}`}}),add=(a,f)=>({assetGroupAssetOperation:{create:{assetGroup:g,asset:'customers/123/assets/'+a,fieldType:f}}}),asset=a=>({assetOperation:{create:{resourceName:'customers/123/assets/'+a,textAsset:{text:'t'+a}}}});
  const refresh=[rm(1,'HEADLINE'),rm(2,'HEADLINE'),rm(3,'SQUARE_MARKETING_IMAGE'),{campaignOperation:{update:{resourceName:'customers/123/campaigns/42'},updateMask:'asset_automation_settings'}},asset(-1),asset(-2),add(-1,'HEADLINE'),add(-2,'HEADLINE'),add(-3,'BUSINESS_NAME'),add(-4,'SQUARE_MARKETING_IMAGE'),add(-5,'LOGO')];
  let ordered=orderAssetGroupMutations(refresh),block=blockOf(ordered,g);
  check(block[0].assetGroupAssetOperation.create.fieldType==='BUSINESS_NAME'&&block[1].assetGroupAssetOperation.create.fieldType==='LOGO'&&contiguous(ordered,g)&&ordered.length===refresh.length,'a group missing its business name and logo gets both first, in one contiguous block');
  check(JSON.stringify(block.slice(2))===JSON.stringify(refresh.filter(o=>o.assetGroupAssetOperation&&!['BUSINESS_NAME','LOGO'].includes(o.assetGroupAssetOperation.create&&o.assetGroupAssetOperation.create.fieldType))),'every other change keeps its approved order');
  ordered=orderAssetGroupMutations([rm(9,'LOGO'),...refresh]);block=blockOf(ordered,g);
  check(block[0].assetGroupAssetOperation.create.fieldType==='BUSINESS_NAME'&&block[1].assetGroupAssetOperation.remove&&block[1].assetGroupAssetOperation.remove.endsWith('~LOGO'),'a logo that replaces the current one stays beside its removal');
  ordered=orderAssetGroupMutations(clone(many.ops));
  check(groups.every(ref=>contiguous(ordered,ref)&&['LOGO','BUSINESS_NAME'].includes(blockOf(ordered,ref)[0].assetGroupAssetOperation.create.fieldType)&&['LOGO','BUSINESS_NAME'].includes(blockOf(ordered,ref)[1].assetGroupAssetOperation.create.fieldType))&&ordered.length===many.ops.length,'a new campaign keeps one block per group with its logo and business name first');
  // 5. The Improve job (creative refresh) of a group without them: drafted, reviewed and published with both first.
  const campaign={id:'42',resourceName:'customers/123/campaigns/42',name:'Necklaces'},existing=['HEADLINE','HEADLINE','HEADLINE','LONG_HEADLINE','DESCRIPTION','DESCRIPTION','SQUARE_MARKETING_IMAGE'];
  const refreshDraft=async(fields,brand=false)=>{const f=engine(),queries=[];let queued=null;
    f.bind({fb:()=>memory(),_productShotsByIds:async ids=>ids.map(id=>({id:'gid://shopify/Product/'+id,title:'Duck necklace',handle:'duck',shots:[{url:'https://cdn.shopify.com/duck.jpg'}]})),enqueueApproval:async item=>{queued=clone(item);return 'refresh-1';},
      gaql:async q=>{queries.push(q);
        if(q.includes('FROM campaign '))return [{campaign:{...campaign,...(brand?{brandGuidelinesEnabled:true}:{})}}];
        if(q.includes('FROM asset_group_asset'))return fields.map((field,i)=>({assetGroupAsset:{resourceName:`customers/123/assetGroupAssets/7~${900+i}~${field}`,fieldType:field},asset:/HEADLINE|DESCRIPTION/.test(field)?{textAsset:{text:field.toLowerCase()+' '+i}}:{}}));
        if(q.includes('FROM asset_group_signal'))return [];
        if(q.includes('FROM asset_group_listing_group_filter'))return [{assetGroupListingGroupFilter:{type:'UNIT_INCLUDED',caseValue:{productItemId:{value:'shopify_US_10_100'}}}}];
        if(q.includes('FROM asset_group '))return [{assetGroup:{id:'7',resourceName:g,name:'Necklaces',finalUrls:['https://britesjewelry.com/products/duck']}}];
        throw Error('Unexpected query '+q);}});
    await f.E.upgradePmaxAdStrength({campaignIds:['42']});return {queued,queries};};
  let r=await refreshDraft(existing);
  check(JSON.stringify(r.queued.payload.meta.addsRequired)==='["LOGO","BUSINESS_NAME"]','a group without a logo and business name is drafted to receive both');
  check(r.queries.some(q=>q.includes('FROM asset_group_asset')&&q.includes("asset_group_asset.status != 'REMOVED'")),'removed links are not read as current assets or removed again');
  check(!(await refreshDraft([...existing,'LOGO','BUSINESS_NAME'])).queued.payload.meta.addsRequired,'a complete group needs nothing added first');
  check(!(await refreshDraft(existing,true)).queued.payload.meta.addsRequired,'brand guidelines keep both on the campaign, so nothing is added to the group');
  const payload=clone(r.queued.payload),copy={headlines:['Duck necklace','Gift a duck','Brites duck'],longHeadlines:['A duck necklace made to order'],descriptions:['Handmade duck necklace.','See the listing for options.']};
  e.T.putCopy(payload,[{key:'g0',ref:g,name:'Necklaces',channel:'pmax',copy}]);
  const mem=memory(),sent=[],p=engine();mem.docs.set('Brites_GAds_Approvals/r1',{type:'creative',tag:r.queued.tag,status:'APPROVED',payload,creative:{schema:1,phase:'ready',logo:image('logo'),groups:[{key:'g0',ref:g,name:'Necklaces',channel:'pmax',assets:{square:image('sq'),landscape:image('la'),portrait:image('po')},review:{pass:true}}]}});
  p.bind({fb:()=>mem,assertCreativeReviewed:()=>{},_loadCreativeAsset:async()=>Buffer.from('image'),
    gaql:async q=>{if(q.includes('campaign.brand_guidelines_enabled FROM campaign WHERE campaign.id = 42'))return [{campaign:{id:'42'}}];throw Error('Unexpected query '+q);},
    mutateAll:async(o,opt)=>{sent.push({ops:clone(o),validateOnly:!!(opt.validateOnly||opt.ctrl.dryRun)});return {mutateOperationResponses:[]};}});
  const result=await p.E.applyApproval('r1',{dryRun:true,maxDailyBudgetTotal:100});block=blockOf(sent[0].ops,g);
  check(result.status==='VALIDATED'&&sent.length===1&&sent[0].validateOnly,'the refresh is checked by Google in dry run');
  check(['BUSINESS_NAME','LOGO'].every(f=>block.slice(0,2).some(o=>o.assetGroupAssetOperation.create&&o.assetGroupAssetOperation.create.fieldType===f))&&contiguous(sent[0].ops,g)&&links(sent[0].ops,'LOGO',g).length===1&&links(sent[0].ops,'BUSINESS_NAME',g).length===1,'the Improve job adds the missing business name and logo first, then replaces the rest');
  // 6. Approvals says so plainly.
  const html=fs.readFileSync(path.resolve(__dirname,'../../brites-adwords.html'),'utf8'),m=/^function apAddsFirst\(.*$/m.exec(html);
  const ui=vm.createContext({esc:s=>String(s)});vm.runInContext(m[0],ui);
  const rows=ui.apAddsFirst({addsRequired:['LOGO','BUSINESS_NAME']});
  check(rows.length===1&&rows[0][0]==='Adds first'&&/Brites logo and business name this group is missing\. Google requires both before it accepts any other change\./.test(rows[0][1])&&!ui.apAddsFirst({}).length,'the refresh card names what is added first and why');
  console.log('PASS '+n+' PMax language, logo and business-name checks');
  require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
