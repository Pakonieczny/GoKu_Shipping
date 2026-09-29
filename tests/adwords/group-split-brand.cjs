// A product split in a brand-guidelines Performance Max campaign never links the business name or logo to its
// new asset groups (Google keeps them on the campaign and rejects them there), from the draft to the published operations.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {createGroupsService}=require('../../netlify/functions/googleAdsGroups');
let n=0;const check=(v,m)=>{assert.ok(v,m);n++;};const clone=v=>JSON.parse(JSON.stringify(v));
const filename=path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js'),realRequire=require('node:module').createRequire(filename);
const ctx=vm.createContext({module:{exports:{}},exports:{},require:realRequire,process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,URLSearchParams,setTimeout,clearTimeout});
vm.runInContext(fs.readFileSync(filename,'utf8')+'\nmodule.exports.brandTest={putCopy:_putCreativeCopy,materialize:materializeReviewedCreative,capture:_captureCampaignEditableSnapshot,guard:_guardProductGroupSplit,set:v=>{if(v.gaql)gaql=v.gaql;if(v.load)_loadCreativeAsset=v.load;if(v.reviewed)assertCreativeReviewed=v.reviewed;if(v.version)_guardCampaignVersion=v.version;}};',ctx);
const engine=ctx.module.exports,T=engine.brandTest;
const ref='customers/123/assetGroups/7',other='customers/123/assetGroups/8';
const products=[{id:'gid://shopify/Product/10',title:'Duck necklace',url:'https://britesjewelry.com/products/duck',handle:'duck',eligibleGroupRefs:[ref],offerIds:['shopify_US_10_100'],images:[]},{id:'gid://shopify/Product/20',title:'Fox necklace',url:'https://britesjewelry.com/products/fox',handle:'fox',eligibleGroupRefs:[ref],offerIds:['shopify_US_20_200'],images:[]}];
const filters=products.map(p=>({assetGroup:ref,type:'UNIT_INCLUDED',caseValue:{productItemId:{value:p.offerIds[0]}}}));
const snapshotWith=brand=>({complete:true,campaignId:'42',channel:'PERFORMANCE_MAX',...(brand===undefined?{}:{brandGuidelinesEnabled:brand}),components:{assetGroups:[{resourceName:ref,name:'Shared necklace',finalUrls:['https://britesjewelry.com/collections/necklaces']}],assetLinks:[{assetGroup:ref,fieldType:'HEADLINE',text:'Shared headline'}],listingGroups:filters}});
const campaign={id:'42',name:'Necklaces',status:'ENABLED'};
let draft=null,queries=[],brandAnswer=()=>[];
const gaql=async q=>{queries.push(q);
  if(q.includes('campaign.brand_guidelines_enabled FROM campaign'))return brandAnswer();
  if(q.includes('FROM asset_group_signal'))return [{assetGroupSignal:{searchTheme:{text:'necklace gifts'}}}];
  if(q.includes('metrics.cost_micros'))return [];
  if(q.includes('FROM asset_group_listing_group_filter'))return filters.map(f=>({assetGroupListingGroupFilter:f}));
  if(q.includes('FROM ad_group_ad')||q.includes('FROM ad_group '))return [];
  if(q.includes('FROM asset_group '))return [ref,other].map((r,i)=>({campaign,assetGroup:{resourceName:r,name:i?'Other':'Shared necklace',status:'ENABLED',primaryStatus:'ELIGIBLE',finalUrls:['https://britesjewelry.com/collections/necklaces']}}));
  throw Error('Unexpected query '+q);};
const service=snapshot=>createGroupsService({CID:'123',reportContext:async()=>({budgetCurrency:'CAD',accountToday:'2026-09-11',accountTimezone:'America/Toronto'}),validatedRange:i=>({start:i.start||'2026-08-13',end:i.end||'2026-09-11'}),verifiedBasis:async()=>({version:3,snapshotHash:'a'.repeat(64),snapshot}),loadContext:async()=>({products}),buildPmax:engine.buildPmaxCampaignOps,enqueueApproval:async item=>{draft=clone(item);return 'split-fixture';},gaql});
const prepare=async snapshot=>{draft=null;queries=[];await service(snapshot).draftSplit({campaignId:'42',groupRef:ref,expectedVersion:3,snapshotHash:'a'.repeat(64)});return draft;};
const links=(ops,field)=>ops.filter(o=>o.assetGroupAssetOperation?.create?.fieldType===field);
const groupsOf=ops=>ops.filter(o=>o.assetGroupOperation).map(o=>o.assetGroupOperation.create.resourceName);
const unlinked=ops=>{const used=JSON.stringify(ops.filter(o=>!o.assetOperation));return ops.filter(o=>o.assetOperation?.create&&!used.includes(JSON.stringify(o.assetOperation.create.resourceName)));};
const textAssets=ops=>ops.filter(o=>o.assetOperation?.create?.textAsset).length;
(async()=>{
  // 1. Draft: the verified snapshot says brand guidelines are on (compared with the same split without them).
  const plainDraft=await prepare(snapshotWith(false));
  let d=await prepare(snapshotWith(true));const brandOps=d.payload.mutateOperations,made=groupsOf(brandOps);
  check(made.length===2&&!links(brandOps,'BUSINESS_NAME').length&&!links(brandOps,'LOGO').length,'brand guidelines: no asset-group business name or logo link in the split draft');
  check(!unlinked(brandOps).length&&textAssets(brandOps)===textAssets(plainDraft.payload.mutateOperations)-2,'brand guidelines: the unused business-name assets are not created either');
  check(made.every(g=>links(brandOps,'HEADLINE').filter(o=>o.assetGroupAssetOperation.create.assetGroup===g).length>=3&&links(brandOps,'LONG_HEADLINE').some(o=>o.assetGroupAssetOperation.create.assetGroup===g)&&links(brandOps,'DESCRIPTION').filter(o=>o.assetGroupAssetOperation.create.assetGroup===g).length>=2),'brand guidelines: each new group keeps its own headlines and descriptions');
  check(d.payload.meta.brandGuidelinesEnabled===true&&!queries.some(q=>q.includes('brand_guidelines_enabled')),'the draft records the setting read from the verified snapshot without another Google read');
  // 2. Brand guidelines off: unchanged group-level business name.
  d=await prepare(snapshotWith(false));const plainOps=d.payload.mutateOperations;
  check(groupsOf(plainOps).every(g=>links(plainOps,'BUSINESS_NAME').filter(o=>o.assetGroupAssetOperation.create.assetGroup===g).length===1)&&d.payload.meta.brandGuidelinesEnabled===false&&!unlinked(plainOps).length,'without brand guidelines each new group links one business name as before');
  // 3. An older snapshot without the setting: read it from Google once (false booleans are omitted).
  brandAnswer=()=>[{campaign:{brandGuidelinesEnabled:true}}];d=await prepare(snapshotWith(undefined));
  check(!links(d.payload.mutateOperations,'BUSINESS_NAME').length&&d.payload.meta.brandGuidelinesEnabled===true&&queries.filter(q=>q.includes('brand_guidelines_enabled')).length===1,'a snapshot without the setting reads it from Google before drafting');
  brandAnswer=()=>[{campaign:{id:'42'}}];d=await prepare(snapshotWith(undefined));
  check(links(d.payload.mutateOperations,'BUSINESS_NAME').length===2&&d.payload.meta.brandGuidelinesEnabled===false,'an omitted Google boolean means brand guidelines are off');
  brandAnswer=()=>{throw Error('Field not selectable');};
  await assert.rejects(()=>prepare(snapshotWith(undefined)),/brand guidelines.*not prepared/i);check(draft===null,'an unknown setting prepares nothing rather than a split Google would refuse');
  // 4. Creative review: new copy for the reviewed groups keeps the business name off brand-guidelines groups.
  const copy={headlines:['Duck necklace','Gift a duck','Brites duck'],longHeadlines:['A duck necklace made to order'],descriptions:['Handmade duck necklace.','See the listing for options.']};
  const reviewed=ops=>groupsOf(ops).map((g,i)=>({key:'g'+i,ref:g,name:'Group '+i,channel:'pmax',copy}));
  const brandPayload=clone((await prepare(snapshotWith(true))).payload);T.putCopy(brandPayload,reviewed(brandPayload.mutateOperations));
  check(!links(brandPayload.mutateOperations,'BUSINESS_NAME').length&&!unlinked(brandPayload.mutateOperations).length&&links(brandPayload.mutateOperations,'HEADLINE').length===6,'reviewed copy replaces the text without adding a group business name');
  const plainPayload=clone((await prepare(snapshotWith(false))).payload);T.putCopy(plainPayload,reviewed(plainPayload.mutateOperations));
  check(links(plainPayload.mutateOperations,'BUSINESS_NAME').length===2&&!unlinked(plainPayload.mutateOperations).length,'reviewed copy keeps one business name per group without brand guidelines');
  // 5. Publication: reviewed images are linked, the logo only where Google accepts it.
  T.set({reviewed:()=>{},load:async()=>Buffer.from('image')});
  const image=h=>({hash:h.padEnd(24,'0'),width:1200,height:1200,bytes:5});
  const creative=payload=>({schema:1,phase:'ready',logo:image('logo'),groups:groupsOf(payload.mutateOperations).map((g,i)=>({key:'g'+i,ref:g,channel:'pmax',assets:{square:image('sq'+i),landscape:image('la'+i),portrait:image('po'+i)},review:{pass:true}}))});
  let ops=await T.materialize({type:'pmax',status:'APPROVED',payload:brandPayload,creative:creative(brandPayload)});
  check(!links(ops,'LOGO').length&&!links(ops,'BUSINESS_NAME').length&&links(ops,'SQUARE_MARKETING_IMAGE').length===2&&!unlinked(ops).length,'brand guidelines: published operations carry images but no logo asset or link');
  ops=await T.materialize({type:'pmax',status:'APPROVED',payload:plainPayload,creative:creative(plainPayload)});
  check(links(ops,'LOGO').length===2&&links(ops,'BUSINESS_NAME').length===2&&!unlinked(ops).length,'without brand guidelines each group still gets the reviewed logo');
  // 6. Apply guard: a draft prepared for the other setting is refused before Google sees it.
  const item=payload=>({payload,status:'APPROVED'});
  T.set({version:async()=>({snapshot:snapshotWith(true)})});
  await assert.rejects(()=>T.guard(item(plainPayload)),/different brand guidelines setting/);n++;
  await T.guard(item(brandPayload));n++;
  // 7. The editable snapshot records the setting for Performance Max only, outside its fingerprint.
  const {snapshotHash}=realRequire('./googleAdsVersionSnapshots');let answer=[{campaign:{id:'42',status:'ENABLED',advertisingChannelType:'PERFORMANCE_MAX',brandGuidelinesEnabled:true}}];
  T.set({gaql:async q=>{if(q.includes('FROM campaign WHERE'))return answer;return [];}});
  const on=await T.capture('42');answer=[{campaign:{id:'42',status:'ENABLED',advertisingChannelType:'PERFORMANCE_MAX'}}];const off=await T.capture('42');
  check(on.brandGuidelinesEnabled===true&&off.brandGuidelinesEnabled===false&&snapshotHash(on)===snapshotHash(off),'the snapshot records brand guidelines without changing the settings fingerprint');
  T.set({gaql:async q=>{if(q.includes('brand_guidelines_enabled'))throw Error('Unrecognized field');if(q.includes('FROM campaign WHERE'))return answer;return [];}});
  check((await T.capture('42')).brandGuidelinesEnabled===null,'an unreadable setting is recorded as unknown, and the snapshot is still captured');
  console.log('PASS '+n+' brand-guidelines product split checks');
  require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
