const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const R=require('../../netlify/functions/googleAdsCampaignStyles');
for(const styles of [[],['pmax','pmax'],['search']])assert.throws(()=>R.selection(styles,{pmax:10},['2840']));
assert.equal(R.selection(R.STYLES,{fixed_display:1.1,responsive_display:2.2,pmax:3.3},['2840']).totalDaily,6.6);
assert.throws(()=>R.selection(['pmax'],{pmax:NaN},['2840']));
assert.throws(()=>R.selection(['pmax'],{pmax:10},[]));
assert.equal(R.fixedProofs([{width:2048,height:2048,asset:{}},{width:300,height:250,asset:{}},{width:300,height:250,asset:{}}]).length,1);
assert.throws(()=>R.validatePhoto({width:1080,height:1920,bytes:100},'portrait'));
assert.doesNotThrow(()=>R.validatePhoto({width:1200,height:300,bytes:100},'landscape_logo'));assert.throws(()=>R.validatePhoto({width:600,height:300,bytes:100},'landscape_logo'));
const fixture=path.join(__dirname,'design-publication.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{')[0];
const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,console,Buffer,Date,URL,setTimeout,clearTimeout});
vm.runInContext(source+'\nthis.factory=engine;this.memoryFactory=memory;',ctx);
(async()=>{
 const E=ctx.factory(),f=ctx.memoryFactory(),root=f.db.collection('workspaces').doc('test'),sharp=require('sharp');
 const assets={};for(const [shape,width,height] of [['square',600,600],['landscape',1200,628],['portrait',600,750]]){const bytes=await sharp({create:{width,height,channels:3,background:'#ddd'}}).jpeg().toBuffer(),p='Brites_GAds_Creative/test/'+shape+'.jpg';assets[shape]={path:p,width,height,bytes:bytes.length,hash:E.E.creativeHash(bytes.toString('base64'))};f.files.set(p,bytes);}
 const copy={headlines:['Peach Charm','For Your Favorite Foodie','A Playful Gift'],longHeadlines:['Give a playful peach charm'],descriptions:['Shop the peach charm at Brites Jewelry.','Choose your favorite metal.']},w={context:{itemIds:['shopify_US_1_2'],handle:'charms',feedLabel:'US'},settings:{productId:'1',groupRef:'g'},job:{result:{assets}}};await root.set(w);
 const item={sourceHash:'source',designReview:{workspaceId:'test',copy,layoutReview:{jobId:'eai_test',reviewVersion:11}}};
 await root.collection('editorAIJobs').doc('eai_test').collection('data').doc('ad_proofs_v11').set({images:[{width:300,height:250,asset:assets.square},{width:2048,height:2048,asset:assets.square}]});
 E.bind({fb:()=>f,_adDesignWorkspaceRef:()=>root,_reportContext:async()=>({budgetCurrency:'CAD'}),merchantCenterId:async()=>123,_saveCreativeAsset:async(ws,bytes,name,meta)=>{const p='Brites_GAds_Creative/test/'+name+'.jpg';f.files.set(p,bytes);return {...meta,path:p,bytes:bytes.length,hash:E.E.creativeHash(bytes.toString('base64'))};}});
 const plan=await E.get('_prepareCampaignStyles')({item,context:{w,product:{id:'1',title:'Peach Charm',url:'https://britesjewelry.com/products/peach',offerIds:['shopify_US_1_2']}},choice:R.selection(R.STYLES,{fixed_display:5,responsive_display:5,pmax:10},['2840']),identity:'a'.repeat(64)});
 assert.equal(plan.summary.totalDaily,20);assert.equal(plan.summary.campaigns.length,3);
 const media=Object.fromEntries(plan.summary.campaigns.map(c=>[c.style,c.media]));
 assert.equal(media.fixed_display.images.length,1);assert.equal(media.fixed_display.copy.headlines.length,0);assert.equal(media.fixed_display.videos.length,0);
 assert.equal(media.responsive_display.images.length,4,'two photos plus the two real logos');assert.equal(media.responsive_display.copy.headlines.length,3);assert.equal(media.responsive_display.callToAction,'Shop now');assert.equal(media.responsive_display.images.some(i=>i.role==='PORTRAIT_MARKETING_IMAGE'),false);
 assert.equal(media.pmax.images.length,5,'three photos plus two real logos');assert.equal(media.pmax.callToAction,'SHOP_NOW');assert.equal(media.pmax.copy.headlines.length,3);
 for(const lane of Object.values(media))for(const image of lane.images)assert(plan.payload.generatedAssets.some(g=>g.tempResourceName===image.resource),'every preview refers to an actual uploaded resource');
 const originalJob=w.job,owners=[];w.job=null;item.designReview.publicationImages=Object.entries(assets).map(([format,asset])=>({format,asset,productIds:['1']}));item.designReview.layoutReview.workspaceId='original-package';E.bind({_adDesignWorkspaceRef:id=>{owners.push(id);return root;}});
 const recoveredPlan=await E.get('_prepareCampaignStyles')({item,context:{w,product:{id:'1',title:'Peach Charm',url:'https://britesjewelry.com/products/peach',offerIds:['shopify_US_1_2']}},choice:R.selection(['pmax','fixed_display'],{pmax:10,fixed_display:5},['2840']),identity:'b'.repeat(64)});
 assert(owners.includes('original-package'),'fixed layouts come from the package’s original review workspace');for(const asset of Object.values(assets))assert(recoveredPlan.payload.generatedAssets.some(g=>g.asset.hash===asset.hash),'pipeline uploads the frozen saved-package photos when the legacy job is absent');w.job=originalJob;delete item.designReview.publicationImages;delete item.designReview.layoutReview.workspaceId;
 const approval={type:'adDesignSubmission',payload:plan.payload,pipelinePlan:plan,pipelineReview:{hash:plan.hash}};
 E.E.assertCreativeReviewed(approval);assert.throws(()=>E.E.assertCreativeReviewed({...approval,pipelineReview:{hash:'stale'}}));
 const ops=await E.get('materializeReviewedCreative')(approval),campaigns=ops.filter(o=>o.campaignOperation).map(o=>o.campaignOperation.create);
 assert.equal(campaigns.length,3);assert(campaigns.every(c=>c.status==='PAUSED'));assert.equal(new Set(campaigns.map(c=>c.resourceName)).size,3);
 const rda=ops.find(o=>o.adGroupAdOperation?.create.ad.responsiveDisplayAd).adGroupAdOperation.create.ad.responsiveDisplayAd;
 assert.equal(rda.marketingImages.length,1);assert.equal(rda.squareMarketingImages.length,1);assert(!rda.portraitMarketingImages);assert.equal(rda.controlSpec.enableAutogenVideo,false);
 // Google supports Ad.name only for image/upload/video/Demand Gen ads; flexible colour keeps native placements eligible.
 assert.equal(ops.find(o=>o.adGroupAdOperation?.create.ad.responsiveDisplayAd).adGroupAdOperation.create.ad.name,undefined);assert.equal(rda.allowFlexibleColor,true);assert(ops.find(o=>o.adGroupAdOperation?.create.ad.imageAd).adGroupAdOperation.create.ad.name);
 // Google's optional 4:1 logo reaches wide placements in both responsive styles, built from the official wide wordmark.
 const wide=plan.payload.generatedAssets.find(a=>a.asset.width===1200&&a.asset.height===300);assert(wide&&wide.asset.bytes<=5120*1024);assert.equal(rda.logoImages.length,1);assert.equal(rda.logoImages[0].asset,wide.tempResourceName);assert.equal(rda.squareLogoImages.length,1);
 assert.equal(ops.filter(o=>o.assetGroupAssetOperation?.create.fieldType==='LANDSCAPE_LOGO'&&o.assetGroupAssetOperation.create.asset===wide.tempResourceName).length,1);
 // The plan states each campaign's bidding exactly as built.
 const bidding=Object.fromEntries(plan.summary.campaigns.map(c=>[c.style,c.bidding]));assert.equal(bidding.pmax,'Maximize conversion value · no target ROAS until it has about 6 weeks and 30 conversions in 30 days');assert.equal(bidding.fixed_display,'Maximize conversions');assert.equal(bidding.responsive_display,'Maximize conversions');
 // A run length is kept per campaign and counts from the day it is enabled (applyApproval and setCampaignStatus move the end date).
 const timed=await E.get('_prepareCampaignStyles')({item,context:{w,product:{id:'1',title:'Peach Charm',url:'https://britesjewelry.com/products/peach',offerIds:['shopify_US_1_2']}},choice:R.selection(['pmax'],{pmax:10},['2840'],{pmax:30}),identity:'b'.repeat(64)}),timedCampaign=timed.payload.mutateOperations.find(o=>o.campaignOperation).campaignOperation.create;
 assert.equal(timed.payload.meta.plannedDays[timedCampaign.resourceName],30);assert.equal(timed.summary.campaigns[0].days,30);assert(!plan.payload.meta.plannedDays&&plan.summary.campaigns.every(c=>c.days===null));
 assert('audienceSignal' in plan.summary.campaigns.find(c=>c.style==='pmax'));assert(/may make one from your images/.test(plan.summary.videoStatus));
 assert.equal(ops.filter(o=>o.adGroupAdOperation?.create.ad.imageAd).length,1);
 const refs=new Set(ops.map(o=>Object.values(o)[0]?.create?.resourceName).filter(Boolean));
 for(const match of JSON.stringify(ops).matchAll(/customers\/123\/(?:assets|campaigns|campaignBudgets|assetGroups|adGroups)\/-\d+/g))assert(refs.has(match[0]),'unresolved temporary reference '+match[0]);
 assert(plan.payload.generatedAssets.filter(a=>a.fixed).every(a=>a.asset.bytes<=150*1024&&a.asset.width===300&&a.asset.height===250));
 // Excluding saved films never reads/attaches a motion job; fixed layouts cannot silently use obsolete copy.
 E.bind({_latestMotionJob:async()=>{throw Error('Excluded videos must not be inspected for inclusion');}});
 const withoutFilms=await E.get('_prepareCampaignStyles')({item:{...item,designReview:{...item.designReview,includeVideos:false}},context:{w,product:{id:'1',title:'Peach Charm',url:'https://britesjewelry.com/products/peach',offerIds:['shopify_US_1_2']}},choice:R.selection(['pmax'],{pmax:10},['2840']),identity:'c'.repeat(64)});
 assert.match(withoutFilms.summary.videoStatus,/Saved videos are excluded/);assert(!withoutFilms.payload.meta.motion);assert(!withoutFilms.payload.mutateOperations.some(o=>o.assetGroupAssetOperation?.create.fieldType==='YOUTUBE_VIDEO'));
 await assert.rejects(()=>E.get('_prepareCampaignStyles')({item:{...item,designReview:{...item.designReview,copyEdited:true}},context:{w,product:{id:'1',title:'Peach Charm'}},choice:R.selection(['fixed_display'],{fixed_display:5},['2840']),identity:'d'.repeat(64)}),/edited messaging differs/);
 // The daily ceiling is refused while preparing, before any plan is built; a plan inside it proceeds.
 const id='design-review-'+'a'.repeat(32);let reviewHash='r'.repeat(64),prepared=0;
 await f.db.collection('Brites_GAds_Approvals').doc(id).set({type:'adDesignSubmission',status:'PENDING',reviewHash,sourceHash:E.get('_adDesignSelectionHash')(w),designReview:{workspaceId:'test',productId:'1',groupRef:'g',context:w.context,copy,publicationImages:Object.entries(assets).map(([format,asset])=>({format,asset,productIds:['1']}))}});
 E.bind({_adDesignPublicationContext:async()=>({ref:root,w,product:{id:'1',title:'Peach Charm',url:'https://britesjewelry.com/products/peach',offerIds:['shopify_US_1_2']}}),control:async()=>({maxDailyBudgetTotal:100,budgetCurrency:'CAD'}),_enabledBudgetTotal:async()=>28,_prepareCampaignStyles:async({identity})=>{prepared++;return {identity,hash:'plan',payload:{},summary:{campaigns:[]}};}});
 const ask=pmax=>E.E.publishAdDesignSubmission({id,hash:reviewHash,prepareOnly:true,styles:['pmax'],budgets:{pmax},countries:['2840'],durations:{pmax:42}});
 await assert.rejects(()=>ask(90),/Over your daily ceiling.*CAD 90\.00.*CAD 72\.00/);assert.equal(prepared,0);
 const firstPrepared=await ask(40);assert.equal(firstPrepared.planHash,'plan');assert(firstPrepared.refreshed);reviewHash=firstPrepared.reviewHash;assert.equal(prepared,1);await ask(40);assert.equal(prepared,1,'matching saved plan is reused');const reviewRef=f.db.collection('Brites_GAds_Approvals').doc(id);await reviewRef.update({reviewHash:'edited'});await E.E.publishAdDesignSubmission({id,hash:'edited',prepareOnly:true,styles:['pmax'],budgets:{pmax:40},countries:['2840'],durations:{pmax:42}});assert.equal(prepared,2,'copy review hash participates in plan identity');
 const beforeRefresh=(await reviewRef.get()).data();await reviewRef.update({sourceHash:'legacy-source',designReview:{...beforeRefresh.designReview,sourceMode:null}});w.messaging={copy,updatedAt:12345};await root.set(w);
 await assert.rejects(()=>E.E.publishAdDesignSubmission({id,hash:'edited',prepareOnly:true,styles:['pmax'],budgets:{pmax:90},countries:['2840'],durations:{pmax:42}}),/Over your daily ceiling/);assert.equal((await reviewRef.get()).data().reviewHash,'edited','failed plan leaves the existing review hash and package intact');
 const fresh=await E.E.publishAdDesignSubmission({id,hash:'edited',prepareOnly:true,styles:['pmax'],budgets:{pmax:40},countries:['2840'],durations:{pmax:42}}),savedFresh=(await reviewRef.get()).data();assert(fresh.refreshed);assert.notEqual(fresh.reviewHash,'edited');assert.equal(savedFresh.sourceHash,require('../../netlify/functions/googleAdsSubmissionReview').snapshotHash(savedFresh.designReview,E.E.creativeHash));assert.equal(savedFresh.status,'PENDING');assert.deepEqual(savedFresh.designReview.publicationImages,beforeRefresh.designReview.publicationImages);assert.deepEqual(savedFresh.designReview.copy,beforeRefresh.designReview.copy);assert.equal(savedFresh.submissionPreferences.budgets.pmax,40);
 await assert.rejects(()=>E.E.publishAdDesignSubmission({id,hash:'edited',confirmed:true,planHash:'plan'}),/review changed/);assert.equal((await reviewRef.get()).data().status,'PENDING','refresh never approves an ad');
 console.log('PASS campaign selection, budgets, all three routed builders, fixed-size filtering, disjoint references, frozen approval, image materialization, supported ad fields, the 4:1 brand logo, disclosed bidding and the ceiling checked at preparation');
 require('./suite-guard.cjs').done();
})();
