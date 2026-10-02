'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {pathToFileURL}=require('node:url'),{JSDOM}=require('jsdom');
const review=require('../../netlify/functions/_britesGrowthCorrectionReview'),readerModule=require('../../netlify/functions/_britesGrowthCorrectionRead');
const epoch=Date.parse('2026-10-02T06:00:00Z'),clone=v=>JSON.parse(JSON.stringify(v));
function fixture(productId='gid://shopify/Product/999001',handle='fixture-charm'){
  const product={id:productId,handle,title:'Fixture Charm',descriptionHtml:'<p>Wrong motif, 14–18 inches.</p>',productType:'Charm',updatedAt:'2026-10-02T05:59:00Z',status:'ACTIVE',onlineStoreUrl:'https://britesjewelry.com/products/'+handle,category:null};
  const variants=[{id:'gid://shopify/ProductVariant/999011',selectedOptions:[{name:'Metal',value:'Silver'},{name:'Size',value:'8.5mm'}],price:'35.00',availableForSale:true},{id:'gid://shopify/ProductVariant/999012',selectedOptions:[{name:'Metal',value:'Gold'},{name:'Size',value:'11mm'}],price:'75.00',availableForSale:true}];
  const dossier={productId,handle,status:'approved',version:review.hash('fixture dossier'),sources:[{id:'own-offer',url:product.onlineStoreUrl,reviewed:true,checkedAt:epoch-1000}]};
  const issueRecord={productId,updatedAt:epoch-500,issues:[{id:'fixture-copy',kind:'content',status:'open',detail:'Fixture copy contradicts the exact current motif.',blocks:['recommendation','meaning'],evidence:[{url:product.onlineStoreUrl,text:'Wrong motif'}]}]};
  const proposal={dossierVersion:dossier.version,expectedTitle:product.title,expectedDescriptionHtmlSha256:review.hash(product.descriptionHtml),expectedProductType:product.productType,sourceIds:['own-offer'],reviewedIssueIds:['fixture-copy'],issueRecordUpdatedAt:issueRecord.updatedAt,snippetPatches:[{field:'descriptionHtml',expectedExactSnippet:'Wrong motif',expectedOccurrences:1,replacementExactSnippet:'Correct motif'}],draftProductType:null,expectedVariantIdSetSha256:review.variantIdSetHash(variants),expectedOptionLabels:review.optionLabels(variants)};
  const read={state:'available',product,variants,collections:[],memberships:[],coverage:{variants:'complete',media:'not_required',collections:'not_required',memberships:'not_required',consistent:true},runtime:{requestedApiVersion:'2026-07',servedApiVersion:'2026-07',shopId:'gid://shopify/Shop/999',shopDomain:'fixture.myshopify.com',compatible:true}};
  return {productId,handle,proposal,dossier,issueRecord,issueState:'available',read,now:epoch};
}
const preview=f=>review.createCorrectionPreview(f);
test('exact current preview preserves raw content and independent holds without an apply permission',()=>{
  const f=fixture(),out=preview(f);assert.equal(out.state,'ready_for_review');assert.equal(out.canApply,false);assert.equal(out.executionCompatibility,'not_established');assert.equal(out.after.descriptionHtml,'<p>Correct motif, 14–18 inches.</p>');assert.equal(out.after.title,f.read.product.title);assert.equal(out.holds.recommendationHold,true);assert.equal(out.holds.cartHold,false);assert.match(out.warnings.join(' '),/not provider-atomic/);
});
test('foreign ID, changed handle and unapproved dossier fail exact binding',()=>{
  for(const change of [f=>{f.read.product.id='gid://shopify/Product/999002';},f=>{f.read.product.handle='different';}]){const f=fixture();change(f);assert.equal(preview(f).state,'identity_conflict');}
  const f=fixture();f.dossier.status='draft';assert.equal(preview(f).state,'dossier_conflict');assert.equal(review.normalizeExactProductId('gid://shopify/Order/99'),null);assert.equal(review.normalizeExactProductId('999001'),f.productId);
});
test('source IDs must resolve to reviewed exact own-product sources',()=>{
  for(const patch of [{url:'https://other.invalid/products/fixture-charm'},{url:'https://britesjewelry.com/products/other'},{reviewed:false}]){const f=fixture();Object.assign(f.dossier.sources[0],patch);assert.equal(preview(f).state,'dossier_conflict');}
  const f=fixture();f.proposal.sourceIds=['unknown'];assert.equal(preview(f).state,'dossier_conflict');
});
test('current issue version, identity and active IDs are required; no resolution is inferred',()=>{
  for(const change of [f=>{f.issueRecord.updatedAt++;},f=>{f.issueRecord.issues[0].status='resolved';},f=>{f.issueState='unavailable';},f=>{f.issueRecord.productId='gid://shopify/Product/99';}]){const f=fixture();change(f);assert.equal(preview(f).state,'issue_conflict');}
  const f=fixture();f.proposal.issueRecordHash=review.hash(f.issueRecord);f.issueRecord.issues[0].detail='Changed';assert.equal(preview(f).state,'issue_conflict');assert.equal(fixture().issueRecord.issues[0].status,'open');
});
test('title, raw HTML and productType drift block description-only proposals',()=>{
  for(const key of ['title','descriptionHtml','productType']){const f=fixture();f.read.product[key]+=' ';assert.equal(preview(f).state,'baseline_conflict');}
  const f=fixture();f.read.product.descriptionHtml=f.read.product.descriptionHtml.replace('–','-');assert.equal(preview(f).state,'baseline_conflict');
});
test('incomplete raw fields, unpublished products and different live source URLs cannot be current baselines',()=>{
  const f=fixture();delete f.read.product.descriptionHtml;assert.equal(preview(f).state,'baseline_conflict');
  for(const change of [f=>{f.read.product.status='DRAFT';},f=>{f.read.product.onlineStoreUrl='https://britesjewelry.com/products/other';}]){const f=fixture();change(f);assert.equal(preview(f).state,'identity_conflict');}
});
test('zero or repeated snippet matches are conflicts even with matching body hash',()=>{
  for(const html of ['<p>No matching phrase</p>','<p>Wrong motif / Wrong motif</p>']){const f=fixture();f.read.product.descriptionHtml=html;f.proposal.expectedDescriptionHtmlSha256=review.hash(html);assert.equal(preview(f).state,'baseline_conflict');assert.equal(preview(f).after.descriptionHtml,html);}
});
test('complete variant IDs/options are bound; absent option combinations are never synthesized',()=>{
  const f=fixture(),out=preview(f);assert.equal(out.variants.length,2);assert.ok(!out.variants.some(v=>v.selectedOptions.some(o=>o.value==='Gold')&&v.selectedOptions.some(o=>o.value==='8.5mm')));
  f.read.variants[1].selectedOptions[1].value='8.5mm';assert.equal(preview(f).state,'baseline_conflict');f.read.coverage.variants='incomplete';assert.equal(preview(f).state,'baseline_conflict');
});
test('updatedAt is an audit marker and inventory prices are distinct from option structure',()=>{
  const f=fixture(),before=preview(f);f.read.product.updatedAt='2026-10-02T06:00:00Z';f.read.variants[0].price='36.00';f.previousBinding=before.binding;assert.equal(preview(f).state,'ready_for_review');
  f.read.coverage.consistent=false;assert.equal(preview(f).state,'baseline_conflict');
});
test('no mutation is invented for a configuration-only correction',()=>{
  const f=fixture();f.proposal.snippetPatches=[];assert.equal(preview(f).state,'configuration_unverified');assert.equal(preview(f).canApply,false);
});
test('type changes surface rules and unknown sources without inventing collection membership',()=>{
  const f=fixture();f.proposal.draftProductType='Necklace';f.read.coverage.collections=f.read.coverage.memberships='complete';f.read.collections=[{id:'gid://shopify/Collection/999',title:'Fixture necklaces',ruleSet:{appliedDisjunctively:true,rules:[{column:'TYPE',relation:'EQUALS',condition:'Necklace'}]},sources:[{id:'source-999',__typename:'FixtureCollectionSource',title:'Source'}]}];const out=preview(f);assert.equal(out.state,'collection_review_required');assert.equal(out.collectionImpact.affectedRules[0].result,'unknown');assert.deepEqual(out.collectionImpact.knownEntries,[]);assert.ok(out.collectionImpact.unknownSources.length);
  f.read.collections=[{id:'gid://shopify/Collection/999',ruleSet:null,sources:[]}];assert.equal(preview(f).collectionImpact.state,'unknown');
});
test('recheck binds content, issues, dossier, shop, version and variant structure',()=>{
  const f=fixture(),before=preview(f);f.previousBinding=clone(before.binding);f.previousBinding.contentHash=review.hash('different');assert.equal(preview(f).state,'baseline_conflict');f.previousBinding=clone(before.binding);f.read.runtime.compatible=false;assert.equal(preview(f).state,'runtime_unavailable');
});
test('forbidden fields and unsafe replacement payloads are rejected',()=>{
  for(const change of [f=>{f.proposal.query='mutation { forbidden }';},f=>{f.proposal.snippetPatches[0].field='title';},f=>{f.proposal.snippetPatches[0].replacementExactSnippet='<script>alert(1)</script>';}]){const f=fixture();change(f);assert.throws(()=>preview(f));}
});
const readEnv={SHOPIFY_STORE:'fixture.myshopify.com',SHOPIFY_CLIENT_ID:'fixture-client',SHOPIFY_CLIENT_SECRET:'fixture-client-secret-not-real'};
function transport(f,opts={}){
  const calls=[];let contentReads=0;
  const fetch=async(url,request)=>{
    calls.push({url,body:request.body});
    if(url.endsWith('/oauth/access_token'))return {ok:true,json:async()=>({access_token:'synthetic-token',expires_in:3600})};
    const body=JSON.parse(request.body),name=/query Correction(\w+)/.exec(body.query)[1];assert.ok(!/\bmutation\b/.test(body.query));let data;
    if(opts.error)throw Error('PRIVATE_PROVIDER_EXCEPTION');
    if(name==='Runtime')data={currentAppInstallation:{accessScopes:[{handle:opts.noScope?'read_orders':'read_products'}]},shop:{id:'gid://shopify/Shop/999',myshopifyDomain:'fixture.myshopify.com',plan:{partnerDevelopment:false}}};
    if(name==='Content'){contentReads++;data={product:{id:f.productId,title:f.read.product.title,descriptionHtml:f.read.product.descriptionHtml,productType:f.read.product.productType,updatedAt:opts.drift&&contentReads>1?'2026-10-02T06:00:00Z':f.read.product.updatedAt}};}
    if(name==='Identity')data={product:{id:f.productId,handle:f.handle,status:'ACTIVE',onlineStoreUrl:f.read.product.onlineStoreUrl,category:null}};
    if(name==='RuleContext')data={product:{id:f.productId,tags:[],vendor:'Fixture'}};
    if(name==='Variants'){const second=!!body.variables.after;data={product:{id:f.productId,variants:{nodes:opts.twoPages?[f.read.variants[second?1:0]]:f.read.variants,pageInfo:{hasNextPage:opts.twoPages&&!second,endCursor:opts.repeatCursor?'same':second?'end':'page-1'}}}};if(opts.missingPageInfo)delete data.product.variants.pageInfo;if(opts.duplicateVariant&&second)data.product.variants.nodes=[f.read.variants[0]];}
    if(name==='Collections'){const second=!!body.variables.after;const nodes=opts.manyCollections?Array.from({length:second?1:250},(_,i)=>({id:'gid://shopify/Collection/'+(second?999999:i+1),title:'Fixture collection',handle:'collection-'+i,ruleSet:null,sources:[]})):[];data={collections:{nodes,pageInfo:{hasNextPage:opts.manyCollections&&!second,endCursor:second?'end':'collections-1'}}};}
    if(name==='Membership')data={product:{id:f.productId,collections:{nodes:[],pageInfo:{hasNextPage:false,endCursor:null}}}};
    return {ok:true,headers:new Headers(opts.missingVersion?{}:{'X-Shopify-API-Version':opts.servedVersion||'2026-07'}),json:async()=>({data})};
  };return {fetch,calls};
}
test('reader follows all variant/collection pages and verifies actual served version with zero mutations',async()=>{
  const f=fixture(),t=transport(f,{twoPages:true,manyCollections:true}),reader=readerModule.createCorrectionReader({env:readEnv,fetch:t.fetch,now:()=>epoch});const out=await reader.read(f.productId,{collections:true});assert.equal(out.state,'available');assert.equal(out.variants.length,2);assert.equal(out.collections.length,251);assert.equal(out.coverage.consistent,true);assert.equal(out.runtime.servedApiVersion,'2026-07');assert.equal(out.runtime.compatible,true);assert.ok(t.calls.every(c=>c.url.endsWith('/oauth/access_token')||!/mutation/.test(JSON.parse(c.body).query)));assert.ok(!JSON.stringify(out).includes('synthetic-token'));assert.equal(reader.capabilities().canApply,false);
});
test('reader refuses absent/different response API version and missing actual product scopes',async()=>{
  for(const options of [{servedVersion:'2026-10'},{missingVersion:true},{noScope:true}]){const f=fixture(),t=transport(f,options),out=await readerModule.createCorrectionReader({env:readEnv,fetch:t.fetch,now:()=>epoch}).read(f.productId);assert.equal(out.state,'unavailable');assert.equal(out.runtime.compatible,false);}
});
test('reader marks incomplete/duplicate pagination and unstable reads unavailable or inconsistent',async()=>{
  for(const options of [{missingPageInfo:true},{twoPages:true,duplicateVariant:true}]){const f=fixture(),t=transport(f,options),out=await readerModule.createCorrectionReader({env:readEnv,fetch:t.fetch,now:()=>epoch}).read(f.productId);assert.equal(out.state,'unavailable');assert.notEqual(out.coverage.variants,'complete');}
  const f=fixture(),t=transport(f,{drift:true}),out=await readerModule.createCorrectionReader({env:readEnv,fetch:t.fetch,now:()=>epoch}).read(f.productId);assert.equal(out.coverage.consistent,false);
});
test('reader keeps absent/redacted credentials and unsupported configured versions explicit without transport',async()=>{
  let count=0;const fetch=async()=>{count++;throw Error('unexpected');};
  for(const env of [{},{...readEnv,SHOPIFY_CLIENT_SECRET:'***REDACTED***'},{...readEnv,BRITES_GROWTH_CORRECTION_READ_VERSION:'2025-10'}]){const out=await readerModule.createCorrectionReader({env,fetch}).read(fixture().productId);assert.equal(out.state,'unavailable');}assert.equal(count,0);
});
test('bounded pagination cannot be relabelled complete when the next variant page was not read',async()=>{
  const f=fixture(),t=transport(f,{twoPages:true});const out=await readerModule.createCorrectionReader({env:readEnv,fetch:t.fetch,now:()=>epoch,maxPages:1}).read(f.productId);assert.equal(out.state,'unavailable');assert.notEqual(out.coverage.variants,'complete');
});
test('provider exceptions are not copied into the response',async()=>{
  const f=fixture(),t=transport(f,{error:true}),out=await readerModule.createCorrectionReader({env:readEnv,fetch:t.fetch}).read(f.productId);assert.equal(out.state,'unavailable');assert.ok(!JSON.stringify(out).includes('PRIVATE_PROVIDER'));
});
async function endpoint(){
  const filename=path.resolve(__dirname,'../../netlify/functions/britesGrowthCorrections.js');let source=fs.readFileSync(filename,'utf8');
  source=source.replace(/from '(\.\/[^']+)'/g,(_,name)=>'from '+JSON.stringify(pathToFileURL(path.resolve(path.dirname(filename),name)).href));return import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
}
function apiFixture(createHandler,f=fixture(),over={}){
  let reads=0,authReads=0;const env={BRITES_GROWTH_ADMIN_KEY:'synthetic-owner-key',BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',...over.env};
  const handle=createHandler({environment:()=>env,now:()=>epoch,savedPasscode:async()=>{authReads++;return over.passcode||null;},researchService:()=>({research:async()=>[f.dossier],productIssues:async()=>[f.issueRecord]}),reader:()=>({capabilities:()=>({mode:'sandbox_review',canApply:false}),read:async()=>{reads++;return f.read;}})});
  const call=async(body,options={})=>{const method=options.method||'POST',r=await handle(new Request('https://sandbox.invalid/api/growth-corrections'+(method==='GET'?'?action=capabilities':''),{method,headers:{...(options.key===null?{}:{'X-Growth-Key':options.key||'synthetic-owner-key'}),...(options.origin?{Origin:options.origin}:{})},...(method==='POST'?{body:JSON.stringify(body)}:{})}));return {status:r.status,data:await r.json()};};return {call,counts:()=>({reads,authReads})};
}
test('protected sandbox endpoint admits typed preview/recheck only; forbidden actions never reach providers',async()=>{
  const {createHandler}=await endpoint(),f=fixture(),a=apiFixture(createHandler,f);for(const action of ['apply','gqlProxy','updateTitle'])assert.equal((await a.call({action})).status,403);assert.equal(a.counts().reads,0);
  const first=await a.call({action:'preview',productId:f.productId,handle:f.handle,proposal:f.proposal});assert.equal(first.status,200);assert.equal(first.data.state,'ready_for_review');assert.equal(first.data.canApply,false);const second=await a.call({action:'verifyBaseline',productId:f.productId,handle:f.handle,proposal:f.proposal,previewBinding:first.data.binding});assert.equal(second.data.state,'ready_for_review');assert.equal(a.counts().reads,2);
});
test('missing auth, production namespace, foreign origin, untyped body and stale version fail before provider reads',async()=>{
  const {createHandler}=await endpoint(),f=fixture(),a=apiFixture(createHandler,f);assert.equal((await a.call({action:'capabilities'},{key:null})).status,401);assert.equal((await a.call({action:'capabilities'},{origin:'https://foreign.invalid'})).status,403);assert.equal((await a.call({action:'preview',query:'mutation {}'})).status,400);
  const b=apiFixture(createHandler,f,{env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'}});assert.equal((await b.call({action:'capabilities'})).status,503);
  const proposal={...f.proposal,dossierVersion:review.hash('stale')};assert.equal((await a.call({action:'preview',productId:f.productId,handle:f.handle,proposal})).data.state,'dossier_conflict');assert.equal(a.counts().reads,0);
});
test('existing owner credential fallback is read only and capabilities never fetch product data',async()=>{
  const {createHandler}=await endpoint(),a=apiFixture(createHandler,fixture(),{env:{BRITES_GROWTH_ADMIN_KEY:''},passcode:'synthetic-existing-passcode'});assert.equal((await a.call({action:'capabilities'},{key:'synthetic-existing-passcode'})).status,200);assert.equal(a.counts().authReads,1);assert.equal(a.counts().reads,0);const b=apiFixture(createHandler,fixture(),{env:{BRITES_GROWTH_ADMIN_KEY:''}});assert.equal((await b.call({action:'capabilities'})).status,401);
});
function browser(){const dom=new JSDOM('<main id="fixture"></main>',{url:'https://sandbox.invalid/',runScripts:'outside-only'});vm.runInNewContext(fs.readFileSync(path.resolve(__dirname,'../../brites-growth-corrections.js'),'utf8'),{window:dom.window,document:dom.window.document,URL,AbortController});return {dom,api:dom.window.BritesGrowthCorrections,container:dom.window.document.getElementById('fixture')};}
function privatePacket(f){return {productId:f.productId,handle:f.handle,title:f.proposal.expectedTitle,dossierVersion:f.dossier.version,sourceBinding:{productId:f.productId,handle:f.handle,dossierVersion:f.dossier.version,ownProductSourceIds:f.proposal.sourceIds},openIssues:f.issueRecord.issues,reviewedIssueRecordUpdatedAt:f.issueRecord.updatedAt,currentOfferEvidence:{variantsComplete:true,variantIdSetSha256:f.proposal.expectedVariantIdSetSha256,optionLabels:f.proposal.expectedOptionLabels},listingRepairDraft:{expectedProductId:f.productId,expectedHandle:f.handle,expectedDescriptionHtmlSha256:f.proposal.expectedDescriptionHtmlSha256,expectedProductType:f.proposal.expectedProductType,snippetPatches:f.proposal.snippetPatches,draftProductType:f.proposal.draftProductType,requestDrafts:[{query:'mutation MustNeverDispatch { forbidden }'}]}};}
test('operator-imported packet translates exact bindings while ignoring old mutation request drafts',async()=>{
  const f=fixture(),b=browser();let sent;const ui=b.api.mount(b.container,{productId:f.productId,handle:f.handle,dossier:f.dossier,request:async body=>{sent=body;return preview(f);}});await ui.review(privatePacket(f));assert.equal(sent.action,'preview');assert.ok(!JSON.stringify(sent).includes('MustNeverDispatch'));assert.match(b.container.textContent,/Promotion held · Cart not held/);assert.ok(![...b.container.querySelectorAll('button')].some(e=>/^Apply/.test(e.textContent)));ui.destroy();b.dom.window.close();
});
test('raw HTML and provider exception text cannot execute or leak through review UI',async()=>{
  const f=fixture(),b=browser(),out=preview(f);out.baseline.descriptionHtml='<img src=x onerror="secret()"><script>danger()</script>';out.after.descriptionHtml='safe';const ui=b.api.mount(b.container,{productId:f.productId,handle:f.handle,request:async()=>out});await ui.review(privatePacket(f));assert.equal(b.container.querySelectorAll('script,img,iframe').length,0);assert.match(b.container.textContent,/<script>/);ui.destroy();const fail=b.api.mount(b.container,{productId:f.productId,handle:f.handle,request:async()=>{throw Error('PRIVATE_EXCEPTION_TOKEN');}});await fail.review(privatePacket(f));assert.ok(!b.container.textContent.includes('PRIVATE_EXCEPTION'));assert.match(b.container.textContent,/no write was attempted/);fail.destroy();b.dom.window.close();
});
test('destroying a previous selected product prevents its delayed response from rendering',async()=>{
  const f=fixture(),g=fixture('gid://shopify/Product/999002','second-fixture'),b=browser();let resolve;const first=b.api.mount(b.container,{productId:f.productId,handle:f.handle,request:()=>new Promise(r=>{resolve=r;})});const pending=first.review(privatePacket(f));first.destroy();const second=b.api.mount(b.container,{productId:g.productId,handle:g.handle,request:async()=>preview(g)});await second.review(privatePacket(g));resolve(preview(f));await pending;assert.match(b.container.textContent,/second-fixture/);assert.ok(!b.container.textContent.includes('999001'));second.destroy();b.dom.window.close();
});
test('UI recheck sends prior binding and refuses a non-read-only or wrong-product response',async()=>{
  const f=fixture(),b=browser();const calls=[];const ui=b.api.mount(b.container,{productId:f.productId,handle:f.handle,request:async body=>{calls.push(body);return preview(f);}});await ui.review(privatePacket(f));await ui.recheck();assert.equal(calls[1].action,'verifyBaseline');assert.ok(calls[1].previewBinding.contentHash);assert.throws(()=>b.api.assertReviewResponse({...preview(f),canApply:true},f.productId,f.handle));assert.throws(()=>b.api.packetToProposal(privatePacket(f),'gid://shopify/Product/99',f.handle));ui.destroy();b.dom.window.close();
});
test('unavailable current issue reads are labelled unknown rather than unheld',async()=>{
  const f=fixture(),b=browser();f.issueState='unavailable';const ui=b.api.mount(b.container,{productId:f.productId,handle:f.handle,request:async()=>preview(f)});await ui.review(privatePacket(f));assert.match(b.container.textContent,/Current holds are unavailable/);assert.ok(!b.container.textContent.includes('Promotion not held'));ui.destroy();b.dom.window.close();
});
