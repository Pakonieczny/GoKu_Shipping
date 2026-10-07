'use strict';
// Exact titles, handles and links reproduce an observed published comparison.
// Variant ids, variant options and all other catalogue rows are synthetic;
// these tests make no live stock, price, audio or provider-session claim.
const test=require('node:test'),assert=require('node:assert/strict');
const core=require('../../netlify/functions/_britesGrowth.js');
const NOW=Date.parse('2026-10-07T18:30:00Z');
const named='Compare both Rhino Profile Stud Earrings and Butterfly Cutout Stud Earrings';
function fixture(){
  const product=(id,handle,title,price=54,type='Earrings')=>({id:'gid://shopify/Product/'+id,handle,title,type,url:'https://britesjewelry.com/products/'+handle,currency:'USD',description:'Synthetic product details.',tags:[],options:[],images:[],checkedAt:NOW,variantsComplete:true,variants:[{id:'gid://shopify/ProductVariant/'+id,numericId:String(id),title:'Sterling Silver',sku:'SYNTHETIC_PRIVATE_SKU',price,available:true,options:[{name:'Metal',value:'Sterling Silver'},...(type==='Necklace'?[{name:'Chain length',value:'18 inches'}]:[])]}]});
  const products=[product('9216010584227','rhino-profile-stud-earrings','Rhino Profile Stud Earrings',51),product('9216010223779','butterfly-cutout-stud-earrings-4','Butterfly Cutout Stud Earrings',48),product('33301','fox-hunter-stud-earrings','Fox Hunter Stud Earrings'),product('33302','rhino-charm-stud-earrings','Rhino Charm Stud Earrings'),product('33303','sea-otter-charm-stud-earrings-1','Sea Otter Charm Stud Earrings'),product('33304','microphone-singer-stud-earrings-1','Microphone Singer Stud Earrings')];
  const calls={handles:[],queries:[],saved:[],research:[],recall:0},issues=[];
  const service={saveProducts:async rows=>{calls.saved.push(rows.map(row=>row.handle));},productIssues:async()=>issues,research:async ids=>{calls.research.push(ids);return [];},storySupplements:async()=>[],catalogueCandidateHandles:async()=>{calls.recall++;return [];}};
  const shopify={byHandle:async handle=>{calls.handles.push(handle);return products.find(product=>product.handle===handle)||null;},search:async query=>{calls.queries.push(query);return {products};},products:async()=>({products})};
  const context={currentHandle:'rhino-profile-stud-earrings',productHandles:products.map(product=>product.handle),currency:'USD'};
  return {products,calls,issues,service,shopify,context,now:()=>NOW,product};
}
const identities=result=>result.products.map(product=>product.id);
const expected=['gid://shopify/Product/9216010584227','gid://shopify/Product/9216010223779'];
async function run(f,extra={}){return core.concierge({...f,message:named,...extra});}
function clarified(result){assert.equal(result.products.length,0);assert.equal(result.comparisonClarification,true);assert.equal(result.preserveSelection,true);assert.equal(result.requestedAction,undefined);assert.deepEqual(result.actions,[]);assert.ok(result.question);}

test('the observed full named pair keeps both exact current live identities, not only the Butterfly motif',async()=>{
  const f=fixture(),result=await run(f);
  assert.deepEqual(identities(result),expected);assert.equal(result.question,null);assert.equal(result.comparisonClarification,undefined);assert.equal(result.requestedAction,undefined);
  assert.deepEqual(f.calls.handles,f.context.productHandles);assert.deepEqual(f.calls.queries,[]);assert.deepEqual(f.calls.research,[expected]);assert.equal(f.calls.recall,0);
  assert.deepEqual(result.products.map(product=>product.url),['https://britesjewelry.com/products/rhino-profile-stud-earrings','https://britesjewelry.com/products/butterfly-cutout-stud-earrings-4']);
  assert.doesNotMatch(JSON.stringify(result),/SYNTHETIC_PRIVATE_SKU|competitor|credential/);
});
test('named selection ignores a saved motif and category without losing the saved budget and metal limits',async()=>{
  const f=fixture(),preferences=core.intentFrom('Moon silver necklace under 60 for my sister');
  const result=await run(f,{preferences});assert.deepEqual(identities(result),expected);assert.equal(result.preferences.query,'moon');assert.equal(result.preferences.type,'necklace');assert.equal(result.preferences.budget,60);assert.equal(result.preferences.metal,'silver');assert.equal(result.preferences.recipient,'sister');
});
test('a complete named comparison searches each explicit name when no current card set is available',async()=>{
  const f=fixture(),result=await run(f,{context:{currency:'USD'}});
  assert.deepEqual(new Set(identities(result)),new Set(expected));assert.deepEqual(f.calls.handles,[]);assert.deepEqual(f.calls.queries,['rhino profile stud earrings','butterfly cutout stud earrings']);assert.equal(f.calls.recall,0);
});
test('duplicate global search hits cannot consume the rank cap or replace any of six exact named identities',async()=>{
  const f=fixture(),message='Compare '+f.products.map(product=>product.title).join(' and '),result=await run(f,{message,context:{currency:'USD'}});
  assert.deepEqual(new Set(identities(result)),new Set(f.products.map(product=>product.id)));assert.equal(result.products.length,6);assert.equal(result.question,null);assert.equal(f.calls.queries.length,6);
});
test('a reversed request preserves the current visible ordering and omits the other four cards',async()=>{
  const f=fixture(),result=await run(f,{message:'Compare Butterfly Cutout Stud Earrings with Rhino Profile Stud Earrings'});assert.deepEqual(identities(result),expected);
});
for(const message of ['Rhino Profile Stud Earrings versus Butterfly Cutout Stud Earrings','Comparison between Rhino Profile Stud Earrings and Butterfly Cutout Stud Earrings','Please compare rhino-profile-stud-earrings and butterfly-cutout-stud-earrings-4'])test('an explicit bounded comparison syntax resolves the same exact names: '+message,async()=>{
  const f=fixture(),result=await run(f,{message});assert.deepEqual(identities(result),expected);assert.equal(result.question,null);
});
for(const message of ['Compare Rhino Profile Stud Earrings','Compare Rhino Profile Stud Earrings and Rhino Profile Stud Earrings'])test('one distinct named identity asks for another instead of selecting a substitute: '+message,async()=>{
  const result=await run(fixture(),{message});clarified(result);assert.match(result.reply,/two distinct/);
});
for(const message of ['Compare Rhino Profile Stud Earrings and Retired Dragon Stud Earrings','Compare Missing Lion Earrings and Retired Dragon Stud Earrings','Compare Rhino Profile Stud Earrings and Butterfly Cutout Stud Earrings and Retired Dragon Stud Earrings'])test('a missing explicitly named operand cannot become an unrelated displayed choice: '+message,async()=>{
  const f=fixture(),result=await run(f,{message});clarified(result);assert.match(result.reply,/couldn’t confirm every named piece/);assert.equal(f.calls.recall,0);
});
test('a title renamed in the freshly rechecked catalogue is not accepted as the old title',async()=>{
  const f=fixture();f.products[1].title='Updated Butterfly Stud Earrings';clarified(await run(f));
});
test('a duplicate complete title belonging to distinct current products asks for exact identity',async()=>{
  const f=fixture();f.products[2].title=f.products[0].title;const result=await run(f);clarified(result);assert.match(result.reply,/unique live match/);
});
test('a short contained title cannot override the longer full identity',async()=>{
  const f=fixture();f.products[2].title='Stud Earrings';assert.deepEqual(identities(await run(f)),expected);
});
test('a complete title containing and remains one named identity',async()=>{
  const f=fixture();f.products[0].title='Rock and Roll Stud Earrings';const result=await run(f,{message:'Compare Rock and Roll Stud Earrings and Butterfly Cutout Stud Earrings'});assert.deepEqual(identities(result),expected);
});
for(const mutate of [f=>{f.products[1].variants[0].available=false;},f=>{f.products[1].checkedAt=NOW-6*60*1000;},f=>{f.products[1].checkedAt=NOW+2*60*1000;},f=>{f.products[1].id='fabricated';},f=>{f.products[1].url='https://competitor.invalid/products/butterfly-cutout-stud-earrings-4';},f=>{f.products[1].url='https://britesjewelry.com/products/a-different-piece';}])test('exact names cannot bypass stock, freshness, valid identity or exact own-product URL eligibility: '+mutate.toString(),async()=>{
  const f=fixture();mutate(f);clarified(await run(f));
});
test('reviewed recommendation hold applies to each exact named product',async()=>{
  const f=fixture();f.issues.push({productId:f.products[1].id,issues:[{kind:'identity',status:'open',blocks:['recommendation','cart']}]});clarified(await run(f));
});
for(const preferences of [core.intentFrom('Keep each item under 50'),core.intentFrom('Gold pieces only under 60'),core.intentFrom('No butterfly pieces under 60')])test('saved exclusions and available variant limits are retained for exact names: '+JSON.stringify(preferences),async()=>{
  clarified(await run(fixture(),{preferences}));
});
test('an explicit budget outside the titles still applies to the exact comparison',async()=>{
  const f=fixture();clarified(await run(f,{message:named+' under $50'}));
});
test('unapplied foreign currency budget is disclosed without inventing converted prices',async()=>{
  const f=fixture(),preferences=core.intentFrom('Under 60 CAD');const result=await run(f,{preferences});assert.deepEqual(identities(result),expected);assert.equal(result.currencyMismatch,true);assert.match(result.reply,/haven’t applied your CAD budget/);assert.equal(result.products.every(product=>product.budgetApplied===false),true);
});
test('a named comparison never creates navigation or cart authority from combined phrasing',async()=>{
  for(const message of [named+' and open the first one',named+' and add the second to my cart',named+'; do not open either']){const result=await run(fixture(),{message});assert.equal(result.requestedAction,undefined);}
});
test('user text resembling an internal reference token cannot supply another identity',async()=>{
  const f=fixture();clarified(await run(f,{message:'Compare Rhino Profile Stud Earrings and safecomparisonref0'}));
});
for(const message of ['Compare these pieces','Compare the first and second gift options','Compare this exact piece with the second one','Compare https://britesjewelry.com/products/rhino-profile-stud-earrings with the first one'])test('ordinary displayed and ordinal comparisons keep their prior live selection behavior: '+message,async()=>{
  const f=fixture(),result=await run(f,{message});assert.equal(result.comparisonClarification,undefined);assert.deepEqual(identities(result),f.products.map(product=>product.id));assert.equal(result.requestedAction,undefined);
});
test('ordinary Butterfly motif discovery still returns the matching live item only',async()=>{
  const f=fixture(),result=await run(f,{message:'Find butterfly earrings',context:{currency:'USD'}});assert.deepEqual(identities(result),[expected[1]]);assert.deepEqual(f.calls.queries,['butterfly']);assert.equal(result.comparisonClarification,undefined);
});
