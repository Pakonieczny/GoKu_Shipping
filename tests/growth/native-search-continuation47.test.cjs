'use strict';
// Production voice/client/widget/bridge/host integration. Transcription events,
// media, provider packets, network and catalogue rows are synthetic. Physical
// microphone recognition, audible speech and live Shopify stock are not tested.
const test=require('node:test'),assert=require('node:assert/strict');
const {fixture,clone,settle,ANIMALS,LARGE_ANIMALS,FLOWERS,EARRINGS,SILVER_BUDGET,NO_BIRDS,ANY_MATERIAL_NO_BIRDS}=require('./native-search47-fixture.cjs');
const sorted=value=>[...value].sort();
const handles=products=>products.map(p=>p.handle);
const grid=f=>[...f.d.querySelectorAll('#demo-products .piece-card[data-product-handle]')].map(card=>card.dataset.productHandle);

function shopperReply(text){
  assert.equal(typeof text,'string');assert.ok(text.trim());
  assert.ok(text.trim().split(/\s+/).length<=65,'A default spoken search reply must stay concise: '+text);
  assert.doesNotMatch(text,/backend|adapter|authority|postcondition|completedActions|variantId|turnVersion|searchResults|searchCriteria|request[_ -]?id|schema\s*[:=]|no further action|whole Shopify|entire Shopify|complete Shopify/i);
}
function assertShown(f,{allowed,expected,count}={}){
  const shown=grid(f),page=f.store.snapshot();
  assert.equal(page.pageKind,'collection');assert.equal(new Set(shown).size,shown.length,'The actual host must not render duplicate result identities');
  assert.equal(new Set(page.loadedPieces.map(p=>p.id)).size,page.loadedPieces.length);
  assert.deepEqual(sorted(handles(page.loadedPieces)),sorted(shown),'The loaded result batch and actual host cards must agree');
  if(count!==undefined)assert.equal(shown.length,count);
  if(expected)assert.deepEqual(sorted(shown),sorted(expected));
  if(allowed)for(const handle of shown)assert.ok(allowed.includes(handle),'Unexpected actual host result: '+handle);
  assert.deepEqual(f.cart(),[]);f.assertNativeOnly();return shown;
}
function assertCounts(f,result,{total,shown,remaining}){
  assert.equal(result?.ok,true,JSON.stringify(result));
  assert.ok(result.searchResults&&typeof result.searchResults==='object','The completed native search must expose its checked result counts');
  assert.deepEqual(clone(result.searchResults),{schema:1,query:f.store.snapshot().search,scope:'checked-loaded-inventory',total,shown,remaining,hasMore:remaining>0});
  assert.equal(result.inventory?.catalogueComplete,false,'Loaded results must not claim whole-shop coverage');
  assert.ok(Number.isSafeInteger(result.checkedAt)&&result.checkedAt>0);
  assert.ok(f.w.Date.now()-result.checkedAt<=300000,'Successful result counts require current checked evidence');
  assert.ok(Array.isArray(result.products)&&result.products.length<=6,'Native/widget product grounding remains bounded');
  for(const p of result.products){assert.ok(grid(f).includes(p.handle));assert.ok(Number.isSafeInteger(p.checkedAt)&&p.checkedAt>0);assert.notEqual(p.stale,true);}
  const saved=JSON.parse(f.w.sessionStorage.getItem('brites-concierge-v1')||'{}');assert.ok((saved.products||[]).length<=6);
}
async function say(f,text){
  const out=await f.say(text);assert.equal(out.result?.handled,true,'The finalized native request must be handled: '+text);assert.equal(out.result?.ok,true,JSON.stringify(out.result));shopperReply(f.lastSpoken());return out;
}
async function animals(f,{allowed=ANIMALS}={}){
  const out=await say(f,'Show me animal jewellery');const first=assertShown(f,{allowed,count:6});assert.equal(f.toolCalls.length,0);return {...out,first};
}
async function narrow(f){
  await animals(f);await say(f,'Just earrings');assertShown(f,{allowed:EARRINGS,count:6});
  await say(f,'Sterling silver');assertShown(f,{expected:SILVER_BUDGET});
  await say(f,'Under USD50');assertShown(f,{expected:SILVER_BUDGET});
  const out=await say(f,'Without birds');assertShown(f,{expected:NO_BIRDS});return out;
}

test('native short animal search reports all 18 eligible checked matches while actually showing six',async t=>{
  const f=await fixture(t),first=await animals(f);assertCounts(f,first.result,{total:18,shown:6,remaining:12});
  assert.equal(f.store.inventoryStatus().loaded,120);assert.equal(f.store.inventoryStatus().ready,true);
});

test('native show more then show the rest presents unseen animal results exactly once and stops when exhausted',async t=>{
  const f=await fixture(t),first=await animals(f);assertCounts(f,first.result,{total:18,shown:6,remaining:12});
  const more=await say(f,'Show more'),second=assertShown(f,{allowed:ANIMALS,count:6});
  assert.equal(second.some(h=>first.first.includes(h)),false);assertCounts(f,more.result,{total:18,shown:12,remaining:6});
  const rest=await say(f,'Show the rest'),third=assertShown(f,{expected:ANIMALS.filter(h=>!first.first.includes(h)&&!second.includes(h))});
  assert.deepEqual(sorted(first.first.concat(second,third)),sorted(ANIMALS));assertCounts(f,rest.result,{total:18,shown:18,remaining:0});
  const before=grid(f),presentations=f.presentations.length,exhausted=await say(f,'Show more');assert.deepEqual(grid(f),before);assert.equal(f.presentations.length,presentations);assertCounts(f,exhausted.result,{total:18,shown:18,remaining:0});
});

test('native show the rest retains the animal scope and displays all 12 unseen results rather than searching for rest',async t=>{
  const f=await fixture(t),first=await animals(f),rest=await say(f,'Show the rest');
  assertShown(f,{expected:ANIMALS.filter(h=>!first.first.includes(h))});assertCounts(f,rest.result,{total:18,shown:18,remaining:0});assert.doesNotMatch(f.store.snapshot().search,/\brest\b/);
  assert.equal(f.toolCalls.length,0);
});

test('native all-unseen batch can exceed 24 real host cards while widget and model grounding stay at most six',async t=>{
  const f=await fixture(t,{largeAnimalListings:true}),first=await animals(f,{allowed:LARGE_ANIMALS}),rest=await say(f,'Show the rest');
  assertShown(f,{expected:LARGE_ANIMALS.filter(h=>!first.first.includes(h)),count:36});
  assert.equal(f.presentations.at(-1).products.length,36);assertCounts(f,rest.result,{total:42,shown:42,remaining:0});assert.equal(f.store.snapshot().visiblePieces.length,24);
});

test('native animal search retains category, exact silver, budget and bird exclusion through successive short refinements',async t=>{
  const f=await fixture(t);await animals(f);await say(f,'Show the rest');const steps=[];
  for(const [text,expected,total] of [['Just earrings',null,10],['Sterling silver',SILVER_BUDGET,10],['Under USD50',SILVER_BUDGET,6],['Without birds',NO_BIRDS,4]]){
    const out=await say(f,text);const shown=assertShown(f,{allowed:EARRINGS,expected,count:expected?.length||6});assert.equal(f.store.snapshot().filter,'earrings');steps.push({result:out.result,total,shown:shown.length});
  }
  // Each refinement starts its own unique presentation count, including pieces
  // previously displayed by the broader animal search.
  for(const step of steps){const metadata=step.result.searchResults;assert.equal(metadata?.total,step.total);assert.equal(metadata?.shown,step.shown);assert.equal(metadata?.remaining,step.total-step.shown);}
  assertCounts(f,steps.at(-1).result,{total:4,shown:4,remaining:0});assert.equal(f.toolCalls.length,0);
});

for(const phrase of ['Only earrings','Show only earrings','The earrings','Of those earrings'])test('native category continuation retains animal motif for '+phrase,async t=>{
  const f=await fixture(t);await animals(f);const out=await say(f,phrase);assertShown(f,{allowed:EARRINGS,count:6});assertCounts(f,out.result,{total:10,shown:6,remaining:4});
});

test('native any material clears only metal and preserves animal earrings, USD50 budget and bird exclusion',async t=>{
  const f=await fixture(t);await narrow(f);const broader=await say(f,'Any material'),first=assertShown(f,{allowed:ANY_MATERIAL_NO_BIRDS,count:6});assertCounts(f,broader.result,{total:7,shown:6,remaining:1});
  const rest=await say(f,'Show the rest'),last=assertShown(f,{expected:ANY_MATERIAL_NO_BIRDS.filter(h=>!first.includes(h))});assert.deepEqual(sorted(first.concat(last)),sorted(ANY_MATERIAL_NO_BIRDS));assertCounts(f,rest.result,{total:7,shown:7,remaining:0});
  // These literal pieces qualify below USD50 only in their Gold Filled option.
  for(const handle of ['bunny-stud-earrings','dragonfly-stud-earrings','dolphin-hoop-earrings'])assert.ok(first.concat(last).includes(handle));
  assert.match(f.store.snapshot().search,/50/);assert.doesNotMatch(f.store.snapshot().search,/sterling/);
});

test('native any price clears only budget and preserves animal earrings, sterling silver and bird exclusion',async t=>{
  const f=await fixture(t);await narrow(f);const broader=await say(f,'Any price'),first=assertShown(f,{allowed:ANY_MATERIAL_NO_BIRDS,count:6});assertCounts(f,broader.result,{total:7,shown:6,remaining:1});
  const rest=await say(f,'Show the rest'),last=assertShown(f,{expected:ANY_MATERIAL_NO_BIRDS.filter(h=>!first.includes(h))});assert.deepEqual(sorted(first.concat(last)),sorted(ANY_MATERIAL_NO_BIRDS));assertCounts(f,rest.result,{total:7,shown:7,remaining:0});
  assert.match(f.store.snapshot().search,/sterling silver/);assert.doesNotMatch(f.store.snapshot().search,/\b(?:under|at least)\b|\b50\b/);
});

test('native explicit new flower topic clears prior animal/category/material/budget exclusions',async t=>{
  const f=await fixture(t);await narrow(f);const flowers=await say(f,'Show me flower jewellery');assertShown(f,{expected:FLOWERS});assertCounts(f,flowers.result,{total:3,shown:3,remaining:0});
  assert.equal(f.store.snapshot().filter,'all');assert.doesNotMatch(f.store.snapshot().search,/animal|earring|sterling|50|without|bird/);assert.ok(grid(f).includes('lotus-hoop-earrings'),'The former USD50 budget must not omit the USD85 silver Lotus');
  const fresh=await say(f,'Show earrings'),all=f.products.filter(p=>p.type==='Earrings').map(p=>p.handle);assertShown(f,{allowed:all,count:6});assertCounts(f,fresh.result,{total:all.length,shown:6,remaining:all.length-6});assert.equal(f.store.snapshot().filter,'earrings');
});

for(const phrase of ['Show all pieces','Reset search'])test('native '+phrase+' clears the complete prior discovery scope',async t=>{
  const f=await fixture(t);await narrow(f);await say(f,phrase);const page=f.store.snapshot();assert.equal(page.search,'');assert.equal(page.filter,'all');assert.equal(page.loadedPieces.length,120);assert.ok(grid(f).length>=24);
  const again=await animals(f);assertCounts(f,again.result,{total:18,shown:6,remaining:12});
});

test('native detailed-product material question and back navigation preserve the committed search without selecting options',async t=>{
  const f=await fixture(t);await narrow(f);const collection=clone(f.store.snapshot());await say(f,'Open Cat Stud Earrings');assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');
  const controls=clone(f.store.snapshot().productControls),presented=f.presentations.length;await say(f,'Do you have this in gold filled?');assert.match(f.lastSpoken(),/gold filled/i);assert.deepEqual(clone(f.store.snapshot().productControls),controls);assert.equal(f.presentations.length,presented);assert.equal(f.store.snapshot().pageKind,'product');
  await say(f,'Go back to the results');assertShown(f,{expected:NO_BIRDS});assert.equal(f.store.snapshot().search,collection.search);const broader=await say(f,'Any price');assertShown(f,{allowed:ANY_MATERIAL_NO_BIRDS,count:6});assertCounts(f,broader.result,{total:7,shown:6,remaining:1});assert.match(f.store.snapshot().search,/sterling silver/);
});

test('native provider function events continue finalized animal ASR even when model arguments request flowers',async t=>{
  const f=await fixture(t,{providerFallback:true});await f.say('Show me animal jewellery');let call=await f.tool('find_jewellery',{message:'Show me flower jewellery'});assert.equal(call.host?.ok,true);assert.deepEqual(Object.keys(call.spoken),['reply']);shopperReply(call.spoken.reply);const first=assertShown(f,{allowed:ANIMALS,count:6});assertCounts(f,call.host,{total:18,shown:6,remaining:12});
  await f.say('Show the rest');call=await f.tool('find_jewellery',{message:'Show earrings in gold filled'});assert.equal(call.host?.ok,true);assertShown(f,{expected:ANIMALS.filter(h=>!first.includes(h))});assertCounts(f,call.host,{total:18,shown:18,remaining:0});assert.deepEqual(Object.keys(call.spoken),['reply']);shopperReply(call.spoken.reply);
  await f.say('Just earrings');call=await f.tool('find_jewellery',{message:'Show all pieces'});assertShown(f,{allowed:EARRINGS,count:6});assertCounts(f,call.host,{total:10,shown:6,remaining:4});assert.deepEqual(Object.keys(call.spoken),['reply']);
});

test('native continuation recomputes current stock and excludes an unseen animal that became unavailable',async t=>{
  const f=await fixture(t),first=await animals(f),removed='owl-ring';assert.equal(first.first.includes(removed),false);const p=f.products.find(p=>p.handle===removed);for(const v of p.variants)v.available=false;await f.refreshInventory();
  const rest=await say(f,'Show the rest');assertShown(f,{expected:ANIMALS.filter(h=>h!==removed&&!first.first.includes(h))});assertCounts(f,rest.result,{total:17,shown:17,remaining:0});
});

test('native result totals and all-unseen presentation exclude held, unknown-stock and unavailable animal listings',async t=>{
  const f=await fixture(t,{includeGuardListings:true}),first=await animals(f),rest=await say(f,'Show the rest');assertShown(f,{expected:ANIMALS.filter(h=>!first.first.includes(h))});assertCounts(f,rest.result,{total:18,shown:18,remaining:0});
  assert.equal(f.store.inventoryStatus().partial,true);assert.equal(f.store.inventoryStatus().catalogueComplete,false);const presented=f.presentations.flatMap(p=>p.products.map(p=>p.handle));for(const handle of ['rabbit-necklace','wolf-stud-earrings','tiger-hoop-earrings'])assert.equal(presented.includes(handle),false);
});

for(const corruption of ['expired checked read','conflicting product identity'])test('native '+corruption+' fails closed without consuming unseen results or replacing the collection',async t=>{
  const f=await fixture(t),first=await animals(f),before=grid(f),presented=f.presentations.length,unseen=new Set(ANIMALS.filter(h=>!before.includes(h)));
  f.setReadPolicy((handle,read)=>{if(!read||!unseen.has(handle))return read;if(corruption==='expired checked read'){const checkedAt=read.checkedAt-300001;return {...read,checkedAt,product:{...read.product,checkedAt}};}return {...read,product:{...read.product,id:'gid://shopify/Product/999999'}};});
  const failed=await f.say('Show the rest');assert.equal(failed.result?.handled,true);assert.notEqual(failed.result?.ok,true);assert.deepEqual(grid(f),before);assert.equal(f.presentations.length,presented);shopperReply(f.lastSpoken());
  f.setReadPolicy(null);const recovered=await say(f,'Show the rest');assertShown(f,{expected:ANIMALS.filter(h=>!first.first.includes(h))});assertCounts(f,recovered.result,{total:18,shown:18,remaining:0});
});

test('native unknown named product is refused and does not advance animal continuation memory',async t=>{
  const f=await fixture(t),first=await animals(f),before=grid(f),presented=f.presentations.length,unknown=await f.say('Open Unicorn Cuff');assert.equal(unknown.result?.handled,true);assert.notEqual(unknown.result?.ok,true);assert.deepEqual(grid(f),before);assert.equal(f.presentations.length,presented);shopperReply(f.lastSpoken());
  const rest=await say(f,'Show the rest');assertShown(f,{expected:ANIMALS.filter(h=>!first.first.includes(h))});assertCounts(f,rest.result,{total:18,shown:18,remaining:0});
});

test('native duplicate and delayed old transcription finals cannot consume another batch or resurrect an old search',async t=>{
  const f=await fixture(t),first=await animals(f),rest=await say(f,'Show the rest'),before=grid(f),presented=f.presentations.length,receipts=f.receipts().length;
  f.final(rest.input,'Show the rest');f.commit(rest.input);await settle();assert.deepEqual(grid(f),before);assert.equal(f.presentations.length,presented);assert.equal(f.receipts().length,receipts);
  const flowers=await say(f,'Show me flower jewellery');assertShown(f,{expected:FLOWERS});f.final(first.input,'Show the rest');await settle();assertShown(f,{expected:FLOWERS});assertCounts(f,flowers.result,{total:3,shown:3,remaining:0});
});

test('native continuation retains shown identities through a temporary eligibility gap while admitting new unseen stock',async t=>{
  const f=await fixture(t,{includeGuardListings:true}),first=await animals(f),missing=first.first[0],old=f.products.find(p=>p.handle===missing),newStock=f.products.find(p=>p.handle==='tiger-hoop-earrings');
  for(const v of old.variants)v.available=false;await f.refreshInventory();
  const more=await say(f,'Show more');assertShown(f,{allowed:ANIMALS.filter(h=>!first.first.includes(h)),count:6});assertCounts(f,more.result,{total:17,shown:11,remaining:6});
  const rest=await say(f,'Show the rest');assertShown(f,{allowed:ANIMALS.filter(h=>h!==missing&&!first.first.includes(h)),count:6});assertCounts(f,rest.result,{total:17,shown:17,remaining:0});
  for(const v of old.variants)v.available=true;for(const v of newStock.variants)v.available=true;await f.refreshInventory();
  const returned=await say(f,'Show the rest');assertShown(f,{expected:['tiger-hoop-earrings']});assertCounts(f,returned.result,{total:19,shown:19,remaining:0});
  const before=grid(f),presented=f.presentations.length,exhausted=await say(f,'Show the rest');assert.deepEqual(grid(f),before);assert.equal(f.presentations.length,presented);assertCounts(f,exhausted.result,{total:19,shown:19,remaining:0});
});
