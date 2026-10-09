'use strict';
// Literal native ASR and provider events through the production widget. No
// typed fixture, microphone, real provider, physical audio or live cart is used.
const test=require('node:test'),assert=require('node:assert/strict');
const {fixture,clone,settle,INITIAL,LEAF}=require('./native-spoken46-fixture.cjs');
function shopperReply(text,{maximum=45}={}){
  assert.ok(text.trim());assert.ok(text.trim().split(/\s+/).length<=maximum,'The default spoken answer must be concise: '+text);
  assert.doesNotMatch(text,/backend|adapter|authority|postcondition|completedActions|variantId|turnVersion|no further action|ORDER DETAILS|PACKAGING|EXPEDITED SHIPPING|Happy Shopping/i);
}
async function openInitial(f){const turn=await f.say('Open Lowercase Initial Necklace');assert.equal(turn.result?.ok,true,JSON.stringify(turn.result));assert.equal(f.store.snapshot().currentHandle,INITIAL);}
async function chooseInitial(f){for(const text of ['Select 16 inches','Choose gold filled','No engraving'])await f.say(text);assert.deepEqual(f.choices(),{'Necklace Length':'16 inch','Metal Choice':'14k Gold Filled','Engraving':'None'});assert.equal(f.store.snapshot().productControls.selectionStatus,'ready');}

test('native shopper selects the actual Lowercase three-group options and adds USD59 without any model tool or typed route',async t=>{
  const f=await fixture(t);await openInitial(f);assert.deepEqual(f.choices(),{});
  await f.say('Select 16 inches');assert.equal(f.choices()['Necklace Length'],'16 inch');assert.equal(f.choices().Engraving,undefined);assert.equal(f.store.snapshot().productControls.variantId,null);
  await f.say('Choose gold filled');assert.equal(f.choices()['Metal Choice'],'14k Gold Filled');assert.equal(f.choices().Engraving,undefined);
  await f.say('Add this piece to my cart');assert.deepEqual(f.cart(),[]);assert.equal(f.choices().Engraving,undefined,'an add request must never silently choose None');assert.deepEqual(clone(f.store.snapshot().productControls.requiredOptions),['Engraving']);assert.equal(f.store.snapshot().productControls.openedOption,'Engraving');
  await f.say('No engraving');assert.equal(f.choices().Engraving,'None');assert.equal(f.store.snapshot().productControls.selectionStatus,'ready');assert.equal(f.store.snapshot().productControls.selectedVariant.price,59);assert.equal(f.d.querySelector('#piece-variant').selectedOptions[0].textContent,'14k Gold Filled / 16 inch / None · $59.00 USD');
  await f.say('Set quantity to two');assert.equal(f.d.querySelector('input[aria-label="Quantity of this exact piece"]').value,'2');const added=await f.say('Add this piece to my cart');assert.equal(added.result?.cartChanged,true);assert.equal(f.cart().length,1);assert.equal(f.cart()[0].variant,'14k Gold Filled / 16 inch / None');assert.equal(f.cart()[0].quantity,2);assert.equal(f.cart()[0].price,59);assert.equal(f.d.querySelector('#bag-count').textContent,'2');assert.equal(f.toolCalls.length,0);f.assertNativeOnly();shopperReply(f.lastSpoken());
});

test('native three-group choice never infers the other missing choices from one spoken material',async t=>{
  const f=await fixture(t);await openInitial(f);await f.say('Choose gold filled');assert.deepEqual(f.choices(),{'Metal Choice':'14k Gold Filled'});assert.equal(f.store.snapshot().productControls.variantId,null);assert.deepEqual(clone(f.store.snapshot().productControls.requiredOptions),['Necklace Length','Engraving']);assert.deepEqual(f.cart(),[]);assert.equal(f.toolCalls.length,0);f.assertNativeOnly();
});

test('native choice commands work from the detail view before an option menu was opened manually',async t=>{
  const f=await fixture(t);await openInitial(f);assert.equal(f.store.snapshot().productControls.optionsOpen,false);await chooseInitial(f);
  assert.equal(f.d.querySelector('[data-option-name="Necklace Length"] [data-option-value="16 inch"]').getAttribute('aria-pressed'),'true');assert.equal(f.d.querySelector('[data-option-name="Metal Choice"] [data-option-value="14k Gold Filled"]').getAttribute('aria-pressed'),'true');assert.equal(f.d.querySelector('[data-option-name="Engraving"] [data-option-value="None"]').getAttribute('aria-pressed'),'true');assert.deepEqual(f.cart(),[]);assert.equal(f.toolCalls.length,0);f.assertNativeOnly();
});

for(const words of ['No engraving, please.','Without engraving.','No engraving for this necklace.'])test('native current engraving choice accepts a natural explicit no-engraving request: '+words,async t=>{
  const f=await fixture(t);await openInitial(f);for(const text of ['Select 16 inches','Choose gold filled'])await f.say(text);const before=clone(f.choices()),chosen=await f.say(words);
  assert.equal(chosen.result?.ok,true,JSON.stringify(chosen.result));assert.equal(f.choices().Engraving,'None');assert.equal(f.choices()['Metal Choice'],before['Metal Choice']);assert.equal(f.choices()['Necklace Length'],before['Necklace Length']);assert.equal(f.store.snapshot().productControls.variantId,f.products[0].variants.find(v=>v.title==='14k Gold Filled / 16 inch / None').id);assert.deepEqual(f.cart(),[]);assert.equal(f.toolCalls.length,0);shopperReply(f.lastSpoken());f.assertNativeOnly();
});

test('native provider function events execute the same finalized three-group shopper choices and exact add',async t=>{
  const f=await fixture(t,{providerFallback:true});await f.say('Open Lowercase Initial Necklace');assert.equal((await f.tool('control_storefront',{type:'open',handle:INITIAL})).host.ok,true);
  for(const [text,name,args] of [
    ['Select 16 inches','prepare_jewellery_action',{handle:INITIAL,action:'options'}],
    ['Choose gold filled','control_storefront',{type:'select-option',handle:INITIAL,optionName:'Metal Choice',optionValue:'14k Gold Filled'}],
    ['No engraving','control_storefront',{type:'select-option',handle:INITIAL,optionName:'Engraving',optionValue:'None'}]
  ]){await f.say(text);const result=await f.tool(name,args);assert.equal(result.host?.ok,true,JSON.stringify(result));shopperReply(result.spoken.reply||result.spoken.customerMessage);}
  assert.deepEqual(f.choices(),{'Necklace Length':'16 inch','Metal Choice':'14k Gold Filled','Engraving':'None'});await f.say('Add this piece to my cart');const added=await f.tool('find_jewellery',{message:'Recommend a necklace'});assert.equal(added.host?.cartChanged,true);assert.equal(f.cart()[0].variant,'14k Gold Filled / 16 inch / None');assert.equal(f.cart()[0].price,59);assert.equal(f.cart()[0].quantity||1,1);f.assertNativeOnly();shopperReply(added.spoken.reply||added.spoken.customerMessage);
});

test('native current selected price is concise and quotes USD59 each and USD118 for two rather than a different variant',async t=>{
  const f=await fixture(t);await openInitial(f);await chooseInitial(f);await f.say('Set quantity to two');const before=clone(f.store.snapshot().productControls),price=await f.say('How much is this piece?');assert.equal(price.result?.ok,true,JSON.stringify(price.result));
  const text=f.lastSpoken();assert.match(text,/(?:USD\s*59\.00|\$59\.00)/);assert.match(text,/(?:USD\s*118\.00|\$118\.00)/);shopperReply(text,{maximum:40});assert.deepEqual(clone(f.store.snapshot().productControls),before);assert.equal(f.toolCalls.length,0);f.assertNativeOnly();
});

test('native charm-size answer uses the published 6–9mm letter size and excludes chain, packing and order text',async t=>{
  const f=await fixture(t);await openInitial(f);const before=clone(f.store.snapshot()),answer=await f.say('How big is the letter pendant?');assert.equal(answer.result?.ok,true,JSON.stringify(answer.result));const text=f.lastSpoken();assert.match(text,/6\s*(?:[-–—]|to)\s*9\s*(?:mm|millimet)/i);assert.match(text,/letter|pendant/i);assert.doesNotMatch(text,/\b(?:inch|inches|Metal Choice|Gold Filled|Engraved)\b/i);shopperReply(text,{maximum:40});assert.deepEqual(clone(f.store.snapshot()),before);f.assertNativeOnly();
});

test('native generic charm-size question extracts the literal measurement from the exact published spaced-header description',async t=>{
  const f=await fixture(t);await openInitial(f);const before=clone(f.store.snapshot()),answer=await f.say('How big is the charm?');assert.equal(answer.result?.ok,true,JSON.stringify(answer.result));const text=f.lastSpoken();assert.match(text,/6\s*(?:[-–—]|to)\s*9\s*(?:mm|millimet)/i);assert.match(text,/charm|pendant|letter/i);assert.doesNotMatch(text,/\b(?:inch|inches|Metal Choice|Gold Filled|Engraved)\b|\bO\s+R\s+D\s+E\s+R\b|\bP\s+A\s+C\s+K\s+A\s+G\s+I\s+N\s+G\b|\bS\s+H\s+I\s+P\s+P\s+I\s+N\s+G\b/i);shopperReply(text,{maximum:40});assert.deepEqual(clone(f.store.snapshot()),before);assert.equal(f.toolCalls.length,0);f.assertNativeOnly();
});

test('native available necklace-length answer lists actual 14,16,18,20 choices without every metal or engraving combination',async t=>{
  const f=await fixture(t);await openInitial(f);const before=clone(f.store.snapshot()),answer=await f.say('What necklace lengths can I choose?');assert.equal(answer.result?.ok,true,JSON.stringify(answer.result));const text=f.lastSpoken();for(const length of [14,16,18,20])assert.match(text,new RegExp('\\b'+length+'\\b'));assert.match(text,/inch/i);assert.doesNotMatch(text,/Sterling|Gold Filled|Solid Gold|Engraved|None|engraving/i);shopperReply(text,{maximum:40});assert.deepEqual(clone(f.store.snapshot()),before);f.assertNativeOnly();
});

test('native material answer is limited to published materials rather than reading shipping and packaging paragraphs',async t=>{
  const f=await fixture(t);await openInitial(f);const before=clone(f.store.snapshot()),answer=await f.say('What materials does this piece come in?');assert.equal(answer.result?.ok,true,JSON.stringify(answer.result));const text=f.lastSpoken();assert.match(text,/sterling silver/i);assert.match(text,/gold filled/i);assert.match(text,/rose/i);assert.match(text,/solid gold/i);shopperReply(text,{maximum:45});assert.deepEqual(clone(f.store.snapshot()),before);f.assertNativeOnly();
});

test('native detailed product identity answer is brief and does not recite the full published product page',async t=>{
  const f=await fixture(t);await openInitial(f);const answer=await f.say('What am I looking at?');assert.equal(answer.result?.ok,true,JSON.stringify(answer.result));const text=f.lastSpoken();assert.match(text,/Lowercase Initial Necklace/);shopperReply(text,{maximum:50});assert.equal(f.store.snapshot().currentHandle,INITIAL);assert.deepEqual(f.cart(),[]);f.assertNativeOnly();
});

test('native Leaf price and return to Lowercase use their own exact current selected variant and quantity',async t=>{
  const f=await fixture(t);await openInitial(f);await chooseInitial(f);await f.say('Set quantity to two');await f.say('How much is this piece?');assert.match(f.lastSpoken(),/59\.00/);
  const opened=await f.say('Open Leaf Pendant Cable Necklace');assert.equal(opened.result?.ok,true);assert.equal(f.store.snapshot().currentHandle,LEAF);assert.deepEqual(f.choices(),{});
  for(const text of ['Choose 14k Solid Gold','Select 16 inches','Choose Engraved','Set quantity to two'])await f.say(text);assert.equal(f.store.snapshot().productControls.selectedVariant.price,316);const leaf=await f.say('How much is this piece?');assert.equal(leaf.result?.ok,true,JSON.stringify(leaf.result));const leafReply=f.lastSpoken();assert.match(leafReply,/316\.00/);assert.match(leafReply,/632\.00/);assert.doesNotMatch(leafReply,/59\.00|118\.00/);shopperReply(leafReply,{maximum:45});
  await f.say('Open Lowercase Initial Necklace');assert.equal(f.store.snapshot().currentHandle,INITIAL);const initial=await f.say('How much is this piece?');assert.equal(initial.result?.ok,true);const initialReply=f.lastSpoken();assert.match(initialReply,/59\.00/);assert.match(initialReply,/118\.00/);assert.doesNotMatch(initialReply,/316\.00|632\.00/);shopperReply(initialReply,{maximum:45});assert.deepEqual(f.cart(),[]);assert.equal(f.toolCalls.length,0);f.assertNativeOnly();
});

test('native delayed transcript and duplicate final events do not borrow a previous spoken choice or repeat the exact cart add',async t=>{
  const f=await fixture(t);await openInitial(f);await chooseInitial(f);const input=f.begin(),before=f.controls.length;f.commit(input);await settle();assert.equal(f.controls.length,before);f.final(input,'Add this piece to my cart');await settle();assert.equal(f.cart().length,1);
  f.final(input,'Add this piece to my cart');f.commit(input);await settle();assert.equal(f.cart().length,1);assert.equal(f.cart()[0].variant,'14k Gold Filled / 16 inch / None');assert.equal(f.cart()[0].price,59);assert.equal(f.toolCalls.length,0);f.assertNativeOnly();
});

test('native change engraving wording reaches the actual cart text field and keeps its contents private',async t=>{
  const f=await fixture(t);await openInitial(f);await chooseInitial(f);await f.say('Select Engraved');await f.say('Set quantity to two');await f.say('Add this piece to my cart then open my cart');
  const before=clone(f.cart()[0]),changed=await f.say('Change the engraving wording of the first item in my cart to TEST46');
  assert.equal(changed.result?.ok,true,JSON.stringify(changed.result));assert.equal(changed.result?.cartChanged,true);assert.equal(f.cart()[0].engravingPreview,'TEST46');
  for(const name of ['variantId','variant','quantity','price'])assert.equal(f.cart()[0][name],before[name],name);
  assert.equal(f.d.querySelector('[data-bag-engraving]').value,'TEST46');assert.doesNotMatch(JSON.stringify(f.store.snapshot()),/TEST46/);assert.doesNotMatch(f.lastSpoken(),/TEST46/);shopperReply(f.lastSpoken());
  const cleared=await f.say('Clear engraving wording for the first item in my cart');assert.equal(cleared.result?.ok,true,JSON.stringify(cleared.result));assert.equal(f.cart()[0].engravingPreview||'','');assert.equal(f.d.querySelector('[data-bag-engraving]').value,'');assert.equal(f.cart()[0].variant,before.variant);assert.equal(f.cart()[0].quantity,before.quantity);assert.equal(f.toolCalls.length,0);f.assertNativeOnly();
});
