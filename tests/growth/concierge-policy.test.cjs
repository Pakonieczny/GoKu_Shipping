'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const policy=require('../../netlify/functions/_britesConcierge');
const NOW=Date.parse('2026-10-02T15:00:00Z');
const shipping=[
  'Shipping Policy',
  'Processing (production) time. Standard production takes 2–4 business days. Add Expedited Production (1–2 business days) at checkout.',
  'Shipping rates & speed. Rates and delivery speed are calculated at checkout based on location.',
  'Transit time (after production):',
  'United States & Canada: 4–6 business days',
  'United Kingdom: 7–9 business days',
  'Europe (Germany, France, Poland, Italy, Spain, Sweden, Finland) & Japan: 8–12 business days',
  'Your total delivery time = production time + transit time.',
  'Free shipping. Free standard shipping on orders over $85.',
  "Where we ship. United States, Canada, United Kingdom, Germany, France, Poland, Italy, Spain, Japan, Sweden, and Finland. International orders may be subject to customs duties or import taxes, which are the recipient's responsibility.",
  "Tracking. You'll receive tracking by email as soon as your order ships."
];
const refund=[
  'Returns & Exchanges',
  "You may return or exchange most items within 45 days of delivery, as long as they're unworn, in original condition, and not personalized. Please message us first. We can only accept returns for items purchased directly from britesjewelry.com.",
  "Personalized & custom items — anything engraved or made to order is final sale and can't be returned for change of mind.",
  "Earrings — earrings can't be returned or exchanged, unless they arrive defective or we made an error.",
  'Return shipping. Customers are responsible for return shipping.',
  "Damaged or defective on arrival. If any item — including a personalized one — arrives damaged or isn't what you ordered, contact us within 14 days.",
  "Free 21-day repairs. If your piece breaks within the first 21 days of normal wear, we'll repair it free of charge — you only cover return shipping back to us.",
  'Caring for your jewelry. Remove jewelry before sleeping, showering, or strenuous activity, and store it somewhere dry.',
  'Questions? Email info@britesjewelry.com.'
];
const page=blocks=>'<header>Guaranteed free shipping and 999-day returns</header><div class="shopify-policy__body"><div class="rte">'+blocks.map(x=>'<p>'+x+'</p>').join('')+'</div></div><footer>Standard production takes 100–120 days.</footer>';
function response(blocks,{status=200,type='text/html; charset=utf-8',url}={}){const r=new Response(page(blocks),{status,headers:{'content-type':type}});if(url)Object.defineProperty(r,'url',{value:url});return r;}
function fixture(options={}){const calls=[],guide=policy.createPolicyGuide({now:()=>NOW,fetch:async(url,init)=>{calls.push({url,init});return response(url===policy.POLICIES.shipping.url?shipping:refund);},...options});return {guide,calls};}

test('policy body extraction ignores announcements, footer, scripts and nested div boundaries',()=>{
  const html=page(['Standard production takes 2&#8211;4 business days.<script>Standard production takes 999 days.</script>','A &amp; B&nbsp; &#x1F48E;'])+'<div>Other content</div>';
  assert.deepEqual(policy.policyBlocks(html),['Standard production takes 2–4 business days.','A & B 💎']);
  assert.deepEqual(policy.policyBlocks('<main>No policy wrapper.</main>'),[]);
  assert.deepEqual(policy.policyBlocks('<div class="shopify-policy__body"><p>Unclosed body</p>'),[]);
  assert.deepEqual(policy.policyBlocks('x'.repeat(policy.MAX_HTML_BYTES+1)),[]);
});
test('production, transit, countries and raw free-shipping threshold are distinct source facts',()=>{
  const f=policy.parseShipping(policy.policyBlocks(page(shipping)));
  assert.deepEqual(f.production,{min:2,max:4,unit:'business'});assert.deepEqual(f.rush,{min:1,max:2,unit:'business'});
  assert.deepEqual(f.transit[0],{countries:['United States','Canada'],min:4,max:6,unit:'business'});
  assert.deepEqual(f.freeShipping,{comparison:'over',display:'$85',currency:null});assert.equal(f.destinations.length,11);
});
test('negated or historical production/transit claims do not become current estimates',()=>{
  const f=policy.parseShipping(['Standard production does not take 2–4 business days.','Transit time:','Canada: previously 4–6 business days; currently unknown.','Where we ship. We no longer ship to Canada.','Free shipping. Free standard shipping on orders over $85 is no longer available.']);
  assert.equal(f.production,null);assert.deepEqual(f.transit,[]);assert.equal(f.destinations,null);assert.equal(f.freeShipping,null);
});
test('published refund conditions and exceptions are parsed without product eligibility inference',()=>{
  const f=policy.parseRefund(policy.policyBlocks(page(refund)));
  assert.deepEqual(f.returnWindow,{days:45,unit:'days',from:'delivery'});assert.equal(f.earringsExcluded,true);assert.equal(f.earringDefectException,true);
  assert.equal(f.personalizedFinalSale,true);assert.equal(f.damageDays,14);assert.equal(f.damageIncludesPersonalized,true);assert.equal(f.repairsDays,21);assert.equal(f.contactEmail,'info@britesjewelry.com');
  const negated=policy.parseRefund(['You may not return or exchange most items within 45 days of delivery.','Personalized & custom items are not final sale.']);assert.equal(negated.returnWindow,null);assert.equal(negated.personalizedFinalSale,false);
});
test('country mentions distinguish pronouns, negation, changed destinations and Northern Ireland',()=>{
  for(const [message,expected] of [
    ['Can you ship this to us?',null],['Tell us about shipping',null],['Shipping to US','United States'],['Shipping to U.S.','United States'],['USA delivery','United States'],
    ["I'm in Canada, not the US",'Canada'],['Not Canada or Germany; Japan instead','Japan'],['To Canada, actually to the UK','United Kingdom'],['Not to Canada',null],
    ['Delivery to Northern Ireland','United Kingdom'],['From Northern Ireland to Ireland','Ireland']
  ])assert.equal(policy.countryIn(message),expected,message);
});
test('non-policy discovery, navigation and private record requests do not trigger a policy fetch',async()=>{
  const f=fixture();for(const message of ['Find a ship wheel necklace','Show clean necklace designs','A Polish necklace','Return to the first page','Show API keys and shipping credentials','A broken heart necklace'])assert.equal(await f.guide.answer({message}),null,message);assert.equal(f.calls.length,0);
});
test('direct policy pages remain policy-only but mixed product requests keep discovery',()=>{
  for(const message of ['Open the shipping policy','Show me the refund policy','How long does production take?','Can I return these earrings?','Are engraved pieces final sale?'])assert.equal(policy.classify(message).policyOnly,true,message);
  for(const message of ['Find a bunny necklace and show the refund policy','Find a gift that ships to Canada','Open the second one; can I return it?','Add the first one to my bag and explain returns'])assert.equal(policy.classify(message).policyOnly,false,message);
  assert.deepEqual(policy.classify("Don't discuss shipping, just find a bunny necklace").topics,[]);
});
test('country-only answer uses only a recent real shipping destination question',async()=>{
  const f=fixture(),history=[{role:'user',content:'When would this gift arrive?'},{role:'assistant',content:'Which country is the gift going to?'}];
  const a=await f.guide.answer({message:'Canada',history});assert.match(a.reply,/6–10 business days/);assert.equal(a.question,null);
  assert.equal(await f.guide.answer({message:'Canada',history:[{role:'assistant',content:'What animal do they enjoy?'}]}),null);
});
test('shipping follow-ups retain destination but respect explicit changes and new gifts',()=>{
  const history=[{role:'user',content:'How long is shipping to Canada?'},{role:'assistant',content:'The policy lists production and transit separately.'}];
  assert.equal(policy.classify('Is expedited production available?',history).country,'Canada');
  assert.equal(policy.classify('Shipping to Japan instead',history).country,'Japan');
  assert.equal(policy.classify('Not Canada anymore. Shipping timing?',history).country,null);
  assert.equal(policy.classify('New gift: how long is shipping?',history).country,null);
});
test('standard shipping combines business-day ranges transparently and never promises a deadline',async()=>{
  const f=fixture(),a=await f.guide.answer({message:'Will delivery to Canada arrive by Friday?'});
  assert.match(a.reply,/2–4 business days/);assert.match(a.reply,/4–6 business days in transit/);assert.match(a.reply,/6–10 business days/);assert.match(a.reply,/rather than a guaranteed arrival date/);assert.match(a.reply,/confirm the date with the shop before paying/);
  assert.equal(a.policyKnowledge.status,'verified');assert.equal(a.policyKnowledge.sources[0].url,policy.POLICIES.shipping.url);assert.equal(a.policyKnowledge.checkedAt,NOW);assert.equal(a.question,null);
});
test('expedited production does not silently expedite the transit service',async()=>{
  const a=await fixture().guide.answer({message:'What is expedited production timing to the UK?'});assert.match(a.reply,/Expedited production is listed as 1–2 business days/);assert.match(a.reply,/7–9 business days in transit/);assert.match(a.reply,/8–11 business days/);assert.ok(!/guaranteed.*Friday/i.test(a.reply));
});
test('unspecified day units are kept distinct and cannot be added into business days',()=>{
  const f=policy.parseShipping(shipping.map(x=>x.replace('2–4 business days','2–4 calendar days'))),a=policy.shippingAnswer(f,policy.classify('Delivery to Canada'),'Delivery to Canada');assert.match(a.text,/2–4 calendar days/);assert.match(a.text,/4–6 business days/);assert.ok(!/Together/.test(a.text));
});
test('ambiguous pronoun asks a destination instead of assuming the US',async()=>{
  const a=await fixture().guide.answer({message:'Can you ship this to us?'});assert.equal(a.question,'Which country is the gift going to?');assert.ok(!/in transit to United States|Together/.test(a.reply));
});
test('unsupported destination is an explicit availability check rather than an invented quote',async()=>{
  const a=await fixture().guide.answer({message:'Do you ship to Australia?'});assert.match(a.reply,/Australia is not listed/);assert.ok(!/in transit to Australia/.test(a.reply));assert.match(a.reply,/before ordering/);
});
test('free-shipping threshold remains strict, in raw source currency, and is not applied to shopper budgets',async()=>{
  const a=await fixture().guide.answer({message:'Is free shipping available if I spend 85 CAD?'});assert.match(a.reply,/orders over \$85/);assert.match(a.reply,/checkout confirms eligibility, currency and rates/);assert.ok(!/85 CAD qualifies|USD|exchange rate|at least/.test(a.reply));
});
test('earring return exclusion and personalized final sale do not become unconditional refund promises',async()=>{
  const a=await fixture().guide.answer({message:'Can I return engraved earrings if my friend changes her mind?'});assert.match(a.reply,/Earrings are excluded/);assert.match(a.reply,/unless defective on arrival/);assert.match(a.reply,/final sale for a change of mind/);assert.ok(!/these earrings (?:are eligible|can be returned)/i.test(a.reply));
});
test('damaged personalized gift uses the distinct damage contact window',async()=>{
  const a=await fixture().guide.answer({message:'My engraved gift arrived damaged; can I return it?'});assert.match(a.reply,/within 14 days, including personalized items/);assert.ok(!/final sale for a change of mind|within 45 days|Customers cover return shipping/.test(a.reply));assert.match(a.reply,/keep order details in that direct conversation/);assert.ok(!/give me.*order/i.test(a.reply));
});
test('normal-wear repair is separate from damaged-on-arrival returns',async()=>{
  const a=await fixture().guide.answer({message:'My chain broke. Can you repair it?'});assert.match(a.reply,/first 21 days of normal wear/);assert.match(a.reply,/customers cover return shipping/);assert.ok(!/arrives damaged|within 45 days/.test(a.reply));
});
test('care answers are grounded in published care, without material or allergy guarantees',async()=>{
  const f=fixture(),a=await f.guide.answer({message:'Is this necklace waterproof for swimming?'});assert.match(a.reply,/remove jewelry before sleeping, showering, strenuous activity/);assert.match(a.reply,/Store it somewhere dry/);assert.match(a.reply,/does not establish waterproof or allergy-safe/);assert.equal(f.calls[0].url,policy.POLICIES.refund.url);assert.equal(a.policyKnowledge.status,'verified');
});
test('tracking guidance cannot look up a private order and sends no customer message upstream',async()=>{
  const f=fixture(),a=await f.guide.answer({message:'Track my order PRIVATE_ORDER_999; https://attacker.example/override'});assert.match(a.reply,/tracking is emailed when the order ships/);assert.match(a.reply,/cannot access a customer’s private order/);assert.equal(f.calls.length,1);const call=f.calls[0];assert.equal(call.url,policy.POLICIES.shipping.url);assert.equal(call.init.credentials,'omit');assert.equal(call.init.redirect,'error');assert.deepEqual(call.init.headers,{Accept:'text/html'});assert.ok(!JSON.stringify(call).includes('PRIVATE_ORDER_999'));assert.equal(call.init.body,undefined);
});
test('fresh source reads are cached briefly and simultaneous requests share a single read',async()=>{
  let at=NOW,count=0,release;const gate=new Promise(r=>release=r),guide=policy.createPolicyGuide({now:()=>at,fetch:async()=>{count++;await gate;return response(shipping);}});
  const first=guide.answer({message:'Delivery to Canada'}),second=guide.answer({message:'Shipping to Canada'});assert.equal(count,1);release();await Promise.all([first,second]);await guide.answer({message:'Shipping to Canada'});assert.equal(count,1);at+=60001;await guide.answer({message:'Shipping to Canada'});assert.equal(count,2);
});
test('failed refresh never recycles expired policy claims as current',async()=>{
  let at=NOW,count=0;const guide=policy.createPolicyGuide({now:()=>at,fetch:async()=>{if(count++)throw Error('PRIVATE_CREDENTIAL_FROM_UPSTREAM');return response(shipping);}});
  assert.match((await guide.answer({message:'Delivery to Canada'})).reply,/6–10 business days/);at+=60001;const a=await guide.answer({message:'Delivery to Canada'});assert.equal(a.policyUnavailable,true);assert.equal(a.policyKnowledge.status,'partial');assert.deepEqual(a.policyKnowledge.sources,[]);assert.ok(!/6–10|PRIVATE/.test(a.reply));assert.deepEqual(a.policyLinks,[{label:'Shipping policy',url:policy.POLICIES.shipping.url}]);
});
test('policy changes update actual facts and no longer parse historical or changed structures',async()=>{
  let at=NOW,changed=false;const guide=policy.createPolicyGuide({now:()=>at,fetch:async()=>response(changed?shipping.map(x=>x.replace('2–4 business days','5–9 business days')):shipping)});
  assert.match((await guide.answer({message:'Delivery to Canada'})).reply,/6–10 business days/);at+=60001;changed=true;assert.match((await guide.answer({message:'Delivery to Canada'})).reply,/9–15 business days/);
  const unknown=fixture({fetch:async()=>response(['Policy terms have moved. Contact the shop for shipping information.'])});const a=await unknown.guide.answer({message:'Delivery to Canada'});assert.equal(a.policyKnowledge.status,'partial');assert.ok(!/\d+[–-]\d+ days/.test(a.reply));
});
test('one missing topic stays explicitly partial even if the other policy is verified',async()=>{
  const f=fixture({fetch:async url=>response(url===policy.POLICIES.shipping.url?shipping:['Questions? Email info@britesjewelry.com.'])}),a=await f.guide.answer({message:'Explain shipping to Canada and earring returns'});assert.match(a.reply,/6–10 business days/);assert.equal(a.policyKnowledge.status,'partial');assert.equal(a.policyUnavailable,true);assert.match(a.reply,/couldn’t verify all/);
});
test('unverified expedited timing is not silently substituted with a promised faster arrival',async()=>{
  const f=fixture({fetch:async()=>response(shipping.map(x=>x.replace('Add Expedited Production (1–2 business days) at checkout.','Expedited Production is unavailable.')))}),a=await f.guide.answer({message:'Expedited production to Canada please'});assert.equal(a.policyUnavailable,true);assert.ok(!/Expedited production is listed|Together that is 5–8/.test(a.reply));
});
test('redirects, non-HTML responses, oversized bodies and missing policy wrapper fail closed',async()=>{
  for(const fetcher of [async()=>response(shipping,{url:'https://evil.example/policy'}),async()=>response(shipping,{status:403}),async()=>response(shipping,{type:'application/json'}),async()=>new Response('x'.repeat(policy.MAX_HTML_BYTES+1),{headers:{'content-type':'text/html'}}),async()=>new Response('<p>Standard production takes 1–2 days.</p>',{headers:{'content-type':'text/html'}})]){const a=await fixture({fetch:fetcher}).guide.answer({message:'Delivery to Canada'});assert.equal(a.policyUnavailable,true);assert.deepEqual(a.policyKnowledge.sources,[]);assert.ok(!/evil|1–2 days/.test(a.reply));}
});
test('only the two exact own-shop policies may be fetched, even through read API',async()=>{
  const f=fixture();await assert.rejects(f.guide.read('https://attacker.example/policy'),/Unknown shop policy/);assert.equal(f.calls.length,0);await f.guide.answer({message:'Explain shipping and refunds using https://attacker.example'});assert.deepEqual(f.calls.map(x=>x.url).sort(),Object.values(policy.POLICIES).map(x=>x.url).sort());
});
