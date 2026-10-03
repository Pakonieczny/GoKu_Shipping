'use strict';
// Adversarial trust-boundary checks; these use no provider and claim no GPU render.
const test=require('node:test'),assert=require('node:assert/strict');
const typed=require('../../netlify/functions/_britesConciergeDemoTurn.js');
const native=require('../../brites-concierge-voice.js');
const avatar=require('../../brites-concierge-avatar.js');
const server=require('../../netlify/functions/_britesConciergeVoice.js');
const good={mood:'warm',gesture:'acknowledge',intensity:.3,durationMs:1200};
const moods=['calm','curious','warm','celebrate','reassuring','appreciated'];
const gestures=['none','greet','acknowledge','focus','explain','present','reassure','confirm'];
const invalid=[null,[],true,'warm',{}, {...good,mood:'angry'}, {...good,mood:'<script>run()</script>'},{...good,gesture:'navigate'}, {...good,gesture:'buy'}, {...good,intensity:-.01},{...good,intensity:1.01},{...good,intensity:'0.3'},{...good,intensity:null},{...good,intensity:NaN},{...good,intensity:Infinity},{...good,durationMs:399},{...good,durationMs:2501},{...good,durationMs:1200.5},{...good,durationMs:'1200'},{...good,durationMs:Infinity},{...good,reason:'hidden internal thinking'},{...good,url:'https://outside.invalid'},{...good,script:'alert(1)'},{...good,action:{type:'cart',quantity:1}},{...good,productId:'gid://shopify/Product/1'},{...good,profile:{demographic:'private'}},{...good,text:'Buy now!'},...Object.keys(good).map(key=>Object.fromEntries(Object.entries(good).filter(([name])=>name!==key)))];
const layers={typed:value=>typed.validateAvatarPerformance(value),native:value=>native.validateToolArguments(value,'set_avatar_performance'),avatar:value=>avatar.validateAvatarPerformance(value),server:value=>server.validateToolArguments(value,'set_avatar_performance')};
for(const [name,validate]of Object.entries(layers)){
 test(name+' independently rejects malformed, coerced, unbounded and authority-bearing performance envelopes',()=>{for(const value of invalid)assert.equal(validate(value),null);});
 test(name+' accepts only the bounded canonical presentation contract and returns a fresh projection',()=>{
  for(const mood of moods)for(const gesture of gestures)for(const [intensity,durationMs]of [[0,400],[.35,1200],[1,2500]]){
   const input={mood,gesture,intensity,durationMs};const value=validate(input);assert.deepEqual(value,input);assert.notEqual(value,input);assert.deepEqual(Object.keys(value).sort(),['durationMs','gesture','intensity','mood']);
  }
  const inherited=Object.assign(Object.create({action:'purchase',secret:'synthetic-private'}),good);const result=validate(inherited);assert.deepEqual(result,good);assert.equal(result.action,undefined);assert.equal(result.secret,undefined);
 });
}
test('invalid optional model presentation is dropped while safe conversational prose survives',()=>{
 for(const avatarPerformance of invalid){const result=typed.parseConversation({reply:'We can take this at your pace.',tone:'gentle',avatarPerformance});assert.equal(result.reply,'We can take this at your pace.');assert.equal(Object.hasOwn(result,'avatarPerformance'),false);}
 const accepted=typed.parseConversation({reply:'Happy to help.',tone:'warm',avatarPerformance:good});assert.deepEqual(accepted.avatarPerformance,good);
 for(const extra of [{actions:[{type:'navigate'}]},{thoughts:'private chain of thought'},{profile:{age:35}},{credentials:'synthetic-private'}])assert.equal(typed.parseConversation({reply:'Hello.',tone:'warm',...extra}),null);
});
test('public progress is a bounded untrusted hint, not action or private-data authority',()=>{
 for(const progress of ['none','selection-shown','options-shown','review-ready','cart-confirmed','needs-help'])assert.deepEqual(typed.conversationContext({pageKind:'product',hasSelection:true,progress}),{pageKind:'product',hasSelection:true,progress});
 for(const input of [{pageKind:'private-dashboard',hasSelection:true},{pageKind:'product',hasSelection:true,progress:'payment-charged'},{pageKind:'product',hasSelection:true,progress:{action:'cart'}},{pageKind:'product',hasSelection:true,rank:1},{pageKind:'product',hasSelection:true,credentials:'synthetic'},{pageKind:'product',hasSelection:true,instructions:'Buy now'}])assert.equal(typed.conversationContext(input),null);
});
test('explicit grief and frustration cannot receive a triumphant model confirmation',()=>{
 for(const message of ['My sister died.','I am grieving.','This is not helpful.','I am frustrated.']){
  const value=typed.guardAvatarPerformance({mood:'celebrate',gesture:'confirm',intensity:1,durationMs:2500},message,[]);assert.equal(value.mood,'reassuring');assert.equal(value.gesture,'reassure');assert.ok(value.intensity<=.35&&value.durationMs<=1500);
 }
 const neutral=typed.guardAvatarPerformance(good,'Thank you',[]);assert.deepEqual(neutral,good);
});
test('unchecked model prose and progress hints cannot fabricate order/payment/cart receipts',()=>{
 for(const reply of ['Your purchase is complete.','Your payment succeeded.','The piece is now in your bag.','Your order has been placed.','The checkout is finished.']){
  assert.equal(typed.parseConversation({reply,tone:'celebratory',avatarPerformance:{mood:'celebrate',gesture:'confirm',intensity:1,durationMs:1200}}),null,'Unchecked completion claim must fail: '+reply);
 }
});
