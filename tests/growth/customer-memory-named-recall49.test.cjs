'use strict';

const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const {fixture,settle}=require('./native-continuity48-fixture.cjs');
const recent={id:'recent-birthday',kind:'conversation',at:Date.now(),messages:[{role:'user',content:'My mother likes bird stud earrings for her birthday.'}]};
const older={id:'older-dormouse',kind:'conversation',at:Date.now()-200000,messages:[{role:'user',content:'The dormouse necklace was a graduation gift for my grandmother.'}]};
const response=value=>({ok:true,status:200,json:async()=>JSON.parse(JSON.stringify(value))});

function mountArchive(t,f,handle){
  const calls=[],baseFetch=f.w.fetch,user={uid:'named-recall-shopper',email:'named-recall@fixture.invalid',isAnonymous:false,getIdToken:async()=> 'named-recall-fixture-token'};
  const auth={currentUser:user,onAuthStateChanged(fn){fn(user);return()=>{};}};
  f.w.firebase={apps:[{}],auth:()=>auth};
  f.w.fetch=async(raw,init={})=>{
    if(new URL(raw,f.w.location.href).pathname!=='/api/concierge-memory')return baseFetch(raw,init);
    const body=JSON.parse(init.body),call={body,signal:init.signal};calls.push(call);
    if(handle){const handled=await handle(call);if(handled)return handled;}
    if(body.action==='read')return response({ok:true,generation:'0',chunks:[recent],nextCursor:'older-window',preferences:{}});
    if(body.action==='sync')return response({ok:true,generation:'0',saved:body.chunks.length,duplicates:0,syncAfterMs:60000});
    throw Error('Unexpected memory operation '+body.action);
  };
  f.w.eval(fs.readFileSync(require.resolve('../../brites-concierge-memory.js'),'utf8'));
  let memory;const create=f.w.BritesConciergeMemory.create;f.w.BritesConciergeMemory.create=options=>(memory=create(options));
  t.after(()=>memory?.dispose());return calls;
}

test('actual named native recall searches older archive windows and quotes the named gift instead of the newest unrelated conversation',async t=>{
  const f=await fixture(t,{query:'?product=cat-stud-earrings'}),calls=mountArchive(t,f,({body})=>{
    if(body.action!=='read'||!body.query)return null;
    assert.match(body.query,/dormouse/);assert.doesNotMatch(body.query,/what|discuss|earlier|conversation/);
    return response({ok:true,generation:'0',chunks:body.cursor?[older]:[],nextCursor:body.cursor?null:'window100',preferences:{}});
  });
  await f.say('What did we discuss in our earlier conversation?');assert.match(f.lastSpoken(),/bird stud earrings/);
  const controls=f.controls.length,page=f.store.snapshot().currentHandle;
  await f.say('What did we discuss about the dormouse graduation gift?');
  assert.match(f.lastSpoken(),/dormouse necklace.*graduation gift.*grandmother/);assert.doesNotMatch(f.lastSpoken(),/bird stud|birthday|purchased|available|in stock/i);
  const queried=calls.filter(call=>call.body.action==='read'&&call.body.query);
  assert.equal(queried.length,2);assert.equal(queried[0].body.cursor,undefined);assert.equal(queried[1].body.cursor,'window100');assert.equal(queried[1].body.query,queried[0].body.query);
  assert.equal(f.controls.length,controls);assert.equal(f.store.snapshot().currentHandle,page);assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('generic native recall retains recent archive behavior without a meaningless keyword search',async t=>{
  const f=await fixture(t,{query:'?product=cat-stud-earrings'}),calls=mountArchive(t,f);
  await f.say('What did we discuss in our earlier conversation?');assert.match(f.lastSpoken(),/bird stud earrings.*birthday/);
  assert.ok(calls.some(call=>call.body.action==='read'));assert.ok(calls.filter(call=>call.body.action==='read').every(call=>call.body.query===''));
  assert.equal(f.cart().length,0);f.assertNativeOnly();
});

test('a newer native request cancels named archive continuation before its older quote can become a reply or action',async t=>{
  let release,held;const gate=new Promise(resolve=>release=resolve);
  const f=await fixture(t,{query:'?product=cat-stud-earrings'}),calls=mountArchive(t,f,call=>{
    if(call.body.action!=='read'||!call.body.query)return null;
    if(!call.body.cursor)return response({ok:true,generation:'0',chunks:[],nextCursor:'window100',preferences:{}});
    held=call;return gate;
  });
  await f.say('What did we discuss about the dormouse graduation gift?');assert.ok(held,'The actual named recall reached its continuation page');
  await f.say('What quantity have I selected?');assert.equal(f.lastSpoken(),'Quantity 1.');assert.equal(held.signal.aborted,true);
  release(response({ok:true,generation:'0',chunks:[older],nextCursor:'window200',preferences:{}}));await settle();
  assert.doesNotMatch(f.root.querySelector('.caption-text').textContent,/dormouse|grandmother/);
  assert.equal(calls.filter(call=>call.body.query).length,2);assert.equal(f.store.snapshot().currentHandle,'cat-stud-earrings');assert.equal(f.cart().length,0);f.assertNativeOnly();
});
