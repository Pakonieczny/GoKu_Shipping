'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),core=require('../../netlify/functions/_britesGrowth'),{createController}=require('../../netlify/functions/_britesGrowthController');
class Database{
  constructor(){this.values=new Map();this.tail=Promise.resolve();}
  ref(name){const db=this;return{firestore:db,async get(){const value=db.values.get(name);return{exists:value!==undefined,data:()=>value===undefined?undefined:structuredClone(value)};}};}
  runTransaction(fn){const run=this.tail.then(async()=>{const writes=[],result=await fn({get:r=>r.get(),set:(r,v)=>writes.push([r,v])});for(const [ref,value]of writes)this.values.set(ref,structuredClone(value));return result;});this.tail=run.catch(()=>{});return run;}
}
function fixture(){const db=new Database(),refs=new Map();let at=Date.parse('2026-10-02T04:00:00Z'),enabled=true,stopAt=core.STOP_AT;const service={col:()=>({doc:name=>{if(!refs.has(name)){const ref=db.ref(name);ref.get=async()=>{const value=db.values.get(ref);return{exists:value!==undefined,data:()=>value===undefined?undefined:structuredClone(value)};};refs.set(name,ref);}return refs.get(name);}}),setup:async()=>({enabled,stopAt})};return{controller:createController(service,{now:()=>at}),db,advance:ms=>at+=ms,setEnabled:value=>enabled=value,setStop:value=>stopAt=value};}
test('parallel resumptions obtain one atomic controller lease without exposing its token through read',async()=>{
  const f=fixture(),claims=await Promise.all(Array.from({length:8},(_,i)=>f.controller.claim('worker-'+i,45))),won=claims.filter(x=>x.ok);assert.equal(won.length,1);assert.equal(claims.filter(x=>x.busy).length,7);const visible=await f.controller.read();assert.equal(visible.lease.owner,won[0].lease.owner);assert.equal(visible.lease.active,true);assert(!JSON.stringify(visible).includes(won[0].token));assert(!JSON.stringify(visible).includes('tokenHash'));
});
test('lease ownership protects checkpoint writes, renewal and release',async()=>{
  const f=fixture(),a=await f.controller.claim('worker-a');await assert.rejects(f.controller.save({phase:'foreign'}),/active controller lease/);await assert.rejects(f.controller.renew('worker-b',a.token),/another writer/);await assert.rejects(f.controller.release('worker-a','wrong-token-that-is-at-least-thirty-two-characters'),/another writer/);
  const saved=await f.controller.save({phase:'owned'},{owner:'worker-a',token:a.token,expectedUpdatedAt:0});assert(saved.updatedAt>0);const renewed=await f.controller.renew('worker-a',a.token,60);assert(renewed.lease.leaseUntil>a.lease.leaseUntil);await f.controller.release('worker-a',a.token);assert.equal((await f.controller.read()).lease.active,false);
});
test('expired writers cannot overwrite a replacement or reclaim an active lease',async()=>{
  const f=fixture(),a=await f.controller.claim('worker-a',5);f.advance(6*60000);const b=await f.controller.claim('worker-b',45);assert(b.ok);await assert.rejects(f.controller.save({phase:'old'},{owner:'worker-a',token:a.token}),/active controller lease/);await assert.rejects(f.controller.renew('worker-a',a.token),/another writer/);assert((await f.controller.claim('worker-a')).busy);assert.equal((await f.controller.read()).lease.owner,'worker-b');
});
test('checkpoint revisions reject stale snapshots and advance even within the same millisecond',async()=>{
  const f=fixture(),a=await f.controller.save({phase:'initial'},{expectedUpdatedAt:0}),b=await f.controller.save({phase:'next'},{expectedUpdatedAt:a.updatedAt});assert(b.updatedAt>a.updatedAt);await assert.rejects(f.controller.save({phase:'lost work'},{expectedUpdatedAt:a.updatedAt}),/Checkpoint changed/);await assert.rejects(f.controller.save([]),/checkpoint object/);await assert.rejects(f.controller.save({x:'a'.repeat(250001)}),/bounded/);
});
test('disabled control and the hard stop prevent new or renewed execution leases',async()=>{
  const f=fixture();f.setEnabled(false);assert.deepEqual(await f.controller.claim('worker-a'),{stopped:true});f.setEnabled(true);const a=await f.controller.claim('worker-a');f.setStop(Date.parse('2026-10-02T04:01:00Z'));f.advance(2*60000);assert.deepEqual(await f.controller.renew('worker-a',a.token),{stopped:true});f.advance(10*86400000);assert.deepEqual(await f.controller.claim('worker-b'),{stopped:true});await assert.rejects(f.controller.claim('invalid owner'),/valid owner/);await assert.rejects(f.controller.claim('worker-a',1000),/minute/);
});
