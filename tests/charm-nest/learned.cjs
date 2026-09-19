// Real geometry tests; the model fixture is explicit and never described as learned evidence.
const assert=require('node:assert/strict'),L=require('../../charm-nest-learned.js'),S=require('../../charm-nest-solver.js');
const rect=(id,w,h,date=1)=>({id,order:id,orderDate:date,w:w*6,h:h*6,scale:6,bits:new Uint8Array(w*h*36).fill(1),areaPt2:w*h});
const job={sheet:{wPt:20,hPt:12,insetPt:0},clearancePt:0,angles:[0,90],fineRes:1,coarseRes:1,maxFill:.74,seed:1,learned:{budgetMs:3000,stallMs:1000},pieces:[rect('new',8,8,3),{...rect('old-a',8,8,1),order:'old'},{...rect('old-b',8,8,1),order:'old'},rect('middle',8,8,2)]};
(async()=>{
 assert.deepEqual(L.groups(job.pieces).map(g=>g.date),[1,2,3]);
 assert.deepEqual(L.polygon(rect('a',4,3)),[[-2,-1.5],[2,-1.5],[2,1.5],[-2,1.5]]);
 const key=L.fingerprint(job);assert.notEqual(key,L.fingerprint({...job,sheet:{...job.sheet,wPt:21}}));
 const changed={...job,pieces:job.pieces.map(p=>({...p,bits:p.bits.slice()}))};changed.pieces[0].bits[0]=0;assert.notEqual(key,L.fingerprint(changed));
 let cp,calls=0;const cb={propose:async()=>{calls++;return {model:'TEST FIXTURE',candidates:[job.pieces.map((p,i)=>({id:p.id,angle:12.3,cxPt:i*4,cyPt:0}))]};},onLearned:p=>{if(p.checkpoint)cp=p.checkpoint;}};
 const r=await L.solve(job,cb);assert(calls===1);assert.deepEqual(new Set(r.placements.map(p=>p.id)),new Set(['old-a','old-b']));assert(S.verify(job,r.placements,6).ok);assert.equal(r.params.gainedPieces,0);assert.equal(r.endedBy,'plateau');assert.equal(cp.key,key);
 const resumed=await L.solve({...job,learned:{...job.learned,checkpoint:cp}}, {shouldStop:()=>true,propose:()=>{throw Error('must not call on stop')}});assert.equal(resumed.endedBy,'stopped');assert.equal(resumed.placements.length,2);
 const retained=await L.solve({...job,learned:{...job.learned,initialLayout:r.placements}},{shouldStop:()=>true});assert.equal(retained.placements.length,2);assert.equal(retained.endedBy,'stopped');
 const unavailable=await L.solve(job,{propose:async()=>{throw Error('offline')}});assert.equal(unavailable.endedBy,'model-unavailable');assert.equal(unavailable.placements.length,2);assert.equal(unavailable.params.model,null);
 const bad=await L.solve(job,{propose:async()=>({model:'TEST',candidates:[[{id:'unknown',cxPt:0,cyPt:0,angle:NaN}]]})});assert.equal(bad.endedBy,'model-unavailable');
 const complete=await L.solve({...job,pieces:[rect('one',4,4)]},{propose:()=>{throw Error('must skip when all fit')}});assert.equal(complete.endedBy,'complete');assert.equal(complete.params.model,null);
 console.log('learned flow OK: FIFO whole orders, geometry, mask fingerprints, resume/stop, malformed responses, runner failure, unnecessary AI skipped');
})().catch(e=>{console.error(e);process.exitCode=1;});
