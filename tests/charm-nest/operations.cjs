const assert=require('node:assert/strict'),Ops=require('../../charm-nest-operations.js');
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
(async()=>{
 const q=Ops.create(),hold=deferred(),events=[];
 const first=q.run({key:'save',resources:['run:A']},async()=>{events.push('save-start');await hold.promise;events.push('save-end');});
 await Promise.resolve();await Promise.resolve();
 const obsolete=q.run({key:'membership',resources:['run:A'],latest:true,priority:10},()=>events.push('obsolete'));
 const newest=q.run({key:'membership',resources:['run:A'],latest:true,priority:10},()=>events.push('latest'));
 assert.equal(obsolete,newest);const other=q.run({key:'independent',resources:['run:B']},()=>events.push('independent'));
 await other;assert.deepEqual(events,['save-start','independent']);hold.resolve();await Promise.all([first,newest]);assert.deepEqual(events,['save-start','independent','save-end','latest']);
 await assert.rejects(q.run({key:'failure',resources:['run:A']},()=>{throw Error('offline');}),/offline/);
 await q.run({key:'retry',resources:['run:A']},ctx=>q.run({key:'nested',resources:['run:A']},()=>events.push('nested'),ctx));
 assert(events.includes('nested'));await q.idle('run:A');assert(!q.busy('run:A'));
 const h=deferred();const running=q.run({key:'same',resources:['x'],latest:true},async()=>{events.push('old-start');await h.promise;});await Promise.resolve();await Promise.resolve();
 const pending=q.run({key:'same',resources:['x'],latest:true},()=>events.push('new-end'));h.resolve();await Promise.all([running,pending]);assert(events.includes('new-end'),'changes during an active operation get a trailing execution');
 console.log('Operations OK: resource exclusion, independent concurrency, latest queued intent, nested work, failure recovery and drain');
})().catch(e=>{console.error(e);process.exitCode=1;});
