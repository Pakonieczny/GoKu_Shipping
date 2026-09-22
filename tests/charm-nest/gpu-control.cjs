const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const S=require('../../charm-nest-solver'),G=require('../../charm-nest-gpu');
const job={sheet:{wPt:100,hPt:50,insetPt:1},pieces:Array.from({length:4},(_,i)=>({id:'p'+i,w:12,h:10,scale:1,bits:new Uint8Array(120).fill(1)})),angles:Array.from({length:36},(_,i)=>i*10),fineRes:2,coarseRes:.5,timeBudgetMs:3000,maxTrials:1,maxFill:.8,useGPU:true};
(async()=>{
 const messages=[],unavailable={create:async()=>{throw Error('WebGPU unavailable');},benchmark:G.benchmark};
 const ctx=vm.createContext({importScripts(){},self:{CharmNestGPU:unavailable,postMessage:m=>messages.push(structuredClone(m))},CharmNestGPU:unavailable,CharmNestSolver:S});
 vm.runInContext(fs.readFileSync('charm-nest-worker.js','utf8'),ctx);
 await ctx.self.onmessage({data:{type:'solve',jobId:'fallback',job}});
 assert(messages.some(m=>m.type==='gpu'&&m.progress.phase==='fallback'));
 const done=messages.find(m=>m.type==='done');assert(done);assert.equal(done.result.placements.length,4);assert.equal(done.result.metrics.gpuPositions,0);
 assert(S.verify(job,done.result.placements,4).ok);
 // Deliberately slow test double checks the production calibration decision.
 // Actual shader correctness/performance is tested separately in gpu.cjs.
 let calls=0,destroys=0;const progress=[];
 const slow={candidates:async args=>{calls++;await new Promise(r=>setTimeout(r,250));return args[0].variants.map(()=>[]);},destroy:()=>destroys++};
 const result=await S.solve({...job,useGPU:false},{gpu:slow,onGPU:p=>progress.push(p)});
 assert.equal(calls,3,'stop submitting to a slower GPU after calibration');assert.equal(destroys,1);
 assert(progress.some(p=>!p.active&&/CPU is faster/.test(p.reason)));assert(result.metrics.gpuPositions>0);assert(S.verify(job,result.placements,4).ok);
 // Explicit GPU mode must not be disabled by the first cold placement samples.
 calls=0;destroys=0;progress.length=0;
 const selected=await S.solve(job,{gpu:slow,onGPU:p=>progress.push(p)});
 assert(calls>=4,'GPU preference survives three slow placement calls');assert.equal(destroys,0);
 assert(!progress.some(p=>!p.active));assert(selected.metrics.gpuPositions>0);assert(S.verify(job,selected.placements,4).ok);
 const failed=[];let brokenDestroyed=0;
 const recovered=await S.solve(job,{gpu:{candidates:async()=>{throw Error('device lost');},destroy:()=>brokenDestroyed++},onGPU:p=>failed.push(p)});
 assert.equal(brokenDestroyed,1);assert(failed.some(p=>!p.active&&/device lost/.test(p.reason)));
 assert.equal(recovered.placements.length,4);assert(S.verify(job,recovered.placements,4).ok);
 let benchmarkDestroyed=false;
 await assert.rejects(()=>G.benchmark(job,S,{createGPU:async()=>({destroy(){benchmarkDestroyed=true;}}),shouldStop:()=>true}),/cancelled/);
 assert(benchmarkDestroyed,'cancelled comparisons release the device');
 console.log('GPU control OK: unavailable-device worker fallback, explicit GPU preference, optional adaptive calibration, device-error recovery, result verification and comparison cancellation');
})().catch(e=>{console.error(e);process.exitCode=1;});
