// Run with the optional native Dawn test runtime. No runtime npm dependency.
const assert=require('node:assert/strict'),S=require('../../charm-nest-solver'),G=require('../../charm-nest-gpu');
const median=a=>a.slice().sort((a,b)=>a-b)[Math.floor(a.length/2)];
function piece(id,w,h,kind=0){const bits=new Uint8Array(w*h);for(let y=0;y<h;y++)for(let x=0;x<w;x++){const dx=(x-w/2)/(w/2),dy=(y-h/2)/(h/2);bits[y*w+x]=kind===0?Number(dx*dx+dy*dy<1):kind===1?Number(x<w*.45||y>h*.6):Number(Math.abs(dx)+Math.abs(dy)<1);}return {id,w,h,bits,scale:2,areaPt2:bits.reduce((a,b)=>a+b,0)/4};}
function variants(p){return Array.from({length:36},(_,i)=>{const v=S.prepareVariant(p,i*10,0,2),co=S.resample(v.fine.bits,v.fine.w,v.fine.h,2,.5);const rg=S.ring(v.fine.bits,v.fine.w,v.fine.h,4);return {...v,coarse:{w:co.w,h:co.h,bits:co.bits,pm:S.packShifted(co.bits,co.w,co.h)},ringFine:S.packShifted(rg.bits,rg.w,rg.h),ringPad:4};});}
(async()=>{
 const {create,globals}=await import(process.env.WEBGPU_TEST_MODULE||'webgpu');Object.assign(globalThis,globals);
 const nativeGPU=create([]);globalThis.testGPU=nativeGPU;
 const gpu=await G.create({gpu:nativeGPU,allowSoftware:true});
 try{
  console.log('Adapter:',gpu.stats.adapter,'(software results are NOT hardware speed claims)');
  const fine=S.makeSheetGrid({wPt:100,hPt:50,insetPt:1},0,2).trackMaterial(true);
  for(let i=0;i<7;i++){const p=piece('obstacle'+i,20,25,i%3),v=S.prepareVariant(p,0,0,2);fine.stamp(v.fine.bits,v.fine.w,v.fine.h,4+i*26,4+(i%2)*40,p.id);}
  fine.buildSAT();const coarse=S.coarseFromFine(fine,4);coarse.trackMaterial();coarse.material.occ.set(coarse.occ);coarse.material.buildSAT();
  const p=piece('probe',28,32,1),vs=variants(p),args=[{...p,variants:vs},fine,coarse,4,.35,0,S.rng(2),0,0,null,false];
  const samples={cpu:[],gpu:[]};let last;
  for(let n=0;n<4;n++){
   let t=performance.now();const cpu=S.search(...args);samples.cpu.push(performance.now()-t);
   t=performance.now();last=await gpu.candidates(args,S,2);const gpuArgs=args.slice();gpuArgs[12]=last;const result=S.search(...gpuArgs);samples.gpu.push(performance.now()-t);
   assert(cpu&&result);assert(fine.fits(result.v.fine.pm,result.x,result.y));
   for(let i=0;i<last.length;i++)for(const c of last[i])assert(coarse.overlap(vs[i].coarse.pm,c.x,c.y,2)<=2,'GPU shortlist respects the same coarse collision tolerance');
  }
  console.log('Warm placement search CPU/GPU median ms:',median(samples.cpu.slice(1)).toFixed(2),median(samples.gpu.slice(1)).toFixed(2));
  assert.equal(gpu.stats.uploads,1,'repeated searches reuse uploaded rotations');assert(gpu.stats.peakBytes<=128*1024*1024);
  assert(gpu.stats.readbackBytes<gpu.stats.candidates*4,'only shortlisted positions are read back');
  // Independent CPU scoring verifies that the reduction keeps the best 32,
  // rather than merely returning legal but inferior locations.
  for(let a=0;a<vs.length;a++){
   const c=vs[a].coarse,ring=c.contactRing,all=[];
   for(let y=0;y+c.h<=coarse.H;y++)for(let x=0;x+c.w<=coarse.W;x++){
    const inner=coarse.boxSum(x,y,x+c.w,y+c.h),ov=coarse.overlap(c.pm,x,y,2);
    if(c.w*c.h-inner<c.pm.cells-2||ov>2)continue;
    let edges=0;for(let ry=0;ry<ring.h;ry++)for(let rx=0;rx<ring.w;rx++){
     const gx=x-1+rx,gy=y-1+ry,bit=(ring.variants[0][ry*ring.words+(rx>>5)]>>>(rx&31))&1;
     if(bit&&(gx<0||gy<0||gx>=coarse.W||gy>=coarse.H||coarse.walls.get(gx,gy)))edges++;
    }
    const contact=coarse.material.overlap(ring,x-1,y-1,1e9)/Math.max(1,ring.cells);
    const s=contact+.35*edges/Math.max(1,ring.cells)+.15*S.straightEdgeAt(c,coarse,x,y,1)-.35*(x+y)/(coarse.W+coarse.H)-.05*ov;
    all.push({x,y,s});
   }
   all.sort((a,b)=>b.s-a.s);const lookup=new Map(all.map(v=>[v.y*coarse.W+v.x,v]));
   assert.equal(last[a].length,Math.min(32,all.length));assert.equal(new Set(last[a].map(v=>v.y*coarse.W+v.x)).size,last[a].length);
   for(const v of last[a]){assert(Math.abs(v.s-lookup.get(v.y*coarse.W+v.x).s)<1e-5,'GPU score matches CPU geometry');assert(v.s>=all[Math.min(31,all.length-1)].s-1e-5,'top-32 reduction');}
  }
  // Full pipeline: geometry, rotations, pinning, fill ceiling and final verification.
  const job={sheet:{wPt:100,hPt:50,insetPt:1},pieces:Array.from({length:16},(_,i)=>piece('p'+i,28+i%4*3,25+i%5*3,i%3)),angles:Array.from({length:36},(_,i)=>i*10),fineRes:2,coarseRes:.5,timeBudgetMs:5000,maxTrials:2,seed:23,maxFill:.8,gpuBenchmark:true};
  let counts;const cpu=await S.solve(job,{}),hybrid=await S.solve(job,{gpu,onMetrics:m=>counts=m});
  assert(S.verify(job,hybrid.placements,4).ok);assert(hybrid.density<=.8);assert.equal(hybrid.trials,counts.layouts);assert(counts.positions>counts.gpuPositions&&counts.gpuPositions>0);
  console.log('Full solve CPU/GPU:',JSON.stringify({cpu:{ms:cpu.elapsedMs,pieces:cpu.placements.length,fill:cpu.density},gpu:{ms:hybrid.elapsedMs,pieces:hybrid.placements.length,fill:hybrid.density},counts}));
  // Device failure cannot lose the CPU result or fabricate GPU checks.
  const failed=await S.solve({...job,maxTrials:1,timeBudgetMs:1000},{gpu:{candidates:async()=>{throw Error('device lost');}}});assert(failed.placements.length);assert.equal(failed.metrics.gpuPositions,0);
  const R=require('../../charm-nest-rose'),remnant=R.plan([{id:'old-cut',paths:[[[0,0],[18,0],[18,22],[12,22],[12,50],[0,50]]]}],100,50,null,.2).profile;
  const partialJob={...job,sheet:{...job.sheet,remnant},pieces:job.pieces.slice(0,5).map((p,i)=>i?p:{...p,pinned:{cxPt:30,cyPt:10,angle:0}}),maxTrials:1,timeBudgetMs:2500};
  const partial=await S.solve(partialJob,{gpu});assert(partial.metrics.gpuPositions>0);assert.equal(partial.placements.find(p=>p.id==='p0').cxPt,30);assert(S.verify(partialJob,partial.placements,4).ok,'GPU cannot refill removed rose-gold stock');
  const stopped=await S.solve(job,{gpu,shouldStop:()=>true});assert.equal(stopped.trials,0);assert.equal(stopped.metrics.positions,0);
  const unavailable=await G.benchmark(job,S,{createGPU:async()=>{throw Error('No adapter');}});assert.equal(unavailable.available,false);
  console.log('GPU OK: actual WGSL compute/reduction, legal shortlist, bounded readback, full solve verification, counters and device-failure fallback');
 }finally{gpu.destroy();delete globalThis.testGPU;}
})().catch(e=>{console.error(e);process.exitCode=1;});
