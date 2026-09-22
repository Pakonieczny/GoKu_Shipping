const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const S=require('../../charm-nest-solver'),L=require('../../charm-nest-lookahead'),G=require('../../charm-nest-gpu');
const piece=(id,w,h,notch=false)=>({id,w,h,scale:1,bits:Uint8Array.from({length:w*h},(_,i)=>+( !notch || i%w<w*.5 || Math.floor(i/w)>h*.5))});
function prepare(p){const variants=[0,90,180,270].map(angle=>{const v=S.prepareVariant(p,angle,0,2),c=S.resample(v.fine.bits,v.fine.w,v.fine.h,2,.5),r=S.ring(v.fine.bits,v.fine.w,v.fine.h,4);return {...v,coarse:{...c,pm:S.packShifted(c.bits,c.w,c.h)},ringFine:S.packShifted(r.bits,r.w,r.h),ringPad:4};});return {...p,variants,footprintCells:Math.min(...variants.map(v=>v.cells))};}
// Slow CPU oracle for the planner contract. WGSL itself is exercised when the
// optional native Dawn runtime is supplied; no synthetic GPU speed is claimed.
const oracle={candidates:async([p,f,c,ratio,,,,,,strip])=>p.variants.map(v=>{
 const out=[];for(let y=0;y+v.coarse.h<=c.H;y++)for(let x=0;x+v.coarse.w<=c.W;x++){
  if(c.overlap(v.coarse.pm,x,y,2)>2)continue;
  out.push({x,y,s:-(strip?Math.max(strip.end/ratio,x+v.coarse.w)*4:0)-.35*(x+y)/(c.W+c.H)});
 }
 return out.sort((a,b)=>b.s-a.s).slice(0,32);
})};
(async()=>{
 let nativeGPU,gpu=oracle;
 if(process.env.WEBGPU_TEST_MODULE){const {create,globals}=await import(process.env.WEBGPU_TEST_MODULE);Object.assign(globalThis,globals);nativeGPU=globalThis.lookaheadTestGPU=create([]);gpu=await G.create({gpu:nativeGPU,allowSoftware:true});}
 try{
 const pieces=Array.from({length:7},(_,i)=>piece('p'+i,9+i%3,7+i%2,i%2===1)),prepared=pieces.map(prepare);
 const sheet={wPt:60,hPt:40,insetPt:1},fine=S.makeSheetGrid(sheet,0,2).trackMaterial(true);fine.buildSAT();
 const coarse=S.coarseFromFine(fine,4).trackMaterial();coarse.buildSAT();
 const metrics={layouts:0,searches:0,positions:0,gpuPositions:0,groups:0,branches:0,pruned:0};
 const v=prepared[0].variants[0];assert.equal(L.uniqueVariants([v,{...v,angle:360}]).length,1);
 const ctx={gpu,pieces:prepared,remaining:prepared,fine,coarse,ratio:4,placedCells:0,maxCells:fine.freeCells()*.8,strip:{axis:'x',end:2,weight:4},deadline:performance.now()+15000,metrics};
 const moves=await L.plan(ctx,S);assert.equal(moves.length,7,'complete seven-charm group');assert.equal(new Set(moves.map(m=>m.p.id)).size,7);
 const placed=moves.map(m=>({id:m.p.id,angle:m.v.angle,cxPt:(m.x+m.v.solid.cx)/2,cyPt:(m.y+m.v.solid.cy)/2,xPt:m.x/2,yPt:m.y/2,wPt:m.v.solid.w/2,hPt:m.v.solid.h/2}));
 const job={sheet,pieces,angles:[0,90,180,270],fineRes:2,coarseRes:.5,maxFill:.8,maxTrials:4,timeBudgetMs:10000,gpuBenchmark:true};
 assert(S.verify(job,placed,4).ok);assert(metrics.groups>0&&metrics.branches>metrics.groups&&metrics.pruned>0);
 assert.equal(fine.parts.length,0,'speculative branches never alter the live grid');
 const cancelled=await L.plan({...ctx,shouldStop:()=>true},S);assert.deepEqual(cancelled,[]);
 const capped=await L.plan({...ctx,maxCells:prepared[0].footprintCells,deadline:performance.now()+2000},S);assert.equal(capped.length,0,'never commit a one-piece or over-cap group');

 // Inspect every trial's actual placements, not just best-only callbacks.
 const source=fs.readFileSync('charm-nest-solver.js','utf8').replace('completedTrials:metrics.layouts, completed:', 'layoutAudit:placements.map(p=>[p.id,p.angle,p.cxPt,p.cyPt]).sort((a,b)=>a[0].localeCompare(b[0])), completedTrials:metrics.layouts, completed:');
 const env=vm.createContext({module:{exports:{}},require:require('node:module').createRequire(require.resolve('../../charm-nest-solver')),performance,setTimeout,clearTimeout,console,Uint8Array,Uint32Array,Int32Array,Int16Array,Float32Array});vm.runInContext(source,env);const traced=env.module.exports,rows=[];
 const result=await traced.solve(job,{gpu,groupSearch:c=>L.plan(c,traced),onTrial:r=>rows.push(r)});
 const completed=rows.filter(r=>r.completed),different=new Set(completed.map(r=>JSON.stringify(r.layoutAudit)));
 assert.equal(result.trials,completed.length);assert(different.size>1,'successive trials really explore different arrangements');
 assert(rows.some(r=>r.groupTrial)&&rows.some(r=>!r.groupTrial),'group search competes with the existing construction');
 assert(result.metrics.groups>0,'integrated solver evaluates complete multi-charm plans');
 assert(S.verify(job,result.placements,4).ok);assert(result.placements.length===7);
 const R=require('../../charm-nest-rose'),remnant=R.plan([{id:'cut',paths:[[[0,0],[12,0],[12,18],[8,18],[8,40],[0,40]]]}],60,40,null,.2).profile;
 const partialJob={...job,sheet:{...sheet,remnant},pieces:pieces.map((p,i)=>i?p:{...p,pinned:{cxPt:20,cyPt:8,angle:0}})};
 let partialGroupSize=0;const partial=await S.solve(partialJob,{gpu,groupSearch:async c=>{const moves=await L.plan(c,S);partialGroupSize=Math.max(partialGroupSize,moves.length);return moves;}});
 assert(partialGroupSize>=2,'partial-sheet construction consumes multi-charm plans');assert(S.verify(partialJob,partial.placements,4).ok,'group search respects removed stock');
 assert.equal(partial.placements.find(p=>p.id==='p0').cxPt,20,'group search cannot move a pin');
 const ordered={...job,sheet:{wPt:32,hPt:24,insetPt:1},maxFill:.65,pieces:pieces.map((p,i)=>({...p,order:'order'+Math.floor(i/2),orderDate:Math.floor(i/2)+1}))};
 const packed=await S.solve(ordered,{gpu,groupSearch:c=>L.plan(c,S)}),ids=new Set(packed.placements.map(p=>p.id));
 assert(S.verify(ordered,packed.placements,4).ok);assert(packed.density<=.65);
 for(let i=0;i<6;i+=2)assert.equal(ids.has('p'+i),ids.has('p'+(i+1)),'orders stay whole');
 const firstMissing=ordered.pieces.find(p=>!ids.has(p.id));if(firstMissing)assert(ordered.pieces.filter(p=>ids.has(p.id)).every(p=>p.orderDate<firstMissing.orderDate),'younger orders cannot bypass a waiting order');
 console.log(JSON.stringify({backend:nativeGPU?'WGSL software adapter':'CPU oracle',completedTrials:completed.length,differentLayouts:different.size,groupPlans:result.metrics.groups,branches:result.metrics.branches,pruned:result.metrics.pruned,verified:true}));
 }finally{gpu.destroy?.();delete globalThis.lookaheadTestGPU;}
})().catch(e=>{console.error(e);process.exitCode=1;});
