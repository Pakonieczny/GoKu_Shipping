const assert=require('node:assert/strict'),R=require('../../charm-nest-rose'),S=require('../../charm-nest-solver');
const piece=(id,w,h)=>({id,w:w*4,h:h*4,scale:4,bits:new Uint8Array(w*h*16).fill(1),areaPt2:w*h,order:id,orderDate:id==='old'?20:id==='new'?10:30});
(async()=>{
 const old=piece('old',8,8),fresh=piece('new',7,7),huge=piece('huge',90,90),fixed={id:'old',cxPt:8,cyPt:8,angle:0,scale:.975};
 const plan=R.plan([{id:'old',paths:[[[4,4],[12,4],[12,12],[4,12]]]}],60,40,null,.2);
 for(const rose of [false,true])for(const clearance of [0,-.5]){
  const job={sheet:{wPt:60,hPt:40,insetPt:1},pieces:[old,fresh,huge],lockedPlacements:[fixed],...(rose?{protectedRose:{...plan,placements:[fixed]}}:{}),angles:[0,90],fineRes:2,coarseRes:.5,maxFill:.8,timeBudgetMs:300,maxTrials:3,clearancePt:clearance,seed:7};
  let snapshots=0;
  const check=r=>{assert.deepEqual(r.placements.find(p=>p.id==='old'),fixed,'saved coordinates/rotation/scale never change');snapshots++;};
  const result=await S.solve(job,{onBest:check});check(result);assert(result.placements.some(p=>p.id==='new'));assert(result.rejects.includes('huge'));assert(S.verify(job,result.placements,4).ok,JSON.stringify(result));assert(snapshots>1);
  const moved=result.placements.map(p=>p.id==='old'?{...p,cxPt:9}:p);assert(!S.verify(job,moved,4).ok,'verifier rejects moved protected charms');
  assert(!S.verify(job,result.placements.filter(p=>p.id!=='old'),4).ok,'verifier rejects missing old charms');
  if(rose){const illegal=[fixed,{id:'new',cxPt:8,cyPt:8,angle:0}];assert(S.verify(job,illegal,4).removedPx>0,'new pieces cannot cross the green boundary');}
  const stopped=await S.solve(job,{shouldStop:()=>true});check(stopped);assert(stopped.rejects.includes('new'),'stop cannot remove the saved layout');
  const full=await S.solve({...job,maxFill:.01},{});check(full);assert.equal(full.placements.length,1);assert(full.rejects.includes('new'));
 }
 console.log('Protected append OK: real CPU solve, unchanged saved placements in every update, contour exclusion, negative clearance, older incoming orders, overflow, stop and full sheet');
})().catch(e=>{console.error(e);process.exitCode=1});
