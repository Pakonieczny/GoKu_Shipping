const assert=require('node:assert/strict'),S=require('../../charm-nest-solver.js');
const grid=new S.Grid(100,60);for(let y=0;y<60;y++)for(let x=0;x<100;x++)if(x<3||y<3||x>=97||y>=57)grid.set(x,y);grid.trackMaterial(true);
const mask=(w,h,fn=()=>true)=>({w,h,bits:Uint8Array.from({length:w*h},(_,i)=>+fn(i%w,Math.floor(i/w)))});
const long=mask(30,5),upright=mask(5,30),tip=mask(30,12,(x,y)=>y>=7 || x>=13&&x<17);
assert.equal(S.straightEdgeAt(long,grid,20,3,4),1,'the long straight side is flush with the usable cut boundary');
assert(S.straightEdgeAt(long,grid,20,3,4)>S.straightEdgeAt(upright,grid,20,3,4),'long charms orient parallel to the edge');
assert(S.straightEdgeAt(long,grid,20,3,4)>S.straightEdgeAt(tip,grid,20,3,4),'a hoop or tip alone cannot look like a full straight-side fit');
assert(S.straightEdgeAt(tip,grid,20,45,4)>S.straightEdgeAt(tip,grid,20,3,4),'rotate the protrusion away from the boundary');
assert.equal(S.straightEdgeAt(long,grid,20,20,4),0,'interior placements earn no wall credit');
assert(S.straightEdgeAt(long,grid,20,3,4)>S.straightEdgeAt(long,grid,20,5,4),'avoid needless gaps inside the allowed inset');
assert.equal(S.straightEdgeAt(long,grid.clone(),20,3,4),1,'cloned layouts retain exact stock boundaries');
assert.equal(S.edgeBandCells(long,grid,20,3,5),150,'edge occupancy measures material inside the usable boundary');
assert.equal(S.edgeBandCells(long,grid,20,20,5),0,'moving an edge charm into the interior loses its perimeter coverage');
assert.equal(S.edgeBandCells(tip,grid,20,3,5),20,'only real silhouette pixels count, not a protrusion bounding box');
const piece=(id,w,h)=>({id,order:id,w,h,scale:1,bits:new Uint8Array(w*h).fill(1),areaPt2:w*h});
(async()=>{
 // Thin strips can disappear in the coarse mask. Exercise all four walls
 // with an exact three-cell slot beside pinned material.
 for(const side of ['top','bottom','left','right']){
  const horizontal=['top','bottom'].includes(side),far=['bottom','right'].includes(side);
  const sheet={wPt:horizontal?40:30,hPt:horizontal?30:40,insetPt:2};
  const fixed={...piece('fixed',horizontal?34:23,horizontal?23:34),pinned:{cxPt:horizontal?20:far?13.5:16.5,cyPt:horizontal?(far?13.5:16.5):20,angle:0}};
  const job={sheet,pieces:[fixed,piece('filler',horizontal?30:3,horizontal?3:30)],angles:[0,90],fineRes:1,coarseRes:.5,clearancePt:0,maxFill:1,seed:1,maxTrials:1,timeBudgetMs:3000};
  const checkpoints=[];
  const result=await S.solve(job,{yield:()=>Promise.resolve(),onBest:b=>checkpoints.push(structuredClone(b))});
  assert.equal(result.placements.length,2,side);assert(S.verify(job,result.placements,2).ok);
  assert(checkpoints.every(b=>!S.betterLayout(b,result,sheet)),'boundary refinement cannot discard an earlier best');
  const pin=result.placements.find(p=>p.id==='fixed'),fill=result.placements.find(p=>p.id==='filler');assert.equal(pin.cxPt,fixed.pinned.cxPt);assert.equal(pin.cyPt,fixed.pinned.cyPt);
  assert(horizontal?fill.wPt>fill.hPt:fill.hPt>fill.wPt,'filler follows the available straight edge');
  const coordinate=horizontal?fill.yPt:fill.xPt;
  assert(far?coordinate>=24:coordinate<=3,side);
 }
 console.log('Perimeter OK: straight-side orientation, protrusion-aware fit, inset preservation, all four pinned edge slots and best-result retention');
})().catch(e=>{console.error(e);process.exitCode=1;});
