const assert = require('node:assert/strict');
const S = require('../../charm-nest-solver.js');
const rect = (id,w=9,h=9) => ({id,order:id,orderDate:+id+1,w,h,scale:1,bits:new Uint8Array(w*h).fill(1),areaPt2:w*h});
const base = {sheet:{wPt:100,hPt:50,insetPt:1},clearancePt:1,angles:[0,90],fineRes:1,coarseRes:1,maxFill:.74,timeBudgetMs:6000,maxTrials:8};
(async () => {
  // Amount of material varies continuously; there is no fixed half-sheet limit.
  let previous = 0;
  for (const [count,maxWidth] of [[8,25],[12,35],[20,57]]) {
    const job = {...base,pieces:Array.from({length:count},(_,i)=>rect(String(i)))};
    const result = await S.solve(job, {yield:()=>Promise.resolve()});
    assert.equal(result.placements.length,count);
    const width = Math.max(...result.placements.map(p=>p.xPt+p.wPt));
    assert(width<=maxWidth,`${count} pieces should leave a rectangular offcut; extent ${width}`);
    assert(width>previous); previous=width;
    assert(S.verify(job,result.placements,2).ok);
    assert(result.placements.every(p=>p.wPt===9 && p.hPt===9),'never shrink charms to compact a partial sheet');
    if(count===12) {
      const portrait={...job,sheet:{wPt:50,hPt:100,insetPt:1}};
      const turned=await S.solve(portrait,{yield:()=>Promise.resolve()});
      assert.equal(turned.placements.length,count);
      assert(S.stripFraction(turned.placements,portrait.sheet)<=maxWidth/100,'portrait stock retains a usable rectangular offcut');
      assert(S.verify(portrait,turned.placements,2).ok);
    }
  }
  // Pins remain exact, even when they prevent a single compact edge block.
  const pinned={...rect('0'),pinned:{cxPt:85.5,cyPt:35.5,angle:0}};
  const job={...base,pieces:[pinned,...Array.from({length:6},(_,i)=>rect(String(i+1)))]};
  const r=await S.solve(job,{yield:()=>Promise.resolve()});
  const p=r.placements.find(p=>p.id==='0');
  assert.equal(p.cxPt,pinned.pinned.cxPt); assert.equal(p.cyPt,pinned.pinned.cyPt); assert.equal(p.angle,0);
  assert(S.verify(job,r.placements,2).ok);
  const stopped=await S.solve(job,{shouldStop:()=>true}); assert.equal(stopped.rejects.length,job.pieces.length);
  // Multi-worker ties must value a useful offcut above raster-density noise.
  const block={placements:[{id:'1',xPt:1,yPt:1,wPt:24,hPt:48}],density:.399};
  const perimeter={placements:[{id:'1',xPt:1,yPt:1,wPt:98,hPt:48}],density:.4};
  assert(S.betterLayout(block,perimeter,base.sheet)); assert(!S.betterLayout(perimeter,block,base.sheet));
  assert(S.betterLayout({...perimeter,placements:[...perimeter.placements,{id:'2'}]},block,base.sheet),'piece count remains the first objective');
  console.log('Partial sheets OK: variable strip sizes, portrait stock, clearances, original sizes, pins, cancellation and worker comparison');
})().catch(e=>{console.error(e);process.exitCode=1;});
