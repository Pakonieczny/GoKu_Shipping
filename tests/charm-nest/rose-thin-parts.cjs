// A charm's thin parts (a jump ring, a bail) vanish from the eroded mask the solver packs with. On a partly cut Rose
// Gold sheet they must still stay off the stock already cut away, which the verifier checks with the whole silhouette.
const assert=require('node:assert/strict');
const S=require('../../charm-nest-solver');
const SC=6;
// A 24 pt disc with a 0.6 pt wide stick reaching 9 pt out of its left side. Erosion by the negative clearance
// removes the stick from the packing mask entirely.
function stickCharm(id){
  const w=34*SC,h=24*SC,bits=new Uint8Array(w*h),cx=22*SC,cy=12*SC,r=12*SC;
  for(let y=0;y<h;y++)for(let x=0;x<w;x++)if((x+.5-cx)**2+(y+.5-cy)**2<=r*r||(x<cx&&x>=SC&&Math.abs(y+.5-cy)<=.3*SC))bits[y*w+x]=1;
  return {id,w,h,scale:SC,bits,areaPt2:bits.reduce((a,b)=>a+b,0)/(SC*SC)};
}
// Stock left of x = 60 pt is already cut on every row.
const W=150,H=60,remnant={version:1,wPt:W,hPt:H,axis:'x',step:.5,values:Array(H/.5).fill(60)};
const sheet={wPt:W,hPt:H,insetPt:1.5,remnant},clearancePt=-.5;
(async()=>{
  const charm=stickCharm('a'),v=S.prepareVariant(charm,0,clearancePt,2),grid=S.makeSheetGrid(sheet,clearancePt,2);
  const at=cx=>[Math.round(cx*2-v.solid.cx),Math.round(30*2-v.solid.cy)];
  // Centred at x = 70 the disc clears the cut by 3 pt, but its stick runs from x = 54 into the cut stock.
  assert(S.verify({sheet,clearancePt,fineRes:2,pieces:[charm]},[{id:'a',cxPt:70,cyPt:30,angle:0}],6).removedPx>0,'the verifier sees the stick in the cut stock');
  assert.equal(grid.fits(v.fine.pm,...at(70)),false,'the solver refuses a position whose thin stick lies on cut stock');
  // Moved 8 pt right the stick starts at x = 62, clear of the cut.
  assert.equal(grid.fits(v.fine.pm,...at(78)),true);
  assert.equal(S.verify({sheet,clearancePt,fineRes:2,pieces:[charm]},[{id:'a',cxPt:78,cyPt:30,angle:0}],6).removedPx,0);
  // A whole search packs such charms against the cut edge at any angle; none may put its stick on the cut stock.
  for(const seed of [1,2,3]){
    const pieces=Array.from({length:5},(_,i)=>stickCharm('c'+i));
    const job={sheet,pieces,clearancePt,angles:Array.from({length:36},(_,i)=>i*10),fineRes:2,coarseRes:.5,maxFill:.8,maxTrials:40,timeBudgetMs:1500,stallMs:60000,seed};
    const r=await S.solve(job,{}),check=S.verify(job,r.placements,6);
    assert(r.placements.length>=3,`seed ${seed}: the sheet still takes charms (${r.placements.length})`);
    assert.equal(check.removedPx,0,`seed ${seed}: a stick was placed on cut stock: ${JSON.stringify(r.placements.map(p=>[p.cxPt,p.cyPt,p.angle]))}`);
    assert(check.ok,`seed ${seed}: ${JSON.stringify(check)}`);
  }
  // A charm appended beside one already placed is held to the same rule.
  const kept={id:'k',angle:0,cxPt:130,cyPt:30};
  const job={sheet,pieces:[stickCharm('k'),stickCharm('n1'),stickCharm('n2')],clearancePt,angles:Array.from({length:36},(_,i)=>i*10),fineRes:2,coarseRes:.5,maxFill:.8,maxTrials:40,timeBudgetMs:1500,stallMs:60000,seed:5,lockedPlacements:[kept],initialLayout:[kept]};
  const r=await S.solve(job,{});
  assert.equal(S.verify(job,r.placements,6).removedPx,0,'an appended charm keeps its stick off the cut stock');
  console.log('Rose Gold thin parts OK: a thin stick the eroded mask loses still stays off stock already cut, in single tests, whole searches and appends');
})().catch(e=>{console.error(e);process.exitCode=1;});
