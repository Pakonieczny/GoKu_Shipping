const assert=require('node:assert/strict'),R=require('../../charm-nest-rose'),S=require('../../charm-nest-solver');
const block=(id,w,h)=>({id,w:w*2,h:h*2,scale:2,bits:new Uint8Array(w*h*4).fill(1),areaPt2:w*h,order:id,orderDate:1});
(async()=>{
 // Layout ranking: charm count, then a straight usable front, then the stock
 // inside the next green line, then contact.
 const sheet={wPt:100,hPt:50,insetPt:1};
 const layout=(n,extent,envelopePt2,contactQuality=0)=>({placements:Array.from({length:n},(_,i)=>({id:String(i),xPt:1,yPt:1,wPt:extent-1,hPt:48})),envelopePt2,contactQuality,density:.3});
 assert(S.betterLayout(layout(3,40,900),layout(3,40,1000,5),sheet),'at an equal front, less green-line stock wins over contact');
 assert(!S.betterLayout(layout(3,40,1000,5),layout(3,40,900),sheet));
 assert(!S.betterLayout(layout(3,41,800),layout(3,40,1000),sheet),'a longer front still loses, whatever its line area');
 assert(!S.betterLayout(layout(2,30,100),layout(3,40,1000),sheet),'fewer charms never win');
 assert(S.betterLayout(layout(3,40,undefined,5),layout(3,40,1000,1),sheet),'a layout without a line area falls back to contact');

 // A partial sheet whose previous green line has a deep bay.
 const prior={version:1,wPt:100,hPt:50,axis:'x',step:.5,values:Array.from({length:100},(_,i)=>i>=30&&i<70?15:45)};
 const pieces=[block('a',14,12),block('b',12,12),block('c',10,9),block('d',9,8),block('e',8,6),block('f',6,6)];
 const job={sheet:{...sheet,remnant:prior},pieces,angles:[0,90],fineRes:2,coarseRes:.5,maxFill:.8,clearancePt:0,timeBudgetMs:4000,maxTrials:4,seed:3};
 let published=0;
 const result=await S.solve(job,{onBest:b=>{if(Number.isFinite(b.envelopePt2))published++;}});
 assert.equal(result.placements.length,pieces.length);
 assert(S.verify(job,result.placements,4).ok,'no charm crosses the previous green line');
 assert(Number.isFinite(result.envelopePt2)&&result.envelopePt2>0,'partial-sheet results carry the next line area');
 assert(published>0,'published bests carry it too, so parallel workers compare alike');
 const area=pieces.reduce((n,p)=>n+p.areaPt2,0);
 assert(result.envelopePt2>=area*.9,'the line encloses every charm');
 const plan=R.plan(result.placements.map(p=>({id:p.id,paths:[[[p.xPt,p.yPt],[p.xPt+p.wPt,p.yPt],[p.xPt+p.wPt,p.yPt+p.hPt],[p.xPt,p.yPt+p.hPt]]]})),100,50,prior,.2);
 assert(Math.abs(plan.removedPt2-result.envelopePt2)<plan.removedPt2*.35,`the estimate tracks the saved contour (${result.envelopePt2|0} vs ${plan.removedPt2|0} pt²)`);
 assert(result.placements.some(p=>p.xPt<45),'the bay in the previous line is used');

 // A fresh sheet is unchanged: no line area, no line fit.
 const fresh=await S.solve({...job,sheet},{});
 assert.equal(fresh.placements.length,pieces.length);assert.equal(fresh.envelopePt2,undefined);
 console.log('Rose line fit OK: ranking order, legal placements against the previous line, published line area, contour estimate, bay use, fresh sheets unchanged');
})().catch(e=>{console.error(e);process.exitCode=1});
