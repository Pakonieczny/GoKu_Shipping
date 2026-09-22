const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../../charm-nest-solver.js'),'utf8');
const piece=(id,w,h,orderDate)=>({id,order:id,orderDate,w,h,scale:1,bits:new Uint8Array(w*h).fill(1),areaPt2:w*h});
async function run(pair=false,cancel=false,interior=false,cap=false,expireAt=0){
 let clock=0,accepted=false,stopChecks=0,clockChecks=0;
 const realm=vm.createContext({module:{exports:{}},performance:{now:()=>expireAt && accepted && ++clockChecks>=expireAt ? 1001 : clock}});
 vm.runInContext(source,realm);const S=realm.module.exports;
 const initialLayout=[{id:'left',cxPt:12,cyPt:11,angle:0},{id:'top',cxPt:27,cyPt:5,angle:0},{id:'loose',cxPt:27,cyPt:15,angle:0}];
 const pending=pair?[{...piece('pair-a',4,4,4),order:'pair'},{...piece('pair-b',4,4,4),order:'pair'}]:[piece('next',4,8,4)];
 const job={sheet:{wPt:32,hPt:22,insetPt:1},pieces:[{...piece('left',22,20,1),pinned:initialLayout[0]},{...piece('top',8,8,2),pinned:initialLayout[1]},piece('loose',4,8,3),...pending],initialLayout,angles:[0],fineRes:1,coarseRes:1,clearancePt:0,maxFill:1,seed:1,maxTrials:10,timeBudgetMs:1000};
 if(interior){
  job.sheet={wPt:42,hPt:32,insetPt:1};
  initialLayout[0].cyPt=16;job.pieces[0]={...piece('left',22,30,1),pinned:initialLayout[0]};
  for(const [id,w,h,cxPt,cyPt] of [['right',10,30,36,16],['bottom',8,10,27,26]]){
   const pin={id,cxPt,cyPt,angle:0};initialLayout.push(pin);job.pieces.push({...piece(id,w,h,1),pinned:pin});
  }
 }
 if(cap){job.maxFill=.92;job.pieces.push(piece('younger',1,1,5));}
 const history=[];
 const result=await S.solve(job,{yield:()=>Promise.resolve(),shouldStop:()=>cancel&&accepted&&++stopChecks>3,onBest:b=>{history.push(structuredClone(S.publicLayout(b)));if(b.retainedInitial){accepted=true;clock=850;}}});
 assert(accepted,'fixture starts from a verified incumbent');
 assert(S.verify(job,result.placements,2).ok);
 if(expireAt)assert([initialLayout.length,initialLayout.length+pending.length].includes(result.placements.length),'expiry retains only complete layouts');
 else assert.equal(result.placements.length,initialLayout.length+(cancel || cap?0:pending.length),'region repair must fit the next whole order or leave the incumbent intact');
 if(!cancel && !cap && !expireAt)assert.equal(result.cornerRepairs,1);
 if(cap)assert(!result.placements.some(p=>p.id==='younger'),'a smaller younger order cannot bypass a capped older order');
 for(const p of job.pieces.filter(p=>p.pinned)){const pin=p.pinned,placed=result.placements.find(q=>q.id===p.id);assert.equal(placed.cxPt,pin.cxPt);assert.equal(placed.cyPt,pin.cyPt);assert.equal(placed.angle,pin.angle);}
 for(const b of history){assert(!S.betterLayout(b,result,job.sheet),'published best never regresses');assert(b.placements.length>=3,'temporary removals are never published');if(pair)assert.equal(b.placements.some(p=>p.id==='pair-a'),b.placements.some(p=>p.id==='pair-b'),'both pieces travel together');}
}
(async()=>{await run();await run(true);await run(true,true);await run(true,false,true);await run(true,false,false,true);for(const expiry of [2,10,100])await run(true,false,false,false,expiry);console.log('Regions OK: corner and interior gaps, complete orders, FIFO, fill cap, pins, cancellation/expiry rollback and monotonic best results');})().catch(e=>{console.error(e);process.exitCode=1;});

const S=require('../../charm-nest-solver.js');
assert.deepEqual(Array.from({length:8},(_,i)=>S.constructionCorner(i)),[[0,0],[1,1],[1,0],[0,1],[0,0],[1,1],[1,0],[0,1]],'all four starts get a turn');
const grid=new S.Grid(30,30);
for(let y=0;y<30;y++)for(let x=0;x<30;x++)if(x<20 || y<20)grid.set(x,y);
assert.equal(S.regionTargets(grid)[0].x,25);assert.equal(S.regionTargets(grid)[0].y,25,'the emptiest bottom-right region is considered first');
