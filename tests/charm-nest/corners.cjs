const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../../charm-nest-solver.js'),'utf8');
const piece=(id,w,h,orderDate)=>({id,order:id,orderDate,w,h,scale:1,bits:new Uint8Array(w*h).fill(1),areaPt2:w*h});
async function run(pair=false,cancel=false){
 let clock=0,accepted=false,stopChecks=0;
 const realm=vm.createContext({module:{exports:{}},performance:{now:()=>clock}});
 vm.runInContext(source,realm);const S=realm.module.exports;
 const initialLayout=[{id:'left',cxPt:12,cyPt:11,angle:0},{id:'top',cxPt:27,cyPt:5,angle:0},{id:'loose',cxPt:27,cyPt:15,angle:0}];
 const pending=pair?[{...piece('pair-a',4,4,4),order:'pair'},{...piece('pair-b',4,4,4),order:'pair'}]:[piece('next',4,8,4)];
 const job={sheet:{wPt:32,hPt:22,insetPt:1},pieces:[{...piece('left',22,20,1),pinned:initialLayout[0]},{...piece('top',8,8,2),pinned:initialLayout[1]},piece('loose',4,8,3),...pending],initialLayout,angles:[0],fineRes:1,coarseRes:1,clearancePt:0,maxFill:1,seed:1,maxTrials:10,timeBudgetMs:1000};
 const history=[];
 const result=await S.solve(job,{yield:()=>Promise.resolve(),shouldStop:()=>cancel&&accepted&&++stopChecks>3,onBest:b=>{history.push(structuredClone(S.publicLayout(b)));if(b.retainedInitial){accepted=true;clock=850;}}});
 assert(accepted,'fixture starts from a verified incumbent');
 assert(S.verify(job,result.placements,2).ok);
 assert.equal(result.placements.length,cancel?3:pair?5:4,'corner repair must fit the next whole order or leave the incumbent intact');
 if(!cancel)assert.equal(result.cornerRepairs,1);
 for(const pin of initialLayout.slice(0,2)){const placed=result.placements.find(p=>p.id===pin.id);assert.equal(placed.cxPt,pin.cxPt);assert.equal(placed.cyPt,pin.cyPt);assert.equal(placed.angle,pin.angle);}
 for(const b of history){assert(!S.betterLayout(b,result,job.sheet),'published best never regresses');assert(b.placements.length>=3,'temporary removals are never published');if(pair)assert.equal(b.placements.some(p=>p.id==='pair-a'),b.placements.some(p=>p.id==='pair-b'),'both pieces travel together');}
}
(async()=>{await run();await run(true);await run(true,true);console.log('Corners OK: awkward corner recovered, complete orders, pins, cancellation rollback and monotonic best results');})().catch(e=>{console.error(e);process.exitCode=1;});
