const assert=require('node:assert/strict'),S=require('../../charm-nest-solver');
// Square test charms: w × h pt, drawn at 4 cells per pt.
const piece=(id,w,h,date=10)=>({id,w:w*4,h:h*4,scale:4,bits:new Uint8Array(w*h*16).fill(1),areaPt2:w*h,order:id,orderDate:date});
const box=p=>({x0:p.cxPt-p.wPt/2,x1:p.cxPt+p.wPt/2,y0:p.cyPt-p.hPt/2,y1:p.cyPt+p.hPt/2});
const gap=(a,b)=>{const A=box(a),B=box(b);return Math.max(0,A.x0-B.x1,B.x0-A.x1,A.y0-B.y1,B.y0-A.y1);};
(async()=>{
 const base={sheet:{wPt:60,hPt:40,insetPt:1},angles:[0,90],fineRes:2,coarseRes:.5,maxFill:.8,timeBudgetMs:20000,maxTrials:50,clearancePt:0,seed:3,careful:true};
 // One saved charm in the corner, one new one: the new charm settles against it, left of the open sheet.
 {const old=piece('old',8,8,1),fresh=piece('new',7,7),fixed={id:'old',cxPt:5,cyPt:5,angle:0};
  const job={...base,pieces:[old,fresh],lockedPlacements:[fixed]},probes=[];
  const r=await S.solve(job,{onProbe:p=>probes.push(p)});
  assert.equal(r.placements.length,2);assert.deepEqual(r.placements.find(p=>p.id==='old'),fixed,'the saved charm never moves');
  assert(S.verify(job,r.placements,4).ok,JSON.stringify(r.placements));
  assert(Number.isFinite(r.fitScore)&&r.careful&&r.trials===1,'the graded placement seats it without restarts');
  const n=r.placements.find(p=>p.id==='new');
  assert(gap(n,{...fixed,wPt:8,hPt:8})<=1,`the new charm sits against the saved one: ${JSON.stringify(n)}`);
  assert(n.cxPt<20,'and at the left, where the sheet fills from');
  const stages=new Set(probes.map(p=>p.stage));for(const s of ['turn','try','place'])assert(stages.has(s),`the page is shown the charm at stage ${s}`);
  assert(probes.every(p=>p.id==='new'&&p.cxPt>0&&p.cxPt<60&&p.cyPt>0&&p.cyPt<40&&[0,90].includes(p.angle)),'every probe is the new charm, on the sheet, at a searched angle');
  const last=probes[probes.length-1];assert(last.stage==='place'&&last.cxPt===n.cxPt&&last.cyPt===n.cyPt,'the last probe is where it was placed');
 }
 // A notch just taller than the new charm against the left edge, open sheet to the right: the charm fills the notch
 // instead of starting a new spot beside it.
 {const a=piece('a',10,15,1),b=piece('b',10,15,2),fresh=piece('new',7,7);
  const fixed=[{id:'a',cxPt:6,cyPt:8.5,angle:0},{id:'b',cxPt:6,cyPt:31.5,angle:0}];
  const job={...base,pieces:[a,b,fresh],lockedPlacements:fixed};
  const r=await S.solve(job,{});assert(S.verify(job,r.placements,4).ok);
  const n=r.placements.find(p=>p.id==='new');
  assert(n.cyPt>16&&n.cyPt<24&&n.cxPt<12,`the charm fills the notch between the saved ones: ${JSON.stringify(n)}`);
 }
 // A hole among saved charms is space they already claimed: a charm put in it is charged its area less (Paul, 25 Sep:
 // charms tried in a big hole went "everywhere else except inside"). One at the open end claims new space.
 {const saved=[['t',120,24,63,15],['b',120,24,63,105],['l',18,66,12,60],['r',30,66,108,60],['far',60,20,153,13]];
  const fixed=saved.map(([id,,,cx,cy])=>({id,cxPt:cx,cyPt:cy,angle:0})),placed=[];
  const job={...base,sheet:{wPt:240,hPt:120,insetPt:1},pieces:[...saved.map(([id,w,h],i)=>piece(id,w,h,i)),piece('new',21,21)],lockedPlacements:fixed};
  const r=await S.solve(job,{onPlaced:p=>placed.push(p)});assert(S.verify(job,r.placements,4).ok);
  const n=r.placements.find(p=>p.id==='new'),g=placed.find(p=>p.id==='new').careful,area=21*21*(25.4/72)**2;
  assert(n.xPt>=21&&n.xPt+n.wPt<=93&&n.yPt>=27&&n.yPt+n.hPt<=93,`the charm goes in the hole: ${JSON.stringify(n)}`);
  assert(Math.abs(g.claim+area)<1,`and claims no new space, its area back: ${g.claim} vs ${-area}`);
 }
 // Rose Gold (block): the charms go on as one block from the left, the strip growing only when nothing fits behind it
 {const pieces=[piece('a',22,30,1),piece('b',26,18,2),piece('c',20,20,3),piece('d',30,14,4),piece('e',18,26,5),piece('f',24,24,6),piece('g',16,34,7)];
  const job={...base,sheet:{wPt:240,hPt:120,insetPt:1},pieces,block:true};
  const r=await S.solve(job,{});assert.equal(r.placements.length,7);assert(S.verify(job,r.placements,4).ok);
  const maxX=Math.max(...r.placements.map(p=>p.xPt+p.wPt));assert(maxX<=42,`a strip two charms wide: ${maxX}`);
 }
 // A charm held back by the fill ceiling is reported at once, with no search for it.
 {const old=piece('old',8,8,1),huge=piece('huge',70,50),fixed={id:'old',cxPt:5,cyPt:5,angle:0};
  const job={...base,pieces:[old,huge],lockedPlacements:[fixed]};
  const r=await S.solve(job,{});assert.deepEqual(r.placements,[fixed]);assert.deepEqual(r.rejects,['huge']);
  assert.equal(r.endedBy,'cap');assert.equal(r.trials,1,'no restarts');assert(r.elapsedMs<5000,'at once: '+r.elapsedMs);
 }
 // A charm with no spot at any angle is set aside and reported at once; the charm that fits is placed. The usual search
 // used to run its whole budget for it, and a full sheet searched for minutes at every later order (24 Sep).
 {const old=piece('old',8,8,1),huge=piece('huge',70,50,0),small=piece('small',7,7,0),fixed={id:'old',cxPt:5,cyPt:5,angle:0};
  const job={...base,pieces:[old,huge,small],lockedPlacements:[fixed]};
  const r=await S.solve(job,{});assert(S.verify(job,r.placements,4).ok);
  assert.deepEqual(r.placements.map(p=>p.id).sort(),['old','small']);assert.deepEqual(r.rejects,['huge']);
  assert.equal(r.endedBy,'no-room');assert.equal(r.trials,1,'no restarts');assert(r.elapsedMs<5000,'at once: '+r.elapsedMs);
 }
 // A sheet never holds part of an order: when one of its charms fits nowhere, the rest of the order is not placed either.
 {const old=piece('old',8,8,1),huge=piece('huge',70,50,0),small=piece('small',7,7,0),fixed={id:'old',cxPt:5,cyPt:5,angle:0};
  huge.order=small.order='o1';
  const probes=[],r=await S.solve({...base,pieces:[old,small,huge],lockedPlacements:[fixed]},{onProbe:p=>probes.push(p)});
  assert.deepEqual(r.placements,[fixed]);assert.deepEqual(r.rejects.sort(),['huge','small']);assert.equal(r.endedBy,'no-room');
  const placed=probes.filter(p=>p.stage==='place').map(p=>p.id),lifted=probes.filter(p=>p.stage==='lift').map(p=>p.id);
  assert(placed.every(id=>lifted.includes(id)),'a charm drawn in place is taken off again when its order leaves');
 }
 // Two holes: a narrow one at the left that fits either a or b but not both, and one at the right that fits only a.
 // Taken alone, a grades best in the left hole. Looking ahead, a is not put where it takes b's only spot: every charm
 // is seated in one pass. Without the look-ahead a goes left and strands b; the second pass seats the stranded charm
 // first, so every charm is placed all the same.
 for(const lookAhead of [true,false]){const blocks=[['r1',54.5,32,31.75,17],['r2',3.5,30,2.75,24],['r3',48.5,6,28.75,36]].map(([id,w,h,cx,cy])=>({p:piece(id,w,h,0),at:{id,cxPt:cx,cyPt:cy,angle:0}}));
  const a=piece('a',2,2,0),b=piece('b',3,7.5,0);
  const job={...base,maxFill:1,lookAhead,fitWeights:{along:20},pieces:[...blocks.map(x=>x.p),a,b],lockedPlacements:blocks.map(x=>x.at)};
  const r=await S.solve(job,{});assert(S.verify(job,r.placements,4).ok,JSON.stringify(r.placements));
  assert.deepEqual(r.rejects,[],'every charm seated: '+JSON.stringify(r.placements));assert.equal(r.careful.passes,lookAhead?1:2,'passes with lookAhead '+lookAhead);
  assert(r.placements.find(p=>p.id==='b').cxPt<5&&r.placements.find(p=>p.id==='a').cxPt>50,'b in the left hole, a in the right');
 }
 // Stopped at once: only the saved layout comes back.
 {const old=piece('old',8,8,1),fresh=piece('new',7,7),fixed={id:'old',cxPt:5,cyPt:5,angle:0};
  const r=await S.solve({...base,pieces:[old,fresh],lockedPlacements:[fixed]},{shouldStop:()=>true});
  assert.deepEqual(r.placements,[fixed]);assert(r.rejects.includes('new'));
 }
 // Several new charms: all seated, each graded against the ones placed before it, none overlapping.
 {const old=piece('old',12,12,1),news=[piece('n1',9,6),piece('n2',6,6),piece('n3',8,5),piece('n4',5,9)],fixed={id:'old',cxPt:7,cyPt:7,angle:0};
  const job={...base,pieces:[old,...news],lockedPlacements:[fixed]};
  const r=await S.solve(job,{});
  assert.equal(r.placements.length,5);assert(S.verify(job,r.placements,4).ok,JSON.stringify(r.placements));assert(r.trials===1);
  assert(Math.max(...r.placements.filter(p=>p.id!=='old').map(p=>box(p).x1))<40,'four small charms stay at the left end with the saved one: '+JSON.stringify(r.placements));
 }
 // Saved charms leave one 9-pt hole (top left) and a 3-pt corridor: only one of two new charms fits. The older order is
 // seated and the younger one waits, whichever grades better (the look-ahead used to give the hole to the younger).
 const holeAndCorridor=[['r1',10,1,56,39],['r2',1,10,10,39]].map(([id,x0,y0,x1,y1])=>({p:piece(id,x1-x0,y1-y0,0),at:{id,cxPt:(x0+x1)/2,cyPt:(y0+y1)/2,angle:0}}));
 for(const [aw,bw] of [[8,8.5],[8.5,8],[7,8.8],[8.8,7]]){const A=piece('A',aw,aw,1),B=piece('B',bw,bw,2);
  const job={...base,maxFill:1,pieces:[...holeAndCorridor.map(x=>x.p),A,B],lockedPlacements:holeAndCorridor.map(x=>x.at)};
  const r=await S.solve(job,{});assert(S.verify(job,r.placements,4).ok);
  assert.deepEqual(r.placements.filter(p=>p.id.length===1).map(p=>p.id),['A'],`the older order is seated (A ${aw} pt, B ${bw} pt)`);assert.deepEqual(r.rejects,['B']);assert.equal(r.endedBy,'no-room');
 }
 // One 21-pt hole takes any one of X or the two charms of order O, not both of O: O leaves whole, and X, turned away while
 // O held the hole, is tried again and seated.
 {const bl=[['rA',22,1,30,39],['r2',1,22,22,39],['rC',30,1,45,39]].map(([id,x0,y0,x1,y1])=>({p:piece(id,x1-x0,y1-y0,0),at:{id,cxPt:(x0+x1)/2,cyPt:(y0+y1)/2,angle:0}}));
  for(const [xs,o1s,o2s] of [[14.5,15.5,15],[14.5,15,15.5],[15.5,14.5,15]]){const X=piece('X',xs,xs,0),O1=piece('O1',o1s,o1s,0),O2=piece('O2',o2s,o2s,0);O1.order=O2.order='O';
   const job={...base,maxFill:.8,pieces:[...bl.map(x=>x.p),X,O1,O2],lockedPlacements:bl.map(x=>x.at)};
   const r=await S.solve(job,{});assert(S.verify(job,r.placements,4).ok);
   assert.deepEqual(r.placements.filter(p=>!p.id.startsWith('r')).map(p=>p.id),['X'],`X is seated once O leaves (X ${xs}, O ${o1s}/${o2s})`);assert.deepEqual([...r.rejects].sort(),['O1','O2']);assert.equal(r.endedBy,'no-room');
  }
 }
 // A stop during the second pass (for a charm the first pass stranded) keeps the first pass: the charm it seated stays.
 {const mk=()=>({...base,maxFill:1,timeBudgetMs:180000,pieces:[...holeAndCorridor.map(x=>x.p),piece('A',8,8,0),piece('B',8.5,8.5,0)],lockedPlacements:holeAndCorridor.map(x=>x.at)});
  const full=await S.solve(mk(),{});const seated=full.placements.filter(p=>!p.id.startsWith('r')).map(p=>p.id);assert.equal(seated.length,1);assert.equal(full.careful.passes,2);
  let stop=false,lifts=0;const r=await S.solve(mk(),{shouldStop:()=>stop,onProbe:p=>{if(p.stage==='lift'&&!lifts++)stop=true;}});
  assert(lifts>0,'the second pass started');assert.deepEqual(r.placements.filter(p=>!p.id.startsWith('r')).map(p=>p.id),seated,'the first pass is kept');assert.equal(r.endedBy,'stopped');assert(S.verify(mk(),r.placements,4).ok);
 }
 // The probes that score pockets keep their size at every angle, shrunk or grown like a piece (drawn up from 1 px/pt by
 // resampling, 3 cells in 4 stayed empty and a probe shrunk for a negative clearance vanished at most angles).
 for(const [clearancePt,erodeFine,halfGapFine] of [[-1,1,0],[0,0,0],[1,0,1]]){const vs=S.probeVariants(2,erodeFine,halfGapFine);
  assert.equal(vs.length,S.PROBES.length);
  vs.forEach((list,k)=>{const cells=list.map(v=>v.cells),[w,h,b]=S.PROBES[k];assert.equal(list.length,8,`probe ${k} at every 45° step`);
   assert(Math.min(...cells)>=.8*Math.max(...cells),`probe ${k} keeps its size when turned: ${cells}`);
   // the same charm as a real piece, drawn at 4 px/pt and prepared like any other
   const src=S.bitsFromBase64(b,w*h),bits=new Uint8Array(w*h*16);for(let y=0;y<h*4;y++)for(let x=0;x<w*4;x++)bits[y*w*4+x]=src[(y>>2)*w+(x>>2)];
   const v=S.prepareVariant({id:'probe',w:w*4,h:h*4,scale:4,bits,areaPt2:w*h},0,clearancePt,2);
   assert(Math.abs(cells[0]-v.fine.pm.cells)<=.15*v.fine.pm.cells,`probe ${k} is the size of the charm it stands for: ${cells[0]} vs ${v.fine.pm.cells} cells (clearance ${clearancePt})`);});
 }
 {const small=S.smallProbeVariants(2,1,0);assert.equal(small.length,S.SMALL_PROBES*24,'the smallest charms at every 15° step');assert(small.every(v=>v.fine.pm&&v.fine.w>0));}
 // Room check (Gold and Silver): does one of the shop's smallest charms still fit anywhere on the sheet? A sheet whose one
 // hole is smaller reports no room, and is released without waiting for later orders to try its gaps.
 {const withHole=(W,H)=>[['r1',1+W,1,59,39],['r2',1,1+H,1+W,39]].map(([id,x0,y0,x1,y1])=>({p:piece(id,x1-x0,y1-y0,0),at:{id,cxPt:(x0+x1)/2,cyPt:(y0+y1)/2,angle:0}}));
  const strip=piece('strip',50,3,5),room=async(W,H,roomCheck=true)=>{const bl=withHole(W,H),job={...base,maxFill:1,roomCheck,pieces:[...bl.map(x=>x.p),strip],lockedPlacements:bl.map(x=>x.at)};const r=await S.solve(job,{});assert.deepEqual(r.rejects,['strip']);return r.smallRoom;};
  assert.equal(await room(32,24),true,'a hole a small charm fits');
  assert.equal(await room(12,10),false,'a hole smaller than the smallest charms');
  assert.equal(await room(32,24,false),undefined,'not asked, not checked');
 }
 // Out of time, the charms already placed stay: a time-out used to lift every charm of an order younger than one not yet
 // tried, and a 60 s ceiling at 2° on a busy machine left a sheet of 17 charms empty (24 Sep). One at a time, the ceiling
 // also grows with the charms waiting, so a batch longer than the usual ceiling is placed whole.
 {
  const fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');let clock=0;
  const realm=vm.createContext({module:{exports:{}},require:m=>require(path.join(__dirname,'../..',m)),performance:{now:()=>clock}});
  vm.runInContext(fs.readFileSync(path.resolve(__dirname,process.env.CN_SOLVER||'../../charm-nest-solver.js'),'utf8'),realm);
  const T=realm.module.exports,fast={yield:()=>Promise.resolve()};
  // the oldest order is the largest, so a smaller, younger one goes in first
  const pcs=()=>[piece('o1',14,14,1),piece('o2',4,4,2),piece('o3',5,5,3),piece('o4',6,6,4)];
  const placed=[];clock=0;
  const r=await T.solve({...base,timeBudgetMs:1000,pieces:pcs()},{...fast,onPlaced:p=>{placed.push(p.id);clock=1e9;}});
  assert.equal(r.endedBy,'budget');assert.equal(placed.length,1);
  assert.notEqual(placed[0],'o1','a younger order went in while the oldest was still waiting');
  assert.deepEqual([...r.placements].map(p=>p.id),placed,'the charm placed before time ran out stays on the sheet');
  assert.deepEqual([...r.rejects].sort(),pcs().map(p=>p.id).filter(id=>id!==placed[0]));
  // each step costs 0.4 s of this clock: the whole batch takes far longer than the 1 s ceiling and is still placed whole
  clock=0;
  const r2=await T.solve({...base,timeBudgetMs:1000,pieces:pcs()},{yield:()=>{clock+=400;return Promise.resolve();}});
  assert.equal(r2.endedBy,'complete');assert.equal(r2.placements.length,4);assert(clock>1000,'the batch ran past the usual ceiling: '+clock);
 }
 // Layouts compare by charms seated first, then by grade.
 assert(S.betterLayout({placements:[1,2],rejects:[],fitScore:-50},{placements:[1,2],rejects:[],fitScore:-80},base.sheet));
 assert(!S.betterLayout({placements:[1],rejects:[2],fitScore:-5},{placements:[1,2],rejects:[],fitScore:-80},base.sheet));
 console.log('Careful append OK: saved charm fixed, new charm against its neighbour at the left, probes for the live view, notch filled, ceiling and no-room reported at once, orders kept whole, stranded charm seated, stop, several charms, oldest order first, retry after an order leaves, time-out keeps what is placed, stop keeps the first pass, solid probes, room check, grade ranking');
})().catch(e=>{console.error(e);process.exitCode=1});
