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
 // Layouts compare by charms seated first, then by grade.
 assert(S.betterLayout({placements:[1,2],rejects:[],fitScore:-50},{placements:[1,2],rejects:[],fitScore:-80},base.sheet));
 assert(!S.betterLayout({placements:[1],rejects:[2],fitScore:-5},{placements:[1,2],rejects:[],fitScore:-80},base.sheet));
 console.log('Careful append OK: saved charm fixed, new charm against its neighbour at the left, probes for the live view, notch filled, ceiling and no-room reported at once, orders kept whole, stranded charm seated, stop, several charms, grade ranking');
})().catch(e=>{console.error(e);process.exitCode=1});
