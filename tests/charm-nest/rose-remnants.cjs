const assert=require('node:assert/strict');
const R=require('../../charm-nest-rose'),S=require('../../charm-nest-solver');
const shape=(id,x,y,w,h)=>({id,paths:[[[x,y],[x+w,y],[x+w,y+h],[x,y+h]]]});
(async()=>{
 const O=require('../../charm-nest-orders'),verified={material:'rose',verified:true,placed:7};
 assert(O.sheetRelease(verified,{seq:1,selected:{rose:true}}).include,'explicit inclusion overrides the former even-set cadence');
 assert(!O.sheetRelease(verified,{seq:2,selected:{rose:false}}).include);
 assert(O.sheetRelease(verified,{seq:2,selected:{}}).include,'existing automatic cadence remains the default');
 assert(!O.sheetRelease({...verified,dirty:true},{seq:1,selected:{rose:true}}).include);

 const first=R.plan([shape('a',2,2,10,15),shape('b',2,22,16,23)],100,50,null,.2);
 assert.equal(first.profile.axis,'x');assert(first.profile.values[60]>first.profile.values[12]);
 assert(first.remainingPt2>4000,'contour retains the indentation instead of a rectangular strip');
 const second=R.plan([shape('c',22,3,12,30)],100,50,first.profile,.2);
 assert(second.profile.values.every((v,i)=>v>=first.profile.values[i]),'removed stock never returns');
 assert(second.remainingPt2<first.remainingPt2);
 assert.deepEqual(R.plan(first.shapes,100,50,first.profile,.2).lines,[],'already cut contours are not repeated');
 const g=S.makeSheetGrid({wPt:100,hPt:50,insetPt:1,remnant:first.profile},0,2);
 assert(g.get(5*2,8*2));assert(!g.get(25*2,8*2));assert(g.usableCells<100*50*4);
 const bitmap={id:'piece',w:6,h:6,scale:1,bits:new Uint8Array(36).fill(1),areaPt2:36};
 const job={sheet:{wPt:100,hPt:50,insetPt:1,remnant:first.profile},pieces:[bitmap],fineRes:2,coarseRes:.5,angles:[0,90],clearancePt:0,maxFill:1,maxTrials:2,timeBudgetMs:200,seed:4};
 assert.equal(S.verify(job,[{id:'piece',cxPt:6,cyPt:8,angle:0}],2).ok,false);
 assert.equal(S.verify({...job,clearancePt:-1},[{id:'piece',cxPt:6,cyPt:8,angle:0}],2).ok,false,'negative clearance cannot restore cut material');
 assert.equal(S.verify(job,[{id:'piece',cxPt:30,cyPt:8,angle:0}],2).ok,true);
 const result=await S.solve(job,{});assert.equal(result.placements.length,1);assert(S.verify(job,result.placements,4).ok);
 const portrait=R.plan([shape('d',2,2,40,8)],50,100,{version:1,wPt:50,hPt:100,axis:'y',step:.5,values:Array(100).fill(0)},.2);assert.equal(portrait.profile.axis,'y');
 assert(S.makeSheetGrid({wPt:50,hPt:100,insetPt:1,remnant:portrait.profile},0,2).get(20,10));
 assert.throws(()=>R.validate(first.profile,101,50),/dimensions/);
 assert.throws(()=>R.plan([shape('bad',99,2,5,5)],100,50),/outside/);
 const outline={subpaths:[[['m',[0,0]],['c',[0,10],[10,10],[10,0]],['l',[0,0]],['h']]]};
 const paths=R.flatten(outline);assert(paths[0].length>20);const tr=R.shapes([{id:'v',outline,centerPt:[5,5],members:[]}],[{id:'v',cxPt:30,cyPt:30,angle:90,scale:1}]);assert.deepEqual(tr[0].paths[0][0],[25,25]);
 // Production export contains only the current separation line, with exact
 // top-left -> PDF bottom-left conversion and an explicit laser layer.
 global.self=global;global.CharmNestRose=R;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');
 const E=require('../../charm-nest-export.js'),doc=await PDFLib.PDFDocument.create();doc.addPage([100,50]).drawLine({start:{x:50,y:20},end:{x:51,y:21},thickness:.1});const front=await doc.save();
 const out=await E.compose(front,{metal:'rose',roseStockId:'stock-test',rosePlanJson:JSON.stringify(second)},[]);
 const parsed=await CharmNestPDF.parseSource(out.ai,'cut'),lines=E.leaves(parsed).filter(p=>p.layer==='ROSE SEPARATION CUT');
 assert(lines.length>0);assert(lines.every(p=>p.layer==='ROSE SEPARATION CUT'));
 const xy=lines.flatMap(p=>p.subpaths.flatMap(s=>s.filter(o=>o[1]).map(o=>o[1])));
 const expected=second.lines.flatMap(path=>path.map(([x,y])=>[x,50-y]));
 assert.equal(xy.length,expected.length);xy.forEach((p,i)=>p.forEach((v,j)=>assert(Math.abs(v-expected[i][j])<.001)));
 assert(E.dxf(E.productionPaths(parsed)).text.includes('ROSE SEPARATION CUT'));
 await assert.rejects(()=>E.compose(front,{metal:'rose',roseStockId:'stock-test'},[]),/Prepare/);
 console.log('Rose remnants OK: conservative contours, successive cuts, both axes, solver exclusion, independent verification, scaled curves and exact AI/DXF layer');
})().catch(e=>{console.error(e);process.exit(1)});
