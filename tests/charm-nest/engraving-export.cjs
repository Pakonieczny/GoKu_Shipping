const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const G=require('../../charm-nest-geom.js'),Fit=require('../../charm-nest-engrave-fit.js'),ot=require('../../vendor/opentype-1.3.4.min.js');
global.window=global;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');
const P=global.CharmNestPDF,font=ot.loadSync('vendor/fonts/SourceSans3-Regular.otf');
const fonts={Regular:font,Semibold:ot.loadSync('vendor/fonts/SourceSans3-Semibold.otf')};
const source=fs.readFileSync('charm-nest-bridge.js','utf8'),start=source.indexOf('  async function verifyBackFile('),end=source.indexOf('  /** back/back-index',start);
const S={settings:{backFileView:'asSeenFromBack'}},ctx=vm.createContext({P,G,S});vm.runInContext(source.slice(start,end),ctx);
const rect=(x,y,w,h)=>[['m',[x,y]],['l',[x+w,y]],['l',[x+w,y+h]],['l',[x,y+h]],['h']];
const path=subpaths=>({kind:'path',layer:'CUT',closed:true,stroke:true,synthetic:true,strokeRGB:[1,0,0],subpaths,bbox:(pts=>[Math.min(...pts.map(p=>p[0])),Math.min(...pts.map(p=>p[1])),Math.max(...pts.map(p=>p[0])),Math.max(...pts.map(p=>p[1]))])(subpaths.flatMap(s=>s.filter(c=>c[1]).map(c=>c[1])))});
// An asymmetric charm's rotated bounds do not share the original raster centre.
// Previously the file check created a new grid and rejected a verified fit.
const outline=path([[['m',[0,0]],['l',[40,0]],['l',[40,55]],['l',[35,60]],['l',[0,60]],['h']]]),hangingHole=path([rect(16,46,8,8)]),opening=path([rect(16,25,8,8)]);
const charm={outline,members:[outline,hangingHole,opening],bbox:outline.bbox};
async function exported(view,fit,{cutMembers=view.cutMembers,sourceCharm=charm,mask=G.engraveMask(view,{marginMm:.8})}={}){
 const glyphs=fit.glyphs.map(g=>({cmds:g.cmds.map(c=>{const o={...c};for(const suffix of ['','1','2'])if(o['x'+suffix]!=null){o['x'+suffix]-=view.cx;o['y'+suffix]-=view.cy;}return o;})}));
 const built=await P.buildBackFile({charm:sourceCharm,parsed:{},cutMembers,cx:view.cx,cy:view.cy,angleDeg:view.angleDeg,glyphs,view:S.settings.backFileView});
 return ctx.verifyBackFile(built.bytes,{view,mask});
}
(async()=>{
 for(const fileView of ['asSeenFromBack','frontCoordinates'])for(const upAngle of [90,73,29]){
  S.settings.backFileView=fileView;
  const view=G.backView(charm,{upAngle}),mask=G.engraveMask(view,{marginMm:.8});
  const fit=G.fitText(['Aug.22/09'],font,mask,{minStrokeMm:0,minGapMm:0});
  assert(fit.ok,fit.reason);assert(G.verifyInk(fit.cmds,mask).ok);
  const safe=await exported(view,fit);assert(safe.ok,fileView+' at '+view.angleDeg+' degrees: '+safe.why);
  const transform=G.mul(view.M,view.R);
  const onHole=G.layoutLines(['O'],font,6,.18,0,G.ap(transform,20,29));
  assert(!G.verifyInk(onHole.cmds,mask).ok);const holeCheck=await exported(view,onHole);assert(!holeCheck.ok);assert.match(holeCheck.why,/cut-out|clearance/);
  const nearEdge=G.layoutLines(['O'],font,2,.18,0,G.ap(transform,1.2,10));
  assert(G.verifyInk(nearEdge.cmds,view.mask).ok,'edge fixture lies on metal');assert(!G.verifyInk(nearEdge.cmds,mask).ok,'edge fixture violates the .8 mm clearance');
  const edgeCheck=await exported(view,nearEdge);assert(!edgeCheck.ok);assert.match(edgeCheck.why,/cut-out|clearance/);
  const missingOpening=await exported(view,fit,{cutMembers:view.cutMembers.filter(m=>m!==opening)});assert(!missingOpening.ok,'a removed physical opening cannot pass export verification');assert.match(missingOpening.why,/cut geometry/);
 }
 // Real production turtle and the wording reported on order 4175254511.
 const members=JSON.parse(fs.readFileSync(__dirname+'/fixtures/sea_turtle2-paths.json'));
 const parsed={segments:members.map((m,index)=>({...m,index,start:index,end:index+1})),nested:[],pageW:80,pageH:80};
 const turtle=P.groupCharms(parsed,{minPt:6}).charms[0];P.integrateRings(turtle);
 for(const upAngle of [90,45])for(const settings of [{marginMm:.8,minStrokeMm:0,minGapMm:0},{marginMm:.8,minStrokeMm:.12,minGapMm:.12},{marginMm:.3,minStrokeMm:.15,minGapMm:.12}]){
  const {marginMm,minStrokeMm,minGapMm}=settings;
  const calc=Fit.calculate({charm:turtle,lines:['Aug.22/09'],opts:{minCapMm:1.6,maxHeightFrac:.4,lineGap:.216,minStrokeMm,minGapMm,tryRotated:true},viewOptions:{res:6,upAngle},maskOptions:{marginMm}},fonts,G);
  assert(calc.fit && calc.check.ok,'the actual turtle placement is verified');
  for(const fileView of ['asSeenFromBack','frontCoordinates']){
   S.settings.backFileView=fileView;
   const result=await exported(calc.view,calc.fit,{sourceCharm:turtle,cutMembers:calc.view.cutMembers.map(m=>({...m,synthetic:true})),mask:calc.mask});
   assert(result.ok,'turtle Aug.22/09, up '+upAngle+', margin '+marginMm+', stroke '+minStrokeMm+', '+fileView+': '+result.why);
  }
 }
 console.log('PASS: actual turtle Aug.22/09 and rotated lettering export in both coordinate modes; cut-outs, edge clearance and changed cut geometry remain protected');
})().catch(e=>{console.error(e);process.exitCode=1;});
