const assert=require('node:assert/strict'),fs=require('fs');
global.self=global;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');
const P=CharmNestPDF,G=require('../../charm-nest-geom.js'),Fit=require('../../charm-nest-engrave-fit.js'),ot=require('../../vendor/opentype-1.3.4.min.js');
const fonts={Regular:ot.loadSync('vendor/fonts/SourceSans3-Regular.otf'),Semibold:ot.loadSync('vendor/fonts/SourceSans3-Semibold.otf')};
function load(name) { const members=JSON.parse(fs.readFileSync(__dirname+'/fixtures/'+name+'-paths.json'));const parsed={segments:members.map((m,index)=>({...m,index,start:index,end:index+1})),nested:[],pageW:80,pageH:80}; const g=P.groupCharms(parsed,{minPt:6});assert.equal(g.charms.length,1,'body and compound hoop recognized together'); const c=g.charms[0];assert.equal(c.outline.layer,'CUT','manufacturing layer selects outline, independent of paint'); const result=P.integrateRings(c);assert.deepEqual(result.left,[]);return {c,parsed,result}; }
const built=[];
for(const [name,text,body] of [['sea_turtle2','Aug.22/09',[18,15]],['middle_5903','FYB',[16,12]]]) {
 const {c,parsed,result}=load(name);const v=G.backView(c,{upAngle:90}),mask=G.engraveMask(v,{marginMm:.8});
 assert.equal(G.at(v.mask,2*v.cx-body[0],body[1]),1,'front artwork does not punch a hole through the body');
 const calc=Fit.calculate({charm:c,lines:[text],opts:{minStrokeMm:.12,minGapMm:.12,minCapMm:1.6,lineGap:.18,tryRotated:true},viewOptions:{upAngle:90},maskOptions:{marginMm:.8}},fonts,G);
 assert(calc.fit && calc.check.ok,name+' text fits on verified material');assert(calc.fit.capMm>.5,'body supports readable text, not a tiny flipper fit');
 const cuts=c.outline.subpaths.concat(P.cutLinesOf(c).flatMap(m=>m.subpaths));
 const holes=cuts.map(sub=>({poly:G.flatten({subpaths:[sub]},24)[0],sub})).sort((a,b)=>Math.abs(G.polyCentroid(a.poly).area)-Math.abs(G.polyCentroid(b.poly).area));
 const h=G.interiorPoint([holes[0].poly]);assert.equal(G.at(v.mask,2*v.cx-h[0],h[1]),0,'real hoop aperture stays open');
 const before=JSON.stringify(c.members);const paints=[];const ctx=new Proxy({set strokeStyle(v){paints.push(v);}}, {get:(o,k)=>k in o?o[k]:()=>{}});P.drawCharm(ctx,c,(x,y)=>[x,y],5);assert(paints.includes('#000'));assert(!paints.includes('rgba(190,40,40,.9)'));assert.equal(JSON.stringify(c.members),before,'preview never recolours export artwork');
 console.log(name, {capMm:+calc.fit.capMm.toFixed(2),cutMembers:v.cutMembers.length,hoopsJoined:result.welded});built.push({c,parsed,calc});
 // Arbitrary engraving colours cannot change the material mask. CUT colours may vary too.
 for(const rgb of [[1,0,0],[0,0,1],[0,0,0],[1,1,1]]) {const recoloured=structuredClone(c);for(const m of recoloured.members){m.strokeRGB=rgb;m.fillRGB=rgb;}recoloured.outline=recoloured.members.find(m=>m.layer==='CUT'&&m.bbox.join()===c.outline.bbox.join());const vv=G.backView(recoloured,{upAngle:90});assert.deepEqual(vv.mask.bits,v.mask.bits,'layer roles are independent of display colour');}
}
for(const rgb of [[1,0,0],[0,0,1],[0,0,0],[0,1,1]]) {const p={kind:'path',closed:true,stroke:true,strokeRGB:rgb};assert(P.isCutLine({...p,layer:'CUT'}));assert(!P.isCutLine({...p,layer:'ENGRAVE'}));assert(!P.isCutLine({...p,layer:'HATCH'}));assert.equal(P.isCutLine(p),G.isCutLine(p));}
const blue={kind:'path',closed:true,stroke:true,strokeRGB:[0,0,1]};assert(!G.isCutLine(blue));assert(!G.isCutLine({...blue,strokeRGB:[1,0,0]}));
// Exact vector export retains the physical hoop, artwork and manufacturing roles.
(async()=>{for(const {c,calc} of built){const out=await P.buildBackFile({charm:c,parsed:{},cutMembers:calc.view.cutMembers.map(m=>({...m,synthetic:true})),cx:calc.view.cx,cy:calc.view.cy,angleDeg:calc.view.angleDeg,glyphs:[],view:'asSeenFromBack'});const p=await P.parseSource(out.bytes,'back.ai');assert(p.segments.concat(p.nested).some(m=>G.pathRole(m)==='cut'));}console.log('Material roles OK: actual turtle/hand, colour invariance, attached hoops, safe fit and back exports');})().catch(e=>{console.error(e);process.exitCode=1;});
