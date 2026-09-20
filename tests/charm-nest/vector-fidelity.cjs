const assert=require('node:assert/strict'),fs=require('fs');
global.self=global;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');
const P=global.CharmNestPDF,V=require('../../charm-nest-vector.js'),E=require('../../charm-nest-export.js'),G=require('../../charm-nest-geom.js');
const rect=(x,y,w,h)=>[['m',[x,y]],['l',[x+w,y]],['l',[x+w,y+h]],['l',[x,y+h]],['h']];
const circle=(cx,cy,r)=>{const k=.5522847498*r;return [['m',[cx+r,cy]],['c',[cx+r,cy+k],[cx+k,cy+r],[cx,cy+r]],['c',[cx-k,cy+r],[cx-r,cy+k],[cx-r,cy]],['c',[cx-r,cy-k],[cx-k,cy-r],[cx,cy-r]],['c',[cx+k,cy-r],[cx+r,cy-k],[cx+r,cy]],['h']]};
const member=(subpaths,index,color=[0,0,0])=>({kind:'path',stroke:true,fill:false,closed:true,paintOp:'S',strokeRGB:color,lwPt:.2,subpaths,index,start:index*100,bbox:(p=>[Math.min(...p.map(p=>p[0])),Math.min(...p.map(p=>p[1])),Math.max(...p.map(p=>p[0])),Math.max(...p.map(p=>p[1]))])(subpaths.flatMap(s=>V.flatten(s).points))});
const area=loops=>Math.abs(loops.reduce((total,loop)=>total+loop.points.reduce((a,p,i)=>a+p[0]*loop.points[(i+1)%loop.points.length][1]-p[1]*loop.points[(i+1)%loop.points.length][0],0)/2,0));
for(const count of [1,2,6,21]){
 // One connected comb; the ring crosses 2*count sides, all belonging to this body.
 const shapes=[rect(-4,-7,8,3)];for(let i=0;i<count;i++){const x=count===1?-.2:-2.7+i*5.4/(count-1);shapes.push(rect(x,-4,.15,.9+4));}
 const body=V.boolean(shapes.map(s=>V.flatten(s).points));const outline=member(body.map(x=>V.subpath(x.points)),0),ring=member([circle(0,0,3),circle(0,0,1.65)],1,[0,1,1]);
 const charm={outline,members:[outline,ring],bbox:outline.bbox,topIndices:[0,1]};const expected=V.boolean(V.boolean(body.map(x=>x.points),[V.flatten(ring.subpaths[0]).points]).map(x=>x.points),[V.flatten(ring.subpaths[1]).points],'difference');
 const r=P.integrateRings(charm);assert.equal(r.welded,1,JSON.stringify(r));assert.equal(r.left.length,0);assert.equal(charm.outline.subpaths.length,1,'all contacts connect into one exterior');assert(!charm.members.includes(ring),'loose outer hoop removed');
 const actual=V.boolean(charm.members.flatMap(m=>m.subpaths.map(s=>V.flatten(s).points)),[],'union','evenodd');assert(Math.abs(area(actual)-area(expected))<1e-6,'exact union minus original aperture');assert.equal(P.integrateRings(charm).welded,0,'welding is idempotent');
 const view=G.backView(charm,{upAngle:90});assert.equal(G.at(view.mask,0,0),0,'aperture remains unavailable for engraving');assert.equal(G.at(view.mask,2.4,0),1,'hoop material survives');
 assert(charm.dropIndices.has(0)&&charm.dropIndices.has(1),'old cut streams are replaced');
}
const outer=rect(0,0,30,30),inner=rect(8,8,14,14);
const fill={kind:'path',layer:'Blue art',fill:true,fillRGB:[0,0,1],subpaths:[outer,inner],paintOp:'f*'};
let loops=V.filled(fill);assert.equal(loops.filter(l=>l.hole).length,1);assert.equal(area(loops),900-196);
loops=V.filled({...fill,paintOp:'f'});assert.equal(loops.length,1,'same-direction nonzero subpath is not a hole');assert.equal(area(loops),900);
const overlap={...fill,subpaths:[rect(0,0,10,10),rect(5,0,10,10)],paintOp:'f'};assert.equal(area(V.filled(overlap)),150,'overlaps fill once');
const dxf=E.dxf([fill]);assert.equal(dxf.entityCount,1);assert(dxf.text.includes('0\r\nHATCH\r\n'));assert(!dxf.text.includes('0\r\nLWPOLYLINE\r\n'),'fill boundaries never become duplicate blue cutting lines');
console.log('Vector fidelity OK: 1/2/6/21 hoop contacts, intact apertures, one connected exterior, idempotence, PDF winding, holes and solid DXF fills');
(async()=>{
 // Two disconnected body lobes are joined by a ring touching both; the entire
 // design is nested in a transformed Illustrator Form with filled blue art.
 const body=member([rect(-4,-8,3,9),rect(1,-8,3,9)],0),ring=member([circle(0,0,3),circle(0,0,1.65)],1,[0,1,1]);
 const blue={...member([rect(-3,-6,1,3)],2),stroke:false,fill:true,fillRGB:[0,0,1],paintOp:'f'};
 const doc=await PDFLib.PDFDocument.create(),page=doc.addPage([30,30]);page.node.normalize();
 page.node.addContentStream(doc.context.register(doc.context.flateStream(P.syntheticOps([body,ring,blue].map(m=>({...m,synthetic:true}))))));
 const nested=await PDFLib.PDFDocument.create(),np=nested.addPage([60,60]),form=await nested.embedPage(page, {left:-10,bottom:-10,right:10,top:10});np.drawPage(form,{x:20,y:20,width:20,height:20});
 const parsed=await P.parseSource(await nested.save(),'nested hoop');
 const outline=parsed.nested.find(p=>p.kind==='path'&&p.stroke&&p.strokeRGB[1]===0);
 const charm={outline,members:parsed.nested.filter(p=>p.kind==='path'),bbox:outline.bbox,topIndices:[outline.parent],name:'compound'};
 assert.equal(P.integrateRings(charm).welded,1);assert.equal(charm.outline.subpaths.length,1);assert(charm.dropIndices.has(outline.parent));
 const copy=await P.parseSource(await P.buildSingleCharm(charm,parsed),'joined copy'),copied=E.leaves(copy).filter(p=>!p.layer.startsWith('SHEET'));
 assert.equal(copied.filter(p=>p.fill&&p.fillRGB[2]===1).length,1,'nested blue fill survives once');
 assert.equal(copied.filter(p=>p.stroke&&p.strokeRGB[1]===1).length,0,'old cyan ring cannot survive inside its original Form');
 assert.equal(copied.filter(p=>p.stroke).length,2,'one exterior and one aperture in the written file');
 const repaired=E.productionPaths({...parsed});
 assert.equal(repaired.filter(p=>p.stroke&&p.strokeRGB[1]===1).length,0,'historical DXF export also joins the nested hoop');
 const dx=E.dxf(repaired);assert(dx.colors.includes(255),'blue RGB retained');assert(dx.text.includes('HATCH'));
 console.log('Nested export OK: compound body joined, loose Form paths removed, original blue fill retained, old sheets repaired for DXF');
})().catch(e=>{console.error(e);process.exitCode=1;});
