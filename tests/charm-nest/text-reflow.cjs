const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),cp=require('node:child_process');
const G=require('../../charm-nest-geom.js'),ot=require('../../vendor/opentype-1.3.4.min.js'),font=ot.loadSync('vendor/fonts/SourceSans3-Regular.otf');
const opts={minStrokeMm:0,minGapMm:0,tryRotated:false,maxHeightFrac:.4};
const m={w:300,h:360,res:6,ox:0,oy:0,cx:25,cy:30,wPt:50,hPt:60,bits:new Uint8Array(300*360)};
for(let y=0;y<m.h;y++)for(let x=0;x<m.w;x++){const px=(x+.5)/6,py=(y+.5)/6;m.bits[y*m.w+x]=py<28 || (px>16&&px<34)?1:0;}
const input=['ANNA    BEN'];
let f=G.reflowAt(input,font,m,opts,{centre:[25,14],size:6});assert(f.ok);assert.equal(f.lines.length,1);assert.equal(f.size,6);
f=G.reflowAt(input,font,m,opts,{centre:[25,43],size:6});assert(f.ok);assert.equal(f.lines.length,2,'narrow region wraps before shrinking');assert.equal(f.size,6);assert(G.verifyInk(f.cmds,m).ok);
f=G.reflowAt(input,font,m,opts,{centre:[25,43],size:3});assert.equal(f.lines.length,1,'smaller manual size unwraps');assert.equal(f.size,3);
f=G.reflowAt(input,font,m,opts,{centre:[25,43],size:12});assert(f.size<12,'shrink only when no wrap holds the requested size');assert(f.size>3);assert(G.verifyInk(f.cmds,m).ok);
f=G.reflowAt(input,font,m,opts,{centre:[25,14],size:12});assert.equal(f.size,12,'moving into a wider region restores the requested size');
const one=G.fitMultiline(['A + V'],font,m,opts),spaces=G.fitMultiline(['A      +   V'],font,m,opts);assert.equal(one.lines.length,1);assert.deepEqual(spaces.lines,one.lines);assert.equal(spaces.size,one.size,'multiple spaces cannot force a new line or smaller text');
const hard=['ANNA BEN','FOREVER'];f=G.reflowAt(hard,font,m,opts,{centre:[25,14],size:3});assert.deepEqual(f.lines,hard,'typed breaks survive auto reflow');
f=G.reflowAt(input,font,m,opts,{centre:[25,43],size:6},'preserve');assert.equal(f.lines.length,1,'As typed does not wrap');assert(f.size<6);
f=G.reflowAt(input,font,m,opts,{centre:[25,14],size:3},2);assert.equal(f.lines.length,2,'fixed line count stays fixed');
// Use the actual editor controls: temporary shrinking does not replace the
// requested size; a later drag/resize recovers it and updates exported lines.
const source=fs.readFileSync('charm-nest-bridge.js','utf8'),ctx={G,fontFor:()=>font,fitOpts:()=>opts,toast(){},refresh(){},reRead(){}};vm.createContext(ctx);
vm.runInContext(source.slice(source.indexOf('  function refit(job, place)'),source.indexOf('  function nudge(')),ctx);
vm.runInContext(source.slice(source.indexOf('  function resize(job, size)'),source.indexOf('  async function resplit')),ctx);
const j={lineInput:input,lines:['ANNA BEN'],fit:one,mask:m};ctx.resize(j,12);assert.equal(j.wantSize,12);ctx.refit(j,{centre:[25,43],angle:0});assert(j.fit.size<12);assert.equal(j.wantSize,12);ctx.refit(j,{centre:[25,14],angle:0});assert.equal(j.fit.size,12);assert.equal(j.text,j.lines.join('\n'));
const rect=(x,y,w,h)=>[['m',[x,y]],['l',[x+w,y]],['l',[x+w,y+h]],['l',[x,y+h]],['h']];
const path=(subpaths,rgb=[1,0,0])=>({kind:'path',closed:true,stroke:true,strokeRGB:rgb,subpaths,bbox:(pts=>[Math.min(...pts.map(p=>p[0])),Math.min(...pts.map(p=>p[1])),Math.max(...pts.map(p=>p[0])),Math.max(...pts.map(p=>p[1]))])(subpaths.flatMap(s=>s.filter(c=>c[1]).map(c=>c[1])))});
const outline=path([[['m',[0,0]],['l',[40,0]],['l',[40,55]],['l',[35,60]],['l',[0,60]],['h']]]),hole=path([rect(16,46,8,8)]),window=path([rect(16,25,8,8)]),detail=path([rect(5,20,5,5)],[0,0,1]);
const charm={outline,members:[outline,hole,window,detail,{...window}],bbox:outline.bbox};
for(const solidBack of [false,true]){
 const view=G.backView(charm,{solidBack,upAngle:90}),mask=G.engraveMask(view,{marginMm:.3});
 assert.equal(G.at(view.mask,20,50),0,'red hanging hole stays open');assert.equal(G.at(view.mask,20,29),0,'overlapping cut paths remain a hole');assert.equal(G.at(view.mask,32,22),1,'blue front engraving remains solid metal');
 assert.equal(G.at(mask,15.5,29),0,'clearance surrounds the cut-out');
 const unsafe=G.layoutLines(['+'],font,6,.18,0,[20,29]);assert(!G.verifyInk(unsafe.cmds,mask).ok,'text on a cut-out is refused');
 const noRoom=G.reflowAt(['+'],font,mask,opts,{centre:[20,29],size:6});assert(!noRoom.ok,'automatic shrinking cannot hide text in a hole');
}
const compound=path([rect(0,0,40,60),rect(16,46,8,8)]);assert.equal(G.at(G.backView({outline:compound,members:[compound],bbox:compound.bbox},{solidBack:true,upAngle:90}).mask,20,50),0,'Solid back preserves stroked compound openings');
const expanded={...outline,fill:true,stroke:false,subpaths:[rect(0,0,40,60),rect(1,1,38,58),rect(16,46,8,8)]};const v=G.backView({outline:expanded,members:[expanded],bbox:expanded.bbox},{solidBack:true,upAngle:90});assert(G.at(v.mask,20,20));assert.equal(G.at(v.mask,20,50),0,'simplifying expanded perimeter ink retains an actual inner opening');
// Verify the reused production algorithms are identical to Design Studio.
const geom=fs.readFileSync('charm-nest-geom.js','utf8');const studio=cp.execFileSync('git',['show','83f592c:shopify/assets/brites-custom-studio.js'],{encoding:'utf8',maxBuffer:8e6}),server=cp.execFileSync('git',['show','83f592c:netlify/functions/geminiImageProxy-background.js'],{encoding:'utf8',maxBuffer:8e6});
function fn(s,name){const m=new RegExp('^([ \t]*)function '+name+'\\(','m').exec(s),a=m.index,b=s.indexOf('\n'+m[1]+'}',a);return s.slice(a,b+m[1].length+2).trim().replace(/\s+/g,' ');}
for(const name of ['mkEdt1d','mkEdt'])assert.equal(fn(geom,name),fn(studio,name),name+' reused from the editor');
for(const name of ['studioLabel','studioLargestComponent','studioOuterFace','studioSpecCutMask'])assert.equal(fn(geom,name),fn(server,name),name+' reused from the material map');
console.log('Text reflow OK: whitespace, wrap/unwrap, manual size recovery, hard breaks, fixed modes, red/compound/overlapping holes, clearance and exact Design Studio masking reuse');
// Exercise the real pointer preview callback: reflow is frame-coalesced and
// visible before mouse-up, and cancellation leaves the verified job untouched.
const handlers={},frames=new Map(),paints=[];let frameId=0;
const pointerJob={lineInput:input,lineMode:'auto',lines:['ANNA BEN'],wantSize:6,mask:m,fit:G.reflowAt(input,font,m,opts,{centre:[25,14],size:6})};
const canvas={width:50,_map:{k:1,tx:(x,y)=>[x,60-y]},_box:{rotate:[-100,-100],corners:[],centrePx:[25,46]},style:{},classList:{add(){},remove(){}},setPointerCapture(){},getBoundingClientRect:()=>({left:0,top:0,width:50}),_paint:p=>paints.push(p),addEventListener:(name,fn)=>(handlers[name] ||= []).push(fn)};
const pc={G,job:pointerJob,fontFor:()=>font,fitOpts:()=>opts,card:{querySelector:()=>null},PT:72/25.4,requestAnimationFrame:fn=>{frames.set(++frameId,fn);return frameId;},cancelAnimationFrame:id=>frames.delete(id),refresh(){},resize(){},rotateTo(){},moveTo(){},toast(){}};vm.createContext(pc);const ws=source.indexOf('    const wire = bc => {'),we=source.indexOf('    const mountBack =',ws);vm.runInContext(source.slice(ws,we)+'\nthis.wire=wire;',pc);pc.wire(canvas);
const emit=(name,x,y)=>handlers[name].forEach(fn=>fn({clientX:x,clientY:y,pointerId:1,preventDefault(){}}));
emit('pointerdown',25,46);emit('pointermove',25,20);emit('pointermove',25,17);assert.equal(frames.size,1,'one calculation per animation frame');const tick=[...frames.values()][0];frames.clear();tick();assert.equal(paints.at(-1).glyphs.length,2,'narrow-region wrap is painted during the drag');assert.equal(pointerJob.lines.length,1,'provisional dragging does not silently approve new text');emit('pointercancel',25,17);
(async()=>{
 global.window=global;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');const P=global.CharmNestPDF;
 const view=G.backView(charm,{upAngle:90}),validation=vm.createContext({P,G,S:{settings:{backFileView:'asSeenFromBack'}}});const a=source.indexOf('  async function verifyBackFile('),b=source.indexOf('  /** back/back-index',a);vm.runInContext(source.slice(a,b),validation);
 async function exportAt(centre){const text=G.layoutLines(['+'],font,6,.18,0,centre),glyphs=text.glyphs.map(g=>({cmds:g.cmds.map(c=>{const o={...c};for(const k of ['','1','2'])if(o['x'+k]!=null){o['x'+k]-=view.cx;o['y'+k]-=view.cy;}return o;})}));const built=await P.buildBackFile({charm,parsed:{},cutMembers:view.cutMembers.map(c=>({...c,synthetic:true})),cx:view.cx,cy:view.cy,angleDeg:view.angleDeg,glyphs,view:'asSeenFromBack'});return validation.verifyBackFile(built.bytes,{view});}
 const unsafe=await exportAt([20,29]);assert(!unsafe.ok);assert.match(unsafe.why,/cut-out|clearance/);const safe=await exportAt([20,15]);assert(safe.ok,safe.why);console.log('Live drag and export guards OK: coalesced previews, provisional reflow, safe export accepted, cut-out intersection refused');
})().catch(e=>{console.error(e);process.exitCode=1;});
