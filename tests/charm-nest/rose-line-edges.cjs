const assert=require('node:assert/strict'),fs=require('fs');
global.self=global;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');
const P=CharmNestPDF,Rose=require('../../charm-nest-rose'),create=require('../../netlify/functions/_charmNestRoseStock'),Readiness=require('../../charm-nest-readiness');
const store=new Map(),clone=x=>structuredClone(x);
function ref(path){return {path,id:path.split('/').at(-1),collection:n=>query(path+'/'+n),get:async()=>snap(path)};}
function snap(path){return {id:path.split('/').at(-1),ref:ref(path),exists:store.has(path),data:()=>clone(store.get(path))};}
function query(path,filters=[],order=null,limit=Infinity,after=null){return {doc:id=>ref(path+'/'+id),where:(...f)=>query(path,[...filters,f],order,limit,after),orderBy:(...o)=>query(path,filters,o,limit,after),limit:n=>query(path,filters,order,n,after),startAfter:n=>query(path,filters,order,limit,n),get:async()=>{let docs=[...store.keys()].filter(k=>k.startsWith(path+'/')&&!k.slice(path.length+1).includes('/')).map(snap);docs=docs.filter(d=>filters.every(([f,op,v])=>d.data()[f]===v));if(order)docs.sort((a,b)=>(a.data()[order[0]]-b.data()[order[0]])*(order[1]==='desc'?-1:1));if(after!==null)docs=docs.filter(d=>d.data()[order[0]]<after);docs=docs.slice(0,limit);return {docs,size:docs.length};}};}
let serial=Promise.resolve();const db={runTransaction:fn=>{const promise=serial.then(async()=>{const writes=[];let wrote=false;const result=await fn({get:async r=>{assert(!wrote,'Firestore requires all reads before writes');return r.get();},set:(r,v,opts)=>{wrote=true;writes.push(()=>store.set(r.path,opts?.merge?{...store.get(r.path),...clone(v)}:clone(v)));},update:(r,v)=>{wrote=true;writes.push(()=>store.set(r.path,{...store.get(r.path),...clone(v)}));},delete:r=>{wrote=true;writes.push(()=>store.delete(r.path));}});writes.forEach(f=>f());return result;});serial=promise.catch(()=>{});return promise;}};
const api=create({db,col:query,FV:{serverTimestamp:()=>123456},Readiness});
// Real production artwork, placed in columns the way the solver packs them:
// the top and bottom charms of each column sit right on the sheet's inset.
function load(name){const members=JSON.parse(fs.readFileSync(__dirname+'/fixtures/'+name+'-paths.json'));const parsed={segments:members.map((m,index)=>({...m,index,start:index,end:index+1})),nested:[],pageW:80,pageH:80};const c=P.groupCharms(parsed,{minPt:6}).charms[0];P.integrateRings(c);c.centerPt ||= [(c.bbox[0]+c.bbox[2])/2,(c.bbox[1]+c.bbox[3])/2];return c;}
const art=[load('sea_turtle2'),load('middle_5903')];
const W=100/Rose.MM,H=50/Rose.MM,EDGE=Rose.EDGE_PT;
const bounds=s=>{let x0=Infinity,y0=Infinity,x1=-Infinity,y1=-Infinity;for(const p of s.paths)for(const [x,y] of p){x0=Math.min(x0,x);y0=Math.min(y0,y);x1=Math.max(x1,x);y1=Math.max(y1,y);}return [x0,y0,x1,y1];};
function columns(inset,count){
  const charms=[],placements=[];
  for(let i=0;i<count;i++){const col=Math.floor(i/8),row=i%8;charms.push({...art[i%2],id:'c'+i});placements.push({id:'c'+i,cxPt:14+col*26,cyPt:12+row*17.5,angle:(i*37)%360,scale:.5});}
  const shapes=Rose.shapes(charms,placements);
  shapes.forEach((s,i)=>{const [x0,y0,,y1]=bounds(s),row=i%8,dy=row===0?inset-y0:row===7?H-inset-y1:0,dx=x0<inset?inset-x0:0;s.paths=s.paths.map(p=>p.map(([x,y])=>[x+dx,y+dy]));s.ink=[];});
  return shapes;
}
// Segments that run along the sheet's edge with unused stock between them and
// the edge: the thin strip a green line must never leave behind.
function edgeRuns(lines,profile){
  const used=(x,y)=>{const [d,t]=profile.axis==='x'?[x,y]:[y,x],i=Math.min(profile.values.length-1,Math.floor(t/profile.step));return d<profile.values[i];};
  const out=[];
  for(const path of lines)for(let i=1;i<path.length;i++){const a=path[i-1],b=path[i],mx=(a[0]+b[0])/2,my=(a[1]+b[1])/2;
    if(a[1]===b[1]&&a[0]!==b[0]&&a[1]>0&&a[1]<H&&(a[1]<=EDGE||a[1]>=H-EDGE)&&!used(mx,a[1]<=EDGE?a[1]/2:(a[1]+H)/2))out.push(['horizontal',a,b]);
    if(a[0]===b[0]&&a[1]!==b[1]&&a[0]>0&&a[0]<W&&(a[0]<=EDGE||a[0]>=W-EDGE)&&!used(a[0]<=EDGE?a[0]/2:(a[0]+W)/2,my))out.push(['vertical',a,b]);}
  return out;
}
function distance(p,a,b){const dx=b[0]-a[0],dy=b[1]-a[1],l=dx*dx+dy*dy,t=l?Math.max(0,Math.min(1,((p[0]-a[0])*dx+(p[1]-a[1])*dy)/l)):0;return Math.hypot(p[0]-a[0]-t*dx,p[1]-a[1]-t*dy);}
// The closest any green line comes to any charm outline.
function clearance(lines,shapes){
  let min=Infinity;
  for(const line of lines)for(let i=1;i<line.length;i++){const a=line[i-1],b=line[i];
    for(const s of shapes)for(const path of s.paths)for(let j=0;j<path.length;j++){const c=path[j],d=path[(j+1)%path.length];
      min=Math.min(min,distance(c,a,b),distance(d,a,b),distance(a,c,d),distance(b,c,d));}}
  return min;
}
// Every row of the final frontier that is not at the sheet's edge is traced by a line.
function traced(profile,lines){
  const on=(d,t)=>lines.some(path=>path.some((a,i)=>{const b=path[i+1];if(!b)return false;const [ad,at]=profile.axis==='x'?a:[a[1],a[0]],[bd,bt]=profile.axis==='x'?b:[b[1],b[0]];return Math.abs(ad-d)<1e-6&&Math.abs(bd-d)<1e-6&&Math.min(at,bt)<=t&&t<=Math.max(at,bt);}));
  const depth=profile.axis==='x'?profile.wPt:profile.hPt,len=profile.axis==='x'?profile.hPt:profile.wPt;
  return profile.values.every((v,i)=>v<=0||v>=depth||on(v,Math.min(len,(i+.5)*profile.step)));
}
const top=lines=>{const pts=lines.flat(),y=Math.min(...pts.map(p=>p[1]));return pts.filter(p=>p[1]===y);};
(async()=>{
  const allowance=.2/Rose.MM;
  for(const inset of [0,1.5,3]){
    const shapes=columns(inset,32),b1=shapes.slice(0,8),b2=shapes.slice(8,32);
    const p1=Rose.plan(b1,W,H,null,.2),p2=Rose.plan(b2,W,H,p1.profile,.2);
    assert.deepEqual(edgeRuns(p1.lines,p1.profile),[],`inset ${inset}: line 1 never runs along the sheet's edge`);
    assert.deepEqual(edgeRuns(p2.lines,p2.profile),[],`inset ${inset}: line 2 never runs along the sheet's edge`);
    // Line 2 starts at the top edge at its own charms, past line 1, like line 1 does.
    const start1=Math.max(...top(p1.lines).map(p=>p[0])),start2=top(p2.lines);
    assert.equal(start2[0][1],0,`inset ${inset}: line 2 reaches the top edge`);
    assert(start2.every(p=>p[0]>start1+10),`inset ${inset}: line 2 starts by its own charms, not back at line 1 (${start1.toFixed(1)} vs ${start2.map(p=>p[0].toFixed(1))})`);
    assert(Math.min(...p2.lines.flat().map(p=>p[0]))>start1-1e-6,`inset ${inset}: line 2 never reaches back past line 1`);
    // Each line keeps its allowance from the charms it separates.
    assert(clearance(p1.lines,b1)>=allowance-1e-3,`inset ${inset}: line 1 keeps the contour allowance`);
    assert(clearance(p2.lines,b2)>=allowance-1e-3,`inset ${inset}: line 2 keeps the contour allowance`);
    // Together the lines still separate everything used from the remnant.
    assert(traced(p1.profile,p1.lines),`inset ${inset}: line 1 traces its frontier`);
    assert(traced(p2.profile,[...p1.lines,...p2.lines]),`inset ${inset}: lines 1 and 2 trace the final frontier`);
    assert(!p1.full&&!p2.full);
  }
  // The same holds when the sheet is used from the top down.
  const turned=columns(1.5,16).map(s=>({...s,paths:s.paths.map(p=>p.map(([x,y])=>[y*W/H*.99,x*H/W*.99]))}));
  const t1=Rose.plan(turned.slice(0,8),W,H,null,.2);
  assert.deepEqual(edgeRuns(t1.lines,t1.profile),[],'a line across the sheet does not run along its edge');

  // Lines saved before this fix ran along the top and bottom edges back to the
  // sheet's start. The server keeps their dates and charms but drops those runs.
  let now=1790000000000;Date.now=()=>now;
  const shapes=columns(1.5,32),b1=shapes.slice(0,8),b2=shapes.slice(8,16),old=Rose.plan(b1,W,H,null,.2);
  const [x0]=top(old.lines)[0],legacy=[[[0,.4],[x0,.4],...old.lines[0].slice(1,-1),[old.lines[0].at(-1)[0],H-.4],[0,H-.4]]];
  const stages=[{n:1,at:now-3600000,ids:b1.map(s=>s.id),lines:[0,1]}];
  const claim=await api.roseClaim({sheetId:'sheet-edges',wPt:W,hPt:H,fresh:true}),stockId=claim.stock.id;
  const place=s=>({id:s.id,cxPt:W/2,cyPt:H/2,angle:0,scale:1});
  store.set('Charm_Nest_Runs/run-edges',{lines:{}});
  store.set('Charm_Nest_Sheets/sheet-edges',{id:'sheet-edges',metal:'rose',verification:{ok:true},status:'complete',dirty:false,saving:false,draft:false,setId:'set-edges',runId:'run-edges',placements:[...b1,...b2].map(place),outputs:{ai:{url:'sheet.ai'}},
    roseProtectedJson:JSON.stringify({profile:old.profile,lines:legacy,shapes:b1,stages,placements:b1.map(place)})});
  const sheet=()=>store.get('Charm_Nest_Sheets/sheet-edges');
  const saved=JSON.parse((await api.rosePlan({sheetId:'sheet-edges',stockId,revision:0,fingerprint:create.fingerprint(sheet()),shapesJson:JSON.stringify([...b1,...b2]),allowanceMm:.2})).planJson);
  assert.deepEqual(edgeRuns(saved.lines,saved.profile),[],'the saved lines no longer run along the edge');
  assert.deepEqual(saved.stages.map(s=>[s.n,s.at,s.ids.length]),[[1,now-3600000,8],[2,now,8]],'both lines keep their dates and charms');
  const line1=saved.lines.slice(...saved.stages[0].lines),line2=saved.lines.slice(...saved.stages[1].lines);
  assert(line1.length&&line2.length,'each dated line still has its own path');
  assert.deepEqual(top(line1).map(p=>p[1]),[0],'line 1 now meets the top edge where it leaves the charms');
  assert(Math.min(...line1.flat().map(p=>p[0]))>EDGE,'line 1 no longer runs back to the sheet\'s start');
  assert(clearance(line1,b1)>=allowance-1e-3,'the tidied line keeps its allowance');
  assert(traced(saved.profile,saved.lines),'the tidied lines still trace the whole frontier');

  // A layout that leaves no room for another charm takes the rest of the sheet:
  // no new line, nothing left over, and the cut can still be recorded.
  const dense=[];
  for(let col=0;col<11;col++)for(let row=0;row<5;row++){const x=1+col*25.7,y=1+row*28;dense.push({id:`d${col}-${row}`,paths:[[[x,y],[x+24,y],[x+24,y+26.5],[x,y+26.5]]]});}
  const full=Rose.plan(dense,W,H,null,.2);
  assert(full.full,'the sheet is full');assert.deepEqual(full.lines,[],'a full sheet needs no green line');assert(Math.abs(full.remainingPt2)<1e-6,'nothing is left');
  assert(!Rose.plan(dense.slice(0,40),W,H,null,.2).full,'a sheet with a free column is not full');
  const fullClaim=await api.roseClaim({sheetId:'sheet-full',wPt:W,hPt:H,fresh:true});
  store.set('Charm_Nest_Runs/run-full',{lines:{a:{poolIds:dense.map(s=>s.id),spec:{engraveCandidate:false}}}});
  store.set('Charm_Nest_Sheets/sheet-full',{id:'sheet-full',metal:'rose',verification:{ok:true},status:'complete',dirty:false,saving:false,draft:false,setId:'set-full',runId:'run-full',poolIds:dense.map(s=>s.id),placedCount:dense.length,placements:dense.map(place),outputs:{ai:{url:'full.ai'},preview:{url:'full.png'}},label:{files:[{path:'qr',url:'qr.png',payload:'test',orders:[]}]},orders:[]});
  const fullSheet=()=>store.get('Charm_Nest_Sheets/sheet-full');
  const planned=await api.rosePlan({sheetId:'sheet-full',stockId:fullClaim.stock.id,revision:0,fingerprint:create.fingerprint(fullSheet()),shapesJson:JSON.stringify(dense),allowanceMm:.2});
  const fullPlan=JSON.parse(planned.planJson);
  assert(fullPlan.full&&!fullPlan.lines.length&&!fullPlan.stages.length,'the saved plan has no green line to date');
  const cut=await api.roseRecordCut({sheetId:'sheet-full',stockId:fullClaim.stock.id,revision:0,planHash:planned.planHash,by:'test'});
  assert.equal(cut.stock.available,false,'nothing is left for later layouts');
  console.log('Rose line edges OK: new and saved green lines stop at the sheet edge by their charms, keep the allowance, trace the whole frontier, and a full sheet needs no line');
})().catch(e=>{console.error(e);process.exit(1)});
