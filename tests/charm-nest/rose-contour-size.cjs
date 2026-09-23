const assert=require('node:assert/strict'),fs=require('fs');
global.self=global;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');
const P=CharmNestPDF,Rose=require('../../charm-nest-rose'),create=require('../../netlify/functions/_charmNestRoseStock'),Readiness=require('../../charm-nest-readiness');
const store=new Map(),clone=x=>structuredClone(x);
function ref(path){return {path,id:path.split('/').at(-1),collection:n=>query(path+'/'+n),get:async()=>snap(path)};}
function snap(path){return {id:path.split('/').at(-1),ref:ref(path),exists:store.has(path),data:()=>clone(store.get(path))};}
function query(path,filters=[],order=null,limit=Infinity,after=null){return {doc:id=>ref(path+'/'+id),where:(...f)=>query(path,[...filters,f],order,limit,after),orderBy:(...o)=>query(path,filters,o,limit,after),limit:n=>query(path,filters,order,n,after),startAfter:n=>query(path,filters,order,limit,n),get:async()=>{let docs=[...store.keys()].filter(k=>k.startsWith(path+'/')&&!k.slice(path.length+1).includes('/')).map(snap);docs=docs.filter(d=>filters.every(([f,op,v])=>d.data()[f]===v));if(order)docs.sort((a,b)=>(a.data()[order[0]]-b.data()[order[0]])*(order[1]==='desc'?-1:1));if(after!==null)docs=docs.filter(d=>d.data()[order[0]]<after);docs=docs.slice(0,limit);return {docs,size:docs.length};}};}
let serial=Promise.resolve();const db={runTransaction:fn=>{const promise=serial.then(async()=>{const writes=[];let wrote=false;const result=await fn({get:async r=>{assert(!wrote,'Firestore requires all reads before writes');return r.get();},set:(r,v,opts)=>{wrote=true;writes.push(()=>store.set(r.path,opts?.merge?{...store.get(r.path),...clone(v)}:clone(v)));},update:(r,v)=>{wrote=true;writes.push(()=>store.set(r.path,{...store.get(r.path),...clone(v)}));},delete:r=>{wrote=true;writes.push(()=>store.delete(r.path));}});writes.forEach(f=>f());return result;});serial=promise.catch(()=>{});return promise;}};
const api=create({db,col:query,FV:{serverTimestamp:()=>123456},Readiness});
// Real production artwork: each fixture is a CUT outline with engraved detail.
function load(name){const members=JSON.parse(fs.readFileSync(__dirname+'/fixtures/'+name+'-paths.json'));const parsed={segments:members.map((m,index)=>({...m,index,start:index,end:index+1})),nested:[],pageW:80,pageH:80};const c=P.groupCharms(parsed,{minPt:6}).charms[0];P.integrateRings(c);c.centerPt ||= [(c.bbox[0]+c.bbox[2])/2,(c.bbox[1]+c.bbox[3])/2];return c;}
const art=[load('sea_turtle2'),load('middle_5903')];
const W=100/Rose.MM,H=50/Rose.MM,charms=[],placements=[];
// A full RG 14/20 sheet: ten columns of five charms, like a busy day's layout.
for(let i=0;i<50;i++){const col=Math.floor(i/5),row=i%5;charms.push({...art[i%2],id:'rg-'+i,hash:'hash-'+i});placements.push({id:'rg-'+i,cxPt:14.123456+col*28.1,cyPt:14.654321+row*28.1,angle:(i*37)%360,scale:.6});}
const report=pl=>pl.map(p=>({id:p.id,hash:'hash-'+p.id.slice(3),angle:p.angle,scale:p.scale,cxPt:+p.cxPt.toFixed(3),cyPt:+p.cyPt.toFixed(3)}));
const save=(pl)=>{const old=store.get('Charm_Nest_Sheets/rg-full-sheet')||{};store.set('Charm_Nest_Sheets/rg-full-sheet',{...old,id:'rg-full-sheet',metal:'rose',verification:{ok:true},dirty:false,saving:false,outputs:{ai:{url:'sheet.ai'}},charms:charms.map(c=>({id:c.id,hash:c.hash})),placements:report(pl)});};
const fp=pl=>create.fingerprint({charms,placements:pl});
// The exact, unsimplified vectors the laser cuts, in sheet coordinates.
function place(c,p,paths){const a=p.angle*Math.PI/180,cos=Math.cos(a),sin=Math.sin(a);return paths.map(path=>path.map(([x,y])=>{const dx=(x-c.centerPt[0])*p.scale,dy=(c.centerPt[1]-y)*p.scale;return [p.cxPt+dx*cos-dy*sin,p.cyPt+dx*sin+dy*cos];}));}
const exact=(c,p)=>place(c,p,Rose.flatten(c.outline));
const fixed4=paths=>paths.map(path=>path.map(pt=>pt.map(v=>+v.toFixed(4))));
function covered(profile,c,p,allowanceMm){
  const allowance=allowanceMm/Rose.MM;
  for(const path of exact(c,p))for(const [x,y] of path){const [d,t]=profile.axis==='x'?[x,y]:[y,x],i=Math.min(profile.values.length-1,Math.floor(t/profile.step));if(profile.values[i]<d+allowance-1e-9)return false;}
  return true;
}
(async()=>{
  const all=Rose.shapes(charms,placements),json=JSON.stringify(all);
  // What earlier pages sent: every flattened outline and engraving point.
  const unsimplified=JSON.stringify(charms.map((c,i)=>({id:c.id,paths:fixed4(exact(c,placements[i])),ink:fixed4(place(c,placements[i],(c.members||[]).filter(m=>m!==c.outline).flatMap(Rose.flatten)))})));
  assert(unsimplified.length>650000,'a full sheet of exact outlines is larger than one contour request');
  assert(json.length*2<650000,'simplified outlines leave room for twice this many charms: '+json.length);
  assert(all.every(s=>s.slim===1&&s.paths.every(path=>path.length>=3)),'every outline stays a closed polygon');
  assert.deepEqual(Rose.slimShape(all[0]),all[0],'simplifying twice changes nothing');
  // First batch: the two left columns get the first green line.
  const first=placements.slice(0,10),claim=await api.roseClaim({sheetId:'rg-full-sheet',wPt:W,hPt:H,fresh:true});save(first);
  const plan1=JSON.parse((await api.rosePlan({sheetId:'rg-full-sheet',stockId:claim.stock.id,revision:0,fingerprint:fp(first),shapesJson:JSON.stringify(Rose.shapes(charms,first)),allowanceMm:.2})).planJson);
  assert(first.every(p=>covered(plan1.profile,charms.find(c=>c.id===p.id),p,.2)),'the first green line clears every exact outline by the full allowance');
  // The latest upload fills the rest of the sheet, then the next green line is prepared.
  await api.roseClaim({sheetId:'rg-full-sheet',stockId:claim.stock.id,revision:0,wPt:W,hPt:H,nesting:true});save(placements);
  const plan2=JSON.parse((await api.rosePlan({sheetId:'rg-full-sheet',stockId:claim.stock.id,revision:0,fingerprint:fp(placements),shapesJson:json,allowanceMm:.2})).planJson);
  assert.deepEqual(plan2.lines.slice(0,plan1.lines.length),plan1.lines,'the first green line is kept exactly');
  assert(plan2.lines.length>plan1.lines.length,'the full sheet gets its own green line');
  assert.equal(plan2.shapes.length,50);
  assert(placements.every((p,i)=>covered(plan2.profile,charms[i],p,.2)),'the new green line clears every exact outline by the full allowance');
  assert(Buffer.byteLength(JSON.stringify(store.get('Charm_Nest_Sheets/rg-full-sheet')))<950000,'the saved sheet stays below the record limit');
  // A page opened before this update still sends every flattened point.
  const legacy=JSON.parse((await api.rosePlan({sheetId:'rg-full-sheet',stockId:claim.stock.id,revision:0,fingerprint:fp(placements),shapesJson:unsimplified,allowanceMm:.2})).planJson);
  assert.deepEqual(legacy,plan2,'unsimplified requests are stored in the same compact form');
  console.log('Rose contour size OK: full 50-charm sheet, second green line, conservative simplified outlines, '+Math.round(json.length/1000)+' kB request');
})().catch(e=>{console.error(e);process.exit(1)});
