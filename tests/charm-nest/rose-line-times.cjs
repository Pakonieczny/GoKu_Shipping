const assert=require('node:assert/strict'),Rose=require('../../charm-nest-rose'),create=require('../../netlify/functions/_charmNestRoseStock'),Readiness=require('../../charm-nest-readiness');
const store=new Map(),clone=x=>structuredClone(x);
function ref(path){return {path,id:path.split('/').at(-1),collection:n=>query(path+'/'+n),get:async()=>snap(path)};}
function snap(path){return {id:path.split('/').at(-1),ref:ref(path),exists:store.has(path),data:()=>clone(store.get(path))};}
function query(path,filters=[],order=null,limit=Infinity,after=null){return {doc:id=>ref(path+'/'+id),where:(...f)=>query(path,[...filters,f],order,limit,after),orderBy:(...o)=>query(path,filters,o,limit,after),limit:n=>query(path,filters,order,n,after),startAfter:n=>query(path,filters,order,limit,n),get:async()=>{let docs=[...store.keys()].filter(k=>k.startsWith(path+'/')&&!k.slice(path.length+1).includes('/')).map(snap);docs=docs.filter(d=>filters.every(([f,op,v])=>d.data()[f]===v));if(order)docs.sort((a,b)=>(a.data()[order[0]]-b.data()[order[0]])*(order[1]==='desc'?-1:1));if(after!==null)docs=docs.filter(d=>d.data()[order[0]]<after);docs=docs.slice(0,limit);return {docs,size:docs.length};}};}
let serial=Promise.resolve();const db={runTransaction:fn=>{const promise=serial.then(async()=>{const writes=[];let wrote=false;const result=await fn({get:async r=>{assert(!wrote,'Firestore requires all reads before writes');return r.get();},set:(r,v,opts)=>{wrote=true;writes.push(()=>store.set(r.path,opts?.merge?{...store.get(r.path),...clone(v)}:clone(v)));},update:(r,v)=>{wrote=true;writes.push(()=>store.set(r.path,{...store.get(r.path),...clone(v)}));},delete:r=>{wrote=true;writes.push(()=>store.delete(r.path));}});writes.forEach(f=>f());return result;});serial=promise.catch(()=>{});return promise;}};
const api=create({db,col:query,FV:{serverTimestamp:()=>123456},Readiness});
const shape=(id,x,y,w,h)=>({id,paths:[[[x,y],[x+w,y],[x+w,y+h],[x,y+h]]]});
const sheet=(id,shapes)=>({id,metal:'rose',verification:{ok:true},status:'complete',dirty:false,saving:false,draft:false,setId:'set-test',runId:'run-test',poolIds:shapes.map(s=>s.id),placedCount:shapes.length,placements:shapes.map(s=>({id:s.id,cxPt:20,cyPt:20,angle:0,scale:1})),outputs:{ai:{url:'saved.ai'},preview:{url:'saved.png'}},label:{files:[{path:'qr',url:'qr.png',payload:'test',orders:[]}]},orders:[]});
// Every green line keeps the date it was first prepared, through later
// uploads, re-planning and the recorded cut.
(async()=>{
 let now=1790000000000;Date.now=()=>now;
 store.set('Charm_Nest_Runs/run-test',{lines:{a:{poolIds:['pool-1','pool-2','pool-3'],spec:{engraveCandidate:false}}}});
 const first=[shape('pool-1',2,2,10,30)],claim=await api.roseClaim({sheetId:'sheet-lines',wPt:100,hPt:50,fresh:true}),stockId=claim.stock.id;
 store.set('Charm_Nest_Sheets/sheet-lines',sheet('sheet-lines',first));
 const doc=()=>store.get('Charm_Nest_Sheets/sheet-lines'),args=(shapes,allowanceMm=.2)=>({sheetId:'sheet-lines',stockId,revision:0,fingerprint:create.fingerprint(doc()),shapesJson:JSON.stringify(shapes),allowanceMm});
 const p1=JSON.parse((await api.rosePlan(args(first))).planJson),t1=now;
 assert.deepEqual(p1.stages,[{n:1,at:t1,ids:['pool-1'],lines:[0,p1.lines.length]}],'the first green line is dated');
 now+=60000;
 const again=JSON.parse((await api.rosePlan(args(first,.35))).planJson);
 assert.equal(again.stages[0].at,t1,'preparing the same batch again keeps its original date');
 // The next charm upload protects the dated line while the sheet is nested again.
 now+=3600000;
 const claim2=await api.roseClaim({sheetId:'sheet-lines',stockId,revision:0,wPt:100,hPt:50,nesting:true});
 assert.deepEqual(JSON.parse(claim2.protectedJson).stages,again.stages,'the protected contour keeps its dates');
 const add=(id,x)=>{const d=doc();d.dirty=false;d.placements.push({id,cxPt:x,cyPt:20,angle:0,scale:1});d.poolIds.push(id);d.placedCount=d.placements.length;};
 add('pool-2',34);const second=[...first,shape('pool-2',30,2,8,25)];
 const p2=JSON.parse((await api.rosePlan(args(second))).planJson),t2=now;
 assert.equal(p2.stages.length,2);assert.deepEqual(p2.stages[0],again.stages[0],'line 1 is unchanged');
 assert.deepEqual(p2.stages[1],{n:2,at:t2,ids:['pool-2'],lines:[again.lines.length,p2.lines.length]},'line 2 has its own date and charms');
 // A third upload dates a third line and keeps both earlier dates.
 now+=7200000;
 await api.roseClaim({sheetId:'sheet-lines',stockId,revision:0,wPt:100,hPt:50,nesting:true});
 add('pool-3',54);const third=[...second,shape('pool-3',50,2,8,25)];
 const r3=await api.rosePlan(args(third)),p3=JSON.parse(r3.planJson);
 assert.deepEqual(p3.stages.map(s=>[s.n,s.at,s.ids]),[[1,t1,['pool-1']],[2,t2,['pool-2']],[3,now,['pool-3']]]);
 assert.deepEqual(p3.stages.map(s=>s.lines),[[0,again.lines.length],[again.lines.length,p2.lines.length],[p2.lines.length,p3.lines.length]]);
 // Recording the cut moves every dated line into the permanent cut history.
 now+=60000;
 const cut=await api.roseRecordCut({sheetId:'sheet-lines',stockId,revision:0,planHash:r3.planHash,by:'test'});
 assert.deepEqual(JSON.parse(cut.cut.planJson).stages,p3.stages);
 assert.deepEqual(JSON.parse((await api.roseGet({stockId})).cuts[0].planJson).stages,p3.stages,'history reloads with every line date');
 // A contour protected before dates were kept shows as one undated line.
 const legacyClaim=await api.roseClaim({sheetId:'sheet-legacy',wPt:100,hPt:50,fresh:true}),old=[shape('pool-1',2,2,10,30)],legacyPlan=Rose.plan(old,100,50,null,.2);
 const legacy=sheet('sheet-legacy',old);legacy.roseProtectedJson=JSON.stringify({profile:legacyPlan.profile,lines:legacyPlan.lines,shapes:legacyPlan.shapes,placements:legacy.placements});
 legacy.placements.push({id:'pool-2',cxPt:34,cyPt:20,angle:0,scale:1});store.set('Charm_Nest_Sheets/sheet-legacy',legacy);
 const lp=JSON.parse((await api.rosePlan({sheetId:'sheet-legacy',stockId:legacyClaim.stock.id,revision:0,fingerprint:create.fingerprint(legacy),shapesJson:JSON.stringify([...old,shape('pool-2',30,2,8,25)]),allowanceMm:.2})).planJson);
 assert.deepEqual(lp.stages,[{n:1,at:null,ids:['pool-1'],lines:[0,legacyPlan.lines.length]},{n:2,at:now,ids:['pool-2'],lines:[legacyPlan.lines.length,lp.lines.length]}]);
 // Preparing such a line again (a reload, a new allowance) does not invent a date for it.
 const undatedClaim=await api.roseClaim({sheetId:'sheet-undated',wPt:100,hPt:50,fresh:true}),undated=sheet('sheet-undated',old);
 undated.rosePlanJson=JSON.stringify({profile:legacyPlan.profile,lines:legacyPlan.lines,shapes:legacyPlan.shapes,allowanceMm:.2});store.set('Charm_Nest_Sheets/sheet-undated',undated);
 const up=JSON.parse((await api.rosePlan({sheetId:'sheet-undated',stockId:undatedClaim.stock.id,revision:0,fingerprint:create.fingerprint(undated),shapesJson:JSON.stringify(old),allowanceMm:.35})).planJson);
 assert.deepEqual(up.stages,[{n:1,at:null,ids:['pool-1'],lines:[0,up.lines.length]}]);
 console.log('Rose line dates OK: first line, re-planning, protected appends, three uploads, permanent cut history and undated earlier lines, including when re-planned');
})().catch(e=>{console.error(e);process.exit(1)});
