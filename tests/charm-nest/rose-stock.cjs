const assert=require('node:assert/strict'),Rose=require('../../charm-nest-rose'),create=require('../../netlify/functions/_charmNestRoseStock'),Readiness=require('../../charm-nest-readiness');
const store=new Map(),clone=x=>structuredClone(x);
function ref(path){return {path,id:path.split('/').at(-1),collection:n=>query(path+'/'+n),get:async()=>snap(path)};}
function snap(path){return {id:path.split('/').at(-1),ref:ref(path),exists:store.has(path),data:()=>clone(store.get(path))};}
function query(path,filters=[],order=null,limit=Infinity,after=null){return {doc:id=>ref(path+'/'+id),where:(...f)=>query(path,[...filters,f],order,limit,after),orderBy:(...o)=>query(path,filters,o,limit,after),limit:n=>query(path,filters,order,n,after),startAfter:n=>query(path,filters,order,limit,n),get:async()=>{let docs=[...store.keys()].filter(k=>k.startsWith(path+'/')&&!k.slice(path.length+1).includes('/')).map(snap);docs=docs.filter(d=>filters.every(([f,op,v])=>d.data()[f]===v));if(order)docs.sort((a,b)=>(a.data()[order[0]]-b.data()[order[0]])*(order[1]==='desc'?-1:1));if(after!==null)docs=docs.filter(d=>d.data()[order[0]]<after);docs=docs.slice(0,limit);return {docs,size:docs.length};}};}
let serial=Promise.resolve();const db={runTransaction:fn=>{const promise=serial.then(async()=>{const writes=[];let wrote=false;const result=await fn({get:async r=>{assert(!wrote,'Firestore requires all reads before writes');return r.get();},set:(r,v)=>{wrote=true;writes.push(()=>store.set(r.path,clone(v)));},update:(r,v)=>{wrote=true;writes.push(()=>store.set(r.path,{...store.get(r.path),...clone(v)}));}});writes.forEach(f=>f());return result;});serial=promise.catch(()=>{});return promise;}};
const api=create({db,col:query,FV:{serverTimestamp:()=>123456},Readiness});
const shape=(id,x,y,w,h)=>({id,paths:[[[x,y],[x+w,y],[x+w,y+h],[x,y+h]]]});
const sheet=(id,shapes)=>({id,metal:'rose',verification:{ok:true},status:'complete',dirty:false,saving:false,draft:false,setId:'set-test',runId:'run-test',poolIds:shapes.map(s=>s.id),placedCount:shapes.length,placements:shapes.map(s=>({id:s.id,cxPt:20,cyPt:20,angle:0,scale:1})),outputs:{ai:{url:'saved.ai'},preview:{url:'saved.png'}},label:{files:[{path:'qr',url:'qr.png',payload:'test',orders:[]}]},orders:[]});
(async()=>{
 const shapes=[shape('pool-1',2,2,10,30)];
 store.set('Charm_Nest_Runs/run-test',{lines:{a:{poolIds:['pool-1','pool-2'],spec:{engraveCandidate:false}}}});
 // Use the same real readiness decisions format as production.
 const first=await api.roseClaim({sheetId:'sheet-first',wPt:100,hPt:50});assert.equal(first.stock.revision,0);
 const stockId=first.stock.id;
 await assert.rejects(()=>api.roseClaim({sheetId:'sheet-other',stockId,wPt:100,hPt:50}),/reserved/);
 assert.equal((await api.roseClaim({sheetId:'sheet-first',wPt:100,hPt:50})).stock.id,stockId,'reload recovers the same reservation');
 const saved=sheet('sheet-first',shapes);store.set('Charm_Nest_Sheets/sheet-first',saved);
 const planArgs={sheetId:saved.id,stockId,revision:0,fingerprint:create.fingerprint(saved),shapesJson:JSON.stringify(shapes),allowanceMm:.2};
 await assert.rejects(()=>api.rosePlan({...planArgs,fingerprint:'stale'}),/layout changed/);
 const p=await api.rosePlan(planArgs);assert(JSON.parse(p.planJson).lines.length);
 const cutArgs={sheetId:saved.id,stockId,revision:0,planHash:p.planHash};
 const actual=store.get('Charm_Nest_Sheets/sheet-first');
 assert(Readiness.sheet({...actual,engraving:Readiness.decisions(Object.values(store.get('Charm_Nest_Runs/run-test').lines))}).ready,'fixture passes actual production checks');
 const [cut,retry]=await Promise.all([api.roseRecordCut(cutArgs),api.roseRecordCut(cutArgs)]);
 assert.equal(cut.stock.revision,1);assert.equal(retry.cut.at,cut.cut.at);
 assert.equal((await api.roseGet({stockId})).cuts.length,1,'duplicate requests append one historical cut');
 assert.equal((await api.roseList()).stocks[0].id,stockId);
 const next=await api.roseClaim({sheetId:'sheet-next',wPt:100,hPt:50});assert.equal(next.stock.id,stockId,'future layouts automatically reuse the saved irregular stock');
 assert.equal(next.stock.profileJson,JSON.stringify(JSON.parse(p.planJson).profile));
 await assert.rejects(()=>api.roseClaim({sheetId:'sheet-next',stockId,revision:0,wPt:100,hPt:50}),/changed/);
 await assert.rejects(()=>api.roseClaim({sheetId:'sheet-next',stockId,wPt:120,hPt:50}),/size/);
 const laterShapes=[shape('pool-2',25,2,8,25)],later=sheet('sheet-next',laterShapes);store.set('Charm_Nest_Sheets/sheet-next',later);
 const p2=await api.rosePlan({sheetId:later.id,stockId,revision:1,fingerprint:create.fingerprint(later),shapesJson:JSON.stringify(laterShapes),allowanceMm:.2});
 const before=JSON.parse(next.stock.profileJson),after=JSON.parse(p2.planJson).profile;assert(after.values.every((v,i)=>v>=before.values[i]));
 await api.roseClaim({sheetId:'sheet-next',stockId,revision:1,wPt:100,hPt:50,nesting:true});
 await assert.rejects(()=>api.roseRecordCut({sheetId:'sheet-next',stockId,revision:1,planHash:p2.planHash}),/changed/,'re-nesting invalidates the old cut plan');
 await assert.rejects(()=>api.roseRelease({sheetId:'sheet-next',stockId}),/current set/);
 store.get('Charm_Nest_Sheets/sheet-next').draft=true;await api.roseRelease({sheetId:'sheet-next',stockId});assert.equal((await api.roseList()).stocks.length,1);
 const fresh=await api.roseClaim({sheetId:'sheet-fresh',wPt:100,hPt:50,fresh:true});assert.notEqual(fresh.stock.id,stockId);
 console.log('Rose stock OK: reservations, recovery, revision conflicts, durable history, idempotent cuts, automatic remnant reuse, re-nest invalidation and explicit new stock');
})().catch(e=>{console.error(e);process.exit(1)});
