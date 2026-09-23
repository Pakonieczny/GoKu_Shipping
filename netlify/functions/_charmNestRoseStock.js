'use strict';
const Rose=require('../../charm-nest-rose');
const crypto=require('crypto');
const fingerprint=s=>JSON.stringify((s.placements||[]).map(p=>[p.id,+p.cxPt.toFixed(3),+p.cyPt.toFixed(3),p.angle,p.scale||1,p.hash||s.charms?.find(c=>c.id===p.id)?.hash||null]));
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
const id=s=>typeof s==='string'&&/^[\w-]{4,80}$/.test(s);
const parse=s=>s?JSON.parse(s):null;
function protectedLayout(sheet){
  const plan=parse(sheet.rosePlanJson);
  return plan?{profile:plan.profile,lines:plan.lines,shapes:plan.shapes,placements:sheet.placements}:parse(sheet.roseProtectedJson);
}
function assertProtected(guard,placements){
  if(!guard)return;
  if(!Array.isArray(placements))throw new Error('The protected Rose Gold layout cannot be moved or removed');
  for(const p of guard.placements){
    const rows=placements.filter(x=>x?.id===p.id),q=rows[0];
    if(rows.length!==1||['cxPt','cyPt','angle'].some(k=>!Number.isFinite(q[k])||Math.abs(q[k]-p[k])>.001)||Math.abs((q.scale||1)-(p.scale||1))>.00001||(p.hash!=null&&q.hash!==p.hash))throw new Error('The protected Rose Gold layout cannot be moved or removed');
  }
}
module.exports=function({db,col,FV,Readiness}){
  const stocks=()=>col('Charm_Nest_Rose_Stock'),sheets=()=>col('Charm_Nest_Sheets');
  async function roseGet(b){
    if(!id(b.stockId))throw new Error('Choose a Rose Gold sheet');
    const snap=await stocks().doc(b.stockId).get();if(!snap.exists)throw new Error('Rose Gold sheet not found');
    let q=stocks().doc(b.stockId).collection('cuts').orderBy('revision','desc');if(b.before)q=q.startAfter(+b.before);
    // Keep even the largest saved contours below the function response limit.
    const cuts=await q.limit(4).get();return {stock:snap.data(),cuts:cuts.docs.map(d=>d.data()),more:cuts.size===4};
  }
  async function roseList(){const snap=await stocks().where('available','==',true).limit(100).get();return {stocks:snap.docs.map(d=>d.data()).sort((a,b)=>a.createdMs-b.createdMs)};}
  async function roseClaim(b){
    if(!id(b.sheetId)||![b.wPt,b.hPt].every(n=>Number.isFinite(n)&&n>=14&&n<=1420))throw new Error('Invalid physical sheet');
    // Resume a reservation after browser recovery before assigning another sheet.
    const held=await stocks().where('owner','==',b.sheetId).limit(1).get();
    let stockId=held.docs[0]?.id||b.stockId;
    if(!stockId&&!b.fresh){const available=(await roseList()).stocks;stockId=available.find(s=>Math.abs(s.wPt-b.wPt)<.01&&Math.abs(s.hPt-b.hPt)<.01)?.id;}
    if(stockId&&!id(stockId))throw new Error('Invalid Rose Gold sheet');
    const ref=stockId?stocks().doc(stockId):stocks().doc('rgs-'+crypto.randomUUID());
    const stock=await db.runTransaction(async tx=>{
      const d=await tx.get(ref),old=d.exists?d.data():null;
      if(old&&(Math.abs(old.wPt-b.wPt)>.01||Math.abs(old.hPt-b.hPt)>.01))throw new Error('The physical sheet size cannot change');
      if(old?.owner&&old.owner!==b.sheetId)throw new Error('This Rose Gold sheet is reserved for another layout');
      if(old&&b.revision!=null&&old.revision!==b.revision)throw new Error('This remnant changed. Reload its history before nesting');
      const sd=await tx.get(sheets().doc(b.sheetId));
      if(sd.exists&&sd.data().roseCutAt)throw new Error("This layout was already cut; start a new sheet");
      const next={...(old||{id:ref.id,wPt:b.wPt,hPt:b.hPt,revision:0,profileJson:null,createdMs:Date.now()}),owner:b.sheetId,available:false,updatedAt:FV.serverTimestamp()};
      const guard=sd.exists?protectedLayout(sd.data()):null,protectedJson=guard?JSON.stringify(guard):null;
      tx.set(ref,next);
      if(sd.exists)tx.update(sheets().doc(b.sheetId),{roseStockId:ref.id,roseRevision:next.revision,...(b.nesting?{dirty:true,roseProtectedJson:protectedJson,rosePlanJson:null,rosePlanHash:null,roseFingerprint:null}:{})});
      return {stock:{...next,updatedAt:null},protectedJson};
    });return stock;
  }
  async function roseRelease(b){
    if(!id(b.stockId)||!id(b.sheetId))throw new Error('Invalid stock reservation');
    await db.runTransaction(async tx=>{const ref=stocks().doc(b.stockId),d=await tx.get(ref);if(!d.exists||d.data().owner!==b.sheetId)throw new Error('This stock reservation changed');
      const sheet=await tx.get(sheets().doc(b.sheetId));if(sheet.exists&&sheet.data().setId&&!sheet.data().draft)throw new Error('Remove the sheet from its current set before releasing its stock');
      if(sheet.exists&&(sheet.data().rosePlanJson||sheet.data().roseProtectedJson))throw new Error('A planned or protected Rose Gold contour cannot be released');
      tx.update(ref,{owner:null,available:true,updatedAt:FV.serverTimestamp()});if(sheet.exists)tx.update(sheets().doc(b.sheetId),{rosePlanJson:null,rosePlanHash:null,roseStockId:null});});return {ok:true};
  }
  async function rosePlan(b){
    if(!id(b.sheetId)||!id(b.stockId)||typeof b.shapesJson!=='string'||b.shapesJson.length>650000)throw new Error('Invalid cut geometry');
    const shapes=parse(b.shapesJson);
    return db.runTransaction(async tx=>{
      const ref=stocks().doc(b.stockId),sr=sheets().doc(b.sheetId),d=await tx.get(ref),sd=await tx.get(sr),stock=d.exists&&d.data(),sheet=sd.exists&&sd.data();
      if(!sheet||sheet.metal!=='rose'||!sheet.verification?.ok||sheet.saving||sheet.dirty||sheet.roseCutAt||!sheet.outputs?.ai)throw new Error('Save and verify this Rose Gold layout first');
      if(!stock||stock.owner!==b.sheetId||stock.revision!==b.revision)throw new Error('The physical sheet changed. Nest it again');
      if(fingerprint(sheet)!==b.fingerprint||shapes.length!==sheet.placements.length||new Set(shapes.map(s=>s.id)).size!==shapes.length||shapes.some(s=>!sheet.placements.some(p=>p.id===s.id)))throw new Error('The layout changed. Prepare its contour again');
      const guard=parse(sheet.roseProtectedJson);assertProtected(guard,sheet.placements);
      const fixed=new Set((guard?.placements||[]).map(p=>p.id));
      const protectedShapes=new Map((guard?.shapes||[]).map(s=>[s.id,s]));
      if(guard&&(protectedShapes.size!==fixed.size||shapes.some(s=>fixed.has(s.id)&&JSON.stringify([s.paths,s.ink])!==JSON.stringify([protectedShapes.get(s.id)?.paths,protectedShapes.get(s.id)?.ink]))))throw new Error('The protected Rose Gold charm outlines cannot be changed');
      const prior=parse(stock.profileJson);
      // Recheck physical exclusions at the persistence boundary, too. This
      // catches a stale rectangular layout attached to a previously cut sheet.
      for(const shape of shapes){const exclusion=!fixed.has(shape.id)&&guard?guard.profile:prior;if(!exclusion)continue;for(const path of shape.paths||[])for(let i=0;i<path.length;i++){
        const a=path[i],z=path[(i+1)%path.length],n=Math.max(1,Math.ceil(Math.hypot(z[0]-a[0],z[1]-a[1])*4));
        for(let j=0;j<=n;j++){const x=a[0]+(z[0]-a[0])*j/n,y=a[1]+(z[1]-a[1])*j/n;if(Rose.intersects(exclusion,x,y,0,0))throw new Error('A charm crosses protected or previously cut material. Nest again on this remnant');}
      }}
      const fresh=guard?shapes.filter(s=>!fixed.has(s.id)):shapes;
      const plan=fresh.length?Rose.plan(fresh,stock.wPt,stock.hPt,guard?.profile||prior,b.allowanceMm):{version:1,profile:guard.profile,lines:[],shapes:[],allowanceMm:b.allowanceMm,remainingPt2:stock.wPt*stock.hPt-Rose.area(guard.profile)};
      if(guard){plan.lines=[...guard.lines,...plan.lines];plan.shapes=shapes.map(s=>protectedShapes.get(s.id)||s);plan.removedPt2=Rose.area(plan.profile)-(prior?Rose.area(prior):0);}
      if(!plan.lines.length)throw new Error('No new material is cut by this layout');
      const planJson=JSON.stringify(plan);if(Buffer.byteLength(JSON.stringify({...sheet,rosePlanJson:planJson}))>950000)throw new Error('Cut geometry is too complex to save');
      const planHash=hash(planJson+fingerprint(sheet)+stock.revision);
      tx.update(sr,{roseStockId:stock.id,roseRevision:stock.revision,rosePlanJson:planJson,rosePlanHash:planHash,roseFingerprint:fingerprint(sheet),updatedAt:FV.serverTimestamp()});
      return {planJson,planHash};
    });
  }
  async function roseRecordCut(b){
    if(!id(b.sheetId)||!id(b.stockId)||!b.planHash)throw new Error('Prepare the cut contour first');
    return db.runTransaction(async tx=>{
      const ref=stocks().doc(b.stockId),sr=sheets().doc(b.sheetId),er=ref.collection('cuts').doc(b.sheetId);
      const d=await tx.get(ref),sd=await tx.get(sr),ed=await tx.get(er),stock=d.exists&&d.data(),sheet=sd.exists&&sd.data();
      if(ed.exists){if(ed.data().planHash!==b.planHash)throw new Error('This cut was already recorded with a different plan');return {ok:true,cut:ed.data(),stock};}
      if(!stock||stock.owner!==b.sheetId||stock.revision!==b.revision||!sheet||sheet.rosePlanHash!==b.planHash||sheet.roseFingerprint!==fingerprint(sheet)||sheet.roseCutAt)throw new Error('The layout or remnant changed. Refresh before recording a cut');
      const run=sheet.runId?await tx.get(col('Charm_Nest_Runs').doc(sheet.runId)):null;
      const engraving=Readiness.decisions(Object.values(run?.exists?run.data().lines||{}:{}));
      if(!Readiness.sheet({...sheet,engraving}).ready)throw new Error('Complete the sheet’s production checks before recording its cut');
      const plan=parse(sheet.rosePlanJson);Rose.validate(plan.profile,stock.wPt,stock.hPt);
      const at=Date.now(),revision=stock.revision+1;
      const cut={sheetId:b.sheetId,stockId:stock.id,revision,at,planHash:b.planHash,planJson:sheet.rosePlanJson,fileBase:sheet.fileBase||b.sheetId,by:String(b.by||'operator').slice(0,80),createdAt:FV.serverTimestamp()};
      const next={...stock,revision,profileJson:JSON.stringify(plan.profile),owner:null,available:plan.remainingPt2>14*14,lastCutAt:at,updatedAt:FV.serverTimestamp()};
      tx.set(er,cut);tx.set(ref,next);tx.update(sr,{roseCutAt:at,roseCutRevision:revision,updatedAt:FV.serverTimestamp()});
      return {ok:true,cut:{...cut,createdAt:null},stock:{...next,updatedAt:null}};
    });
  }
  // A rehearsal has its own collection and cannot reserve physical stock,
  // create production-ready sheets, export jobs, or complete Etsy orders.
  async function roseDemo(b){
    if(b.sandbox!==true)throw new Error('Rose Gold rehearsal is available only in the sandbox');
    if(!id(b.demoId)||!b.demoId.startsWith('rgdemo-'))throw new Error('Invalid rehearsal ID');
    const ref=db.collection('Sandbox_Charm_Nest_Rose_Rehearsals').doc(b.demoId);
    return db.runTransaction(async tx=>{
      const snap=await tx.get(ref),old=snap.exists?JSON.parse(snap.data().stateJson):null;
      if(b.action==='get')return {state:old};
      if(b.action==='start'){
        if(old)return {state:old};
        const state={id:b.demoId,version:1,revision:0,batch:1,phase:'empty',wPt:100/Rose.MM,hPt:50/Rose.MM,profile:null,cuts:[],placements:[],plan:null,createdMs:Date.now()};
        tx.set(ref,{stateJson:JSON.stringify(state),updatedAt:FV.serverTimestamp()});return {state};
      }
      if(!old)throw new Error('Start a rehearsal first');
      // A retry of the same mutation returns the saved result; stale tabs must reload.
      if(id(b.requestId)&&old.lastRequestId===b.requestId)return {state:old};
      if(!id(b.requestId)||b.revision!==old.revision)throw new Error('This rehearsal changed. Reload saved progress');
      const state={...old,revision:old.revision+1,lastRequestId:b.requestId};
      if(b.action==='nest'){
        if(!['empty','cut'].includes(old.phase)||old.batch>3)throw new Error('Complete the current batch first');
        const {charms,job}=Rose.demoBatch(old.batch,old.profile),pp=b.placements;
        if(!Array.isArray(pp)||pp.length!==job.pieces.length||new Set(pp.map(p=>p.id)).size!==pp.length||pp.some(p=>!job.pieces.some(c=>c.id===p.id)||![p.cxPt,p.cyPt,p.angle].every(Number.isFinite)||Math.abs(p.angle)>360))throw new Error('The complete sample batch must fit before saving');
        const placements=pp.map(p=>({id:p.id,cxPt:p.cxPt,cyPt:p.cyPt,angle:p.angle,scale:1}));
        if(!require('../../charm-nest-solver').verify(job,placements,6).ok)throw new Error('Sample layout overlaps a cut area or another charm. Try nesting again');
        const shapes=Rose.shapes(charms,placements);
        // Check the vector contour as well as the conservative raster mask.
        if(old.profile&&shapes.some(s=>s.paths.some(path=>path.some(([x,y])=>Rose.intersects(old.profile,x,y,0,0)))))throw new Error('Sample vector crosses the saved remnant');
        Object.assign(state,{placements,shapes,phase:'nested',plan:null});
      }else if(b.action==='include'){
        if(old.phase!=='nested')throw new Error('Nest the sample batch first');
        Object.assign(state,{plan:Rose.plan(old.shapes,old.wPt,old.hPt,old.profile,.2),phase:'included'});
      }else if(b.action==='cut'){
        if(old.phase!=='included'||!old.plan)throw new Error('Include this sample batch before simulating its cut');
        const at=Date.now(),cut={revision:old.cuts.length+1,at,plan:old.plan,fileBase:'Sample batch '+old.batch,sheetId:old.id+'-'+old.batch};
        Object.assign(state,{cuts:[...old.cuts,cut],profile:old.plan.profile,phase:old.batch===3?'complete':'cut',batch:old.batch+1,plan:null,placements:[],shapes:[]});
      }else throw new Error('Unknown rehearsal action');
      const stateJson=JSON.stringify(state);if(Buffer.byteLength(stateJson)>900000)throw new Error('Rehearsal history is too large');
      tx.set(ref,{stateJson,updatedAt:FV.serverTimestamp()});return {state};
    });
  }
  return {roseGet,roseList,roseClaim,roseRelease,rosePlan,roseRecordCut,roseDemo};
};
module.exports.fingerprint=fingerprint;

module.exports.protectedLayout=protectedLayout;
module.exports.assertProtected=assertProtected;
