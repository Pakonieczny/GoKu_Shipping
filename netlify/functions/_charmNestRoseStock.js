'use strict';
const Rose=require('../../charm-nest-rose');
const crypto=require('crypto');
const fingerprint=s=>JSON.stringify((s.placements||[]).map(p=>[p.id,+p.cxPt.toFixed(3),+p.cyPt.toFixed(3),p.angle,p.scale||1,p.hash||s.charms?.find(c=>c.id===p.id)?.hash||null]));
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
const id=s=>typeof s==='string'&&/^[\w-]{4,80}$/.test(s);
const parse=s=>s?JSON.parse(s):null;
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
      tx.set(ref,next);
      if(sd.exists)tx.update(sheets().doc(b.sheetId),{roseStockId:ref.id,roseRevision:next.revision,...(b.nesting?{dirty:true,rosePlanJson:null,rosePlanHash:null,roseFingerprint:null}:{})});
      return {...next,updatedAt:null};
    });return {stock};
  }
  async function roseRelease(b){
    if(!id(b.stockId)||!id(b.sheetId))throw new Error('Invalid stock reservation');
    await db.runTransaction(async tx=>{const ref=stocks().doc(b.stockId),d=await tx.get(ref);if(!d.exists||d.data().owner!==b.sheetId)throw new Error('This stock reservation changed');
      const sheet=await tx.get(sheets().doc(b.sheetId));if(sheet.exists&&sheet.data().setId&&!sheet.data().draft)throw new Error('Remove the sheet from its current set before releasing its stock');
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
      const prior=parse(stock.profileJson);
      // Recheck physical exclusions at the persistence boundary, too. This
      // catches a stale rectangular layout attached to a previously cut sheet.
      if(prior)for(const shape of shapes)for(const path of shape.paths||[])for(let i=0;i<path.length;i++){
        const a=path[i],z=path[(i+1)%path.length],n=Math.max(1,Math.ceil(Math.hypot(z[0]-a[0],z[1]-a[1])*4));
        for(let j=0;j<=n;j++){const x=a[0]+(z[0]-a[0])*j/n,y=a[1]+(z[1]-a[1])*j/n;if(Rose.intersects(prior,x,y,0,0))throw new Error('A charm crosses previously cut material. Nest again on this remnant');}
      }
      const plan=Rose.plan(shapes,stock.wPt,stock.hPt,prior,b.allowanceMm);
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
  return {roseGet,roseList,roseClaim,roseRelease,rosePlan,roseRecordCut};
};
module.exports.fingerprint=fingerprint;
