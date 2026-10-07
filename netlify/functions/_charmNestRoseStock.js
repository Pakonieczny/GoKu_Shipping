'use strict';
const Rose=require('../../charm-nest-rose');
const crypto=require('crypto');
const fingerprint=s=>JSON.stringify((s.placements||[]).map(p=>[p.id,+p.cxPt.toFixed(3),+p.cyPt.toFixed(3),p.angle,p.scale||1,p.hash||s.charms?.find(c=>c.id===p.id)?.hash||null]));
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
const id=s=>typeof s==='string'&&/^[\w-]{4,80}$/.test(s);
const parse=s=>s?JSON.parse(s):null;
function protectedLayout(sheet){
  const plan=parse(sheet.rosePlanJson);
  return plan?{profile:plan.profile,lines:plan.lines,shapes:plan.shapes,placements:sheet.placements,stages:plan.stages}:parse(sheet.roseProtectedJson);
}
// Every green line keeps the date it was first prepared and the charms it
// separates. Lines saved before dates were kept show as one undated line.
function stagesOf(guard){
  if(!guard)return [];
  return guard.stages||(guard.lines?.length?[{n:1,at:null,ids:(guard.shapes||guard.placements||[]).map(p=>p.id),lines:[0,guard.lines.length]}]:[]);
}
function assertProtected(guard,placements,metal){
  if(!guard)return;
  const word=Rose.cutWord(metal||'rose');
  if(!Array.isArray(placements))throw new Error('The protected '+word+' layout cannot be moved or removed');
  for(const p of guard.placements){
    const rows=placements.filter(x=>x?.id===p.id),q=rows[0];
    if(rows.length!==1||['cxPt','cyPt','angle'].some(k=>!Number.isFinite(q[k])||Math.abs(q[k]-p[k])>.001)||Math.abs((q.scale||1)-(p.scale||1))>.00001||(p.hash!=null&&q.hash!==p.hash))throw new Error('The protected '+word+' layout cannot be moved or removed');
  }
}
// Requests may carry unsimplified outlines from pages opened before the
// simplification; the saved record size is checked after slimming.
const OUTLINE_TOLERANCE_PT=.1,MAX_SHAPES_JSON=3000000;
function bounds(paths){
  let box=null;for(const path of paths||[])for(const [x,y] of path){if(!Number.isFinite(x)||!Number.isFinite(y))return null;box=box?[Math.min(box[0],x),Math.min(box[1],y),Math.max(box[2],x),Math.max(box[3],y)]:[x,y,x,y];}
  return box;
}
function sameOutline(a,b){
  const p=bounds(a?.paths),q=bounds(b?.paths);
  return !!(p&&q)&&p.every((v,i)=>Math.abs(v-q[i])<=OUTLINE_TOLERANCE_PT);
}
/* Pieces leave a Rose Gold sheet that is not cut yet (a cancelled order's, Paul 29 Sep: "none of them disappears from the
   sheet after being cancelled"). The guard (the saved green lines, the pieces inside them) gives them up:
   - a line that still has a piece inside it stays exactly as saved, byte for byte;
   - a line with nothing left inside it goes, with its part of the lines and its stage (the later lines are numbered again);
   - the profile (the stock the lines have taken) goes back to what the lines that stay have taken. It is worked out again
     from the pieces of those lines, with the same function the lines were drawn with, and used only when that draws the
     lines that stay exactly as they were saved (an allowance nobody kept, or a line from an older version, may not);
     otherwise it is kept as it is (more stock counted as used: never less than is really gone) and `exact` says false.
   Only the pieces named leave; nothing new is ever added (only Cut Sheet draws a line). ctx: { wPt, hPt, prior (the
   stock's profile before this layout), allowanceMm }. Returns { guard (null when no piece is left in one), removed: the
   lines that went, kept: the lines that stay, exact, changed }. */
function withoutPieces(guard, gone, ctx = {}) {
  const off = id => gone.has(id), stages = stagesOf(guard), all = guard.shapes || [];
  const trimmed = stages.map(s => ({ ...s, ids: (s.ids || []).filter(id => !off(id)) }));
  const emptied = stages.map((s, i) => ((s.ids || []).length && !trimmed[i].ids.length ? i : -1)).filter(i => i >= 0);
  const shapes = all.filter(s => !off(s.id)), placements = (guard.placements || []).filter(p => !off(p.id));
  const hit = (guard.placements || []).length !== placements.length || all.length !== shapes.length || stages.some((s, i) => (s.ids || []).length !== trimmed[i].ids.length);
  const summary = (i, st) => ({ n: st[i].n, at: st[i].at == null ? null : st[i].at, pieces: (stages[i].ids || []).length, ids: (stages[i].ids || []).slice() });
  if (!hit) return { guard, removed: [], kept: stages.map((s, i) => summary(i, stages)), exact: true, changed: false };
  const range = i => stages[i].lines || [0, 0], slice = i => (guard.lines || []).slice(range(i)[0], range(i)[1]);
  const keepIdx = stages.map((s, i) => i).filter(i => !emptied.includes(i)), removed = emptied.map(i => summary(i, stages));
  const build = (profile, lines, list) => {
    const g = { profile, lines, shapes, placements };
    if (guard.stages) g.stages = list;
    return placements.length ? g : null;
  };
  // no line went: everything but the pieces stays as saved
  if (!emptied.length) return { guard: build(guard.profile, guard.lines, trimmed), removed, kept: keepIdx.map(i => summary(i, stages)), exact: true, changed: true };
  // a line went: its lines go and the numbers close up
  const first = emptied[0], W = +ctx.wPt, H = +ctx.hPt, shapesOf = ids => { const want = new Set(ids); return all.filter(s => want.has(s.id)); };
  const allowances = [...new Set([+ctx.allowanceMm, .2].filter(a => Number.isFinite(a) && a >= .05 && a <= 2))];
  let done = null;
  if (W > 0 && H > 0 && Rose) for (const a of allowances) {
    try {
      let profile = ctx.prior || null; const lines = [], list = []; let good = true;
      for (let i = 0; i < stages.length && good; i++) {
        if (emptied.includes(i)) continue;
        // a line before the first to go is drawn again to learn the profile it left, and must come out as it was saved;
        // a line after it is drawn against the profile that is left (its own pieces, as it was drawn)
        const p = Rose.plan(shapesOf(stages[i].ids || []).map(Rose.slimShape), W, H, profile, a), was = i < first ? slice(i) : null;
        if (was && JSON.stringify(p.lines) !== JSON.stringify(was)) { good = false; break; }
        const own = was || p.lines;
        list.push({ ...trimmed[i], n: list.length + 1, lines: [lines.length, lines.length + own.length] }); lines.push(...own); profile = p.profile;
      }
      if (good) { done = { profile, lines, list }; break; }
    } catch (_) { /* this allowance does not draw them: the next, then the profile is kept */ }
  }
  if (done) return { guard: build(done.profile, done.lines, done.list), removed, kept: keepIdx.map(i => summary(i, stages)), exact: true, changed: true };
  const lines = [], list = [];
  for (const i of keepIdx) { const own = slice(i); list.push({ ...trimmed[i], n: list.length + 1, lines: [lines.length, lines.length + own.length] }); lines.push(...own); }
  return { guard: build(guard.profile, lines, list), removed, kept: keepIdx.map(i => summary(i, stages)), exact: false, changed: true };
}
/* remnantSync (PS3, _charmNestRemnants.js `sync`): the partial sheet's record (Charm_Nest_Remnants/{stockId}-{revision}) follows the stock's owner INSIDE roseClaim's
   and roseRelease's own transactions: claimed -> 'inUse' (+ lastUsedAt), released -> 'available' again. The stock stays the one source of truth for who holds it. */
module.exports=function({db,col,FV,Readiness,decisionsOfRun,productionReadiness,stamp,sheetLabel,recordRemnant,remnantSync}){
  const stocks=()=>col('Charm_Nest_Rose_Stock'),sheets=()=>col('Charm_Nest_Sheets');
  // Rose Gold, 10K and 14K solid gold share these operations (charm-nest-rose.js: cuts(metal)); a physical sheet belongs to one metal.
  // Stock saved before the metal was kept is Rose Gold's.
  const metalOf=d=>(d&&d.metal)||'rose',metalWord=m=>Rose.cutWord(m);
  const asMetal=m=>{const k=m==null||m===''?'rose':String(m);if(!Rose.cuts(k))throw new Error('Choose a Rose Gold, 10K Gold or 14K Gold sheet');return k;};
  async function roseGet(b){
    if(!id(b.stockId))throw new Error('Choose a sheet');
    const snap=await stocks().doc(b.stockId).get();if(!snap.exists)throw new Error('Sheet not found');
    let q=stocks().doc(b.stockId).collection('cuts').orderBy('revision','desc');if(b.before)q=q.startAfter(+b.before);
    // Keep even the largest saved contours below the function response limit.
    const cuts=await q.limit(4).get();return {stock:snap.data(),cuts:cuts.docs.map(d=>d.data()),more:cuts.size===4};
  }
  // the leftovers that can be used again; {metal} keeps one metal's (no metal: every metal's, as before)
  async function roseList(b){const want=b&&b.metal?asMetal(b.metal):null;const snap=await stocks().where('available','==',true).limit(100).get();return {stocks:snap.docs.map(d=>d.data()).filter(d=>!want||metalOf(d)===want).sort((a,b)=>a.createdMs-b.createdMs)};}
  async function roseClaim(b){
    if(!id(b.sheetId)||![b.wPt,b.hPt].every(n=>Number.isFinite(n)&&n>=14&&n<=1420))throw new Error('Invalid physical sheet');
    const metal=asMetal(b.metal);
    // Resume a reservation after browser recovery before assigning another sheet.
    const held=await stocks().where('owner','==',b.sheetId).limit(1).get();
    let stockId=held.docs[0]?.id||b.stockId;
    // exact (a chosen partial sheet, partialClaim): never quietly keep another physical sheet this layout holds, never create one
    if(b.exact){
      if(!id(b.stockId))throw new Error('Choose a partial sheet');
      if(held.docs[0]&&held.docs[0].id!==b.stockId&&!b.swap)throw new Error('This sheet already holds another physical sheet. Give it back before choosing a partial sheet');
      stockId=b.stockId;
    }
    if(!stockId&&!b.fresh){const available=(await roseList({metal})).stocks;stockId=available.find(s=>Math.abs(s.wPt-b.wPt)<.01&&Math.abs(s.hPt-b.hPt)<.01)?.id;}
    // onlyRemnant (10K and 14K nest on a leftover when one fits, and otherwise claim nothing): no leftover, nothing is created or written
    if(b.onlyRemnant&&!stockId)return {stock:null,protectedJson:null};
    if(stockId&&!id(stockId))throw new Error('Invalid '+metalWord(metal)+' sheet');
    const ref=stockId?stocks().doc(stockId):stocks().doc('rgs-'+crypto.randomUUID());
    const stock=await db.runTransaction(async tx=>{
      const d=await tx.get(ref),old=d.exists?d.data():null;
      if(b.exact&&!old)throw new Error('Partial sheet not found');
      if(old&&metalOf(old)!==metal)throw new Error('This physical sheet is '+metalWord(metalOf(old))+', not '+metalWord(metal));
      if(old&&(Math.abs(old.wPt-b.wPt)>.01||Math.abs(old.hPt-b.hPt)>.01))throw new Error('The physical sheet size cannot change');
      if(old?.owner&&old.owner!==b.sheetId)throw new Error('This '+metalWord(metal)+' sheet is reserved for another layout');
      if(old&&b.revision!=null&&old.revision!==b.revision)throw new Error('This remnant changed. Reload its history before nesting');
      const sd=await tx.get(sheets().doc(b.sheetId));
      if(sd.exists&&sd.data().roseCutAt)throw new Error("This layout was already cut; start a new sheet");
      if(sd.exists&&sd.data().metal&&sd.data().metal!==metal)throw new Error('This sheet is '+metalWord(sd.data().metal)+', not '+metalWord(metal));
      // the partial sheet's record (a read, before the first write); a chosen partial (partialId) must be available or already this sheet's
      const rem=remnantSync&&old&&(b.partialId||old.owner!==b.sheetId)?await remnantSync.read(tx,ref.id,old.revision,b.partialId):null;   // (a sheet that already holds it claimed it before: nothing to read, nothing to change)
      if(b.partialId)remnantSync.check(rem,{metal,sheetId:b.sheetId,stockId:ref.id,revision:old.revision});
      // swap (a chosen partial for a sheet that holds another physical sheet): that one is given back in THIS transaction, as roseRelease would (same refusals), so a refused claim loses nothing
      let off=null;const heldId=held.docs[0]?.id;
      if(b.swap&&heldId&&heldId!==ref.id){
        const oref=stocks().doc(heldId),od=await tx.get(oref),os=od.exists?od.data():null;
        if(os&&os.owner===b.sheetId){
          const own=sd.exists?sd.data():null;
          if(own&&own.setId&&!own.draft)throw new Error('Remove the sheet from its current set before choosing another partial sheet');
          if(own&&(own.rosePlanJson||own.roseProtectedJson))throw new Error('A planned or protected '+metalWord(metal)+' contour cannot be given back');
          off={ref:oref,fresh:metalOf(os)!=='rose'&&!os.revision&&!os.profileJson,rem:remnantSync?await remnantSync.read(tx,heldId,os.revision):null};
        }
      }
      const next={...(old||{id:ref.id,wPt:b.wPt,hPt:b.hPt,revision:0,profileJson:null,createdMs:Date.now()}),metal,owner:b.sheetId,available:false,updatedAt:FV.serverTimestamp()};
      const guard=sd.exists?protectedLayout(sd.data()):null,protectedJson=guard?JSON.stringify(guard):null;
      if(off){if(off.fresh)tx.delete(off.ref);else tx.update(off.ref,{owner:null,available:true,updatedAt:FV.serverTimestamp()});if(remnantSync&&!off.fresh)remnantSync.released(tx,off.rem,{sheetId:b.sheetId,at:Date.now()});}
      tx.set(ref,next);
      if(sd.exists)tx.update(sheets().doc(b.sheetId),{roseStockId:ref.id,roseRevision:next.revision,...(b.nesting?{dirty:true,roseProtectedJson:protectedJson,rosePlanJson:null,rosePlanHash:null,roseFingerprint:null}:{})});
      const partial=remnantSync?remnantSync.claimed(tx,rem,{sheetId:b.sheetId,sheetName:b.sheetName||(sd.exists&&sheetLabel?sheetLabel(sd.data()):''),by:b.by,at:Date.now()}):null;
      return {stock:{...next,updatedAt:null},protectedJson,...(partial?{partial}:{})};
    });return stock;
  }
  async function roseRelease(b){
    if(!id(b.stockId)||!id(b.sheetId))throw new Error('Invalid stock reservation');
    await db.runTransaction(async tx=>{const ref=stocks().doc(b.stockId),d=await tx.get(ref);if(!d.exists||d.data().owner!==b.sheetId)throw new Error('This stock reservation changed');
      const sheet=await tx.get(sheets().doc(b.sheetId));if(sheet.exists&&sheet.data().setId&&!sheet.data().draft)throw new Error('Remove the sheet from its current set before releasing its stock');
      if(sheet.exists&&(sheet.data().rosePlanJson||sheet.data().roseProtectedJson))throw new Error('A planned or protected Rose Gold contour cannot be released');
      // the partial sheet's record (a read, before the first write): it is available again when the stock is
      const rem=remnantSync?await remnantSync.read(tx,b.stockId,d.data().revision):null;
      // a 10K or 14K sheet nobody has cut lets go of its fresh physical sheet by deleting it: an uncut sheet is no leftover (Rose Gold's stays as it was)
      const fresh=metalOf(d.data())!=='rose'&&!d.data().revision&&!d.data().profileJson;
      if(fresh)tx.delete(ref);else tx.update(ref,{owner:null,available:true,updatedAt:FV.serverTimestamp()});if(sheet.exists)tx.update(sheets().doc(b.sheetId),{rosePlanJson:null,rosePlanHash:null,roseStockId:null});
      if(remnantSync&&!fresh)remnantSync.released(tx,rem,{sheetId:b.sheetId,at:Date.now()});});return {ok:true};
  }
  async function rosePlan(b){
    if(!id(b.sheetId)||!id(b.stockId))throw new Error('Choose a sheet');
    if(typeof b.shapesJson!=='string')throw new Error('Invalid cut geometry');
    if(b.shapesJson.length>MAX_SHAPES_JSON)throw new Error('Too much charm outline detail to save this contour. Nest fewer new charms on this sheet');
    // Older pages sent unsimplified outlines; store every contour in the same compact form.
    let shapes;try{shapes=parse(b.shapesJson).map(Rose.slimShape);}catch(_){throw new Error('Invalid cut geometry');}
    return db.runTransaction(async tx=>{
      const ref=stocks().doc(b.stockId),sr=sheets().doc(b.sheetId),d=await tx.get(ref),sd=await tx.get(sr),stock=d.exists&&d.data(),sheet=sd.exists&&sd.data();
      if(!sheet||!Rose.cuts(sheet.metal)||!sheet.verification?.ok||sheet.saving||sheet.dirty||sheet.roseCutAt||!sheet.outputs?.ai)throw new Error('Save and verify this '+metalWord(sheet&&sheet.metal)+' layout first');
      if(!stock||stock.owner!==b.sheetId||stock.revision!==b.revision)throw new Error('The physical sheet changed. Nest it again');
      if(metalOf(stock)!==sheet.metal)throw new Error('This physical sheet is '+metalWord(metalOf(stock))+', not '+metalWord(sheet.metal));
      if(fingerprint(sheet)!==b.fingerprint||shapes.length!==sheet.placements.length||new Set(shapes.map(s=>s.id)).size!==shapes.length||shapes.some(s=>!sheet.placements.some(p=>p.id===s.id)))throw new Error('The layout changed. Prepare its contour again');
      const guard=parse(sheet.roseProtectedJson);assertProtected(guard,sheet.placements,sheet.metal);
      const fixed=new Set((guard?.placements||[]).map(p=>p.id));
      // The saved guard is never rewritten; outlines saved before they were
      // simplified are slimmed only for the new plan copy.
      const protectedShapes=new Map((guard?.shapes||[]).map(s=>[s.id,Rose.slimShape(s)]));
      // Saved placements are rounded to 3 decimals, so a reloaded layout
      // rebuilds its protected outlines a few thousandths of a point away.
      // The stored outlines stay authoritative; only a real move is refused.
      if(guard&&(protectedShapes.size!==fixed.size||shapes.some(s=>fixed.has(s.id)&&!sameOutline(s,protectedShapes.get(s.id)))))throw new Error('The protected '+metalWord(sheet.metal)+' charm outlines cannot be changed');
      const prior=parse(stock.profileJson);
      // Recheck physical exclusions at the persistence boundary, too. This
      // catches a stale rectangular layout attached to a previously cut sheet.
      for(const shape of shapes){const exclusion=!fixed.has(shape.id)&&guard?guard.profile:prior;if(!exclusion)continue;for(const path of shape.paths||[])for(let i=0;i<path.length;i++){
        const a=path[i],z=path[(i+1)%path.length],n=Math.max(1,Math.ceil(Math.hypot(z[0]-a[0],z[1]-a[1])*4));
        for(let j=0;j<=n;j++){const x=a[0]+(z[0]-a[0])*j/n,y=a[1]+(z[1]-a[1])*j/n;if(Rose.intersects(exclusion,x,y,0,0))throw new Error('A charm crosses protected or previously cut material. Nest again on this remnant');}
      }}
      const fresh=guard?shapes.filter(s=>!fixed.has(s.id)):shapes;
      const plan=fresh.length?Rose.plan(fresh,stock.wPt,stock.hPt,guard?.profile||prior,b.allowanceMm):{version:1,profile:guard.profile,lines:[],shapes:[],allowanceMm:b.allowanceMm,remainingPt2:stock.wPt*stock.hPt-Rose.area(guard.profile)};
      // Protected lines saved before lines stopped at the sheet's edge lose
      // their runs along it; lines saved since are kept exactly.
      const allowance=Number.isFinite(+b.allowanceMm)?Math.min(2,Math.max(.2,+b.allowanceMm)):.2;
      const kept=guard?Rose.tidy(guard.lines,stagesOf(guard),stock.wPt,stock.hPt,shapes.map(s=>protectedShapes.get(s.id)||s),allowance/Rose.MM):{lines:[],stages:[]};
      if(guard){plan.lines=[...kept.lines,...plan.lines];plan.shapes=shapes.map(s=>protectedShapes.get(s.id)||s);plan.removedPt2=Rose.area(plan.profile)-(prior?Rose.area(prior):0);}
      // A layout that uses the rest of the sheet needs no new line, but it
      // must still cut new material.
      if(!(plan.removedPt2>0))throw new Error('No new material is cut by this layout');
      const earlier=kept.stages,from=kept.lines.length;
      if(plan.lines.length>from){
        const ids=fresh.map(s=>s.id),saved=stagesOf(parse(sheet.rosePlanJson)),last=saved[saved.length-1],key=list=>JSON.stringify([...list].sort());
        // Preparing the same batch again (a new allowance, a reload) keeps its
        // original date, and a line prepared before dates were kept stays undated.
        const same=last&&last.n===earlier.length+1&&key(last.ids)===key(ids);
        // Only a Cut Sheet press adds a green line (Paul, 29 Sep: Send to Sheet drew line 2 by itself). A page opened
        // before this rule plans on its own, so the rule is kept here; nothing is written.
        if(!same&&b.cut!==true)throw new Error('Only Cut Sheet adds a green line. Reload the page, then press Cut Sheet');
        plan.stages=[...earlier,{n:earlier.length+1,at:same?last.at:Date.now(),ids,lines:[from,plan.lines.length]}];
      }else plan.stages=earlier;
      const planJson=JSON.stringify(plan);if(Buffer.byteLength(JSON.stringify({...sheet,rosePlanJson:planJson}))>950000)throw new Error('Cut geometry is too complex to save');
      const planHash=hash(planJson+fingerprint(sheet)+stock.revision);
      tx.update(sr,{roseStockId:stock.id,roseRevision:stock.revision,rosePlanJson:planJson,rosePlanHash:planHash,roseFingerprint:fingerprint(sheet),updatedAt:FV.serverTimestamp()});
      return {planJson,planHash};
    });
  }
  async function roseRecordCut(b){
    if(!id(b.sheetId)||!id(b.stockId)||!b.planHash)throw new Error('Prepare the cut contour first');
    const out=await db.runTransaction(async tx=>{
      const ref=stocks().doc(b.stockId),sr=sheets().doc(b.sheetId),er=ref.collection('cuts').doc(b.sheetId);
      const d=await tx.get(ref),sd=await tx.get(sr),ed=await tx.get(er),stock=d.exists&&d.data(),sheet=sd.exists&&sd.data();
      if(ed.exists){if(ed.data().planHash!==b.planHash)throw new Error('This cut was already recorded with a different plan');return {ok:true,cut:ed.data(),stock};}
      if(!stock||stock.owner!==b.sheetId||stock.revision!==b.revision||!sheet||sheet.rosePlanHash!==b.planHash||sheet.roseFingerprint!==fingerprint(sheet)||sheet.roseCutAt)throw new Error('The layout or remnant changed. Refresh before recording a cut');
      if(!Rose.cuts(sheet.metal)||metalOf(stock)!==sheet.metal)throw new Error('This physical sheet is '+metalWord(metalOf(stock))+', not '+metalWord(sheet.metal));
      const run=sheet.runId?await tx.get(col('Charm_Nest_Runs').doc(sheet.runId)):null,runData=run?.exists?run.data():null;
      // the lines of orders the run is done with are in its line archive (charmNestLibrary: decisionsOfRun)
      const engraving=decisionsOfRun?await decisionsOfRun(sheet.runId,runData,sheet.poolIds||[]):Readiness.decisions(Object.values(runData?.lines||{}));
      const checked={...sheet,engraving};
      if(productionReadiness)await productionReadiness([checked],{tx});
      if(!Readiness.sheet(checked).ready)throw new Error('Complete every item in the sheet’s orders and its production checks before recording its cut');
      const plan=parse(sheet.rosePlanJson);Rose.validate(plan.profile,stock.wPt,stock.hPt);
      const at=Date.now(),revision=stock.revision+1;
      const cut={sheetId:b.sheetId,stockId:stock.id,revision,at,planHash:b.planHash,planJson:sheet.rosePlanJson,fileBase:sheet.fileBase||b.sheetId,by:String(b.by||'operator').slice(0,80),createdAt:FV.serverTimestamp()};
      // the stock is the saved leftover: its shape (profileJson), its real size (wPt, hPt), its metal, and who cut it from which sheet and when
      const next={...stock,metal:sheet.metal,revision,profileJson:JSON.stringify(plan.profile),owner:null,available:plan.remainingPt2>14*14,lastCutAt:at,lastCutBy:cut.by,lastCutSheetId:b.sheetId,lastCutLabel:sheetLabel?sheetLabel(sheet):String(sheet.fileBase||b.sheetId).slice(0,80),updatedAt:FV.serverTimestamp()};
      // GC3: the leftover sheet this cut makes (its exact outline, real size, who, when) is saved in THIS transaction: a cut never exists without it.
      // It reads (the stock's previous leftover) before it writes, so it comes before the first write below. Rose Gold, 10K and 14K all end here.
      if(recordRemnant)await recordRemnant(tx,{stock:{...next,id:ref.id},cut,sheet,plan,metal:sheet.metal,device:b.device,via:b.via});
      tx.set(er,cut);tx.set(ref,next);tx.update(sr,{roseCutAt:at,roseCutRevision:revision,updatedAt:FV.serverTimestamp()});
      return {ok:true,cut:{...cut,createdAt:null},stock:{...next,updatedAt:null},cutSheet:sheet};
    });
    // every order on the sheet gets the cut on its timeline (charmNestLibrary's stamp never throws); the revision is its id
    const sheet=out.cutSheet;delete out.cutSheet;
    // who cut it, as the sorter's sign-in names them: none is "" and signedIn false, never the ledger's 'operator'
    // (station tracking B); the page it was pressed on is the device. Orders from the pieces' pool ids when unlisted.
    if(sheet&&stamp)await stamp(()=>{const c=out.cut,label=sheetLabel?sheetLabel(sheet):String(sheet.fileBase||c.sheetId).slice(0,80);
      const who=String(c.by||'').trim().slice(0,80),signedIn=!!who&&who!=='operator',device=String(b.device||'').replace(/[^\w.-]/g,'').slice(0,40);
      const orders=Array.isArray(sheet.orders)&&sheet.orders.length?sheet.orders:(sheet.poolIds||[]).map(k=>(/^(\d{1,30})_/.exec(String(k))||[])[1]||'').filter(Boolean);
      return [...new Set(orders.map(String))].slice(0,300).map(orderId=>({orderId,type:'roseCut',at:c.at,by:signedIn?who:'',station:'laser',device,sheetId:c.sheetId,sheet:label,setId:sheet.setId||'',text:label,data:{stockId:c.stockId,revision:c.revision,signedIn},id:`${c.sheetId}-${c.revision}`}));},'rose cut');
    return out;
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
  /* Pieces leave a Rose Gold sheet that is not cut yet ({sheetId, ids: the charm ids, by, at, allowanceMm?}): the page calls
     this before it takes them off its copy of the sheet and saves it (putSheet refuses a save that moves or removes a
     piece of the guard). Only the sheet's saved green lines change (withoutPieces): a line with a piece left stays as
     saved, one with nothing left goes. The sheet is not otherwise written: its files still show the pieces until the
     page writes them again. Idempotent: pieces in no line change nothing, and a repeat finds the lines already gone.
     A sheet that was cut, or marked completed, is refused (its lines stay for good). */
  async function roseTakeOff(b){
    if(!id(b.sheetId))throw new Error('Choose a Rose Gold sheet');
    const ids=[...new Set((Array.isArray(b.ids)?b.ids:[]).map(x=>String(x||'')).filter(Boolean))].slice(0,400);
    if(!ids.length)return {ok:true,changed:false};
    const gone=new Set(ids);
    const out=await db.runTransaction(async tx=>{
      const sr=sheets().doc(b.sheetId),sd=await tx.get(sr);
      if(!sd.exists)return {ok:true,changed:false,missing:true};
      const sheet=sd.data();
      if(!Rose.cuts(sheet.metal))return {ok:true,changed:false};
      if(sheet.roseCutAt||+sheet.laserDoneAt>0)throw new Error('This layout was already cut: its green lines stay');
      const guard=protectedLayout(sheet);
      if(!guard)return {ok:true,changed:false,protectedJson:null};
      const stockDoc=id(sheet.roseStockId)?await tx.get(stocks().doc(sheet.roseStockId)):null,stock=stockDoc&&stockDoc.exists?stockDoc.data():null;
      let prior=null;try{prior=stock?parse(stock.profileJson):null;}catch(_){prior=null;}
      const r=withoutPieces(guard,gone,{wPt:stock?.wPt,hPt:stock?.hPt,prior,allowanceMm:+b.allowanceMm||+sheet.roseAllowanceMm||.2});
      if(!r.changed)return {ok:true,changed:false,protectedJson:sheet.roseProtectedJson||null,planned:!!sheet.rosePlanJson};
      const protectedJson=r.guard?JSON.stringify(r.guard):null;
      // (what the sheet's charms were: the orders whose timeline says the line went)
      const orderOf=new Map((sheet.charms||[]).map(c=>[c.id,String(c.order||(/^(\d{1,30})_/.exec(String(c.poolId||''))||[])[1]||'')]));
      tx.update(sr,{roseProtectedJson:protectedJson,rosePlanJson:null,rosePlanHash:null,roseFingerprint:null,updatedAt:FV.serverTimestamp()});
      return {ok:true,changed:true,protectedJson,removedLines:r.removed,keptLines:r.kept,exact:r.exact,sheet:{id:sheet.id||b.sheetId,label:sheetLabel?sheetLabel(sheet):String(sheet.fileBase||b.sheetId).slice(0,80),setId:sheet.setId||''},
        orders:r.removed.map(l=>({n:l.n,at:l.at,orders:[...new Set(l.ids.map(x=>orderOf.get(x)).filter(Boolean))]}))};
    });
    // each order that had a piece in a line that went: a note on its timeline, kept for good (charmNestLibrary's stamp never throws)
    if(out.changed&&stamp&&out.removedLines.length){
      const when=Number.isFinite(+b.at)&&+b.at>1e12?Math.round(+b.at):Date.now(),by=String(b.by||'System').slice(0,80);
      await stamp(()=>out.orders.flatMap(l=>l.orders.map(orderId=>({orderId,type:'note',at:when,by,station:'sorter',sheetId:out.sheet.id,sheet:out.sheet.label,setId:out.sheet.setId,
        text:`Green line ${l.n} taken off ${out.sheet.label}: nothing was left inside it`,data:{roseLineOff:l.n,lineAt:l.at,sheets:[out.sheet.id],cancel:!!b.cancel},id:`roseLineOff-${out.sheet.id}-${l.n}-${l.at||0}`}))),'rose line off');
    }
    delete out.orders;
    return out;
  }
  return {roseGet,roseList,roseClaim,roseRelease,rosePlan,roseRecordCut,roseTakeOff,roseDemo};
};
module.exports.fingerprint=fingerprint;

module.exports.protectedLayout=protectedLayout;
module.exports.withoutPieces=withoutPieces;
module.exports.stagesOf=stagesOf;
module.exports.assertProtected=assertProtected;
