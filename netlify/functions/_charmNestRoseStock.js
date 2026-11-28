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
// (Paul, 27 Oct 2026, 14K Sheet 1 of Set 1: a piece's own artwork edited in the charm editor gives the piece a hash of its own, its master's with "~ae…" after it (charm-nest-art-edit.js SUFFIX).
//  Its place and its cut line stay exactly as they were (artPut refuses a cut-line change on a piece already on a sheet), so the protected layout is the same layout: the write of the sheet's files
//  that the edit owes was refused here, the record kept "filesOwed" for good and the page held the sheet out of its set)
const masterHash=h=>h==null?h:String(h).replace(/~ae[0-9a-z]+$/,'');
function assertProtected(guard,placements,metal){
  if(!guard)return;
  const word=Rose.cutWord(metal||'rose');
  if(!Array.isArray(placements))throw new Error('The protected '+word+' layout cannot be moved or removed');
  for(const p of guard.placements){
    const rows=placements.filter(x=>x?.id===p.id),q=rows[0];
    if(rows.length!==1||['cxPt','cyPt','angle'].some(k=>!Number.isFinite(q[k])||Math.abs(q[k]-p[k])>.001)||Math.abs((q.scale||1)-(p.scale||1))>.00001||(p.hash!=null&&masterHash(q.hash)!==masterHash(p.hash)))throw new Error('The protected '+word+' layout cannot be moved or removed');
  }
}
/* Pairs and groups (PAIRPARTIAL, Paul 9 Oct: "every piece of an order line is tracked at every step"). A group = one order line, receipt:transaction (the pool id is
   rid_tid_copy; a charm saved with `groupKey` says it itself). The saved sheet record keeps `order` and `poolId` for each charm, so an old record reads the same way. */
const groupOf=c=>{
  if(c&&c.groupKey)return String(c.groupKey);
  const m=/^(\d{1,30})_([^_]*)_\d{1,3}$/.exec(String(c&&c.poolId||''));
  return m?m[1]+':'+m[2]:String(c&&c.order!=null?c.order:c&&c.id);
};
const orderIdOf=c=>String(c&&c.order!=null?c.order:'').split('/')[0];
/* Which pieces of which group a cut took: [{ k, order, cut, onSheet, size, sides, ids }] (flat maps; ids is a flat string list, never an array in an array) for the pieces inside the
   plan's lines (plan.shapes), and a summary { pieces, pairs, split }: pairs = groups whose pieces were cut together (2 pieces, or a size of 2), split = groups of which only some
   pieces were cut (their size is known: charms saved with groupSize; an older record cannot say). */
function cutGroups(sheet,plan){
  const cutIds=new Set((plan&&plan.shapes||[]).map(s=>s.id)),by=new Map();
  for(const c of sheet&&sheet.charms||[]){
    if(!c||c.excluded)continue;
    const k=groupOf(c),g=by.get(k)||{k,order:orderIdOf(c),cut:0,onSheet:0,size:0,sides:'',ids:[]};
    g.onSheet++;if(+c.groupSize>g.size)g.size=+c.groupSize;
    if(cutIds.has(c.id)){g.cut++;g.ids.push(String(c.id));if(c.side==='L'||c.side==='R')g.sides+=c.side;}
    by.set(k,g);
  }
  const groups=[...by.values()].filter(g=>g.cut>0).slice(0,300).map(g=>({k:g.k,order:g.order,cut:g.cut,onSheet:g.onSheet,size:g.size||null,sides:g.sides,ids:g.ids.slice(0,60)}));
  // pairs cut together: a Left with a Right (Amendment 2: every earring pair is one of each, matching or mismatched; a quantity-2 line cut whole is 2 pairs); a group with no sides saved is one pair when it is 2 pieces
  const pairsCut=g=>{const l=(g.sides.match(/L/g)||[]).length,r=(g.sides.match(/R/g)||[]).length;return l&&r?Math.min(l,r):(g.size||g.cut)===2&&g.cut===2?1:0;};
  return {groups,summary:{pieces:groups.reduce((n,g)=>n+g.cut,0),pairs:groups.reduce((n,g)=>n+pairsCut(g),0),split:groups.filter(g=>g.size&&g.cut<g.size).length}};
}
/* pieces taken off a sheet: the groups of which only SOME pieces go (the rest stay): [{ k, order, taken, left }] */
function partialGroups(sheet,gone){
  const by=new Map();
  for(const c of sheet&&sheet.charms||[]){if(!c||c.excluded)continue;const k=groupOf(c),g=by.get(k)||{k,order:orderIdOf(c),taken:0,left:0};if(gone.has(c.id))g.taken++;else g.left++;by.set(k,g);}
  return [...by.values()].filter(g=>g.taken>0&&g.left>0).slice(0,100);
}
/* A request may say what the sheet is about to nest ({ unit: pieces of its smallest order, areaMm2, minMm, maxMm }): an AUTOMATIC choice of a leftover then skips one whose most
   generous estimate holds fewer pieces than that order, because it could only strand part of a pair. No hint, no change. Pure CPU on documents roseList already read. */
function fitCheck(fit){
  const unit=fit&&Math.floor(+fit.unit);
  // (LEFTOVERNOROOM, Paul 27 Nov 2026: a leftover that holds NO piece of the sheet is skipped too, whatever its orders: one with a single-piece order was taken without a look)
  if(!(unit>=1&&unit<=50))return ()=>true;
  const typical={};for(const [k,v] of [['areaMm2',fit.areaMm2],['minMm',fit.minMm],['maxMm',fit.maxMm]])if(Number.isFinite(+v)&&+v>0)typical[k]=+v;
  return s=>{
    if(!s||!s.profileJson)return true;
    try{
      const g=require('./_charmNestRemnants').leftover(parse(s.profileJson));
      return !g.rings.length?false:require('../../charm-nest-partial').estimateFit(g.rings,typical,{sheetWMm:g.sheetWMm,sheetHMm:g.sheetHMm}).high>=unit;
    }catch(_){return true;}
  };
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
/* THELINE (Paul, 23 Nov 2026: "Unless a sheet is marked as laser cut including RG 10k, 14k the user should be able to update/remove/cancel a given charm/charms for a given order on that
   particular sheet. nothing should prevent that"): the pieces of a sheet whose green line is drawn change, its line never does. Every line, its date and the metal it takes stay exactly as
   saved: a piece that leaves leaves a hole; one made again in its place (into: [{ placement, shape }]) stands inside the same line. Only Cut Sheet draws a line. A recorded cut is left alone.
   gone: a Set of charm ids. -> the sheet's fields to write, {} when no line holds them. No read, no write. */
function lineKeeps(sheet, gone, into = []) {
  const guard = sheet.roseCutAt && sheet.rosePlanJson ? null : protectedLayout(sheet), off = x => gone.has(String(x));
  if (!guard || !(guard.placements || []).some(p => off(p.id))) return {};
  const placements = guard.placements.filter(p => !off(p.id)).concat(into.map(x => x.placement)), next = { ...guard, placements, shapes: (guard.shapes || []).filter(s => !off(s.id)).concat(into.map(x => x.shape)) };
  let add = into.map(x => x.placement.id);
  if (guard.stages) next.stages = guard.stages.map(s => { const ids = s.ids || [], mine = ids.some(off) ? add : []; if (mine.length) add = []; return { ...s, ids: ids.filter(x => !off(x)).concat(mine) }; });
  return { roseProtectedJson: placements.length ? JSON.stringify(next) : null, rosePlanJson: null, rosePlanHash: null, roseFingerprint: null };
}
/* remnantSync (PS3, _charmNestRemnants.js `sync`): the partial sheet's record (Charm_Nest_Remnants/{stockId}-{revision}) follows the stock's owner INSIDE roseClaim's
   and roseRelease's own transactions: claimed -> 'inUse' (+ lastUsedAt), released -> 'available' again. The stock stays the one source of truth for who holds it. */
module.exports=function({db,col,FV,Readiness,decisionsOfRun,productionReadiness,stamp,sheetLabel,recordRemnant,remnantSync,fence}){
  // (CLOUDDRIVE P7, cloud plan v2 §1.3: the first read of roseClaim's, rosePlan's and roseRecordCut's transactions for the cloud's own call; null for every page's call)
  const fenced=async tx=>{const f=fence&&fence(tx);if(f)await f;};
  const COMPLETED='This sheet is Completed (laser cut): its metal has been cut, so it cannot move to another partial sheet';
  const stocks=()=>col('Charm_Nest_Rose_Stock'),sheets=()=>col('Charm_Nest_Sheets');
  // Rose Gold, 10K and 14K solid gold share these operations (charm-nest-rose.js: cuts(metal)); a physical sheet belongs to one metal.
  // Stock saved before the metal was kept is Rose Gold's.
  const metalOf=d=>(d&&d.metal)||'rose',metalWord=m=>Rose.cutWord(m);
  const asMetal=m=>{const k=m==null||m===''?'rose':String(m);if(!Rose.cuts(k))throw new Error('Choose a Rose Gold, 10K Gold or 14K Gold sheet');return k;};
  async function roseGet(b){
    if(!id(b.stockId))throw new Error('Choose a sheet');
    const snap=await stocks().doc(b.stockId).get();if(!snap.exists)throw new Error('Sheet not found');
    // noCuts (the partial sheet trial pack, charm-nest-partial-nest.js): only the physical sheet itself, ONE document read and none of the cuts' heavy plans
    if(b.noCuts===true)return {stock:snap.data(),cuts:[],more:false};
    let q=stocks().doc(b.stockId).collection('cuts').orderBy('revision','desc');if(b.before)q=q.startAfter(+b.before);
    // Keep even the largest saved contours below the function response limit.
    // (HISTFILL, Paul 14 Nov 2026: RG Sheet 1's view lost its first cut after cut 3 was undone and recorded again): a set-aside or superseded cut no longer takes one of the four places;
    //  up to eight are read and the first four that count are sent, so the picture keeps every cut it showed before
    const cuts=await q.limit(8).get(),kept=(seen=>cuts.docs.map(d=>d.data()).filter(c=>!c.setAsideAt&&!(c.sheetId&&(seen.has(c.sheetId)||!seen.add(c.sheetId)))))(new Set());
    const sent=kept.slice(0,4),more=kept.length>4||cuts.size===8;
    return {stock:snap.data(),cuts:sent,more};   // (ASIDE: a cut set aside is no part of the sheet's history: nothing grey is drawn for it)
  }
  // the leftovers that can be used again; {metal} keeps one metal's (no metal: every metal's, as before)
  async function roseList(b){const want=b&&b.metal?asMetal(b.metal):null;const snap=await stocks().where('available','==',true).limit(100).get();return {stocks:snap.docs.map(d=>d.data()).filter(d=>!want||metalOf(d)===want).sort((a,b)=>a.createdMs-b.createdMs)};}
  async function roseClaim(b){
    if(!id(b.sheetId)||![b.wPt,b.hPt].every(n=>Number.isFinite(n)&&n>=14&&n<=1420))throw new Error('Invalid physical sheet');
    const metal=asMetal(b.metal);
    // Resume a reservation after browser recovery before assigning another sheet.
    const held=await stocks().where('owner','==',b.sheetId).limit(1).get();
    let stockId=held.docs[0]?.id||b.stockId;
    /* LEFTOVERNOROOM (Paul, 27 Nov 2026, RG Draft 1 of Nov 26: "Preview unavailable", 0 of its 22 pieces placed): the draft opened on the newest cut's leftover (RGLINK, the nest's hint)
       took it with no look at its room, a strip about 16 pt wide that holds no piece, and stood there for good. A leftover only NAMED (no exact choice, not held by this sheet) is taken
       when the room check takes it and this sheet has not found it without room before (b.skip, the sheet's greenNoRoom); else the automatic choice below, as with no name given. */
    const skip=new Set((Array.isArray(b.skip)?b.skip:[]).map(String).slice(0,20));
    if(!held.docs[0]&&b.stockId&&!b.exact&&(b.fit||skip.size)){
      const hd=id(b.stockId)?await stocks().doc(b.stockId).get():null,hs=hd&&hd.exists?hd.data():null;
      if(!hs||skip.has(String(b.stockId))||!fitCheck(b.fit)(hs))stockId=null;
    }
    // exact (a chosen partial sheet, partialClaim): never quietly keep another physical sheet this layout holds, never create one
    if(b.exact){
      if(!id(b.stockId))throw new Error('Choose a partial sheet');
      if(held.docs[0]&&held.docs[0].id!==b.stockId&&!b.swap)throw new Error('This sheet already holds another physical sheet. Give it back before choosing a partial sheet');
      stockId=b.stockId;
    }
    // (RGC1, Paul 9 Nov 2026: one physical sheet is used up before a new one is started: the automatic choice takes the metal's NEWEST leftover with room (the one the last green line made),
    //  then any other sheet on the list oldest first as before, and a brand-new sheet only when none fits. Every metal with a green line. A chosen sheet (stockId, exact) and fresh are as they were)
    if(!stockId&&!b.fresh){const available=(await roseList({metal})).stocks,room=fitCheck(b.fit),left=s=>+s.revision>0||!!s.profileJson,cutAt=s=>+s.lastCutAt||0;
      const order=available.filter(left).sort((x,y)=>cutAt(y)-cutAt(x)).concat(available.filter(s=>!left(s)));
      stockId=order.find(s=>Math.abs(s.wPt-b.wPt)<.01&&Math.abs(s.hPt-b.hPt)<.01&&room(s)&&!skip.has(String(s.id)))?.id;}
    // onlyRemnant (10K and 14K nest on a leftover when one fits, and otherwise claim nothing): no leftover, nothing is created or written
    if(b.onlyRemnant&&!stockId)return {stock:null,protectedJson:null};
    if(stockId&&!id(stockId))throw new Error('Invalid '+metalWord(metal)+' sheet');
    const ref=stockId?stocks().doc(stockId):stocks().doc('rgs-'+crypto.randomUUID());
    const stock=await db.runTransaction(async tx=>{
      await fenced(tx);
      const d=await tx.get(ref),old=d.exists?d.data():null;
      if(b.exact&&!old)throw new Error('Partial sheet not found');
      if(old&&metalOf(old)!==metal)throw new Error('This physical sheet is '+metalWord(metalOf(old))+', not '+metalWord(metal));
      if(old&&old.deleted)throw new Error('This sheet was deleted');   // (a sheet a person deleted, sheetDelete: kept for its history, never nested on again)
      if(old&&(Math.abs(old.wPt-b.wPt)>.01||Math.abs(old.hPt-b.hPt)>.01))throw new Error('The physical sheet size cannot change');
      if(old?.owner&&old.owner!==b.sheetId)throw new Error('This '+metalWord(metal)+' sheet is reserved for another layout');
      if(old&&b.revision!=null&&old.revision!==b.revision)throw new Error('This remnant changed. Reload its history before nesting');
      const sd=await tx.get(sheets().doc(b.sheetId)),own=sd.exists?sd.data():null;
      // a recorded cut (Paul, 7 Oct: "This sheet is already in use so why can't I choose to use it?"): only a re-seat (an exact claim with swap) may set it aside, and only while the
      // metal is not really cut. Cut Sheet records the cut and draws the green line; the metal is cut for good when the laser marks the sheet (or its set) Completed (laserDoneAt).
      const reseat=b.exact===true&&b.swap===true,wasCut=!!(own&&own.roseCutAt);
      if(wasCut&&!reseat)throw new Error("This layout was already cut; start a new sheet");
      if(wasCut){
        if(+own.laserDoneAt>0)throw new Error(COMPLETED);
        const cutSet=id(own.setId)?await tx.get(col('Charm_Nest_Sets').doc(own.setId)):null;
        if(cutSet&&cutSet.exists&&+cutSet.data().laserDoneAt>0)throw new Error(COMPLETED);
      }
      if(own&&own.metal&&own.metal!==metal)throw new Error('This sheet is '+metalWord(own.metal)+', not '+metalWord(metal));
      // the partial sheet's record (a read, before the first write); a chosen partial (partialId) must be available or already this sheet's
      const rem=remnantSync&&old&&(b.partialId||old.owner!==b.sheetId)?await remnantSync.read(tx,ref.id,old.revision,b.partialId,old.made):null;   // (a sheet that already holds it claimed it before: nothing to read, nothing to change)
      if(b.partialId)remnantSync.check(rem,{metal,sheetId:b.sheetId,stockId:ref.id,revision:old.revision});
      // swap (a chosen partial for a sheet that holds another physical sheet): that one is given back in THIS transaction, so a refused claim loses nothing.
      // (Paul, 7 Oct: "There's no reason why I should be prevented.") A sheet in a committed or the current set, approved for Laser cutting, or holding a saved
      // green line that was never cut MAY swap: its set, seals and history are not touched here. So may a sheet with a recorded cut that is not Completed (below: the cut is
      // set aside; a cut makes a new revision of the stock, so a stock a cut was recorded on is a leftover, never "held" by the sheet). Only Completed (laserDoneAt) refuses.
      let off=null;const heldId=held.docs[0]?.id;
      if(b.swap&&heldId&&heldId!==ref.id){
        const oref=stocks().doc(heldId),od=await tx.get(oref),os=od.exists?od.data():null;
        if(os&&os.owner===b.sheetId)off={ref:oref,fresh:metalOf(os)!=='rose'&&!os.revision&&!os.profileJson&&!os.made,rem:remnantSync?await remnantSync.read(tx,heldId,os.revision,null,os.made):null};
      }
      // the leftover the sheet's own recorded cut made (stock of the cut, at the revision the cut gave it), read before the first write: it is the partial being chosen (the sheet
      // seats on its own leftover), or free metal that must not stay on offer once the cut is set aside, or already taken by another sheet (then the sheet cannot move)
      let cutLeft=null;
      if(wasCut&&id(own.roseStockId)){
        const cutRef=stocks().doc(own.roseStockId),csd=own.roseStockId===ref.id?d:await tx.get(cutRef),cs=csd.exists?csd.data():null;
        const rev=own.roseCutRevision!=null&&Number.isFinite(+own.roseCutRevision)?+own.roseCutRevision:(cs&&cs.lastCutSheetId===b.sheetId?+cs.revision:null);   // (a cut saved before the revision was kept on the sheet: the stock says whose last cut it was)
        if(rev>=1){
          const mine=own.roseStockId===ref.id&&!!old&&+old.revision===rev;
          cutLeft={ref:cutRef,stock:cs,revision:rev,id:own.roseStockId+'-'+rev,mine,rem:mine?rem:(remnantSync?await remnantSync.read(tx,own.roseStockId,rev,null,false):null)};
        }
      }
      let cutNote=null,discard=false;
      if(wasCut){
        cutNote={at:+own.roseCutAt||null,revision:cutLeft?cutLeft.revision:(own.roseCutRevision!=null?+own.roseCutRevision:null),stockId:own.roseStockId||null,leftover:cutLeft?cutLeft.id:null,how:'none'};
        if(cutLeft&&cutLeft.mine)cutNote.how='claimed';
        else if(cutLeft){
          const r=cutLeft.rem&&cutLeft.rem.exists?cutLeft.rem.data:null,cs=cutLeft.stock,current=!!cs&&+cs.revision===cutLeft.revision;
          const usedBy=r&&r.status==='used'&&r.usedBySheetId!==b.sheetId?(r.usedBySheetName||'another sheet'):cs&&+cs.revision>cutLeft.revision&&!(r&&r.status==='used')?'another sheet':null;
          const holder=usedBy?null:r&&r.status==='inUse'&&r.inUseBySheetId!==b.sheetId?(r.inUseBySheetName||'another sheet'):current&&cs.owner&&cs.owner!==b.sheetId?'another sheet':null;
          if(holder||usedBy)throw new Error(`The leftover sheet this sheet's cut made is already taken (${holder?'in use by ':'used by '}${holder||usedBy}), so its metal is not free. This sheet cannot move to another partial sheet`);
          discard=r?r.status==='available':!!(current&&cs.available);
          cutNote.how=discard?'discarded':'kept';
        }
      }
      const next={...(old||{id:ref.id,wPt:b.wPt,hPt:b.hPt,revision:0,profileJson:null,createdMs:Date.now()}),metal,owner:b.sheetId,available:false,updatedAt:FV.serverTimestamp()};
      // a re-seat (an exact claim with swap) starts the layout over on the new outline: the green line saved for the old seat (a plan, or the lines an append kept) is
      // dropped with it, never carried onto another sheet's outline; every other claim keeps the saved lines as it always did (protected layout)
      const guard=own&&!reseat?protectedLayout(own):null,protectedJson=guard?JSON.stringify(guard):null,at=Date.now();
      if(off){if(off.fresh)tx.delete(off.ref);else tx.update(off.ref,{owner:null,available:true,updatedAt:FV.serverTimestamp()});if(remnantSync&&!off.fresh)remnantSync.released(tx,off.rem,{sheetId:b.sheetId,at});}
      // the old cut's leftover, not the partial being chosen: no longer free metal (its record 'discarded' with why, who and when, kept for good and shown under Discarded; the stock
      // stops being offered to the older path too). A person can still put it back from its card. Nothing is deleted.
      if(discard){
        if(remnantSync&&remnantSync.superseded&&cutLeft.rem&&cutLeft.rem.exists)remnantSync.superseded(tx,cutLeft.rem,{sheetId:b.sheetId,sheetName:b.sheetName||(own&&sheetLabel?sheetLabel(own):''),by:b.by,at});
        if(cutLeft.stock&&+cutLeft.stock.revision===cutLeft.revision&&!cutLeft.stock.owner)tx.update(cutLeft.ref,{available:false,updatedAt:FV.serverTimestamp()});
      }
      tx.set(ref,next);
      // (what went is noted on the sheet's record, small and append-only: when, who, which seat, what kind; a recorded cut that was set aside, which leftover it made and what became of it)
      const dropped=reseat&&own&&(own.rosePlanJson||own.roseProtectedJson||wasCut)?[...(Array.isArray(own.roseReseated)?own.roseReseated:[]).slice(-9),{at,by:String(b.by||'').slice(0,80),fromStockId:heldId||null,toStockId:ref.id,plan:!!own.rosePlanJson,kept:!!own.roseProtectedJson,...(cutNote?{cut:cutNote}:{})}]:null;
      // (the sheet owes a new Cut Sheet on its new seat: its recorded cut is cleared on the sheet, the old cut's own record, the leftover and the history stay)
      if(sd.exists)tx.update(sheets().doc(b.sheetId),{roseStockId:ref.id,roseRevision:next.revision,...(b.nesting||reseat?{dirty:true,roseProtectedJson:protectedJson,rosePlanJson:null,rosePlanHash:null,roseFingerprint:null}:{}),...(wasCut?{roseCutAt:null,roseCutRevision:null}:{}),...(dropped?{roseReseated:dropped}:{})});
      const partial=remnantSync?remnantSync.claimed(tx,rem,{sheetId:b.sheetId,sheetName:b.sheetName||(sd.exists&&sheetLabel?sheetLabel(sd.data()):''),by:b.by,at}):null;
      return {stock:{...next,updatedAt:null},protectedJson,...(partial?{partial}:{}),...(cutNote?{setAside:cutNote}:{})};
    });return stock;
  }
  async function roseRelease(b){
    if(!id(b.stockId)||!id(b.sheetId))throw new Error('Invalid stock reservation');
    await db.runTransaction(async tx=>{const ref=stocks().doc(b.stockId),d=await tx.get(ref);if(!d.exists||d.data().owner!==b.sheetId)throw new Error('This stock reservation changed');
      const sheet=await tx.get(sheets().doc(b.sheetId));if(sheet.exists&&sheet.data().setId&&!sheet.data().draft)throw new Error('Remove the sheet from its current set before releasing its stock');
      if(sheet.exists&&(sheet.data().rosePlanJson||sheet.data().roseProtectedJson))throw new Error('A planned or protected Rose Gold contour cannot be released');
      // the partial sheet's record (a read, before the first write): it is available again when the stock is
      const rem=remnantSync?await remnantSync.read(tx,b.stockId,d.data().revision,null,d.data().made):null;
      // a 10K or 14K sheet nobody has cut lets go of its fresh physical sheet by deleting it: an uncut sheet is no leftover (Rose Gold's stays as it was)
      const fresh=metalOf(d.data())!=='rose'&&!d.data().revision&&!d.data().profileJson&&!d.data().made;   // (a sheet a person made, sheetMake, is a repository sheet: it is never deleted, it goes back on the list)
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
      await fenced(tx);
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
  // (Paul, 26 Oct 2026: "adjust the logic for the 10K and 14 K gold sheets so that they don't get automatically approved and the green cut lines do not get automatically engaged". The automatic
  //  Cut Sheet press (Gate.autoCut, via 'auto') is Rose Gold's only: a 10K / 14K sheet's cut is a person's press, with every check. Refused here too, whatever a page sends)
  const AUTO_REFUSED=new Set(['gold10k','gold14k']);
  const autoRefusal=metal=>'Not cut: a '+metalWord(metal)+' sheet is never cut automatically. It waits in its set for a person to approve it and cut its green line (Approve & cut green line)';
  async function roseRecordCut(b){
    if(!id(b.sheetId)||!id(b.stockId)||!b.planHash)throw new Error('Prepare the cut contour first');
    if(b.via==='auto'&&AUTO_REFUSED.has(b.metal))throw new Error(autoRefusal(b.metal));
    const out=await db.runTransaction(async tx=>{
      await fenced(tx);
      const ref=stocks().doc(b.stockId),sr=sheets().doc(b.sheetId);
      const d=await tx.get(ref),sd=await tx.get(sr),stock=d.exists&&d.data(),sheet=sd.exists&&sd.data();
      if(b.via==='auto'&&sheet&&AUTO_REFUSED.has(sheet.metal))throw new Error(autoRefusal(sheet.metal));   // (the sheet's own metal decides, not the request's)
      // a cut is kept under its sheet's id on the stock. A sheet that set an earlier cut on this very stock aside (it was moved onto the leftover that cut made, roseClaim) cuts again
      // under the revision it makes, so the old cut's record stays exactly as it was; a retry of the same press (the same request revision) finds the same document
      const again=!!(sheet&&Array.isArray(sheet.roseReseated)&&sheet.roseReseated.some(x=>x&&x.cut&&x.cut.stockId===b.stockId));
      const er=ref.collection('cuts').doc(again?`${b.sheetId}-${(+b.revision||0)+1}`:b.sheetId),ed=await tx.get(er);
      if(ed.exists){if(ed.data().planHash!==b.planHash)throw new Error('This cut was already recorded with a different plan');return {ok:true,cut:ed.data(),stock};}
      if(!stock||stock.owner!==b.sheetId||stock.revision!==b.revision||!sheet||sheet.rosePlanHash!==b.planHash||sheet.roseFingerprint!==fingerprint(sheet)||sheet.roseCutAt)throw new Error('The layout or remnant changed. Refresh before recording a cut');
      if(!Rose.cuts(sheet.metal)||metalOf(stock)!==sheet.metal)throw new Error('This physical sheet is '+metalWord(metalOf(stock))+', not '+metalWord(sheet.metal));
      const run=sheet.runId?await tx.get(col('Charm_Nest_Runs').doc(sheet.runId)):null,runData=run?.exists?run.data():null;
      // the lines of orders the run is done with are in its line archive (charmNestLibrary: decisionsOfRun)
      const engraving=decisionsOfRun?await decisionsOfRun(sheet.runId,runData,sheet.poolIds||[]):Readiness.decisions(Object.values(runData?.lines||{}));
      const checked={...sheet,engraving};
      if(productionReadiness)await productionReadiness([checked],{tx});
      // (Paul, 26 Oct 2026: the AUTOMATIC cut of a green-line sheet whose Gold Filled / Sterling Silver set is complete (Gate.autoCut, via 'auto') is not held for its back engraving:
      //  the set's Approve for laser cutting holds it until the backs are approved, so the cut is not held twice. Every other check stands; a person's Cut Sheet press is as before)
      // (CUTFREE, Paul 12 Nov 2026: "Nothing can prevent a sheet from being cut with the green cut line." The cut is recorded whatever the sheet's orders and production checks
      //  say: a design still to come, an engraving still to approve, a piece of another metal not on a saved sheet yet. Those hold the SET's Approve for laser cutting, never the
      //  green line. Only what makes a cut record wrong stays above: the plan must be the one saved for this layout, on this sheet's own physical stock)
      const rd=Readiness.sheet(checked),auto=b.via==='auto',cutReady=true;
      if(!cutReady){
        // (Paul, 26 Oct 2026: the automatic cut says WHY it waits, in the shop's words, for the sheet's card: "order 4199104309: waiting for its own design"; a person's press keeps its one line)
        if(auto){
          const STAGE={layout:'its layout is not saved and verified',front:'its sheet files are not made',qr:'its QR label is not made',orders:'an order on it waits'};
          let why=Object.entries(rd.stages).filter(([k,v])=>!v&&k!=='approval'&&k!=='backs').map(([k])=>STAGE[k]||k);
          try{ if(!rd.stages.orders&&Readiness.orderIds&&Readiness.forSheet){ const sid=b.sheetId,os=Readiness.orderIds(checked).filter(id=>{const f=Readiness.forSheet(checked.orderReadiness&&checked.orderReadiness[id],sid,Readiness.setOf?Readiness.setOf(checked):null,checked.metal);return !(f&&f.ready===true);}).slice(0,3);
            const said=os.map(id=>{const f=Readiness.forSheet(checked.orderReadiness&&checked.orderReadiness[id],sid,Readiness.setOf?Readiness.setOf(checked):null,checked.metal);const w=f&&(Readiness.blockText?Readiness.blockText(f):f.why);return `order ${id}${w?': '+w:''}`;});
            if(said.length)why=why.filter(x=>x!==STAGE.orders).concat(said); } }catch(_){ /* the stage words stand */ }
          if(!rd.included)why.unshift('it is not in a set');
          throw new Error('Not cut yet: '+(why.join('; ')||'its production checks are not done'));
        }
        throw new Error('Complete every item in the sheet’s orders and its production checks before recording its cut');
      }
      const plan=parse(sheet.rosePlanJson);Rose.validate(plan.profile,stock.wPt,stock.hPt);
      const at=Date.now(),revision=stock.revision+1;
      // which pieces of which order line this cut took (permanent, with the cut): a pair cut together, or a group of which only some pieces were cut
      const taken=cutGroups(sheet,plan);
      const cut={sheetId:b.sheetId,stockId:stock.id,revision,at,planHash:b.planHash,planJson:sheet.rosePlanJson,fileBase:sheet.fileBase||b.sheetId,by:String(b.by||'operator').slice(0,80),...(taken.groups.length?{groups:taken.groups}:{}),createdAt:FV.serverTimestamp()};
      // the stock is the saved leftover: its shape (profileJson), its real size (wPt, hPt), its metal, and who cut it from which sheet and when
      const next={...stock,metal:sheet.metal,revision,profileJson:JSON.stringify(plan.profile),owner:null,available:plan.remainingPt2>14*14,lastCutAt:at,lastCutBy:cut.by,lastCutSheetId:b.sheetId,lastCutLabel:sheetLabel?sheetLabel(sheet):String(sheet.fileBase||b.sheetId).slice(0,80),updatedAt:FV.serverTimestamp()};
      // GC3: the leftover sheet this cut makes (its exact outline, real size, who, when) is saved in THIS transaction: a cut never exists without it.
      // It reads (the stock's previous leftover) before it writes, so it comes before the first write below. Rose Gold, 10K and 14K all end here.
      if(recordRemnant)await recordRemnant(tx,{stock:{...next,id:ref.id},cut,sheet,plan,metal:sheet.metal,device:b.device,via:b.via,summary:taken.groups.length?taken.summary:null});
      tx.set(er,cut);tx.set(ref,next);tx.update(sr,{roseCutAt:at,roseCutRevision:revision,roseReopened:null,updatedAt:FV.serverTimestamp()});   // (roseReopened: a cut set aside by a change by hand is recorded again: the card's note goes)
      return {ok:true,cut:{...cut,createdAt:null},stock:{...next,updatedAt:null},cutSheet:sheet};
    });
    // every order on the sheet gets the cut on its timeline (charmNestLibrary's stamp never throws); the revision is its id
    const sheet=out.cutSheet;delete out.cutSheet;
    // who cut it, as the sorter's sign-in names them: none is "" and signedIn false, never the ledger's 'operator'
    // (station tracking B); the page it was pressed on is the device. Orders from the pieces' pool ids when unlisted.
    if(sheet&&stamp)await stamp(()=>{const c=out.cut,label=sheetLabel?sheetLabel(sheet):String(sheet.fileBase||c.sheetId).slice(0,80);
      const who=String(c.by||'').trim().slice(0,80),signedIn=!!who&&who!=='operator',device=String(b.device||'').replace(/[^\w.-]/g,'').slice(0,40);
      const orders=Array.isArray(sheet.orders)&&sheet.orders.length?sheet.orders:(sheet.poolIds||[]).map(k=>(/^(\d{1,30})_/.exec(String(k))||[])[1]||'').filter(Boolean);
      // (pieces: how many of this order's pieces the cut took: the same count the green line's own event carries)
      const pieces=new Map();for(const g of c.groups||[])pieces.set(String(g.order),(pieces.get(String(g.order))||0)+g.cut);
      return [...new Set(orders.map(String))].slice(0,300).map(orderId=>({orderId,type:'roseCut',at:c.at,by:signedIn?who:'',station:'laser',device,sheetId:c.sheetId,sheet:label,setId:sheet.setId||'',text:label,data:{stockId:c.stockId,revision:c.revision,signedIn,...(pieces.get(orderId)?{pieces:pieces.get(orderId)}:{})},id:`${c.sheetId}-${c.revision}`}));},'rose cut');
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
  /* Pieces leave a sheet whose green line is drawn ({sheetId, ids: the charm ids}): a page calls this before it takes them off its copy of the sheet and saves it (putSheet refuses a save that
     moves or removes a piece of the guard). THELINE: every line stays as saved, only the pieces are no longer named inside it. The sheet is not otherwise written. Idempotent. */
  async function roseTakeOff(b){
    if(!id(b.sheetId))throw new Error('Choose a Rose Gold sheet');
    const gone=new Set((Array.isArray(b.ids)?b.ids:[]).map(x=>String(x||'')).filter(Boolean).slice(0,400));
    if(!gone.size)return {ok:true,changed:false};
    return db.runTransaction(async tx=>{
      const sr=sheets().doc(b.sheetId),sd=await tx.get(sr);
      if(!sd.exists)return {ok:true,changed:false,missing:true};
      const sheet=sd.data();
      if(!Rose.cuts(sheet.metal))return {ok:true,changed:false};
      if(+sheet.laserDoneAt>0)throw new Error('This sheet is marked laser cut: its pieces stay');
      const upd=lineKeeps(sheet,gone),parts=partialGroups(sheet,gone),more=parts.length?{partialGroups:parts}:{};   // (groups of which only some pieces were asked to leave: the answer says so)
      if(!('roseProtectedJson' in upd))return {ok:true,changed:false,protectedJson:sheet.roseProtectedJson||null,planned:!!sheet.rosePlanJson,...more};
      tx.update(sr,{...upd,updatedAt:FV.serverTimestamp()});
      return {ok:true,changed:true,protectedJson:upd.roseProtectedJson,...more};
    });
  }
  /* REOPEN (Paul, 27 Oct 2026: "the users should be able to add and remove charms from 10 K and 14 K gold sheets even after the green cut line has been approved and even after the entire
     sheet has been approved. This must be allowed to happen until such time a sheet has been cut on a laser and marked as cut by the user at which point a sheet is locked and could never be
     changed"). A 10K / 14K sheet whose charms are about to change by hand (the sheet window's Manual Control, the sheet hop) first gives up what was drawn or recorded for its layout as it stood:
     its contour and green lines (a plan, the lines a nest kept: the line follows the charms, and the page draws it again for the new layout) and a recorded cut (the Cut Sheet press, the line's
     approval: Cut Sheet records it again). Nothing physical is undone: the metal is cut only when the laser marks the sheet or its set Completed (laserDoneAt), and that alone refuses.
     A recorded cut is set aside as a re-seat sets one aside (roseClaim): the stock it was recorded on is the sheet's again with the outline it had before that cut (the earlier cut's on that
     stock; none for its first), the leftover that cut made is no longer offered ('discarded' with why, who and when: remnantSync.superseded), the cut's own record and the history stay. Its
     metal must still be free: a leftover another sheet holds or was cut from is that sheet's outline now, so that alone also refuses, in words. The sheet's note `roseReseated` gets the
     entry roseRecordCut reads, so the next press records a new cut under its own revision. {sheetId, by} → {ok, changed, lines, cut: {at, revision, stockId, leftover, how} | null, stock | null} */
  const REOPEN_METALS=new Set(['gold10k','gold14k']);
  async function roseReopen(b){
    if(!id(b.sheetId))throw new Error('Choose a 10K or 14K Gold sheet');
    const by=String(b.by||'').slice(0,80),at=Date.now();
    const out=await db.runTransaction(async tx=>{
      const sr=sheets().doc(b.sheetId),sd=await tx.get(sr);
      if(!sd.exists)return {ok:true,changed:false,missing:true};
      const sheet=sd.data(),name=String((sheetLabel&&sheetLabel(sheet))||sheet.fileBase||'This sheet').slice(0,80);
      // (IPL2, Paul 9 Nov 2026: "Unless the sheet has been laser cut, the update must be allowed to proceed": a charm REPLACED by an updated SKU or an updated charm from the repository (b.replace) sets
      //  aside a Rose Gold sheet's green line too; a change by hand on Rose Gold stays refused, as before)
      if(!REOPEN_METALS.has(sheet.metal)&&!(b.replace===true&&sheet.metal==='rose'))throw new Error('Only a 10K or 14K Gold sheet changes after its green line');
      const lasered=`${name} has been cut on the laser: its charms can no longer change`;
      if(+sheet.laserDoneAt>0)throw new Error(lasered);
      const set=id(sheet.setId)?await tx.get(col('Charm_Nest_Sets').doc(sheet.setId)):null;
      if(set&&set.exists&&+set.data().laserDoneAt>0)throw new Error(lasered);
      const wasCut=!!sheet.roseCutAt;
      const linesOf=j=>{try{const g=parse(j);return g&&Array.isArray(g.lines)?g.lines.length:0;}catch(_){return 0;}},lines=Math.max(linesOf(sheet.rosePlanJson),linesOf(sheet.roseProtectedJson));
      /* ASIDE (Paul, 12 Nov 2026: RG Sheet 1's cut was set aside twice, and its physical sheet came back with its left part grey, "already used", though nothing was cut on the laser): a cut this
         sheet set aside (its roseReseated entries with reopen) never counts again. The outline before a cut is the one the latest cut that still stands left (another sheet's cut of a leftover
         included), or the whole sheet when none stands. */
      const asideRevs=new Set((Array.isArray(sheet.roseReseated)?sheet.roseReseated:[]).filter(x=>x&&x.reopen&&x.cut&&x.cut.revision!=null&&Number.isFinite(+x.cut.revision)).map(x=>+x.cut.revision));
      const priorOf=async(sref,rev)=>{
        for(let r=rev-1;r>=1;r--){
          const q=await tx.get(sref.collection('cuts').where('revision','==',r).limit(1)),c=q.docs[0]?q.docs[0].data():null;
          if(asideRevs.has(r)&&(!c||c.sheetId===b.sheetId))continue;
          if(c&&c.setAsideAt)continue;   // (RGMERGE: the cut of a sheet merged into another is set aside on its own record)
          const p=c?parse(c.planJson):null;
          if(!p||!p.profile)throw new Error(`The outline ${name}'s physical sheet had before its green line was recorded could not be read: its charms stay as they are`);
          return JSON.stringify(p.profile);
        }
        return null;
      };
      if(!wasCut&&!sheet.rosePlanJson&&!sheet.roseProtectedJson&&!sheet.rosePlanHash){
        // (ASIDE) nothing drawn or cut: only the outline a set-aside cut left on its physical sheet is put right (the stock's last cut is this sheet's, and it was set aside)
        const sid=id(sheet.roseStockId)?sheet.roseStockId:null;
        if(sid){
          const hr=stocks().doc(sid),hd=await tx.get(hr),hs=hd.exists?hd.data():null,marks=[];
          for(const r of asideRevs){ const q=await tx.get(hr.collection('cuts').where('revision','==',r).limit(1)),d=q.docs[0]; if(d&&d.data().sheetId===b.sheetId&&!d.data().setAsideAt)marks.push(d.ref); }   // (its set-aside cuts, marked: no history drawn for them)
          let want,heal=false;
          if(hs&&+hs.revision>=1&&hs.lastCutSheetId===b.sheetId&&asideRevs.has(+hs.revision)&&(!hs.owner||hs.owner===b.sheetId)){ want=await priorOf(hr,+hs.revision); heal=(hs.profileJson||null)!==want; }
          for(const m of marks)tx.update(m,{setAsideAt:at,setAsideBy:by});
          if(heal){
            tx.update(hr,{profileJson:want,owner:b.sheetId,available:false,reopenedAt:at,reopenedBy:by,updatedAt:FV.serverTimestamp()});
            return {ok:true,changed:true,healed:true,marked:marks.length,lines:0,cut:null,stock:{...hs,profileJson:want,owner:b.sheetId,available:false,updatedAt:null}};
          }
          if(marks.length)return {ok:true,changed:true,marked:marks.length,lines:0,cut:null,stock:null};
        }
        return {ok:true,changed:false,lines:0,cut:null,stock:null};
      }
      // (reads first: the stock the cut was recorded on, the leftover it made, the earlier cut's outline)
      let cut=null,stockOut=null,ref=null,rem=null,prior=null,curCutRef=null;
      if(wasCut){
        cut={at:+sheet.roseCutAt||null,revision:sheet.roseCutRevision!=null&&Number.isFinite(+sheet.roseCutRevision)?+sheet.roseCutRevision:null,stockId:id(sheet.roseStockId)?sheet.roseStockId:null,leftover:null,how:'none'};
        if(cut.stockId){
          ref=stocks().doc(cut.stockId);const d=await tx.get(ref),stock=d.exists?d.data():null;
          if(stock&&cut.revision==null&&stock.lastCutSheetId===b.sheetId)cut.revision=+stock.revision;   // (a cut saved before the revision was kept on the sheet: the stock says whose last cut it was)
          if(stock&&cut.revision>=1){
            if(+stock.revision!==cut.revision)throw new Error(`The leftover sheet ${name}'s green line made has been cut from since, by another sheet: its metal is that sheet's now, so ${name}'s charms stay as they are`);
            if(stock.owner&&stock.owner!==b.sheetId)throw new Error(`The leftover sheet ${name}'s green line made is in use by another sheet: give it back first, then ${name}'s charms can change`);
            rem=remnantSync?await remnantSync.read(tx,ref.id,cut.revision,null,false):null;
            const r=rem&&rem.exists?rem.data:null;
            if(r&&r.status==='inUse'&&r.inUseBySheetId&&r.inUseBySheetId!==b.sheetId)throw new Error(`The leftover sheet ${name}'s green line made is in use by ${r.inUseBySheetName||'another sheet'}: give it back first, then ${name}'s charms can change`);
            if(r&&r.status==='used'&&r.usedBySheetId&&r.usedBySheetId!==b.sheetId)throw new Error(`The leftover sheet ${name}'s green line made was used by ${r.usedBySheetName||'another sheet'}: its metal is that sheet's now, so ${name}'s charms stay as they are`);
            if(cut.revision>1)prior=await priorOf(ref,cut.revision);   // (ASIDE: a cut set aside before is skipped)
            { const cq=await tx.get(ref.collection('cuts').where('revision','==',cut.revision).limit(1)); curCutRef=cq.docs[0]?cq.docs[0].ref:null; }   // (ASIDE: this cut, marked set aside below)
            cut.leftover=`${ref.id}-${cut.revision}`;cut.how=r?(r.status==='available'?'discarded':'kept'):'none';
            stockOut={...stock,profileJson:prior,owner:b.sheetId,available:false,updatedAt:null};
          }else ref=null;
        }
      }
      // writes
      if(curCutRef)tx.update(curCutRef,{setAsideAt:at,setAsideBy:by});
      if(ref&&stockOut){
        tx.update(ref,{profileJson:prior,owner:b.sheetId,available:false,reopenedAt:at,reopenedBy:by,updatedAt:FV.serverTimestamp()});
        if(rem&&remnantSync&&remnantSync.superseded)remnantSync.superseded(tx,rem,{sheetId:b.sheetId,sheetName:name,by,at,reason:`Its sheet (${name}) changed after its green line was recorded`});
      }
      const reseated=cut&&cut.stockId?[...(Array.isArray(sheet.roseReseated)?sheet.roseReseated:[]).slice(-9),{at,by,fromStockId:cut.stockId,toStockId:cut.stockId,plan:!!sheet.rosePlanJson,kept:!!sheet.roseProtectedJson,reopen:true,cut}]:null;
      tx.update(sr,{rosePlanJson:null,rosePlanHash:null,roseFingerprint:null,roseProtectedJson:null,...(wasCut?{roseCutAt:null,roseCutRevision:null}:{}),...(stockOut?{roseRevision:+stockOut.revision}:{}),...(reseated?{roseReseated:reseated}:{}),roseReopened:{at,by,lines,cut:!!wasCut,...(cut&&cut.at?{cutAt:cut.at}:{})},updatedAt:FV.serverTimestamp()});
      return {ok:true,changed:true,lines,cut,stock:stockOut,sheet:{id:b.sheetId,label:name,setId:sheet.setId||'',orders:Array.isArray(sheet.orders)?sheet.orders.map(String):[]}};
    });
    // (a recorded cut set aside: on the timeline of each order on the sheet, kept for good; charmNestLibrary's stamp never throws)
    if(out.changed&&out.cut&&stamp){
      await stamp(()=>[...new Set(out.sheet.orders)].slice(0,300).map(orderId=>({orderId,type:'note',at,by:by||'',station:'sorter',sheetId:out.sheet.id,sheet:out.sheet.label,setId:out.sheet.setId,
        text:`${out.sheet.label}: its charms changed after its green line was recorded, so the line is approved again by Cut Sheet`,data:{roseReopen:true,cutAt:out.cut.at,revision:out.cut.revision},id:`roseReopen-${out.sheet.id}-${at}`})),'rose reopen');
    }
    delete out.sheet;
    return out;
  }
  return {roseGet,roseList,roseClaim,roseRelease,rosePlan,roseRecordCut,roseTakeOff,roseDemo,roseReopen};
};
module.exports.fingerprint=fingerprint;
const bare=s=>fingerprint(Object.assign({},s,{placements:(s.placements||[]).map(p=>Object.assign({},p,{hash:String(p.hash||s.charms?.find(c=>c.id===p.id)?.hash||'').replace(/~ae[0-9a-z]+$/,'')}))}));
module.exports.drawnAlike=(s,old)=>bare(Object.assign({charms:old.charms},s))===bare(old);

module.exports.protectedLayout=protectedLayout;
module.exports.stagesOf=stagesOf;
module.exports.cutGroups=cutGroups;
module.exports.partialGroups=partialGroups;
module.exports.fitCheck=fitCheck;
module.exports.assertProtected=assertProtected;
module.exports.lineKeeps=lineKeeps;
/* RGHAND (Paul, 27 Nov 2026: "There should be nothing preventing the user from moving the pieces around on a given RG sheets even if the green cut line has already been marked" ... "the pieces must
   use the green cut line as a boundary a barrier of the sheet so that pieces must stay to the left of the green cut line for that particular RG sheet"). A Rose Gold sheet's green line (drawn, or its
   cut recorded) stays exactly where it was drawn while its charms are moved by hand: nothing is set aside, its physical sheet and its leftover (often another sheet's already) are not touched.
   keptBand: the metal that line keeps for its charms, read in the move's own transaction. cut: the line's frontier (its plan's profile); prior: the outline before it (a recorded cut: the latest
   earlier cut of its physical sheet that still stands, as roseGet and roseReopen read them, ASIDE included; a line drawn only: its physical sheet as it was when the line was drawn); shapes / from:
   the outlines the line was drawn from and the poses they were drawn for (its fingerprint). stocksCol: the Charm_Nest_Rose_Stock collection. → { cut, prior, allowanceMm, shapes, from } | { error } */
module.exports.keptBand=async function keptBand(tx,stocksCol,sheet){
  const name=String(sheet.fileBase||'This sheet').slice(0,80);
  let plan=null,fp=null;try{plan=parse(sheet.rosePlanJson);fp=parse(sheet.roseFingerprint);}catch(_){plan=null;}
  if(!plan||!plan.profile||!Array.isArray(plan.profile.values)||!Array.isArray(plan.shapes)||!Array.isArray(fp))return {error:`${name}'s green line is not in its record, so its charms stay where they are`};
  const sid=id(sheet.roseStockId)?sheet.roseStockId:null;
  if(!sid)return {error:`${name} holds no physical sheet, so its green line cannot be read`};
  const sref=stocksCol.doc(sid);
  let prior=null;
  if(sheet.roseCutAt){
    let own=sheet.roseCutRevision!=null&&Number.isFinite(+sheet.roseCutRevision)?+sheet.roseCutRevision:null;
    if(own==null){const sd=await tx.get(sref),st=sd.exists?sd.data():null;if(st&&st.lastCutSheetId===sheet.id)own=+st.revision;}   // (a cut saved before its revision was kept on the sheet: the stock says whose last cut it was)
    if(!(own>=1))return {error:`${name}'s green line cut cannot be found on its physical sheet, so its charms stay where they are`};
    const aside=new Set((Array.isArray(sheet.roseReseated)?sheet.roseReseated:[]).filter(x=>x&&x.reopen&&x.cut&&x.cut.revision!=null&&Number.isFinite(+x.cut.revision)).map(x=>+x.cut.revision));
    const q=await tx.get(sref.collection('cuts').where('revision','<',own).orderBy('revision','desc').limit(8));
    const c=q.docs.map(d=>d.data()).find(c=>c&&!c.setAsideAt&&!(aside.has(+c.revision)&&c.sheetId===sheet.id));
    if(c){const p=parse(c.planJson);if(!p||!p.profile)return {error:`The outline ${name}'s physical sheet had before its green line was cut could not be read, so its charms stay where they are`};prior=p.profile;}
  }else{
    const sd=await tx.get(sref),st=sd.exists?sd.data():null;
    if(!st)return {error:`${name}'s physical sheet is not in the cloud, so its green line cannot be read`};
    if(+st.revision!==+sheet.roseRevision)return {error:`${name}'s physical sheet changed since its green line was drawn: draw its line again, then move its charms`};
    prior=parse(st.profileJson);
  }
  return {cut:plan.profile,prior,allowanceMm:+plan.allowanceMm||+sheet.roseAllowanceMm||.2,shapes:new Map(plan.shapes.map(s=>[s.id,s])),from:new Map(fp.map(r=>[r[0],{cxPt:+r[1],cyPt:+r[2],angle:+r[3]||0,scale:+r[4]||1}]))};
};
/* RGHAND: the ids of `ids` (charms moved by hand, `placements` as they now stand) that would cross the line keptBand read: a charm whose outline comes nearer the line than its own allowance,
   or one the line was not drawn for. Metal an earlier cut took is the page's check (the charm stays where it was put, red, and the sheet goes to no set: MCFREE), never this refusal. */
module.exports.pastLine=function pastLine(band,placements,ids){
  const out=[];
  for(const i of ids){
    const p=(placements||[]).find(z=>z&&z.id===i);if(!p)continue;
    const s=band.shapes.get(i),f=band.from.get(i);if(!s||!f){out.push(i);continue;}
    if(Rose.inside([Rose.moved(s,f,{cxPt:+p.cxPt,cyPt:+p.cyPt,angle:+p.angle||0,scale:+p.scale||1})],band.cut,band.prior,band.allowanceMm).past.length)out.push(i);
  }
  return out;
};
