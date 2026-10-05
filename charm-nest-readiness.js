/* One production-readiness policy shared by the Library and the server.
 * Nesting completion alone never grants permission to start the laser. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CharmNestReadiness=api;})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const idsOf=s=>[...new Set((s.poolIds || []).filter(Boolean))];
  const held=s=>!!(s && s.laserHold && +s.laserHold.at>0);
  const url=x=>typeof x==='string'?x:x?.url;
  function decisions(rows){
    const out={};
    for(const row of rows || [])for(const id of row.poolIds || []) {
      const e=row.engrave;
      // a cancelled order's piece left on a released sheet is cut and set aside, so it waits on no engraving decision
      // A line with no personalization, message or note has nothing to engrave, before its engraving check has run too.
      // The page reads that from the row's reading, the server from the run's line record.
      const candidate=row.spec ? row.spec.engraveCandidate : row.engraveCandidate;
      out[id]=row.state==='gone' && !e?.approved ? {needed:false,state:'none',approved:true} : e ? {needed:!!e.needed,state:e.state,approved:!!e.approved} : candidate===false ? {needed:false,state:'none',approved:true} : {needed:true,state:'unknown',approved:false};
    }
    return out;
  }
  const orderIds=s=>[...new Set([...(Array.isArray(s.orders)?s.orders:[]),...idsOf(s).map(id=>String(id).split('_')[0]).filter(id=>/^\d+$/.test(id))].map(String))];
  function sheet(s,options={}){
    const ids=idsOf(s), sid=s.id || s.sheetId, backs=new Map((s.backPool || s.backs || []).filter(b=>!b.invalidated && (!b.sheetId || b.sheetId===sid)).map(b=>[b.poolId,b]));
    let approved=0,waiting=0,saved=0,plain=0;
    for(const id of ids){
      const d=s.engraving?.[id],b=backs.get(id);
      const noBack=d && d.needed===false && d.approved===true && ['none','skipped'].includes(d.state);
      if(noBack && !b){plain++;continue;}
      const accepted=!!(b?.approvedAt && b.approvedBy) || !!(d?.approved && ['approved','written'].includes(d.state));
      if(accepted)approved++;else waiting++;
      if(b?.approvedAt && b.approvedBy && b.verified?.geometry?.ok && b.verified?.file?.ok && b.outputs?.ai?.path && url(b.outputs.ai))saved++;
    }
    // Missing identities are unresolved, never interpreted as plain charms.
    const unidentified=Math.max(0,(+s.placedCount || s.placements?.length || 0)-ids.length);waiting+=unidentified;
    const total=ids.length+unidentified,required=total-plain;
    const labels=s.label?.files || [],covered=new Set(labels.flatMap(f=>f.orders || []).map(String)),orders=s.orders || s.label?.orders || [];
    const stages={
      layout:total>0 && !(s.metal==='rose' && s.roseStockId && !s.rosePlanHash) && s.verification?.ok===true && !s.dirty && !s.saving && !['nesting','finishing','queued','error'].includes(s.status),
      front:!!url(s.outputs?.ai) && !!(s.preview || url(s.outputs?.preview)),
      approval:total>0 && waiting===0,
      backs:total>0 && saved===required,
      qr:labels.length>0 && labels.every(f=>f.path && f.url && f.payload) && orders.every(id=>covered.has(String(id))),
      // an order waits for its OTHER pieces only: whatever sits on this sheet is this sheet's own readiness, shown by its own steps
      orders:options.physicalOnly===true || orderIds(s).every(id=>forSheet(s.orderReadiness?.[id],sid)?.ready===true)
    };
    // a person's hold (Library move back to In progress, LibraryFlow) keeps every approval and seal but takes it out of Laser cutting
    const included=!s.draft && s.solidIncluded!==false && !s.archived && !held(s);
    return {total,required,approved,waiting,saved,plain,saving:Math.max(0,approved-saved),stages,included,ready:included && Object.values(stages).every(Boolean)};
  }
  function set(s,sheets){
    const unique=[...new Map((sheets || []).map(x=>[x.id || x.sheetId,x])).values()],reports=unique.map(sheet);
    const expected=s.sheetIds || unique.map(x=>x.id || x.sheetId),complete=expected.length>0 && expected.length===unique.length && expected.every(id=>unique.some(x=>(x.id || x.sheetId)===id));
    const stages=Object.fromEntries(['layout','front','approval','backs','qr','orders'].map(k=>[k,complete && reports.every(r=>r.stages[k])]));
    return {...Object.fromEntries(['total','required','approved','waiting','saved','plain','saving'].map(k=>[k,reports.reduce((n,r)=>n+r[k],0)])),stages,included:complete && reports.every(r=>r.included),ready:complete && reports.every(r=>r.ready),sheets:unique.length};
  }
  // Completion is proof that this saved sheet already passed through Laser cutting.
  // Reopening clears its current completion flag, not that approval. Blue readiness seals
  // alone never grant this return path; first-time sheets still pass every intake check.
  const completedBefore=s=>+s.laserDoneAt>0 || (s.processSeals || []).some(x=>x.how==='laserDone' && +x.at>0);
  function laserSheet(s){
    const report=sheet(s);
    return {...report,ready:report.included && (completedBefore(s) || report.ready)};
  }
  // Laser work keeps a set together, including when a completed set is reopened.
  // Missing, archived, draft or newly added unfinished members cannot borrow its old approval.
  function laserGroup(s,sheets){
    const unique=[...new Map((sheets || []).filter(x=>!x.archived).map(x=>[x.id || x.sheetId,x])).values()];
    const expected=[...new Set(s.sheetIds || unique.map(x=>x.id || x.sheetId))];
    const complete=expected.length>0 && expected.length===unique.length && expected.every(id=>unique.some(x=>(x.id || x.sheetId)===id));
    return {...set(s,unique),ready:complete && unique.every(x=>laserSheet(x).ready)};
  }
  /* ── Pieces, and what an order waits for (Paul, 5 Oct: "Pooled orders are only truthful when a given order has multi pieces that
   * are spread across 2 or more sheets. Same with the SKU missing issues"). One piece is one copy of a line (a line of quantity 2 is
   * two pieces; their pool ids are "<line key>_<n>"). Where a piece sits is read from the sheets that list its pool id, never from the
   * line's stored state (a line stays 'pooled' in its record until its set is written, and its problems list can be older than its
   * match): a piece listed by a saved sheet is NESTED, and that wins over a stale state or problem.
   * An order is an issue for sheet X only when X holds a piece of it AND some OTHER piece (one not on X) is
   *   · not on any sheet yet ('pooled'), or has no SKU ('noSku'), a SKU no master has ('unmatched') or no design ('noDesign') there, or
   *   · held (a person's hold or an Etsy change waiting for review), or
   *   · on a different sheet that is not ready ('otherSheetNotReady': that sheet's own physical readiness, never its order check, so
   *     two sheets can never wait on each other).
   * Pieces on X itself never block through the order check (X's own readiness shows through its own steps), single-piece orders are
   * never an issue for what other pieces do (they have none), and cancelled ('gone') and no-design pieces never block. One exception,
   * because it is a person's or Etsy's explicit stop and not an inference about where pieces are: a HELD piece (a person's hold, or an
   * Etsy change waiting for review) holds the sheet wherever it sits, even a one-piece order on this very sheet (as before). */
  const KEYS=['pooled','noSku','unmatched','noDesign','held','otherSheetNotReady'];   // which kind names an order that has several
  const PROBLEM_WORDS={unmatchedSku:'SKU not in a master',needsMaterial:'needs material',needsMapping:'needs an option mapped',missingSize:'no design for that size',blockedSku:'SKU is blocked',oversize:'does not fit the plate'};
  const PROBLEM_KEY={needsMaterial:'unmatched',needsMapping:'unmatched',blockedSku:'noDesign',missingSize:'noDesign',oversize:'noDesign'};
  const STAGE_WORDS={layout:'layout needs verification',front:'cutting files are missing',approval:'engraving needs approval',backs:'back engraving files are not saved',qr:'QR labels are missing'};
  const rank=k=>{const i=KEYS.indexOf(k);return i<0?KEYS.length:i;};
  const tailNo=k=>{const m=/_(\d+)$/.exec(String(k||''));return m?+m[1]:0;};
  const qtyOf=l=>Math.max(1,Math.floor(+(l.spec?.quantity || l.quantity) || 1));
  const titleOf=l=>String(l.line?.title || l.snap?.title || l.spec?.designSku || l.sku || '').replace(/\s+/g,' ').trim();
  // the copies of a line: its pool ids, and (when the record lost some) the "<line key>_<n>" ids the pool gives them
  function copyIds(l,key){
    const ids=[...new Set((Array.isArray(l.poolIds)?l.poolIds:[]).filter(Boolean).map(String))],want=qtyOf(l);
    for(let n=1;ids.length<want;n++){const d=`${key}_${n}`;if(!ids.includes(d))ids.push(d);}
    return ids;
  }
  // a line's own problem as a kind: no SKU at all, a SKU no master has, or no design for it
  function problemOf(l){
    const p=Array.isArray(l.problems)?l.problems[0]:null;
    let kind=p && typeof p==='object'?p.kind:p;
    // an older record may carry only the state the page gave the line
    if(!kind){if(l.state==='unmatched')kind='unmatchedSku';else if(l.state==='oversize')kind='oversize';else if(l.state==='held')return {key:'held',why:String(l.reason || 'Held for review')};else return null;}
    let key=PROBLEM_KEY[kind] || 'unmatched';
    if(kind==='unmatchedSku'){
      const sku=String(l.spec?.designSku ?? l.sku ?? l.line?.sku ?? (p && p.sku) ?? '').trim();
      key=!sku || (p && typeof p==='object' && /no SKU/i.test(p.reason || ''))?'noSku':'unmatched';
    }
    return {key,why:PROBLEM_WORDS[kind] || String(kind)};
  }
  // a sheet's readiness for another sheet's order: the physical stages only (never its own order check), a cut sheet, or one cut before
  const physicalReady=(s,r)=>+s.laserDoneAt>0 || (r.included && (completedBefore(s) || r.ready));
  function readOrders(rows,sheets){
    const copies=new Map(),phys=new Map(),groups=new Map(),orders=new Map();
    for(const s of sheets || [])if(!s.archived){
      const r=sheet(s,{physicalOnly:true});
      phys.set(s,{ok:physicalReady(s,r),stage:Object.keys(r.stages).find(k=>!r.stages[k]) || (held(s)?'held':r.included?'':'included')});
      for(const id of idsOf(s)){const xs=copies.get(id)||[];xs.push(s);copies.set(id,xs);}
    }
    for(const row of rows || []){const id=String(row.order?.receiptId || row.orderId || String(row.key || '').split('_')[0] || '');if(!id)continue;const xs=groups.get(id)||[];xs.push(row);groups.set(id,xs);}
    for(const [id,lines] of groups){
      const pieces=[];let customer='',listingId='';
      for(const l of lines.slice().sort((a,b)=>tailNo(a.key)-tailNo(b.key))){
        customer=customer || l.order?.buyer?.name || l.snap?.buyer || '';
        listingId=listingId || String(l.line?.listingId || l.snap?.listingId || '');
        if(l.state==='gone')continue;                                                     // cancelled: cut and set aside, never waited for
        if(l.spec?.noDesign || l.noDesign || l.state==='noDesign')continue;               // nothing to cut
        const key=String(l.key || [id,l.transactionId].filter(Boolean).join('_') || id),hold=l.hold || l.changePending?String(l.hold || 'Order changes need review'):'',problem=problemOf(l);
        for(const pid of copyIds(l,key)){
          const on=copies.get(pid) || [];
          let block=null;
          if(hold)block={key:'held',why:hold};
          else if(!on.length)block=problem?{key:problem.key,why:problem.why}:{key:'pooled',why:'A piece is not on a saved sheet yet'};
          else if(!on.some(s=>+s.laserDoneAt>0 || phys.get(s).ok)){
            const s=on.find(x=>!phys.get(x).ok) || on[0],stage=phys.get(s).stage;
            block={key:'otherSheetNotReady',stage,why:`${s.metalLabel || s.metal || 'Sheet'}: ${STAGE_WORDS[stage] || (stage==='held'?'held back from Laser cutting':stage==='included'?'not in a set yet':'not ready for laser cutting')}`};
          }
          const at=on.length?on[0]:null;
          pieces.push({index:pieces.length+1,key:pid,poolId:pid,lineKey:key,label:titleOf(l),state:l.state,sheetId:at?(at.id || at.sheetId):null,sheetLabel:at?sheetLabel(at):null,sheetIds:on.map(s=>s.id || s.sheetId),block});
        }
      }
      orders.set(id,{pieces,customer,listingId});
    }
    return orders;
  }
  const head=bs=>{const b=bs.slice().sort((x,y)=>rank(x.key)-rank(y.key))[0];return {key:b.key,why:b.why,line:b.lineKey,...(b.sheetId?{sheetId:b.sheetId,sheetLabel:b.sheetLabel}:{}),...(b.stage?{stage:b.stage}:{})};};
  /** orderReports(rows, allSheets): what each order waits for, whichever sheet asks. { [orderId]: {ready:true} | {ready:false, key, why, line, sheetId?, sheetLabel?, stage?,
   *  blocks:[{key,index,label,poolId,lineKey,sheetId|null,sheetLabel|null,why,stage?}], onSheets, pieceCount, customer, listingId} }.
   *  `blocks` are the pieces that hold the order back (see above), each with the sheet it sits on; forSheet() reads them from one sheet. */
  function orderReports(rows,sheets){
    const out={};
    for(const [id,o] of readOrders(rows,sheets)){
      const blocks=o.pieces.filter(p=>p.block).map(p=>({key:p.block.key,index:p.index,label:p.label,poolId:p.poolId,lineKey:p.lineKey,sheetId:p.sheetId,sheetLabel:p.sheetLabel,...(p.sheetIds.length>1?{sheetIds:p.sheetIds}:{}),why:p.block.why,...(p.block.stage?{stage:p.block.stage}:{})}));
      out[id]=blocks.length?{ready:false,...head(blocks),blocks,onSheets:[...new Set(o.pieces.flatMap(p=>p.sheetIds))],pieceCount:o.pieces.length,customer:o.customer,listingId:o.listingId}:{ready:true};
    }
    return out;
  }
  /** forSheet(report, sheetId): an order's report as one sheet reads it. The pieces on that sheet are not its order check (that sheet's own steps show
   *  them), and an order it holds nothing of is not its order. {ready:true} when nothing else holds the order back. Reports without blocks (an older record,
   *  or the server's "not verified") are read as they are. */
  function forSheet(r,sid){
    if(!r || r.ready===true || !Array.isArray(r.blocks) || !sid)return r;
    if(Array.isArray(r.onSheets) && !r.onSheets.includes(sid))return {ready:true};
    // (a held piece stops the sheet wherever it sits: a person's hold or an Etsy change waiting for review is no inference about where pieces are)
    const mine=r.blocks.filter(b=>b.key==='held' || (b.sheetId!==sid && !(b.sheetIds && b.sheetIds.includes(sid))));
    if(!mine.length)return {ready:true};
    if(mine.length===r.blocks.length)return r;
    return {ready:false,...head(mine),blocks:mine,onSheets:r.onSheets,pieceCount:r.pieceCount,customer:r.customer,listingId:r.listingId};
  }
  /** pieces(rows, allSheets): every live piece of every order and where it sits: { [orderId]: [{index,key,poolId,lineKey,label,state,sheetId|null,sheetLabel|null,sheetIds,problem|null,held}] }. */
  function pieces(rows,sheets){
    const out={};
    for(const [id,o] of readOrders(rows,sheets))out[id]=o.pieces.map(({block,...p})=>({...p,problem:block && ['noSku','unmatched','noDesign'].includes(block.key)?block.key:null,held:!!(block && block.key==='held')}));
    return out;
  }
  const orderBlockers=s=>{
    const sid=s.id || s.sheetId;
    return orderIds(s).map(id=>[id,s.orderReadiness?.[id]?forSheet(s.orderReadiness[id],sid):{ready:false,why:'Order readiness has not been verified'}]).filter(([,r])=>r.ready!==true).map(([id,r])=>({id,...r}));
  };
  const filed=s=>+s.laserDoneAt>0 && !s.laserSetPending;
  // These are historical facts, independent of today's readiness or completion flag.
  // Old completion records have an exact signer/time; the former blue icon did not.
  // Keep that missing provenance explicit instead of dating it at a redraw or inventing a signer.
  function processStamps(s={}) {
    const stamps=(Array.isArray(s.processSeals)?s.processSeals:[]).map(x=>({...x}));
    const at=+(s.laserDoneAt || s.archivedLaserDoneAt || (s.kind?s.at:0)),by=s.laserDoneBy || s.archivedLaserDoneBy || s.by || '';
    if(at>0 && !stamps.some(x=>x.how==='laserDone' && +x.at===at)) {
      if(!stamps.some(x=>x.how==='laserReady'))stamps.push({id:'legacy-ready',how:'laserReady',at:0,by:'',legacy:true});
      stamps.push({id:'legacy-done-'+at,how:'laserDone',at,by,legacy:true});
    }
    return stamps;
  }
  // Readiness is expressed by the section and saved-back counter, never a preview stamp.
  // Recorded process seals have their own historical row; rendering them here would duplicate them.
  function seal(r,scope='Sheet',source){return '';}
  function counter(r,scope='Sheet',source){
    // A sheet with no engraved backs has nothing to count.
    return (r.required ? `<span class="backSavedCount" title="Engraved backs saved" aria-label="${r.saved} of ${r.required} backs saved"><b>${r.saved} / ${r.required}</b></span>` : '')+seal(r,scope,source);
  }
  /* ── explain: why a sheet or set is not in Laser cutting yet, in plain words ──────────────────────────────────────
   * The Library's step rail, its "what is left" line and its checklist read this, and the server may too: it is pure
   * (no network, no page state) and decides nothing itself. Every "done" below is the same stage the gate above reads
   * (sheet(), laserSheet(), laserGroup(), orderReports()), so the words can never say ready where the section says not. */
  const CODE={gold:'GF',silver:'SS',rose:'RG',gold10k:'10K',gold14k:'14K'};
  const STEPS=[['nesting','Nesting'],['engraving','Engraving'],['backFiles','Back files'],['qr','QR label'],['orders','Order check'],['laser','Laser cutting'],['completed','Completed']];
  const GATED=['nesting','engraving','backFiles','qr','orders'],LISTED=30;   // the steps before Laser cutting · items listed per step (its words count the rest)
  const stepName=k=>STEPS.find(t=>t[0]===k)[1];
  const sheetNo=s=>s.sheetIndex || +((/_Sheet-(\d+)/.exec(s.folder || s.fileBase || '') || [])[1]) || s.page || 1;
  const sheetLabel=s=>`${CODE[s.metal] || s.metalLabel || ''} Sheet ${sheetNo(s)}`.trim();
  const setName=s=>s.name || (s.seq || s.setSeq ? `Set ${s.seq || s.setSeq}` : 'This set');
  const count=(n,one,more)=>`${n} ${n===1?one:(more || one+'s')}`;
  const lower=t=>String(t || '').replace(/^./,c=>c.toLowerCase());
  const sentence=t=>{t=String(t || '').replace(/\s+/g,' ').trim().replace(/[.\s]+$/,'');return t?t[0].toUpperCase()+t.slice(1)+'.':'';};
  const brief=(t,max)=>{t=String(t || '').replace(/\s+/g,' ').trim();return t.length>max?t.slice(0,max-1)+'…':t;};
  // where an order's other piece stands, in words (orderReports names the step that holds it)
  const MISSING={layout:'its layout needs verification',front:'its cutting files are missing',approval:'its engraving needs approval',backs:'its back engraving files are not saved',qr:'its QR labels are missing',held:'it is held back from Laser cutting',included:'it is not in a set yet'};
  // what a piece's engraving still waits for (the page's job states, and the server's)
  const ENGRAVE_WAIT={unknown:'Its engraving has not been read yet',classify:'Its engraving has not been read yet',words:'Its engraving words wait for a decision',review:'Its back engraving waits for approval',blocked:'Its back engraving needs attention first'};
  function names(ctx){
    const buyers=new Map(),pools=new Map();
    for(const row of ctx.rows || []){
      const rid=String(row.order?.receiptId || row.orderId || String(row.key || '').split('_')[0] || '');
      if(rid && !buyers.get(rid))buyers.set(rid,row.order?.buyer?.name || '');
      for(const id of row.poolIds || [])pools.set(id,row);
    }
    const order=id=>`Order ${id}${buyers.get(String(id)) ? ` (${buyers.get(String(id))})` : ''}`;
    const charm=id=>{
      const rid=String(id).split('_')[0],row=pools.get(id),what=brief(row?.line?.title || row?.spec?.designSku || '',48);
      return `${/^\d+$/.test(rid) ? order(rid) : 'A charm'}${what ? ` · ${what}` : ''}`;
    };
    return {order,charm};
  }
  // the same per-copy reading sheet() makes, kept with each copy's id so a checklist can name it
  function copies(s){
    const sid=s.id || s.sheetId,backs=new Map((s.backPool || s.backs || []).filter(b=>!b.invalidated && (!b.sheetId || b.sheetId===sid)).map(b=>[b.poolId,b]));
    return idsOf(s).map(id=>{
      const d=s.engraving?.[id],b=backs.get(id) || null;
      if(d && d.needed===false && d.approved===true && ['none','skipped'].includes(d.state) && !b)return {id,plain:true};
      const accepted=!!(b?.approvedAt && b.approvedBy) || !!(d?.approved && ['approved','written'].includes(d.state));
      const saved=!!(b?.approvedAt && b.approvedBy && b.verified?.geometry?.ok && b.verified?.file?.ok && b.outputs?.ai?.path && url(b.outputs.ai));
      return {id,accepted,saved,back:b,state:d?.state || 'unknown'};
    });
  }
  // one sheet's own five steps: ok[k] (the gate's stage), hard[k] (a problem, not only waiting), items[k], detail[k], short[k]
  function sheetSteps(s,N){
    const id=s.id || s.sheetId,label=sheetLabel(s),r=sheet(s),again=completedBefore(s),cs=copies(s),st=r.stages;
    const items={nesting:[],engraving:[],backFiles:[],qr:[],orders:[]},hard={},ok={},detail={},short={};
    const own=(k,why,isHard)=>{items[k].push({kind:'sheet',id,label,why});if(isHard)hard[k]=true;};
    // Nesting: a layout that is verified, saved cutting files, and a place in a set
    const working=!!(s.dirty || s.saving || ['nesting','finishing','queued'].includes(s.status));
    if(s.archived)own('nesting','This sheet was removed (archived), so it cannot be cut',true);
    else if(s.draft)own('nesting','Not in a set yet: it joins a set when it is full or released');
    else if(s.solidIncluded===false)own('nesting','Not included in its set yet: turn Include on for this metal',true);
    else if(held(s))own('nesting',`Held back from Laser cutting${s.laserHold.by?` by ${s.laserHold.by}`:''} (it was moved back to In progress): press Approve for laser cutting, or move it to Laser cutting, to release it. Its approvals and seals are kept`,true);
    if(again){
      // cut once before: reopening keeps that approval (laserSheet), only a place in a set is still asked
      for(const k of GATED)ok[k]=k==='nesting'?r.included:true;
      for(const k of GATED){hard[k]=k==='nesting' && !r.included && !s.draft;detail[k]=ok[k]?'Passed before Laser cutting; it stays on record.':items.nesting[0]?.why || 'Not in a set.';short[k]=lower(items.nesting[0]?.why || 'it is not in a set');}
      return {id,label,r,again,ok,hard,items,detail,short};
    }
    if(r.total===0)own('nesting','No charms are placed on it yet');
    if(s.metal==='rose' && s.roseStockId && !s.rosePlanHash)own('nesting','The green dash line has not been calculated: press Cut Sheet',true);
    if(s.status==='error')own('nesting','Nesting stopped with an error: nest this sheet again',true);
    else if(working)own('nesting',s.saving?'The sheet is still being saved':'The sheet is still being nested or changed');
    if(s.verification?.ok===false)own('nesting','The layout check found a problem: open the sheet and nest it again',true);
    else if(s.verification?.ok!==true && !working && r.total>0)own('nesting','The layout has not been checked yet');
    if(!url(s.outputs?.ai))own('nesting','The cutting file (.ai) is not saved yet',!working);
    if(!(s.preview || url(s.outputs?.preview)))own('nesting','The sheet picture is not saved yet',!working);
    ok.nesting=r.included && st.layout && st.front;
    detail.nesting=ok.nesting?'Layout verified and cutting files saved.':items.nesting.length?sentence(items.nesting.slice(0,2).map(x=>x.why).join('; ')):'The layout is not ready.';
    short.nesting=lower(items.nesting[0]?.why || 'the layout is not ready');
    // Engraving: every back approved, or the piece needs none
    const pending=cs.filter(c=>!c.plain && !c.accepted),unknown=Math.max(0,(+s.placedCount || s.placements?.length || 0)-idsOf(s).length);
    for(const c of pending.slice(0,LISTED)){items.engraving.push({kind:'charm',id:c.id,label:N.charm(c.id),why:ENGRAVE_WAIT[c.state] || 'Its back engraving is not approved yet'});if(c.state==='blocked')hard.engraving=true;}
    if(pending.length>LISTED)items.engraving.push({kind:'sheet',id,label,why:`${count(pending.length-LISTED,'more piece')} also wait for approval`});
    if(unknown)items.engraving.push({kind:'sheet',id,label,why:`${count(unknown,'placed piece')} cannot be matched to an order, so ${unknown===1?'its':'their'} engraving cannot be checked`});
    ok.engraving=st.approval;
    detail.engraving=ok.engraving?(r.required?`Every back engraving is approved (${r.required} of ${r.required}).`:'No back engravings are needed on this sheet.'):`${count(r.waiting,'back engraving')} still ${r.waiting===1?'needs':'need'} approval (${r.approved} of ${r.required} approved).`;
    short.engraving=`${count(r.waiting,'back engraving')} still ${r.waiting===1?'needs':'need'} approval`;
    // Back files: each approved back saved as a verified file
    const unsaved=cs.filter(c=>!c.plain && c.accepted && !c.saved);
    for(const c of unsaved.slice(0,LISTED)){
      const failed=!!c.back && (c.back.verified?.file?.ok===false || c.back.verified?.geometry?.ok===false);
      items.backFiles.push({kind:'charm',id:c.id,label:N.charm(c.id),why:failed?'Its saved back file failed its check: approve the back again':c.back?.approvedAt?'Approved, and its back file is still being saved':'Approved, but its back file is not saved yet'});
      if(failed)hard.backFiles=true;
    }
    if(unsaved.length>LISTED)items.backFiles.push({kind:'sheet',id,label,why:`${count(unsaved.length-LISTED,'more back file')} also wait to be saved`});
    ok.backFiles=st.backs;
    detail.backFiles=ok.backFiles?(r.required?`All ${r.required} approved backs are saved as files.`:'No back files are needed on this sheet.'):`${r.saved} of ${r.required} back files are saved${r.waiting?`; ${count(r.waiting,'back')} still ${r.waiting===1?'waits':'wait'} for approval first`:''}.`;
    short.backFiles=unsaved.length?`${count(unsaved.length,'approved back file')} still ${unsaved.length===1?'needs':'need'} saving`:'the back files wait for the engravings';
    // QR label: one label that covers every order on the sheet
    const files=s.label?.files || [],covered=new Set(files.flatMap(f=>f.orders || []).map(String)),orders=(s.orders || s.label?.orders || []).map(String);
    const uncovered=files.length?orders.filter(o=>!covered.has(o)):[];
    if(!files.length)own('qr','There is no QR label yet');
    else{
      if(files.some(f=>!(f.path && f.url && f.payload)))own('qr','The QR label is incomplete: make it again',true);
      for(const o of uncovered.slice(0,LISTED))items.qr.push({kind:'order',id:o,label:N.order(o),why:'This order is not on the QR label'});
      if(uncovered.length>LISTED)items.qr.push({kind:'sheet',id,label,why:`${count(uncovered.length-LISTED,'more order')} are not on the QR label either`});
      if(uncovered.length)hard.qr=true;
    }
    ok.qr=st.qr;
    detail.qr=ok.qr?`QR label made for ${count(orders.length,'order')}.`:!files.length?'No QR label has been made for this sheet yet.':uncovered.length?`The QR label leaves out ${count(uncovered.length,'order')}.`:'The QR label file is incomplete.';
    short.qr=!files.length?'the QR label has not been made':uncovered.length?`the QR label must also cover ${count(uncovered.length,'order')}`:'the QR label is incomplete';
    // Order check: built on issues(): only an order with another piece holding it back is listed (a piece on this sheet, or an order of one piece, never is)
    const ids=orderIds(s),checked=!!s.orderReadiness,blockers=checked?sheetIssues(s,{perOrder:true},['orders']):[];
    if(!checked && ids.length)own('orders','Its orders have not been checked yet: they are checked when the Library refreshes');
    else{
      const seen=new Set();
      for(const b of blockers.slice(0,LISTED))items.orders.push({kind:'order',id:b.orderId,label:N.order(b.orderId),why:b.why,...(b.otherSheetId?{sheetId:b.otherSheetId}:{})});
      for(const b of blockers)for(const p of b.pieces)if(p.kind==='otherSheetNotReady' && p.sheetId && p.sheetId!==id && !seen.has(p.sheetId)){
        seen.add(p.sheetId);
        const n=blockers.filter(x=>x.pieces.some(y=>y.sheetId===p.sheetId && y.kind==='otherSheetNotReady')).length;
        items.orders.push({kind:'sheet',id:p.sheetId,label:p.sheetLabel,why:`Holds ${count(n,'order')} of this sheet back: ${MISSING[p.stage] || 'it is not ready'}`});
      }
      if(blockers.length>LISTED)items.orders.push({kind:'sheet',id,label,why:`${count(blockers.length-LISTED,'more order')} also wait for other pieces`});
      if(blockers.length)hard.orders=true;
    }
    ok.orders=st.orders;
    const first=items.orders[0];
    detail.orders=ok.orders?(ids.length?`All ${count(ids.length,'order')} on this sheet have every other piece ready.`:'No orders to check.'):checked?`${blockers.length} of ${count(ids.length,'order')} ${blockers.length===1?'waits':'wait'} for other pieces to be ready.`:'Its orders have not been checked yet.';
    short.orders=!checked?'its orders have not been checked yet':!blockers.length?'its orders wait for other pieces':blockers.length===1?`${N.order(blockers[0].orderId)} waits for another piece: ${lower(first.why)}`:`${count(blockers.length,'order')} wait for other pieces, for example ${N.order(blockers[0].orderId)}: ${lower(first.why)}`;
    return {id,label,r,again,ok,hard,items,detail,short};
  }
  /* ── issues: only what truly holds a sheet back (the "!" panel, the Order check step, the server's gate all read this) ─────────────
   * Never a finished step, never a back-engraving count. One entry per order that has another piece holding it back (see "Pieces"
   * above), at most one entry for the sheet's own current blocker (no order), and, for a sheet that is itself ready, one per set mate
   * that is not. Pure: the sheet's own record (its orderReadiness, as the server answers it) or, when ctx.rows and ctx.allSheets are
   * given (the page, or a test), the pieces read from those. */
  const PIECE_PHRASE={pooled:'not on a sheet yet',noSku:'no SKU',held:'held'};
  const pieceLine=b=>b.key==='pooled'?'Not on a sheet yet':b.key==='noSku'?'No SKU':b.key==='otherSheetNotReady'?`On ${b.sheetLabel || 'another sheet'}, not ready yet`:sentence(b.why);
  // the words of what holds an order back, from its blocks (older reports without blocks: their own words, as they were mapped before)
  function orderWhy(r){
    const bs=Array.isArray(r.blocks)?r.blocks:[];
    if(!bs.length){
      const w=String(r.why || '');
      return /^Not every copy has a saved sheet/i.test(w)?'Not every piece of this order is on a saved sheet yet':/not on a saved sheet/i.test(w)?'One of its other pieces is not on a saved sheet yet':/^(line|piece) is /i.test(w)?`One of its other pieces is still '${w.replace(/^(line|piece) is /i,'')}'`:sentence(w || 'Not checked yet').replace(/\.$/,'');
    }
    if(bs.length===1){
      const b=bs[0];
      return b.key==='pooled'?'Its other piece is not on a sheet yet':b.key==='noSku'?'Its other piece has no SKU':b.key==='held'?`A piece of this order is held: ${b.why}`:b.key==='otherSheetNotReady'?`Its other piece is on ${b.sheetLabel || 'another sheet'}, and ${MISSING[b.stage] || 'it is not ready'}`:`Its other piece: ${b.why}`;
    }
    const phrase=b=>PIECE_PHRASE[b.key] || (b.key==='otherSheetNotReady'?`on ${b.sheetLabel || 'another sheet'}`:b.why);
    return `${bs.length} of its other pieces wait: ${[...new Set(bs.map(phrase))].join(', ')}`;
  }
  function orderIssue(id,r,ctx){
    const bs=Array.isArray(r.blocks)?r.blocks:[],h=bs.length?head(bs):null,first=h && bs.find(b=>b.key===h.key);
    return {step:'orders',key:h?h.key:'unverified',orderId:id,orderLabel:`Order ${id}`,customer:r.customer || '',listingId:r.listingId || '',thumb:typeof ctx.thumb==='function'?ctx.thumb(id) || null:null,
      pieceCount:r.pieceCount || 0,pieces:bs.map(b=>({index:b.index,key:b.poolId,poolId:b.poolId,label:b.label,kind:b.key,sheetId:b.sheetId || null,sheetLabel:b.sheetLabel || null,stage:b.stage || '',why:pieceLine(b)})),
      why:orderWhy(r),open:{type:'order',id,...(first?{poolId:first.poolId}:{})},...(h && h.sheetId?{otherSheetId:h.sheetId}:{})};
  }
  // the sheet's own current blocker: the first of its own steps still behind, as a short label (no counts, no engraving rows)
  function ownIssue(s){
    const id=s.id || s.sheetId,label=sheetLabel(s),r=sheet(s),st=r.stages,mk=(step,key,text)=>({step,key,label:text,sheetId:id,sheetLabel:label,open:{type:'sheet',id}});
    if(s.archived)return mk('nesting','archived','Sheet removed');
    if(held(s))return mk('nesting','held','Held back');
    if(s.draft)return mk('nesting','notInSet','Not in a set yet');
    if(s.solidIncluded===false)return mk('nesting','notInSet','Not included yet');
    if(completedBefore(s))return null;                      // cut once before: reopening keeps that approval, only a place in a set is asked
    if(s.metal==='rose' && s.roseStockId && !s.rosePlanHash)return mk('nesting','roseLine','Green line needed');
    if(!(st.layout && st.front))return mk('nesting','layout',st.layout && !st.front?'Cutting files missing':'Layout not ready');
    if(!st.approval)return mk('engraving','approvalsNeeded','Approvals needed');
    if(!st.backs)return mk('backFiles','backFilesMissing','Back files missing');
    if(!st.qr)return mk('qr','qrMissing','QR label missing');
    return null;
  }
  const ALL_STEPS=['nesting','engraving','backFiles','qr','orders','laser'];
  function sheetIssues(s,ctx,steps){
    const sid=s.id || s.sheetId,want=k=>steps.includes(k),out=[],label=sheetLabel(s);
    let rec=s;
    // pieces read from the page's rows and every live sheet, instead of the record's own answer
    if(ctx.rows && ctx.allSheets){const reps=orderReports(ctx.rows,ctx.allSheets);rec={...s,orderReadiness:Object.fromEntries(orderIds(s).map(id=>[id,forSheet(reps[id],sid) || {ready:false,why:'Order readiness has not been verified'}]))};}
    const own=ownIssue(rec);
    if(own && want(own.step))out.push(own);
    if(want('orders') && !completedBefore(rec) && rec.orderReadiness){
      const unread=[];
      for(const b of orderBlockers(rec)){
        // an order nothing was read for is one entry for the sheet ("not checked yet"), not one for each of its orders
        if(!Array.isArray(b.blocks) && !ctx.perOrder){unread.push(b.id);continue;}
        out.push({...orderIssue(b.id,b,ctx),sheetId:sid,sheetLabel:label});
      }
      if(unread.length)out.push({step:'orders',key:'unverified',label:'Orders not checked yet',orderIds:unread,sheetId:sid,sheetLabel:label,open:{type:'sheet',id:sid}});
    }
    // a sheet that is ready itself is cut with its set: the set's other sheets that are not ready hold it
    if(want('laser') && !(+s.laserDoneAt>0) && s.setId && !s.draft && s.solidIncluded!==false && sheet(rec).included && (completedBefore(rec) || sheet(rec).ready)){
      if(ctx.set){
        const mates=[...new Map((ctx.sheets || []).filter(m=>!m.archived).map(m=>[m.id || m.sheetId,m])).values()].filter(m=>(m.id || m.sheetId)!==sid);
        for(const m of mates)if(!laserSheet(m).ready)out.push({step:'laser',key:'waitsOnSheet',label:sheetLabel(m),sheetId:sid,sheetLabel:label,open:{type:'sheet',id:m.id || m.sheetId}});
        for(const i of ctx.set.sheetIds || [])if(i!==sid && !mates.some(m=>(m.id || m.sheetId)===i))out.push({step:'laser',key:'missingSheet',label:'Sheet missing',sheetId:sid,sheetLabel:label,open:{type:'sheet',id:i}});
      }else if(ctx.setMissing)out.push({step:'laser',key:'setMissing',label:'Set not loaded',sheetId:sid,sheetLabel:label,open:{type:'sheet',id:sid}});
    }
    return out;
  }
  /** issues(sheetOrSet, ctx): the real issues holding a sheet or set back from Laser cutting.
   *  → [{ step:'nesting'|'engraving'|'backFiles'|'qr'|'orders'|'laser', key, sheetId, sheetLabel, ... }]
   *   order issue (step 'orders', one per order): key 'pooled'|'noSku'|'unmatched'|'noDesign'|'held'|'otherSheetNotReady' (the first of these among the pieces holding it),
   *     orderId, orderLabel, customer, listingId, thumb, pieceCount, pieces:[{index,key,poolId,label,kind,sheetId|null,sheetLabel|null,stage,why}] (the OTHER pieces that hold it), why, open:{type:'order',id,poolId}
   *   own blocker (no order): key 'archived'|'held'|'notInSet'|'roseLine'|'layout'|'approvalsNeeded'|'backFilesMissing'|'qrMissing', label (a few words, no counts), open:{type:'sheet',id}
   *   set mate (step 'laser'): key 'waitsOnSheet'|'missingSheet'|'setMissing', label, open:{type:'sheet',id}
   *  ctx (all optional): steps (the steps to report, default all) · rows + allSheets (every order row and EVERY live sheet: the pieces are read from these) · sheets/set/setMissing
   *  as explain reads them · thumb(orderId) → url. A cut sheet or set has none. */
  function issues(subject,ctx={}){
    const s=subject || {},steps=Array.isArray(ctx.steps)?ctx.steps:ALL_STEPS,kind=ctx.kind || (s.kind==='set' || (Array.isArray(s.sheetIds) && !s.poolIds && !s.id && !s.sheetId)?'set':'sheet');
    if(kind!=='set')return +s.laserDoneAt>0?[]:sheetIssues(s,ctx,steps);
    if(+s.laserDoneAt>0)return [];
    const raw=[...new Map((ctx.sheets || s.sheets || (ctx.allSheets || []).filter(x=>(s.sheetIds || []).includes(x.id || x.sheetId))).map(x=>[x.id || x.sheetId,x])).values()],live=raw.filter(x=>!x.archived),out=[];
    for(const i of [...new Set(s.sheetIds || live.map(x=>x.id || x.sheetId))])if(!live.some(x=>(x.id || x.sheetId)===i) && steps.includes('nesting'))out.push({step:'nesting',key:'missingSheet',label:'Sheet missing',sheetId:i,sheetLabel:'A sheet of this set',open:{type:'sheet',id:i}});
    for(const m of live)out.push(...issues(m,{...ctx,kind:'sheet',set:s,sheets:raw,steps:steps.filter(k=>k!=='laser')}));
    return out;
  }
  // the card the page draws: x[k] = {ok,hard,items,detail,short} for the five steps, in the words of a sheet or a set
  function build(kind,id,label,x,ready,done,laserWords,laserShort,laserItems){
    const behind=GATED.filter(k=>!x[k].ok),now=done?'completed':behind[0] || 'laser';
    const steps=STEPS.map(([key,name])=>{
      if(GATED.includes(key))return {key,label:name,state:x[key].ok?'done':x[key].hard?'blocked':'waiting',detail:x[key].detail,items:x[key].items};
      if(key==='laser')return {key,label:name,state:done?'done':'waiting',detail:done?'Cut on the laser.':ready?'Everything is done. Waiting to be cut on the laser.':laserWords,items:ready || done?[]:laserItems || []};
      return {key,label:name,state:done?'done':'waiting',detail:done?'Cut on the laser and marked complete.':'Marked complete after it is cut.',items:[]};
    });
    steps.find(s=>s.key===now).current=true;
    let nextText;
    if(done)nextText=`${label} has been cut on the laser.`;
    else if(ready)nextText='Everything is done: ready for laser cutting.';
    else if(behind.length){const later=behind.slice(1).map(stepName);nextText=sentence(x[behind[0]].short).replace(/\.$/,'')+(later.length?` (then ${later.join(', ')})`:'')+'.';}
    else nextText=sentence(laserShort || laserWords);
    return {kind,id,label,ready,done,step:now,steps,nextText};
  }
  /** explain(sheetOrSet, ctx): where a sheet or set stands on the way to Laser cutting, and what is still needed.
   *  → { kind, id, label, ready, done, step, steps:[{key,label,state:'done'|'waiting'|'blocked',detail,current?,items:[{kind:'order'|'charm'|'sheet',id,label,why}]}], nextText }
   *  ctx (all optional): kind ('sheet'|'set', else read from the record: a set has sheetIds and no pieces) · rows (the page's order
   *  rows: they name the buyer and the piece) · sheets (a set's member records; for a sheet in a set, the set's sheets) · set (the
   *  sheet's set record) · setMissing (the sheet's set is not loaded, so it cannot be released alone). Records are read as laserSheet
   *  and laserGroup read them, so ready is exactly the gate's answer. */
  // ctx.lookup: a names() result, or a function giving one (built once by the caller and only when a name is needed)
  const wrap=ctx=>{const get=()=>typeof ctx.lookup==='function'?ctx.lookup():ctx.lookup;return ctx.lookup?{order:id=>get().order(id),charm:id=>get().charm(id)}:names(ctx);};
  function explain(subject,ctx={}){
    const s=subject || {},N=wrap(ctx),kind=ctx.kind || (s.kind==='set' || (Array.isArray(s.sheetIds) && !s.poolIds && !s.id && !s.sheetId)?'set':'sheet');
    if(kind==='set')return explainSet(s,ctx,N);
    const e=sheetSteps(s,N),done=+s.laserDoneAt>0;
    let ready=e.r.included && (e.again || e.r.ready),laserWords='Starts once the steps before it are done.',laserShort='',laserItems=[];
    // a sheet in a set is cut with the whole set (canCut): its mates, or its set not being loaded, hold it
    if(ready && !done && s.setId && !s.draft && s.solidIncluded!==false){
      if(ctx.set){
        const g=explainSet(ctx.set,{...ctx,sheets:[...(ctx.sheets || []).filter(m=>(m.id || m.sheetId)!==e.id),s]},N);
        ready=g.ready;
        if(!ready){
          laserItems=g.steps.filter(t=>GATED.includes(t.key)).flatMap(t=>t.items.filter(i=>i.kind==='sheet' && i.id!==e.id)).filter((i,n,a)=>a.findIndex(j=>j.id===i.id)===n);
          laserWords=`${setName(ctx.set)} is cut together, so every sheet of it must be ready first.`;
          laserShort=`This sheet is done; ${setName(ctx.set)} is cut together and still waits for ${laserItems.slice(0,3).map(i=>`${i.label} (${lower(i.why)})`).join(', ') || 'its other sheets'}`;
        }
      }else if(ctx.setMissing){ready=false;laserWords=laserShort='The set this sheet belongs to has not been loaded yet, so it cannot be released alone.';}
    }
    return build('sheet',e.id,e.label,Object.fromEntries(GATED.map(k=>[k,{ok:!!e.ok[k],hard:!!e.hard[k],items:e.items[k],detail:e.detail[k],short:e.short[k]}])),ready,done,laserWords,laserShort,laserItems);
  }
  function explainSet(st,ctx,N){
    const id=st.setId || st.id || '',label=setName(st),raw=[...new Map((ctx.sheets || st.sheets || []).map(x=>[x.id || x.sheetId,x])).values()],live=raw.filter(x=>!x.archived);
    const expected=[...new Set(st.sheetIds || live.map(x=>x.id || x.sheetId))],gone=expected.filter(i=>!live.some(x=>(x.id || x.sheetId)===i));
    const parts=live.map(m=>sheetSteps(m,N)),single=parts.length===1 && !gone.length,ready=laserGroup(st,raw).ready,done=+st.laserDoneAt>0;
    const x={},every=parts.length+gone.length;
    for(const k of GATED){
      const behind=parts.filter(p=>!p.ok[k]),lost=k==='nesting'?gone.length:0,n=behind.length+lost;
      let items;
      if(single)items=parts[0].items[k];
      else{
        items=[];
        for(const i of k==='nesting'?gone:[]){const a=raw.find(y=>(y.id || y.sheetId)===i);items.push({kind:'sheet',id:i,label:a?sheetLabel(a):'A sheet of this set',why:a?.archived?'It was removed (archived) but is still listed in this set':'It is listed in this set but cannot be found, so the set is incomplete'});}
        for(const p of behind)items.push({kind:'sheet',id:p.id,label:p.label,why:p.detail[k].replace(/[.\s]+$/,'')});
        if(k==='orders'){const seen=new Set();for(const p of behind)for(const i of p.items.orders)if(i.kind==='order' && !seen.has(i.id)){seen.add(i.id);items.push(i);}}
      }
      x[k]={ok:!n,hard:lost>0 || parts.some(p=>p.hard[k]),items,
        detail:single?parts[0].detail[k]:!n?`All ${count(every,'sheet')} are through this step.`:`${n} of ${count(every,'sheet')} ${n===1?'is':'are'} not through this step.`,
        short:single?parts[0].short[k]:`${behind.map(p=>p.label).concat(Array(lost).fill('a missing sheet')).slice(0,3).join(', ')} ${n===1?'is':'are'} not through ${stepName(k)}`};
    }
    const laserWords=gone.length?`${count(gone.length,'sheet')} of ${label} cannot be found, so the set cannot be cut yet.`:`${label} is cut together, so every sheet of it must be ready first.`;
    const e=build('set',id,label,x,ready,done,laserWords,laserWords);
    // several sheets: name the ones that hold the set back
    if(!ready && !done && !single){
      const holding=parts.filter(p=>!(p.r.included && (p.again || p.r.ready))),bits=holding.slice(0,3).map(p=>`${p.label} (${p.short[GATED.find(k=>!p.ok[k])] || 'not ready'})`);
      const lead=gone.length?`${count(gone.length,'sheet')} of this set cannot be found${holding.length?`, and ${count(holding.length,'sheet')} ${holding.length===1?'is':'are'} not ready`:''}`:holding.length?`${holding.length} of ${parts.length} sheets ${holding.length===1?'is':'are'} not ready: ${bits.join(', ')}${holding.length>3?`, and ${holding.length-3} more`:''}`:'';
      if(lead)e.nextText=sentence(lead);
    }
    return e;
  }
  return {idsOf,orderIds,decisions,held,sheet,set,completedBefore,laserSheet,laserGroup,orderReports,forSheet,pieces,issues,copyIds,orderBlockers,filed,processStamps,seal,counter,explain,lookup:names,STEPS:STEPS.map(([key,label])=>({key,label})),sheetLabel};
});
