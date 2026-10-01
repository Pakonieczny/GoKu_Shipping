/* One production-readiness policy shared by the Library and the server.
 * Nesting completion alone never grants permission to start the laser. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CharmNestReadiness=api;})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const idsOf=s=>[...new Set((s.poolIds || []).filter(Boolean))];
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
      orders:options.physicalOnly===true || orderIds(s).every(id=>s.orderReadiness?.[id]?.ready===true)
    };
    const included=!s.draft && s.solidIncluded!==false && !s.archived;
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
  // An order travels whole: every line and copy needs its design, files, decisions and labels,
  // including a second metal on another sheet. No-design and cancelled lines require no cutting.
  function orderReports(rows,sheets){
    const groups=new Map(),copies=new Map(),physical=new Map();
    for(const s of sheets || [])if(!s.archived){physical.set(s,sheet(s,{physicalOnly:true}));for(const id of idsOf(s)){const xs=copies.get(id)||[];xs.push(s);copies.set(id,xs);}}
    for(const row of rows || []){const id=String(row.order?.receiptId || row.orderId || String(row.key || '').split('_')[0] || '');if(!id)continue;const xs=groups.get(id)||[];xs.push(row);groups.set(id,xs);}
    const out={},problems={unmatchedSku:'SKU not in a master',needsMaterial:'needs material',needsMapping:'needs an option mapped',missingSize:'no design for that size'};
    for(const [id,lines] of groups){
      let block=null;
      for(const l of lines){
        let why='';
        if(l.state==='gone')continue;
        if(l.hold || l.changePending)why=l.hold || 'Order changes need review';
        else if(l.spec?.noDesign || l.noDesign || l.state==='noDesign')continue;
        else if(l.problems?.length){const p=l.problems[0].kind || l.problems[0];why=problems[p] || String(p);}
        else if(!['written','labelled','committed'].includes(l.state))why=l.reason || `line is ${l.state || 'not ready'}`;
        else if(!l.poolIds?.length || l.poolIds.length<(+(l.spec?.quantity || l.quantity) || 1))why='Not every copy has a saved sheet';
        else for(const pid of l.poolIds){
          const on=copies.get(pid)||[];
          if(!on.some(s=>+s.laserDoneAt>0 || physical.get(s).ready)){
            const s=on[0],r=s && physical.get(s),stage=r && Object.keys(r.stages).find(k=>!r.stages[k]);
            const missing={layout:'layout needs verification',front:'cutting files are missing',approval:'engraving needs approval',backs:'back engraving files are not saved',qr:'QR labels are missing'};
            why=s?`${s.metalLabel || s.metal || 'Sheet'}: ${missing[stage] || 'not ready for laser cutting'}`:'An item is not on a saved sheet';break;
          }
        }
        if(why){block={ready:false,line:l.key || [id,l.transactionId].filter(Boolean).join('_'),why};break;}
      }
      out[id]=block || {ready:true};
    }
    return out;
  }
  const orderBlockers=s=>orderIds(s).filter(id=>s.orderReadiness?.[id]?.ready!==true).map(id=>({id,...(s.orderReadiness?.[id] || {ready:false,why:'Order readiness has not been verified'})}));
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
  const escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  // The badge stays visible; only verified readiness lights it up.
  function seal(r,scope='Sheet',source){
    const Sl=typeof window!=='undefined' && window.Seal;if(!Sl?.face)return '';
    const saved=r.ready && processStamps(source || r).filter(s=>s.how==='laserReady' && +s.at>0).at(-1);
    const model=saved?Sl.modelOf(saved):{family:'laser',action:r.ready?'LASER READY':'AWAITING LASER',status:true,ghost:!r.ready,at:0,by:''};
    const label=escape(scope+(r.ready?' ready for laser cutting':' not ready for laser cutting')+(saved?' · '+Sl.titleOf(saved):''));
    return `<span class="laserSeal ${r.ready?'earned':'pending'}" tabindex="0" role="img" aria-label="${label}" title="${label}">${Sl.face(model)}</span>`;
  }
  function counter(r,scope='Sheet',source){
    // a sheet with no engraved backs has nothing to count: its cards read "0 / 0" beside the seal
    return (r.required ? `<span class="backSavedCount" title="Engraved backs saved" aria-label="${r.saved} of ${r.required} backs saved"><b>${r.saved} / ${r.required}</b></span>` : '')+seal(r,scope,source);
  }
  return {idsOf,orderIds,decisions,sheet,set,completedBefore,laserSheet,laserGroup,orderReports,orderBlockers,filed,processStamps,seal,counter};
});
