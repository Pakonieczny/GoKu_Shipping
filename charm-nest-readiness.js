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
      out[id]=e ? {needed:!!e.needed,state:e.state,approved:!!e.approved} : row.spec?.engraveCandidate===false ? {needed:false,state:'none',approved:true} : {needed:true,state:'unknown',approved:false};
    }
    return out;
  }
  function sheet(s){
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
      layout:total>0 && s.verification?.ok===true && !s.dirty && !s.saving && !['nesting','finishing','queued','error'].includes(s.status),
      front:!!url(s.outputs?.ai) && !!(s.preview || url(s.outputs?.preview)),
      approval:total>0 && waiting===0,
      backs:total>0 && saved===required,
      qr:labels.length>0 && labels.every(f=>f.path && f.url && f.payload) && orders.every(id=>covered.has(String(id)))
    };
    const included=!s.draft && s.solidIncluded!==false && !s.archived;
    return {total,required,approved,waiting,saved,plain,saving:Math.max(0,approved-saved),stages,included,ready:included && Object.values(stages).every(Boolean)};
  }
  function set(s,sheets){
    const unique=[...new Map((sheets || []).map(x=>[x.id || x.sheetId,x])).values()],reports=unique.map(sheet);
    const expected=s.sheetIds || unique.map(x=>x.id || x.sheetId),complete=expected.length>0 && expected.length===unique.length && expected.every(id=>unique.some(x=>(x.id || x.sheetId)===id));
    const stages=Object.fromEntries(['layout','front','approval','backs','qr'].map(k=>[k,complete && reports.every(r=>r.stages[k])]));
    return {...Object.fromEntries(['total','required','approved','waiting','saved','plain','saving'].map(k=>[k,reports.reduce((n,r)=>n+r[k],0)])),stages,included:complete && reports.every(r=>r.included),ready:complete && reports.every(r=>r.ready),sheets:unique.length};
  }
  const escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const icon=ready=>`<svg viewBox="0 0 32 38" aria-hidden="true"><path d="M8 24 5 36l11-5 11 5-3-12"/><circle cx="16" cy="15" r="12"/>${ready?'<path d="m10 15 4 4 8-9"/>':'<path d="M16 9v7m0 5v.1"/>'}</svg>`;
  const names={layout:'Layout checked',front:'Front files saved',approval:'Engraving approved',backs:'Back files saved',qr:'QR labels ready'};
  function seal(r,scope='Sheet'){return `<span class="laserSeal ${r.ready?'earned':'pending'}">${icon(r.ready)}<span><b>${r.ready?'Ready for laser':'Awaiting approval'}</b><small>${escape(scope)} · ${Object.values(r.stages).filter(Boolean).length} / 5 checks complete</small></span></span>`;}
  function panel(r,title='Sheet'){
    return `<section class="laserApproval" aria-label="${escape(title)} production approvals">${seal(r,title)}<div class="laserTotals" aria-live="polite"><span><b>${r.waiting}</b>Awaiting approval</span><span><b>${r.approved}</b>Approved charms</span><span><b>${r.saved} / ${r.required}</b>Backs saved</span></div><div class="laserBadges">${Object.entries(names).map(([key,label])=>`<span class="laserBadge ${r.stages[key]?'earned':'pending'}" title="${r.stages[key]?'Complete':'Pending'}: ${label}">${icon(r.stages[key])}<span>${label}<small>${r.stages[key]?'Complete':'Pending'}</small></span></span>`).join('')}</div>${r.plain?`<p class="laserNote">${r.plain} charm${r.plain===1?'':'s'} confirmed without back engraving.</p>`:''}${!r.included?'<p class="laserNote">Sheet selection or set membership still needs attention.</p>':''}</section>`;
  }
  return {idsOf,decisions,sheet,set,panel,seal};
});
