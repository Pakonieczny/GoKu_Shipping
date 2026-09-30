/* Immutable engraving decisions, shared by saved backs and every engraving view. */
(function(root,factory){const api=factory(root);if(typeof module==='object'&&module.exports)module.exports=api;else root.CNEngravingSeals=api;})(typeof window!=='undefined'?window:null,function(root){
  'use strict';
  const kinds=new Set(['engraveApproved','engravePlain']);
  function merge(...records){
    const stamps=new Map();
    const add=s=>{if(!s || !kinds.has(s.how))return;const at=+s.at || 0,by=String(s.by || '').trim(),id=s.id || `${s.how}:${at}:${by}`;const identity=`${s.how}:${at}:${by}`;if(!stamps.has(identity))stamps.set(identity,{id,how:s.how,at,by});};
    for(const r of records.filter(Boolean)){
      for(const s of [...(r.engravingSeals || []),...(r.seals || []),...(r.engrave?.seals || []),...(r.row?.engrave?.seals || [])])add(s);
      if(+r.approvedAt>0)add({how:'engraveApproved',at:+r.approvedAt,by:r.approvedBy || ''});
      if(r.state==='skipped' && +r.decidedAt>0)add({how:'engravePlain',at:+r.decidedAt,by:r.decidedBy || r.decision?.by || ''});
    }
    return [...stamps.values()].sort((a,b)=>a.at-b.at || a.id.localeCompare(b.id));
  }
  function list(job){const seals=merge(job,job?.row?.engrave,...(job?.backs || []),job?.editOriginal);if(!seals.length && (['approved','written','skipped'].includes(job?.state) || job?.needed && job?.approved))seals.push({id:'legacy:'+job.key,how:job.state==='skipped'?'engravePlain':'engraveApproved',at:0,by:job.approvedBy || job.decision?.by || ''});return seals;}
  function keep(job){const seals=list(job);job.engravingSeals=seals;if(job.row){job.row.engrave ||= {};job.row.engrave.seals=seals;}return seals;}
  function add(job,how,by,at=Date.now()){keep(job);const seal={id:`${how}:${at}:${String(by).trim()}`,how,at,by};job.engravingSeals=merge(job,{seals:[seal]});if(job.row){job.row.engrave ||= {};job.row.engrave.seals=job.engravingSeals;}return seal;}
  function html(job){const seals=list(job);return seals.length && root?.Seal?`<span class="sealRow engravingSeals" role="group" aria-label="Engraving approval history">${seals.map(s=>root.Seal.html(s,56,'engravingSeal')).join('')}</span>`:'';}
  const esc=s=>String(s ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const check='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="m5 12 4 4L19 6"/></svg>';
  function record(e){return {key:e.job?.key,engravingSeals:merge(e.job,e.job?.row?.engrave,...(e.job?.backs || []),e.job?.editOriginal,e.back,e.saved),state:e.kind==='approved'?'approved':e.kind==='skipped'?'skipped':e.job?.state,approvedAt:e.at,approvedBy:e.by};}
  /** Both sheet inspectors use exactly the same back preview, words, approval and historical seals. */
  function panel(e={kind:'none'}){
    const states={approve:'To approve',words:'Words to confirm',preparing:'Being prepared',approved:'Approved',skipped:'Cut plain'},kind=e.kind || 'none';
    const title=kind==='approve'?'Check the back, then approve':kind==='approved'?'Engraved on the back':kind==='words'?'The words need a decision':kind==='none'?'No back engraving':'Back engraving';
    const all=list(record(e)),latest=kind==='approved'?all.filter(s=>s.how==='engraveApproved').at(-1):null;
    const history=html({seals:all.filter(s=>s!==latest)});
    const approval=kind==='approve' && e.job || kind==='approved';
    const approve=approval?`<span class="egApproveWrap"><button type="button" class="btn sage sm egApproveButton"${kind==='approved'?' disabled aria-label="Back engraving approved"':' data-e="approve"'}>${check}${kind==='approved'?'Approved':'Approve'}</button>${latest && root?.Seal?`<span class="sealRow egButtonSeal">${root.Seal.html(latest,56,'engravingSeal')}</span>`:''}</span>`:'';
    const open=kind==='none' || kind==='skipped'?'':`<button type="button" class="btn ghost sm" data-e="engrave">${kind==='approve'?'Adjust in Engrave':kind==='approved'?'View in Engrave':kind==='words'?'Confirm the words in Engrave':'Open in Engrave'} <span aria-hidden="true">→</span></button>`;
    const preview=['approve','approved'].includes(kind)?'<div class="pv" data-engraving-preview></div>':'';
    return `<span class="fLabel">Back engraving</span><div class="swEng" data-state="${esc(kind)}"><div class="top"><b>${title}</b>${states[kind]?`<span>${states[kind]}</span>`:''}</div>${preview}${e.text?`<div class="words">${esc(e.text)}</div>`:''}${e.note?`<div class="by">${esc(e.note)}</div>`:''}<div class="acts">${approve}${open}</div>${history?`<div class="egHistory">${history}</div>`:''}</div>`;
  }
  function wirePanel(host,e,{approve,open}={}){
    host.querySelector('[data-e=approve]')?.addEventListener('click',ev=>approve?.(ev.currentTarget));
    host.querySelector('[data-e=engrave]')?.addEventListener('click',ev=>open?.(ev.currentTarget));
    const slot=host.querySelector('[data-engraving-preview]');if(!slot)return;
    const job=e.job,img=e.back && (e.back.outputs?.png?.url || e.back.png || e.back.preview);
    // A fresh placement must use the current fit; an old approved thumbnail must not disguise edits.
    if(job?.fit && job?.view && root?.Engrave?.renderBack){try{const cv=root.Engrave.renderBack(job,600,{hatch:false,grid:false});cv.setAttribute('role','img');cv.setAttribute('aria-label','The full back engraving');slot.replaceChildren(cv);return;}catch(_){}}
    if(img){const im=root.document.createElement('img');im.alt='The full approved back engraving';im.src=/^https?:/.test(img) && root.cors?root.cors(img):img;slot.replaceChildren(im);return;}
    slot.innerHTML='<span class="by">Open in Engrave to see the back</span>';
  }
  async function press(button,stamp){
    if(!button || !root?.Seal)return;
    let wrap=button.closest('.egApproveWrap');if(!wrap){wrap=root.document.createElement('span');wrap.className='egApproveWrap';button.before(wrap);wrap.append(button);}
    let row=wrap.querySelector('.sealRow');if(!row){row=root.document.createElement('span');row.className='sealRow egButtonSeal';wrap.append(row);}
    row.innerHTML=root.Seal.html(stamp,56,'engravingSeal pending');button.disabled=true;
    try{await root.Seal.press(row.firstElementChild);}finally{button.disabled=false;}
  }
  return {merge,list,keep,add,html,press,record,panel,wirePanel};
});
