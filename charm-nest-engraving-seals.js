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
  function fromEvents(events,row){
    const rid=String(row.order?.receiptId || ''),tx=String(row.line?.transactionId || ''),copies=new Set(row.poolIds || []);
    return merge({seals:(events || []).filter(e=>(e.type==='engraveApproved' || e.type==='engraveChanged' && e.data?.how==='skipped') && (!e.orderId || String(e.orderId)===rid) && (e.lineKey===row.key || tx && String(e.transactionId)===tx || copies.has(e.data?.poolId))).map(e=>({id:e.id,how:e.type==='engraveApproved'?'engraveApproved':'engravePlain',at:+(e.type==='engraveChanged'?e.data?.decidedAt ?? e.at:e.at) || 0,by:e.by || ''}))});
  }
  function add(job,how,by,at=Date.now()){keep(job);const seal={id:`${how}:${at}:${String(by).trim()}`,how,at,by};job.engravingSeals=merge(job,{seals:[seal]});if(job.row){job.row.engrave ||= {};job.row.engrave.seals=job.engravingSeals;}return seal;}
  const regularSize=()=>root?.Seal?.BASE_SIZE || 84;
  function html(job){const seals=list(job);return seals.length && root?.Seal?`<span class="sealRow engravingSeals" data-seal-group data-seal-count="${seals.length}" role="group" aria-label="Engraving approval history">${seals.map(s=>root.Seal.html(s,regularSize(),'engravingSeal')).join('')}</span>`:'';}
  const esc=s=>String(s ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function record(e){return {key:e.job?.key,engravingSeals:merge(e.job,e.job?.row?.engrave,...(e.job?.backs || []),e.job?.editOriginal,e.back,e.saved),state:e.kind==='approved'?'approved':e.kind==='skipped'?'skipped':e.job?.state,approvedAt:e.kind==='approved'?e.at:0,approvedBy:e.kind==='approved'?e.by:'',decidedAt:e.kind==='skipped'?e.at:0,decidedBy:e.kind==='skipped'?e.by:''};}
  /** Both sheet inspectors use exactly the same back preview, words, approval and historical seals. */
  function panel(e={kind:'none'}){
    const states={approve:'To approve',words:'Words to confirm',preparing:'Being prepared',approved:'Approved',skipped:'Cut plain'},kind=e.kind || 'none';
    const title=kind==='approve'?'Check the back, then approve':kind==='approved'?'Engraved on the back':kind==='words'?'The words need a decision':kind==='none'?'No back engraving':'Back engraving';
    const all=list(record(e)),latest=kind==='approved'?all.filter(s=>s.how==='engraveApproved').at(-1):null;
    const history=html({seals:all.filter(s=>s!==latest)});
    const approval=kind==='approve' && e.job || kind==='approved';
    const approve=approval?`<span class="egApproveWrap"><button type="button" class="btn sage sm egApproveButton"${kind==='approved'?' disabled aria-label="Back engraving approved"':' data-e="approve"'}>Approved</button>${latest && root?.Seal?`<span class="sealRow egButtonSeal" data-seal-group data-seal-count="1">${root.Seal.html(latest,regularSize(),'engravingSeal')}</span>`:''}</span>`:'';
    const open=kind==='none' || kind==='skipped'?'':`<button type="button" class="btn ghost sm" data-e="engrave">${kind==='approve'?'Adjust in Engrave':kind==='approved'?'View in Engrave':kind==='words'?'Confirm the words in Engrave':'Open in Engrave'} <span aria-hidden="true">→</span></button>`;
    const preview=['approve','approved'].includes(kind)?'<div class="pv" data-engraving-preview></div>':'';
    return `<span class="fLabel">Back engraving</span><div class="swEng" data-state="${esc(kind)}"><div class="top"><b>${title}</b>${states[kind]?`<span>${states[kind]}</span>`:''}</div>${preview}${e.text?`<div class="words">${esc(e.text)}</div>`:''}${e.note?`<div class="by">${esc(e.note)}</div>`:''}<div class="acts">${approve}${open}</div>${history?`<div class="egHistory">${history}</div>`:''}</div>`;
  }
  function wirePanel(host,e,{approve,open,imageUrl}={}){
    host.querySelector('[data-e=approve]')?.addEventListener('click',ev=>approve?.(ev.currentTarget));
    host.querySelector('[data-e=engrave]')?.addEventListener('click',ev=>open?.(ev.currentTarget));
    const slot=host.querySelector('[data-engraving-preview]');if(!slot)return;
    const job=e.job,img=e.back && (e.back.outputs?.png?.url || e.back.png || e.back.preview);
    // A fresh placement must use the current fit; an old approved thumbnail must not disguise edits.
    if(job?.fit && job?.view && root?.Engrave?.renderBack){try{const cv=root.Engrave.renderBack(job,600,{hatch:false,grid:false});cv.setAttribute('role','img');cv.setAttribute('aria-label','The full back engraving');slot.replaceChildren(cv);return;}catch(_){}}
    if(img){const im=root.document.createElement('img');im.alt='The full approved back engraving';im.crossOrigin='anonymous';im.src=imageUrl?imageUrl(img):img;slot.replaceChildren(im);return;}
    slot.innerHTML='<span class="by">Open in Engrave to see the back</span>';
  }
  async function press(button,stamp){
    if(!button || !root?.Seal)return;
    let wrap=button.closest('.egApproveWrap');if(!wrap){wrap=root.document.createElement('span');wrap.className='egApproveWrap';button.before(wrap);wrap.append(button);}
    let row=wrap.querySelector('.sealRow');if(!row){row=root.document.createElement('span');row.className='sealRow egButtonSeal';wrap.append(row);}
    row.dataset.sealGroup='';row.dataset.sealCount='1';
    const wasDisabled=button.disabled,wasBusy=button.getAttribute('aria-busy');
    row.innerHTML=root.Seal.html(stamp,regularSize(),'engravingSeal pending');button.disabled=true;button.textContent='Approved';button.setAttribute('aria-busy','true');
    try{await root.Seal.press(row.firstElementChild);}finally{button.disabled=wasDisabled;if(wasBusy==null)button.removeAttribute('aria-busy');else button.setAttribute('aria-busy',wasBusy);}
  }
  return {merge,list,keep,add,html,press,record,panel,wirePanel,fromEvents};
});
