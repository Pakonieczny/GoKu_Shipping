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
  function list(job){const seals=merge(job,job?.row?.engrave,...(job?.backs || []),job?.editOriginal);if(!seals.length && ['approved','written','skipped'].includes(job?.state))seals.push({id:'legacy:'+job.key,how:job.state==='skipped'?'engravePlain':'engraveApproved',at:0,by:job.approvedBy || job.decision?.by || ''});return seals;}
  function keep(job){const seals=list(job);job.engravingSeals=seals;if(job.row){job.row.engrave ||= {};job.row.engrave.seals=seals;}return seals;}
  function add(job,how,by,at=Date.now()){keep(job);const seal={id:`${how}:${at}:${String(by).trim()}`,how,at,by};job.engravingSeals=merge(job,{seals:[seal]});if(job.row){job.row.engrave ||= {};job.row.engrave.seals=job.engravingSeals;}return seal;}
  function html(job){const seals=list(job);return seals.length && root?.Seal?`<span class="sealRow engravingSeals" role="group" aria-label="Engraving approval history">${seals.map(s=>root.Seal.html(s,56,'engravingSeal')).join('')}</span>`:'';}
  async function press(button,stamp){
    if(!button || !root?.Seal)return;
    let wrap=button.closest('.egApproveWrap');if(!wrap){wrap=root.document.createElement('span');wrap.className='egApproveWrap';button.before(wrap);wrap.append(button);}
    let row=wrap.querySelector('.sealRow');if(!row){row=root.document.createElement('span');row.className='sealRow egButtonSeal';wrap.append(row);}
    row.innerHTML=root.Seal.html(stamp,56,'engravingSeal pending');button.disabled=true;
    try{await root.Seal.press(row.firstElementChild);}finally{button.disabled=false;}
  }
  return {merge,list,keep,add,html,press};
});
