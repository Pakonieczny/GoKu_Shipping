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
    // one ear (or one disc) of a pair: only its own pieces' approvals are its seals (the line's transaction id is shared by the other ear: it must not count)
    const slot=row.parentRow && row.slot ? row.slot : null,lineKey=slot ? row.parentRow.key : null;
    const mine=e=>slot ? copies.has(e.data?.poolId) || e.type==='engraveChanged' && e.data?.slot===slot && e.lineKey===lineKey : e.lineKey===row.key || tx && String(e.transactionId)===tx || copies.has(e.data?.poolId);
    return merge({seals:(events || []).filter(e=>(e.type==='engraveApproved' || e.type==='engraveChanged' && e.data?.how==='skipped') && (!e.orderId || String(e.orderId)===rid) && mine(e)).map(e=>({id:e.id,how:e.type==='engraveApproved'?'engraveApproved':'engravePlain',at:+(e.type==='engraveChanged'?e.data?.decidedAt ?? e.at:e.at) || 0,by:e.by || ''}))});
  }
  function add(job,how,by,at=Date.now()){keep(job);const seal={id:`${how}:${at}:${String(by).trim()}`,how,at,by};job.engravingSeals=merge(job,{seals:[seal]});if(job.row){job.row.engrave ||= {};job.row.engrave.seals=job.engravingSeals;}return seal;}
  const regularSize=()=>root?.Seal?.BASE_SIZE || 50;
  function html(job){const seals=list(job);return seals.length && root?.Seal?`<span class="sealRow engravingSeals" data-seal-group data-seal-count="${seals.length}" role="group" aria-label="Engraving approval history">${seals.map(s=>root.Seal.html(s,regularSize(),'engravingSeal')).join('')}</span>`:'';}
  const esc=s=>String(s ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function record(e){return {key:e.job?.key,engravingSeals:merge(e.job,e.job?.row?.engrave,...(e.job?.backs || []),e.job?.editOriginal,e.back,e.saved),state:e.kind==='approved'?'approved':e.kind==='skipped'?'skipped':e.job?.state,approvedAt:e.kind==='approved'?e.at:0,approvedBy:e.kind==='approved'?e.by:'',decidedAt:e.kind==='skipped'?e.at:0,decidedBy:e.kind==='skipped'?e.by:''};}
  /* ── the back engraving card, one card for every place an order is shown (the order window's Overview and Sheet tab, the sheet window) ──
   * What it says is read from the real data the host hands over (e: {kind, job, text, note, working, reason, back, by, at, saved,
   * pieceLabel, pieceMeta, compact}): one quiet status, the plain reason what is missing, the words as read and what is unclear, the back
   * preview, the piece it belongs to, and two real buttons: Fix in Engraving (EngraveLink) and Approve engraving (the host's own approval,
   * which presses the permanent BACK ENGRAVING seal on that very button). Approving is offered only where it is possible; where it is not
   * (the words are not confirmed, the back is not ready) the button is there, disabled, with one line of why. */
  const STATUS={approve:'To approve',words:'Words to confirm',preparing:'Being prepared',approved:'Approved',skipped:'Cut plain',none:'No back engraving'};
  const WHY={approve:'Waiting for your approval',words:'The words need a decision',approved:'The back is approved',skipped:'Cut plain: nothing on the back'};
  const SOURCE={personalization:'the personalisation box',personalisation:'the personalisation box',buyerMessage:"the buyer's message",staffNote:'the staff note',messages:'the staff messages'};
  const NOT_ENGRAVABLE=/not engravable|cannot (?:be |take )engrav|design.*engrav/i;
  let askN=0;
  /* What an approval that did not go through said, kept for the card until the next press or until the piece is no longer to approve (a toast sits
     under a modal order window where nobody sees it; the card says it where the button is). Keyed by the job. */
  const fails=new Map();
  const badToasts=()=>new Map([...(root?.document?.querySelectorAll('#toasts .toast.bad') || [])].map(n=>[n,n.dataset.n]));
  /** Who presses: the name kept on this computer, else the small name bar inside the open window (CNEmployee.edit: never a browser pop-up on
   *  the order window). A string when the name is known (so a press goes on without a wait), else a promise of the name, '' when it is put away. */
  function who(why){
    const E=root?.CNEmployee;let n='';
    try{n=String((E && E.name && E.name()) || '').trim();}catch(_){}
    if(n)return n;
    try{return Promise.resolve(E && E.edit?E.edit({why:why || 'Kept with this approval and its seal.'}):E && E.ask?E.ask():'').then(v=>String(v || '').trim(),()=>'');}catch(_){return Promise.resolve('');}
  }
  /** The wait of an approval, said on its button: it cannot be pressed again, and a small spinner says what is being done. */
  function busyLabel(btn){if(!btn || !btn.isConnected)return;btn.disabled=true;btn.setAttribute('aria-busy','true');btn.innerHTML='<span class="spin"></span>Approving…';}
  /** Resolves when the approval stands (the stamp is down and the job is approved), or when `run` (the approval itself, which goes on to save the back file) ends. */
  function stamped(job,run){return new Promise(res=>{let t=0;const done=()=>{clearInterval(t);res();};t=setInterval(()=>{if(['approved','written'].includes(job?.state))done();},80);Promise.resolve(run).then(done,done);});}
  /** The words of the piece's back file while it is saved or waits for the cloud: the approval stands either way. */
  function savingNote(job){
    if(!job)return null;
    if(job.backPending)return {note:'The back file waits for the cloud: it is saved by itself when it is back',working:false};
    if(job.state==='approved')return {note:'Saving the back file…',working:true};
    return null;
  }
  /** What is unclear about the words, in plain lines: what was read with a question, and what the buyer asked for that the words alone do not carry. */
  function unclear(e){
    const job=e.job || {},out=[],add=t=>{t=String(t ?? '').trim();if(t && !NOT_ENGRAVABLE.test(t) && !out.includes(t))out.push(t);};
    if(e.kind!=='words' && e.kind!=='approve')return out;
    if(e.kind==='words')add(e.reason || job.reason);
    for(const q of job.questions || [])add(q);
    const r=job.requests || {};
    if(r.side && !['back','unspecified'].includes(r.side))add(`Asked for the ${r.side} side`);
    if(r.font)add(`Asked for the font ${r.font}`);
    const of=job.row?.spec?.font;   // a font chosen in a drop-down that the app does not have (Font: Typewriter): said like one asked for in a note
    if(of?.asked && !of.id && String(of.asked).toLowerCase()!==String(r.font || '').toLowerCase())add(`Asked for the font ${of.asked} (a drop-down option)`);
    if(r.handwriting)add('Asked for handwriting');
    if(r.image)add('Asked for an image');
    return out.slice(0,4);
  }
  /** Can this card approve the back, and when not, the one plain reason why. */
  function canApprove(e){
    const job=e.job;
    if(e.kind==='words')return {why:'Confirm the words in Engraving first: words that are not confirmed are never approved.'};
    if(e.kind==='preparing')return {why:'Not ready to approve: the back is still being prepared.'};
    if(e.kind==='approve'){
      if(!job)return {why:'Not loaded here yet: use Fix in Engraving to approve it.'};
      if(job.row?.state==='gone')return {why:'This order has left the pull (cancelled or shipped), so it is not approved here.'};
      return {ok:true};
    }
    return {};
  }
  /** Where the words came from, and who settled them. */
  function readFrom(e){
    const job=e.job || {};
    if(job.decision?.by)return `Words confirmed by ${job.decision.by}`;
    if(!job.source || job.source==='none')return '';
    const pct=Math.round((+job.confidence || 0)*100);
    return `Read from ${SOURCE[job.source] || job.source}${pct>0?` · ${pct}% sure`:''}`;
  }
  /** Both sheet inspectors and the order window use exactly the same back preview, words, approval and historical seals. */
  function panel(e={kind:'none'}){
    const kind=e.kind || 'none',status=STATUS[kind] || '',compact=!!e.compact;
    const all=list(record(e)),ap=canApprove(e),done=kind==='approved' || kind==='skipped';
    const sealsHtml=all.length && root?.Seal?`<span class="sealRow egButtonSeal" data-seal-group data-seal-count="${all.length}" role="group" aria-label="Engraving approval history">${all.map(s=>root.Seal.html(s,regularSize(),'engravingSeal')).join('')}</span>`:'';
    // (the seals rest on the button once it is approved; before that an earlier approval's seals sit below, so no seal covers the words "Approve engraving": the new one is pressed on the button)
    const history=kind==='approved'?'':html({seals:all});
    const why=kind==='preparing'?(e.job?.state==='classify'?'The words are not read yet':'Preview not ready yet'):(WHY[kind] || 'Back engraving');
    const reasonId='egWhy'+(++askN);
    let approve='';
    if(kind==='approved')approve=`<span class="egApproveWrap"><button type="button" class="btn sage sm egApproveButton" disabled aria-label="Back engraving approved">Approved</button>${sealsHtml}</span>`;
    else if(ap.ok)approve=`<span class="egApproveWrap"><button type="button" class="btn sage sm egApproveButton egAsk" data-e="approve">Approve engraving</button></span>`;
    else if(ap.why)approve=`<span class="egApproveWrap"><button type="button" class="btn sm egApproveButton egAsk egOff" disabled aria-describedby="${reasonId}">Approve engraving</button></span><span class="egOffWhy" id="${reasonId}">${esc(ap.why)}</span>`;
    const open=kind==='none'?'':`<button type="button" class="btn ${kind==='words'?'gold':'ghost'} sm egOpen" data-e="engrave">${done?'View in Engraving':'Fix in Engraving'} <span aria-hidden="true">→</span></button>`;
    const acts=kind==='words'?open+approve:approve+open;
    if(kind==='none')return `${compact?'':'<span class="fLabel">Back engraving</span>'}<div class="swEng egCard${compact?' egCompact':''}" data-state="none"><div class="top"><b>No back engraving</b></div></div>`;
    const preview=['approve','approved'].includes(kind)?'<div class="pv" data-engraving-preview></div>':'';
    const lbl=kind==='approve' || kind==='approved'?'Words on the back':'Words as read',from=kind==='approved'?'':readFrom(e);
    const words=e.text?`<div class="egWords"><span class="egLbl">${lbl}</span><div class="words">${esc(e.text)}</div>${from?`<span class="egMeta">${esc(from)}</span>`:''}</div>`
      :kind==='words' || kind==='preparing'?`<div class="egWords"><span class="egLbl">${lbl}</span><div class="words egNone">Nothing read yet</div></div>`:'';
    const un=unclear(e),unclearHtml=un.length?`<div class="egUnclear"><span class="egLbl">${kind==='words'?'What is unclear':'Worth a look'}</span><ul>${un.map(t=>`<li>${esc(t)}</li>`).join('')}</ul></div>`:'';
    const saving=kind==='approved' && !e.note?savingNote(e.job):null,note=e.note || saving?.note || '',working=e.note?e.working:saving?.working;
    const sub=note?`<span class="by egSub">${working?'<span class="owSpin" aria-hidden="true"></span>':''}<span>${esc(note)}</span></span>`:'';
    const failKey=e.job?.key,failed=kind==='approve' && failKey?fails.get(failKey):'';if(kind!=='approve' && failKey)fails.delete(failKey);
    const forPiece=e.pieceLabel?`<span class="egFor"><b>${esc(e.pieceLabel)}</b>${e.pieceMeta?` <small>${esc(e.pieceMeta)}</small>`:''}</span>`:'';
    const pill=status?`<span class="egPill" data-s="${esc(kind)}"><i aria-hidden="true"></i>${status}</span>`:'';
    return `${compact?'':'<span class="fLabel">Back engraving</span>'}<div class="swEng egCard${compact?' egCompact':''}" data-state="${esc(kind)}"><div class="top egTop">${forPiece}${pill}</div><div class="egWhy"><b>${why}</b>${sub}</div>${preview}${words}${unclearHtml}<div class="acts">${acts}</div>${failed?`<div class="egFail" role="alert">${esc(failed)}</div>`:''}${history?`<div class="egHistory">${history}</div>`:''}</div>`;
  }
  /** The preview zooms and pans where it lies (charm-nest-zoompan.js, the one module for every order picture): a click zooms in on the
   *  point clicked, a drag pans, the frame keeps its size. `zoom` ({id, key}) names the place and what it shows, so a card drawn again for
   *  the same piece keeps its zoom; the picture drawn from the fitted words is drawn again larger when the zoom settles. */
  function zoomPreview(slot,zoom,job,fitted){
    const Z=root?.CNZoomPan;if(!Z||!slot)return;
    try{Z.attach(slot,{id:(zoom&&zoom.id)||'eng',key:((zoom&&zoom.key)||job?.key||'')+(fitted?'|fit':'|png'),label:'Back engraving preview',maxPx:1800,
      hires:fitted?async({px})=>{const size=Math.max(900,Math.min(1800,Math.ceil(px/300)*300)),cv=root.Engrave.renderBack(job,size,{hatch:false,grid:false});cv.setAttribute('role','img');cv.setAttribute('aria-label','The full back engraving');return{el:cv,px:size};}:undefined});}catch(_){}
  }
  function wirePanel(host,e,{approve,open,imageUrl,zoom,stale}={}){
    // Approve engraving: the host's own approval runs (its name bar, Engrave.approve, the BACK ENGRAVING seal pressed on this very button). The wait
    // before the stamp is said on the button, with a small spinner; a refusal gives the button back as it was.
    // A refusal says why where the button is: the line a toast would have said is shown in the card (a toast sits under a modal order window).
    const key=e.job?.key || '',fit0=e.job?.fit,view0=e.job?.view;
    host.querySelector('[data-e=approve]')?.addEventListener('click',ev=>{
      const btn=ev.currentTarget;if(btn.disabled || !approve)return;
      // a card that is out of date approves nothing: only the placement it shows, still waiting for approval (the host draws the card again, and a new press is the person's)
      if(e.job && (e.job.state!=='review' || e.job.fit!==fit0 || e.job.view!==view0)){try{stale && stale();}catch(_){}return;}
      const was=btn.innerHTML,wasToasts=badToasts();let run;
      if(key)fails.delete(key);host.querySelector('.egFail')?.remove();
      const tell=err=>{
        if(!btn.isConnected || btn.closest('[data-state=approved]') || !key || ['approved','written'].includes(e.job.state))return;   // (it went through: nothing to explain)
        const toasted=[...badToasts()].filter(([n,c])=>wasToasts.get(n)!==c).map(([n])=>n.dataset.msg).filter(Boolean).pop(),msg=toasted || (err && String(err.message || err));
        if(!msg)return;
        fails.set(key,msg);
        const acts=btn.closest('.acts');if(!acts)return;
        let n=acts.parentElement.querySelector('.egFail');if(!n){n=root.document.createElement('div');n.className='egFail';n.setAttribute('role','alert');acts.after(n);}
        n.textContent=msg;
      };
      try{run=approve(btn);}catch(err){console.warn('engraving card: approve',err);tell(err);return;}
      if(!run || typeof run.then!=='function')return;
      if(btn.disabled && btn.isConnected)btn.innerHTML='<span class="spin"></span>Approving…';
      const back=()=>{if(btn.isConnected && !btn.closest('[data-state=approved]')){btn.innerHTML=was;btn.removeAttribute('aria-busy');}};
      run.then(()=>{back();tell();},err=>{console.warn('engraving card: approve',err);back();tell(err);});
    });
    // Fix / View in Engraving: the host opens that exact order and piece (EngraveLink.open); the wait is a small labelled spinner on the button.
    host.querySelector('[data-e=engrave]')?.addEventListener('click',ev=>{
      const btn=ev.currentTarget;if(btn.disabled)return;
      const was=btn.innerHTML;let run;
      try{run=open?.(btn);}catch(err){console.warn('engraving card: open',err);return;}
      if(!run || typeof run.then!=='function')return;
      btn.disabled=true;btn.setAttribute('aria-busy','true');btn.innerHTML='<span class="spin"></span>Opening Engraving…';
      const back=()=>{if(btn.isConnected){btn.disabled=false;btn.removeAttribute('aria-busy');btn.innerHTML=was;}};
      run.then(back,err=>{console.warn('engraving card: open',err);back();});
    });
    const slot=host.querySelector('[data-engraving-preview]');if(!slot)return;
    const job=e.job,img=e.back && (e.back.outputs?.png?.url || e.back.png || e.back.preview);
    // A fresh placement must use the current fit; an old approved thumbnail must not disguise edits.
    if(job?.fit && job?.view && root?.Engrave?.renderBack){try{const cv=root.Engrave.renderBack(job,600,{hatch:false,grid:false});cv.setAttribute('role','img');cv.setAttribute('aria-label','The full back engraving');slot.replaceChildren(cv);zoomPreview(slot,zoom,job,true);return;}catch(_){}}
    if(img){const im=root.document.createElement('img');im.alt='The full approved back engraving';im.crossOrigin='anonymous';im.src=imageUrl?imageUrl(img):img;slot.replaceChildren(im);zoomPreview(slot,zoom,job,false);return;}
    slot.classList.add('egPvNone');slot.innerHTML='<span class="by">No preview here: see the back in Engraving</span>';
  }
  async function press(button,stamp){
    if(!button || !root?.Seal)return;
    let wrap=button.closest('.egApproveWrap');if(!wrap){wrap=root.document.createElement('span');wrap.className='egApproveWrap';button.before(wrap);wrap.append(button);}
    let row=wrap.querySelector('.sealRow');if(!row){row=root.document.createElement('span');row.className='sealRow egButtonSeal';wrap.append(row);}
    row.dataset.sealGroup='';
    const wasDisabled=button.disabled,wasBusy=button.getAttribute('aria-busy');
    let seal=[...row.querySelectorAll('.seal')].find(s=>{try{const m=JSON.parse(s.querySelector('svg')?.getAttribute('data-seal-model'));return +m.at===+stamp.at && m.by===String(stamp.by || '') && m.action===(stamp.how==='engravePlain'?'CUT PLAIN':'BACK ENGRAVING');}catch(_){return false;}});
    if(seal && !seal.classList.contains('pending'))return;
    if(!seal){const holder=root.document.createElement('span');holder.innerHTML=root.Seal.html(stamp,regularSize(),'engravingSeal pending');seal=holder.firstElementChild;row.append(seal);}
    row.dataset.sealCount=String(row.querySelectorAll('.seal').length);root.Seal.fit?.(row);button.disabled=true;button.textContent='Approved';button.setAttribute('aria-busy','true');
    try{await root.Seal.press(seal);}finally{button.disabled=wasDisabled;if(wasBusy==null)button.removeAttribute('aria-busy');else button.setAttribute('aria-busy',wasBusy);}
  }
  return {merge,list,keep,add,html,press,record,panel,wirePanel,fromEvents,who,busyLabel,stamped};
});
