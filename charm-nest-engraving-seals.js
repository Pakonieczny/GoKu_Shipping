/* Immutable engraving decisions, shared by saved backs and every engraving view. */
(function(root,factory){const api=factory(root);if(typeof module==='object'&&module.exports)module.exports=api;else root.CNEngravingSeals=api;})(typeof window!=='undefined'?window:null,function(root){
  'use strict';
  const kinds=new Set(['engraveApproved','engravePlain']);
  /* ── the FACE of an engraving (front / back; the model is charm-nest-engrave-faces.js; a job, a seal or a record that does not say "front" is the back) ──
   * Every word this card says about "the back" is said about "the front" for a front job, and not otherwise: a back (every engraving made before the front existed) is drawn byte for byte as it was. */
  const faceLib=()=>root?.CharmNestEngraveFaces || (typeof module==='object' && typeof require==='function'?(()=>{try{return require('./charm-nest-engrave-faces.js');}catch(_){return null;}})():null);
  const faceOf=x=>{const F=faceLib();return F && F.faceOf?F.faceOf(x):x && (x.face==='front' || String(x.key || '').endsWith('~front'))?'front':'back';};
  const aboutFace=(text,face)=>face==='front'?String(text).replace(/\bback\b/g,'front').replace(/\bBack\b/g,'Front'):text;
  /* FACES (plans/front-back-1011): a seal of a FRONT engraving says `face:'front'` and its id is `how:front:at:by`; a back seal has no `face` and the id it always had (`how:at:by`), so
     every seal already made reads and merges exactly as before, and a front and a back approved in the same instant by the same person are two seals. The face of a seal given by a
     record is that record's own (a front job, a front back-file record); a seal that carries its face keeps it. */
  const frontOf=r=>!!r && (r.face==='front' || (typeof r.key==='string' && r.key.endsWith('~front')));
  function merge(...records){
    const stamps=new Map();
    const add=(s,rf)=>{if(!s || !kinds.has(s.how))return;const at=+s.at || 0,by=String(s.by || '').trim(),front=s.face==='front' || (s.face!=='back' && rf),face=front?'front:':'';const identity=`${s.how}:${face}${at}:${by}`,id=s.id || identity;if(!stamps.has(identity))stamps.set(identity,front?{id,how:s.how,at,by,face:'front'}:{id,how:s.how,at,by});};
    for(const r of records.filter(Boolean)){
      const rf=frontOf(r);
      for(const s of [...(r.engravingSeals || []),...(r.seals || []),...(r.engrave?.seals || []),...(r.row?.engrave?.seals || [])])add(s,rf);
      if(+r.approvedAt>0)add({how:'engraveApproved',at:+r.approvedAt,by:r.approvedBy || ''},rf);
      if(r.state==='skipped' && +r.decidedAt>0)add({how:'engravePlain',at:+r.decidedAt,by:r.decidedBy || r.decision?.by || ''},rf);
    }
    return [...stamps.values()].sort((a,b)=>a.at-b.at || a.id.localeCompare(b.id));
  }
  function list(job){const seals=merge(job,job?.row?.engrave,...(job?.backs || []),job?.editOriginal);if(!seals.length && (['approved','written','skipped'].includes(job?.state) || job?.needed && job?.approved))seals.push({id:'legacy:'+job.key,how:job.state==='skipped'?'engravePlain':'engraveApproved',at:0,by:job.approvedBy || job.decision?.by || '',...(frontOf(job)?{face:'front'}:{})});return seals;}
  function keep(job){const seals=list(job);job.engravingSeals=seals;if(job.row){job.row.engrave ||= {};job.row.engrave.seals=seals;}return seals;}
  function fromEvents(events,row){
    const rid=String(row.order?.receiptId || ''),tx=String(row.line?.transactionId || ''),copies=new Set(row.poolIds || []);
    // one ear (or one disc) of a pair: only its own pieces' approvals are its seals (the line's transaction id is shared by the other ear: it must not count)
    const slot=row.parentRow && row.slot ? row.slot : null,lineKey=slot ? row.parentRow.key : null;
    // (the row of ONE face of a piece sees only that face's events: a front's `data.face` is 'front'; the line's own row sees them all, each seal says its face)
    const ownFace=row.parentRow ? (frontOf(row) ? e=>e.data?.face==='front' : e=>e.data?.face!=='front') : ()=>true;
    const mine=e=>ownFace(e) && (slot ? copies.has(e.data?.poolId) || e.type==='engraveChanged' && e.data?.slot===slot && e.lineKey===lineKey : e.lineKey===row.key || tx && String(e.transactionId)===tx || copies.has(e.data?.poolId));
    return merge({seals:(events || []).filter(e=>(e.type==='engraveApproved' || e.type==='engraveChanged' && e.data?.how==='skipped') && (!e.orderId || String(e.orderId)===rid) && mine(e)).map(e=>{const s={id:e.id,how:e.type==='engraveApproved'?'engraveApproved':'engravePlain',at:+(e.type==='engraveChanged'?e.data?.decidedAt ?? e.at:e.at) || 0,by:e.by || ''};if(e.data?.face==='front')s.face='front';return s;})});
  }
  function add(job,how,by,at=Date.now()){keep(job);const front=frontOf(job),seal=front?{id:`${how}:front:${at}:${String(by).trim()}`,how,at,by,face:'front'}:{id:`${how}:${at}:${String(by).trim()}`,how,at,by};job.engravingSeals=merge(job,{seals:[seal]});if(job.row){job.row.engrave ||= {};job.row.engrave.seals=job.engravingSeals;}return seal;}
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
    if(r.side && ![faceOf(job),'unspecified'].includes(r.side))add(`Asked for the ${r.side} side`);   // (a side other than the face this card is about)
    if(r.font)add(`Asked for the font ${r.font}`);
    const of=job.row?.spec?.font;   // a font chosen in a drop-down that the app does not have (Font: Script): said like one asked for in a note; one the app has is engraved in its lookalike (spec.font.id) and is no question
    if(of?.asked && !of.id && String(of.asked).toLowerCase()!==String(r.font || '').toLowerCase())add(`Requested font: ${of.asked} (a drop-down option, engraved in Source Sans 3)`);
    if(r.handwriting)add('Asked for handwriting');
    if(r.image)add('Asked for an image');
    return out.slice(0,4);
  }
  /** The font the order's drop-down chose and the installed font it is engraved in, in one plain line ("Font: Stylish → Playwrite US Trad"); '' when the line has no font or the shop does not offer the word (that one is said under unclear). */
  const orders=()=>root?.CharmNestOrders || (typeof module==='object' && typeof require==='function'?(()=>{try{return require('./charm-nest-orders.js');}catch(_){return null;}})():null);
  function fontMeta(e){
    const O=orders(),sp=e.job?.row?.spec;if(!O || !O.fontLine || !O.pieceFont || !sp?.font)return '';
    const m=/^D(\d+)$/.exec(String(e.job?.slot || '')),ix=m?+m[1]-1:null;
    try{return O.pieceFont(sp,ix).mapped?O.fontLine(sp,ix):'';}catch(_){return '';}
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
    // the words of a front or back taken from the order's own reading (spec.engraving: the option and the labelled note), with no paid reading: a person's edit says who
    if(job.wordsSource==='spec.engraving'){
      const ov=job.row?.engravingOverride || job.row?.parentRow?.engravingOverride;
      return job.source==='person' || ov?.by?`Words set by ${ov?.by || 'a person'}`:`Read from the order's own labels${pct>0?` · ${pct}% sure`:''}`;
    }
    return `Read from ${SOURCE[job.source] || job.source}${pct>0?` · ${pct}% sure`:''}`;
  }
  /** What the intake read of this line's faces, in the one plain line it wrote for Paul (spec.engraving.why), or who set them; '' for a line that engraves the back only and was never edited, so those cards are exactly what they were. */
  function setupWhy(e){
    const F=faceLib(),row=e.job?.row,line=row?.parentRow || row;
    if(!F || !F.setupOf || !line || !line.spec)return '';
    let st=null;try{st=F.setupOf(line);}catch(_){return '';}
    if(!st || !(st.faces.includes('front') || st.edited))return '';
    return st.edited?`Faces set by ${st.by || 'a person'}: ${F.modeLabel(st.mode).toLowerCase()}`:String(st.why || '');
  }
  /** The pieces of a cycle as piece groups (a piece with its Front and Back job), and the key to go to from the shown one: Back / Next go piece by piece, keeping the face that is shown. */
  const groupsOf=c=>{const F=faceLib();return F && F.groupPieces && c && Array.isArray(c.pieces)?F.groupPieces(c.pieces.map(p=>p.job).filter(Boolean)):[];};
  function neighbour(c,dir){
    const gs=groupsOf(c);if(gs.length<2)return null;
    const at=gs.findIndex(g=>g.jobs.some(j=>j.key===c.key));if(at<0)return null;
    const here=gs[at].jobs.find(j=>j.key===c.key),face=here?faceOf(here):'back',to=gs[(at+(dir<0?gs.length-1:1))%gs.length];
    const job=(face==='front'?to.front || to.back:to.back || to.front);
    return job?{key:job.key,name:to.label || 'the piece'}:null;
  }
  /** The counted discs of one order, one at a time (Paul, 10 Oct 2026): the very switch of the Engrave editor (charm-nest-piece-switch.js: one button for each disc with its words, its font by name
   *  and where it stands) and Back / Next round the discs. `c` = { pieces (CharmNestPieceSwitch.list of the line's discs), key (the disc shown) }. Nothing here for any other line: no `cycle`, no markup. */
  function cycleHtml(c){
    const PS=root?.CharmNestPieceSwitch;if(!c || !PS || !Array.isArray(c.pieces))return '';
    // a line with a piece that has a front AND a back: the piece switch (when there are several pieces) and, under it, the Front | Back switch of the piece shown, both drawn by the switch component itself
    if(c.faces){
      const cur=PS.cursor(c.pieces,c.key);if(!cur.current)return '';
      const prev=neighbour(c,-1),next=neighbour(c,1);
      const nav=prev && next?`<span class="nav" role="group" aria-label="Back and next piece"><button type="button" class="btn ghost xs" data-a="discPrev" title="the previous piece: ${esc(prev.name)}">‹ Back</button><button type="button" class="btn ghost xs" data-a="discNext" title="the next piece: ${esc(next.name)}">Next ›</button></span>`:'';
      return `<div class="egCycle" data-cycle>${PS.html(c.pieces,c.key,{fonts:true,faces:true})}${nav}</div>`;
    }
    if(c.pieces.length<2)return '';
    const cur=PS.cursor(c.pieces,c.key);if(!cur.current)return '';
    return `<div class="egCycle" data-cycle>${PS.html(c.pieces,c.key,{fonts:true})}<span class="nav" role="group" aria-label="Back and next disc"><button type="button" class="btn ghost xs" data-a="discPrev" title="the previous disc: ${esc(cur.prev.name)}">‹ Back</button><button type="button" class="btn ghost xs" data-a="discNext" title="the next disc: ${esc(cur.next.name)}">Next ›</button></span></div>`;
  }
  /** Both sheet inspectors and the order window use exactly the same back preview, words, approval and historical seals. */
  function panel(e={kind:'none'}){
    const kind=e.kind || 'none',face=e.face || faceOf(e.job),say=t=>aboutFace(t,face),status=say(STATUS[kind] || ''),compact=!!e.compact,cycle=cycleHtml(e.cycle);
    const all=list(record(e)),ap=canApprove(e),done=kind==='approved' || kind==='skipped';
    const sealsHtml=all.length && root?.Seal?`<span class="sealRow egButtonSeal" data-seal-group data-seal-count="${all.length}" role="group" aria-label="Engraving approval history">${all.map(s=>root.Seal.html(s,regularSize(),'engravingSeal')).join('')}</span>`:'';
    // (the seals rest on the button once it is approved; before that an earlier approval's seals sit below, so no seal covers the words "Approve engraving": the new one is pressed on the button)
    const history=kind==='approved'?'':html({seals:all});
    const why=kind==='preparing'?(e.job?.state==='classify'?'The words are not read yet':'Preview not ready yet'):say(WHY[kind] || 'Back engraving');
    const reasonId='egWhy'+(++askN);
    let approve='';
    if(kind==='approved')approve=`<span class="egApproveWrap"><button type="button" class="btn sage sm egApproveButton" disabled aria-label="${say('Back engraving approved')}">Approved</button>${sealsHtml}</span>`;
    else if(ap.ok)approve=`<span class="egApproveWrap"><button type="button" class="btn sage sm egApproveButton egAsk" data-e="approve">Approve engraving</button></span>`;
    else if(ap.why)approve=`<span class="egApproveWrap"><button type="button" class="btn sm egApproveButton egAsk egOff" disabled aria-describedby="${reasonId}">Approve engraving</button></span><span class="egOffWhy" id="${reasonId}">${esc(say(ap.why))}</span>`;
    // one press for both faces of the piece shown ("Approve both faces"): offered by the host (e.both = { ok, why }) only when this piece has a Front and a Back still to approve; off, with the reason as its tip, until both placements have been looked at
    if(e.both && e.cycle?.faces && ap.ok && kind==='approve')approve+=`<button type="button" class="btn ghost sm egApproveBoth" data-e="approveBoth"${e.both.ok?'':` disabled title="${esc(e.both.why || '')}"`}>Approve both faces</button>`;
    const open=kind==='none'?'':`<button type="button" class="btn ${kind==='words'?'gold':'ghost'} sm egOpen" data-e="engrave">${done?'View in Engraving':'Fix in Engraving'} <span aria-hidden="true">→</span></button>`;
    const acts=kind==='words'?open+approve:approve+open;
    if(kind==='none')return `${compact?'':`<span class="fLabel">${say('Back engraving')}</span>`}<div class="swEng egCard${compact?' egCompact':''}" data-state="none"><div class="top">${cycle}<b>${status}</b></div></div>`;
    const preview=['approve','approved'].includes(kind)?'<div class="pv" data-engraving-preview></div>':'';
    const lbl=say(kind==='approve' || kind==='approved'?'Words on the back':'Words as read'),from=kind==='approved'?'':readFrom(e);
    const words=e.text?`<div class="egWords"><span class="egLbl">${lbl}</span><div class="words">${esc(e.text)}</div>${from?`<span class="egMeta">${esc(from)}</span>`:''}${fontMeta(e)?`<span class="egMeta egFont">${esc(fontMeta(e))}</span>`:''}</div>`
      :kind==='words' || kind==='preparing'?`<div class="egWords"><span class="egLbl">${lbl}</span><div class="words egNone">Nothing read yet</div></div>`:'';
    const sw=setupWhy(e),setupHtml=sw?`<span class="egMeta egSetup" data-engraving-setup>${esc(sw)}</span>`:'';
    const un=unclear(e),unclearHtml=un.length?`<div class="egUnclear"><span class="egLbl">${kind==='words'?'What is unclear':'Worth a look'}</span><ul>${un.map(t=>`<li>${esc(t)}</li>`).join('')}</ul></div>`:'';
    const saving=kind==='approved' && !e.note?savingNote(e.job):null,note=e.note || (saving?say(saving.note):'') || '',working=e.note?e.working:saving?.working;
    const sub=note?`<span class="by egSub">${working?'<span class="owSpin" aria-hidden="true"></span>':''}<span>${esc(note)}</span></span>`:'';
    const failKey=e.job?.key,failed=kind==='approve' && failKey?fails.get(failKey):'';if(kind!=='approve' && failKey)fails.delete(failKey);
    const forPiece=e.pieceLabel?`<span class="egFor"><b>${esc(e.pieceLabel)}</b>${e.pieceMeta?` <small>${esc(e.pieceMeta)}</small>`:''}</span>`:'';
    const pill=status?`<span class="egPill" data-s="${esc(kind)}"><i aria-hidden="true"></i>${status}</span>`:'';
    return `${compact?'':`<span class="fLabel">${say('Back engraving')}</span>`}<div class="swEng egCard${compact?' egCompact':''}" data-state="${esc(kind)}"${face==='front'?' data-face="front"':''}><div class="top egTop">${cycle}${forPiece}${pill}</div><div class="egWhy"><b>${why}</b>${sub}</div>${preview}${words}${setupHtml}${unclearHtml}<div class="acts">${acts}</div>${failed?`<div class="egFail" role="alert">${esc(failed)}</div>`:''}${history?`<div class="egHistory">${history}</div>`:''}</div>`;
  }
  /** The preview zooms and pans where it lies (charm-nest-zoompan.js, the one module for every order picture): a click zooms in on the
   *  point clicked, a drag pans, the frame keeps its size. `zoom` ({id, key}) names the place and what it shows, so a card drawn again for
   *  the same piece keeps its zoom; the picture drawn from the fitted words is drawn again larger when the zoom settles. */
  function zoomPreview(slot,zoom,job,fitted){
    const Z=root?.CNZoomPan;if(!Z||!slot)return;
    const face=faceOf(job);
    try{Z.attach(slot,{id:(zoom&&zoom.id)||'eng',key:((zoom&&zoom.key)||job?.key||'')+(fitted?'|fit':'|png'),label:aboutFace('Back engraving preview',face),maxPx:1800,
      hires:fitted?async({px})=>{const size=Math.max(900,Math.min(1800,Math.ceil(px/300)*300)),cv=root.Engrave.renderBack(job,size,{hatch:false,grid:false});cv.setAttribute('role','img');cv.setAttribute('aria-label',aboutFace('The full back engraving',face));return{el:cv,px:size};}:undefined});}catch(_){}
  }
  function wirePanel(host,e,{approve,approveBoth,open,imageUrl,zoom,stale,pick}={}){
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
    // Approve both faces: the host runs the ordinary approval of each face of the piece in turn (two approvals, two seals, two back files: as if each Approve had been pressed); the wait is said on the button, with a small spinner.
    host.querySelector('[data-e=approveBoth]')?.addEventListener('click',ev=>{
      const btn=ev.currentTarget;if(btn.disabled || !approveBoth)return;
      const was=btn.innerHTML;let run;
      try{run=approveBoth(btn);}catch(err){console.warn('engraving card: approve both',err);return;}
      if(!run || typeof run.then!=='function')return;
      btn.disabled=true;btn.setAttribute('aria-busy','true');btn.innerHTML='<span class="spin"></span>Approving both…';
      const back=()=>{if(btn.isConnected){btn.disabled=false;btn.removeAttribute('aria-busy');btn.innerHTML=was;}};
      run.then(back,err=>{console.warn('engraving card: approve both',err);back();});
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
    // The discs of a counted order, one at a time: a press on a disc's button, or Back / Next, asks the host to show that disc (pick(jobKey)); the host draws the card again for it.
    const cyc=host.querySelector('[data-cycle]'),PS=root?.CharmNestPieceSwitch;
    if(cyc && PS && pick && e.cycle){
      const cur=PS.cursor(e.cycle.pieces,e.cycle.key);
      PS.bind(cyc,k=>{if(k!==e.cycle.key)pick(k);});
      // (a line with a Front and a Back: Back / Next go piece by piece and keep the face shown; the face is picked on its own switch)
      const go=dir=>{const n=e.cycle.faces?neighbour(e.cycle,dir):(dir<0?cur.prev:cur.next);if(n && n.key!==e.cycle.key)pick(n.key);};
      cyc.querySelector('[data-a=discPrev]')?.addEventListener('click',()=>go(-1));
      cyc.querySelector('[data-a=discNext]')?.addEventListener('click',()=>go(1));
    }
    const slot=host.querySelector('[data-engraving-preview]');if(!slot)return;
    const job=e.job,img=e.back && (e.back.outputs?.png?.url || e.back.png || e.back.preview);
    // A fresh placement must use the current fit; an old approved thumbnail must not disguise edits.
    if(job?.fit && job?.view && root?.Engrave?.renderBack){try{const cv=root.Engrave.renderBack(job,600,{hatch:false,grid:false});cv.setAttribute('role','img');cv.setAttribute('aria-label',aboutFace('The full back engraving',faceOf(job)));slot.replaceChildren(cv);zoomPreview(slot,zoom,job,true);return;}catch(_){}}
    if(img){const im=root.document.createElement('img');im.alt=aboutFace('The full approved back engraving',faceOf(job));im.crossOrigin='anonymous';im.src=imageUrl?imageUrl(img):img;slot.replaceChildren(im);zoomPreview(slot,zoom,job,false);return;}
    slot.classList.add('egPvNone');slot.innerHTML=`<span class="by">${aboutFace('No preview here: see the back in Engraving',faceOf(job))}</span>`;
  }
  async function press(button,stamp){
    if(!button || !root?.Seal)return;
    let wrap=button.closest('.egApproveWrap');if(!wrap){wrap=root.document.createElement('span');wrap.className='egApproveWrap';button.before(wrap);wrap.append(button);}
    let row=wrap.querySelector('.sealRow');if(!row){row=root.document.createElement('span');row.className='sealRow egButtonSeal';wrap.append(row);}
    row.dataset.sealGroup='';
    const wasDisabled=button.disabled,wasBusy=button.getAttribute('aria-busy');
    let seal=[...row.querySelectorAll('.seal')].find(s=>{try{const m=JSON.parse(s.querySelector('svg')?.getAttribute('data-seal-model'));return +m.at===+stamp.at && m.by===String(stamp.by || '') && m.action===(stamp.how==='engravePlain'?faceOf(stamp)==='front'?'FRONT CUT PLAIN':'CUT PLAIN':faceOf(stamp)==='front'?'FRONT ENGRAVING':'BACK ENGRAVING');}catch(_){return false;}});
    if(seal && !seal.classList.contains('pending'))return;
    if(!seal){const holder=root.document.createElement('span');holder.innerHTML=root.Seal.html(stamp,regularSize(),'engravingSeal pending');seal=holder.firstElementChild;row.append(seal);}
    row.dataset.sealCount=String(row.querySelectorAll('.seal').length);root.Seal.fit?.(row);button.disabled=true;button.textContent='Approved';button.setAttribute('aria-busy','true');
    try{await root.Seal.press(seal);}finally{button.disabled=wasDisabled;if(wasBusy==null)button.removeAttribute('aria-busy');else button.setAttribute('aria-busy',wasBusy);}
  }
  return {merge,list,keep,add,html,press,record,panel,wirePanel,fromEvents,who,busyLabel,stamped};
});
