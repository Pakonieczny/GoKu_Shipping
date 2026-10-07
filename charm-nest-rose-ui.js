/* Cut Sheet for the metals that have a green line (charm-nest-rose.js CharmNestRose.cuts(): Rose Gold, 10K Gold, 14K Gold): physical stock,
   vector-derived cut plans and immutable cut history. The three metals share every rule here; only these differ:
   - Rose Gold takes a physical sheet at the start of every nest and joins a set only by its own Cut Sheet press;
   - 10K and 14K take one only when a leftover of their metal and size fits at nest (nestClaim), or when the sheet is in the set and still
     needs a line (claimLate), or at Cut Sheet; they join a set by their own Include switch, and Cut Sheet on a held sheet includes it first
     (Gate.cutInclude). So a gold sheet keeps its size editable (Options, Merge sheets) until it holds a physical sheet. */
(function init(){
  'use strict';
  if(!window.CN){setTimeout(init,150);return;}
  const R=window.CharmNestRose,C=window.CN,S=C.S,esc=C.esc;
  const hasLine=sh=>!!sh&&R.cuts(sh.metal),word=sh=>R.cutWord(sh&&sh.metal),isRose=sh=>!!sh&&sh.metal==='rose';
  const api=(op,data={})=>C.api('charmNestLibrary',{op,...data},{label:R.cutWord(data.metal||'rose')+' stock'});
  const fingerprint=s=>JSON.stringify((s.placements||[]).map(p=>[p.id,+p.cxPt.toFixed(3),+p.cyPt.toFixed(3),p.angle,p.scale||1,p.hash||s.charms?.find(c=>c.id===p.id)?.hash||null]));
  const refresh=sh=>{window.Session?.schedule();if(sh.el){C.renderCard(sh);C.drawPreview(sh);}};
  const parse=s=>s?JSON.parse(s):null;
  const decode=cuts=>cuts.map(c=>({...c,plan:parse(c.planJson)}));
  const when=at=>Number.isFinite(at)?new Date(at).toLocaleString(undefined,{year:'numeric',month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):null;
  const stamp=at=>Number.isFinite(at)?`<time datetime="${new Date(at).toISOString()}">${esc(when(at))}</time>`:'<span class="roseUndated">Time not recorded</span>';
  const count=list=>{const n=list?.length||0;return n+' charm'+(n===1?'':'s');};
  async function load(sh,older=false){
    const stockId=sh.roseStock?.id||sh.recalled?.roseStockId;if(!stockId)return;
    const result=await api('roseGet',{stockId,metal:sh.metal,...(older?{before:Math.min(...sh.roseHistory.map(c=>c.revision))}:{})});
    // An already cut layout can display the full physical history. An active
    // reservation must keep its original revision until prepare checks it.
    if(!sh.roseStock||sh.roseCutAt)sh.roseStock=result.stock;
    sh.roseHistory=older?[...(sh.roseHistory||[]),...decode(result.cuts)]:decode(result.cuts);
    sh.roseMore=result.more;sh._roseLoaded=true;
    const completed=sh.roseHistory.find(c=>c.sheetId===sh.sheetId);if(completed){sh.roseCutAt=completed.at;sh.roseStock=result.stock;}
    if(sh.recalled){const rec=(await api('getSheet',{id:sh.sheetId})).sheet;sh.roseCutAt=rec.roseCutAt||null;sh.rosePlan=parse(rec.rosePlanJson);sh.rosePlanHash=rec.rosePlanHash;sh.roseRevision=rec.roseRevision;sh.roseProtected=parse(rec.roseProtectedJson);}
    refresh(sh);
  }
  async function restore(sh,rec){
    const result=await api('roseGet',{stockId:rec.roseStockId,metal:sh.metal});
    sh.roseStock=result.stock;sh.roseHistory=decode(result.cuts);sh.roseMore=result.more;sh.roseCutAt=rec.roseCutAt||null;sh.roseRevision=rec.roseRevision;sh.rosePlan=parse(rec.rosePlanJson);sh.rosePlanHash=rec.rosePlanHash;sh.roseProtected=parse(rec.roseProtectedJson);sh._roseLoaded=true;
  }
  function protect(sh){
    if(!hasLine(sh)||sh.roseCutAt)return;
    if(sh.roseProtected)for(const p of sh.roseProtected.placements||[]){
      const rows=sh.placements.filter(x=>x.id===p.id),q=rows[0];
      if(rows.length!==1||!sh.charms.some(c=>c.id===p.id)||['cxPt','cyPt','angle'].some(k=>!Number.isFinite(q[k])||Math.abs(q[k]-p[k])>.001)||Math.abs((q.scale||1)-(p.scale||1))>.00001)throw new Error(`Reload the saved ${word(sh)} layout before adding charms`);
    }
    if(sh.rosePlan?.profile){
      const ids=new Set(sh.rosePlan.shapes.map(s=>s.id));
      const placements=sh.placements.filter(p=>ids.has(p.id));
      if(placements.length!==ids.size)throw new Error(`Reload the saved ${word(sh)} layout before adding charms`);
      sh.roseProtected={profile:sh.rosePlan.profile,lines:sh.rosePlan.lines,placements:placements.map(p=>({...p})),shapes:sh.rosePlan.shapes,stages:sh.rosePlan.stages};
    }
  }
  /* Pieces leave an uncut Rose Gold sheet (a cancelled order's, or taken off by hand: Paul, 29 Sep, "I cancelled the 2 orders
     ... none of them disappears from the sheet"). The saved green lines they are inside give them up first, on the server
     (roseTakeOff): a line with a piece left stays exactly as saved, a line with nothing left inside it goes, and this page
     takes its copy of the lines from the answer, so protect() and the save that follows see the sheet as the cloud has it.
     Nothing is drawn or added: only Cut Sheet draws a line. Resolves the answer ({changed, removedLines, keptLines, exact}),
     or null when the sheet has no saved record or is cut; rejects when the cloud could not be asked (nothing is changed then).
     o: by, at (when), cancel (it is a cancel's). */
  async function takeOff(sh,ids,o={}){
    if(!hasLine(sh)||sh.roseCutAt||!sh.sheetId||!ids||!ids.length)return null;
    if(!S.cloud.ok)throw new Error(`Reconnect to take the pieces off the ${word(sh)} sheet`);
    const r=await C.api('charmNestLibrary',{op:'roseTakeOff',sheetId:sh.sheetId,ids:[...ids],by:o.by||undefined,at:o.at||undefined,cancel:!!o.cancel,allowanceMm:sh.roseAllowanceMm||undefined},{quiet:true});
    if(r&&r.changed){
      if(r.protectedJson)sh.roseProtected=parse(r.protectedJson);else delete sh.roseProtected;
      delete sh.rosePlan;delete sh.rosePlanHash;delete sh.rosePlanKey;sh._roseFullKey=null;sh._roseError=null;
      window.Session?.schedule();
    }
    return r||null;
  }
  async function prepare(sh,opts={}){
    if(!hasLine(sh))return;
    if(sh.roseCutAt)throw new Error('This layout has already been cut');
    if(!S.cloud.ok)throw new Error(`Reconnect to load the physical ${word(sh)} sheet before nesting`);
    sh.sheetId ||= (isRose(sh)?'rose':sh.metal)+'-'+Date.now().toString(36)+'-'+C.uid();
    protect(sh);
    const st=C.stockFor(sh.metal,sh);
    const r=await api('roseClaim',{sheetId:sh.sheetId,metal:sh.metal,wPt:st.wPt,hPt:st.hPt,stockId:sh.roseStock?.id||sh.roseChoice,revision:sh.roseStock?.revision,fresh:!!sh.roseFresh,...opts});
    if(!r.stock)return;   // (onlyRemnant: no leftover fits, so nothing was claimed or written)
    sh.roseStock=r.stock;sh.roseRevision=r.stock.revision;
    if(r.protectedJson){
      const guard=parse(r.protectedJson);
      for(const p of guard.placements){const q=sh.placements.find(x=>x.id===p.id);if(!q||['cxPt','cyPt','angle'].some(k=>Math.abs(q[k]-p[k])>.001)||Math.abs((q.scale||1)-(p.scale||1))>.00001)throw new Error(`Reload the saved ${word(sh)} layout before adding charms`);}
      sh.roseProtected=guard;
      if(opts.nesting){delete sh.rosePlan;delete sh.rosePlanHash;delete sh.rosePlanKey;}
    }
    await load(sh);window.Session?.schedule();
  }
  // The start of a nest. Rose Gold takes its physical sheet here, always. 10K and 14K take one only when a leftover of their metal and
  // size fits (it was cut before: the layout must go around what is gone) or when the sheet already holds one; otherwise nothing is
  // claimed or written, and the size stays the person's to change.
  async function nestClaim(sh){
    if(!hasLine(sh)||sh.roseCutAt)return;
    const PN=window.PartialNest;
    // A page of a partial-sheet chain was made with its leftover as its stock (charm-nest-partial-nest.js nextPage): it claims exactly that one.
    // When another sheet took it meanwhile, the page says so and goes on by the metal's own rule below.
    if(PN&&PN.pending&&PN.pending(sh)){try{await prepare(sh,{nesting:true,exact:true,partialId:sh._partialId});PN.claimed(sh);return;}catch(e){PN.lost(sh,e);}}
    // The metal's policy (Options, Partial Sheet): 'new' = never take a partial by itself. Rose Gold takes a brand new physical sheet; 10K and 14K
    // take none at nest, as GC1 built it; a sheet never nested takes the size the person stipulated. 'auto' (the default) is everything below.
    if(PN&&PN.mode&&PN.mode(sh.metal)==='new'&&!sh.roseStock&&!sh.roseChoice&&!sh.roseProtected&&!sh.rosePlan){
      PN.newSheet(sh);
      return isRose(sh)?prepare(sh,{nesting:true,fresh:true}):undefined;
    }
    if(isRose(sh)||sh.roseStock||sh.roseChoice||sh.roseProtected||sh.rosePlan)return prepare(sh,{nesting:true});
    return prepare(sh,{nesting:true,onlyRemnant:true});
  }
  // Partial sheets (charm-nest-partial-nest.js): a sheet in use moves onto the leftover the person chose. With swap the server gives back the physical
  // sheet the sheet holds (a fresh uncut gold sheet is deleted, a leftover returns to the list) and reserves the chosen leftover in ONE transaction
  // (roseClaim with the leftover's id and revision, exact + partialId: the chosen partial is checked itself; nesting:true marks the saved record changed,
  // as any re-nest does). A refused claim loses nothing: the sheet still holds what it held. A sheet in a set, with a saved green line or a recorded
  // cut is refused by the server.
  async function seatOn(sh,stock,o={}){
    if(!hasLine(sh)||sh.roseCutAt)throw new Error('This sheet cannot take a partial sheet');
    if(!S.cloud.ok)throw new Error(`Reconnect to reserve the ${word(sh)} partial sheet`);
    sh.sheetId ||= (isRose(sh)?'rose':sh.metal)+'-'+Date.now().toString(36)+'-'+C.uid();
    // exact + partialId: the server checks the chosen partial sheet itself (available or this sheet's, same metal, same revision) and refuses to keep or create another
    const r=await api('roseClaim',{sheetId:sh.sheetId,metal:sh.metal,wPt:stock.wPt,hPt:stock.hPt,stockId:stock.id,revision:stock.revision,nesting:true,...(stock.partialId?{exact:true,partialId:stock.partialId}:{}),...(o.swap?{swap:true}:{})});
    if(!r.stock)throw new Error('The partial sheet could not be reserved');
    sh.roseStock=r.stock;sh.roseRevision=r.stock.revision;sh.roseChoice=null;sh.roseFresh=false;sh.roseHistory=[];sh._roseLoaded=false;sh._roseFullKey=null;
    if(r.protectedJson)sh.roseProtected=parse(r.protectedJson);   // (the saved record already held green lines: the sheet stays as it is; the caller sees roseProtected)
    try{await load(sh);}catch(_){}   // (the cut history is only for the timeline: the reservation stands without it)
    window.Session?.schedule();refresh(sh);return r.stock;
  }
  // A 10K or 14K sheet that is in the set, has charms outside any line and room for one holds a physical sheet from then on, so the set
  // waits for its Cut Sheet exactly as a Rose Gold set does (the saved record then names its stock: CharmNestReadiness). A full sheet
  // takes the rest of the metal whole and claims nothing. Never adds a line.
  // A sheet already approved for Laser cutting, in a committed set or with the laser is never claimed late (Paul's 14K Sheet 1: a claim here made
  // readiness hold it for "Cut Sheet" and dropped an approved sheet out of Laser cutting). A sheet not yet approved is still claimed. Rose Gold
  // never came here (isRose below), so it keeps today's behaviour exactly.
  const approved=sh=>{
    const sets=(window.Sets&&window.Sets.ofRun&&window.Sets.ofRun(sh.runId))||[];
    return !!(+sh.laserDoneAt>0||sh.laserSetPending||sh.processReady||sets.some(s=>(s.sheetIds||[]).includes(sh.sheetId)&&(s.committedAt||s.processReady||+s.laserDoneAt>0||s.laserSetPending)));
  };
  async function claimLate(sh){
    if(isRose(sh)||!hasLine(sh)||sh.roseStock||sh.roseCutAt||sh.recalled||!inSet(sh)||approved(sh)||sh._roseClaiming||!sh.sheetId||!sh.persistedDone||!sh.verification?.ok||sh.dirty||!sh.placements.length||['nesting','finishing','queued'].includes(sh.status)||!S.cloud.ok)return;
    sh._roseClaiming=true;sh._roseStep='claim';sh._roseError=null;refresh(sh);
    try{await prepare(sh,{fresh:true});}catch(e){sh._roseError=e.message;}
    finally{sh._roseClaiming=false;sh._roseStep=null;refresh(sh);}
  }
  // Taken out of the set before any line was drawn: a 10K or 14K sheet nobody cut lets go of its fresh physical sheet (the server deletes
  // a stock with no cut), so its size can be changed again. A leftover it was nested on, a planned or protected sheet and a cut one stay.
  async function letGo(sh){
    if(isRose(sh)||!hasLine(sh)||!sh.roseStock?.id||sh.roseCutAt||sh.recalled||sh.rosePlan||sh.roseProtected||sh.roseStock.revision||sh.roseStock.profileJson||!sh.sheetId||inSet(sh)||sh._roseAction||sh._rosePlanning||!S.cloud.ok)return false;
    try{await api('roseRelease',{stockId:sh.roseStock.id,sheetId:sh.sheetId,metal:sh.metal});}catch(_){return false;}
    delete sh.roseStock;delete sh.roseRevision;delete sh.roseChoice;delete sh.rosePlanHash;delete sh.rosePlanKey;sh.roseHistory=[];sh._roseLoaded=false;sh._roseFullKey=null;
    window.Session?.schedule();refresh(sh);return true;
  }
  // Only a Cut Sheet press adds a green line (Paul, 29 Sep: Send to Sheet drew line 2 by itself). The contour is also
  // made again when a sheet joins a set, is saved, is nested or its allowance changes: that may redraw the lines it has
  // or take a full sheet whole, never add one. Charms past the last line stay uncut until the next press.
  async function plan(sh,opts={}){
    if(!opts.cut&&addsLine(sh))return claimLate(sh);
    if(!opts.cut&&!isRose(sh)&&!sh.roseStock)return;   // (10K, 14K: a sheet nobody asked a line for holds no physical sheet and saves no contour)
    if(sh._rosePlanning)return sh._rosePlanning;
    const task=(async()=>{
      if(sh.roseCutAt)return;
      if(!sh.persistedDone||!sh.verification?.ok||sh.dirty||!sh.placements.length)throw new Error('Nest and save this layout before preparing its contour');
      const key=fingerprint(sh);
      sh._roseError=null;
      if(!sh.roseStock)await prepare(sh,{fresh:true});
      const shapes=window.CharmNestBackground ? await CharmNestBackground.run('roseShapes',{charms:sh.charms.map(c=>({id:c.id,outline:c.outline,members:c.members,centerPt:c.centerPt,bbox:c.bbox})),placements:sh.placements}) : R.shapes(sh.charms,sh.placements);
      const result=await api('rosePlan',{sheetId:sh.sheetId,metal:sh.metal,stockId:sh.roseStock.id,revision:sh.roseStock.revision,fingerprint:key,shapesJson:JSON.stringify(shapes),allowanceMm:sh.roseAllowanceMm||.2,...(opts.cut?{cut:true}:{})});
      if(fingerprint(sh)!==key||sh.dirty)throw new Error('Layout changed while saving its contour');
      sh.rosePlan=parse(result.planJson);sh.rosePlanHash=result.planHash;sh.rosePlanKey=key;sh.roseRevision=sh.roseStock.revision;
      window.SheetEvents?.roseLines(sh);   // a new green line: on the timelines of the orders it covers (idle time)
    })();
    sh._rosePlanning=task;refresh(sh);
    try{await task;}catch(e){sh._roseError=e.message;throw e;}finally{sh._rosePlanning=null;refresh(sh);C.flushManualIntake?.(sh.metal);}
  }
  // Cut Sheet: one press draws this layout's green line with today's date and
  // records the cut. Its charms turn grey, and the next charms nest past the
  // line on the same physical sheet until it is full.
  async function record(sh,o={}){   // o.by: the person who pressed (charm-nest-flow-rose.js passes it); else the signed-in one
    // A held sheet joins the current set first (Paul, 25 Sep: "the Cut Sheet button is not working"): it was greyed
    // until the sheet was ticked in Options, with only a hover note to say so. A cut sheet stays in its set, and its
    // charms must be made, so pressing Cut Sheet is taken as including it.
    if(!inSet(sh)){
      const G=window.Gate;if(!G?.changeMembership)throw new Error('Sets are not loaded yet · try again in a moment');
      sh._roseStep='include';refresh(sh);
      // (Rose Gold joins by its metal's tick; a 10K or 14K sheet by its own Include, which keeps the rule of orders that span two sheets)
      try{if(isRose(sh))await G.changeMembership('rose',true);else if(G.cutInclude)await G.cutInclude(sh);else await G.changeMembership(sh.metal,true,sh);}finally{sh._roseStep=null;}
      if(!inSet(sh))throw new Error('Not cut: '+(G.policy?.(sh)?.reason||'this sheet is not in a set yet'));
      refresh(sh);
    }
    if(!sh.rosePlanHash||(!sh.recalled&&sh.rosePlanKey!==fingerprint(sh)))await plan(sh,{cut:true});
    // who cut it: the sorter's signed-in person (its own name, or the Design Station's sign-in); nobody is sent as ""
    // and the server keeps 'operator' in the stock ledger only, the order's roseCut then says "not signed in"
    let who='';try{who=String(o.by||window.CNEmployee?.name?.()||window.B?.employee||'').trim();}catch(_){who=String(o.by||window.B?.employee||'').trim();}
    const r=await api('roseRecordCut',{sheetId:sh.sheetId,metal:sh.metal,stockId:sh.roseStock.id,revision:sh.roseRevision,planHash:sh.rosePlanHash,by:who,device:'charm-nest-1',via:o.via||(S.mode==='library'?'library':'nest')});   // via: which tab pressed it, saved on the leftover sheet this cut makes (_charmNestRemnants.js)
    sh.roseCutAt=r.cut.at;sh.roseStock=r.stock;sh.roseHistory=[...decode([r.cut]),...(sh.roseHistory||[]).filter(c=>c.sheetId!==sh.sheetId)];
    refresh(sh);C.toast(`Sheet cut · the next ${word(sh)} charms nest past this green line`,'ok');
    // the efficiency record (station-activity.js, through charm-nest-laser-act.js): the person pressed Cut Sheet and the cut is recorded;
    // the pieces are the sheet's charms, one event for each order on it. Cutting is the Laser station's work, not the sorter's.
    try{window.CNLaserAct&&window.CNLaserAct.rose(sh);}catch(_){}
    try{window.PartialSheetsUI&&window.PartialSheetsUI.changed();}catch(_){}   // the partial sheet this cut saved: an open Partial Sheet panel reads its list again (charm-nest-partial-ui.js)
  }
  const inSet=sh=>!!sh.setId&&!sh.draft;
  const observed=new WeakMap();
  const observer=window.IntersectionObserver?new IntersectionObserver(entries=>{for(const e of entries)if(e.isIntersecting){observer.unobserve(e.target);const sh=observed.get(e.target);if(sh&&!sh._roseLoaded&&!sh._roseLoading){sh._roseLoading=true;load(sh).catch(error=>{sh._roseError=error.message;}).finally(()=>{sh._roseLoading=false;refresh(sh);});}}},{rootMargin:'120px'}):null;
  function render(sh){
    if(!hasLine(sh)||!sh.el)return;
    let host=sh.el.querySelector('.roseHistory');if(!host){host=document.createElement('div');host.className='roseHistory';sh.el.querySelector('.shPreviewWrap').before(host);}
    const stock=sh.roseStock,history=(sh.roseHistory||[]).slice().sort((a,b)=>a.revision-b.revision),busy=!!(sh._rosePlanning||sh._roseLoading||sh._roseAction);
    const stockId=stock?.id||sh.recalled?.roseStockId;
    const ready=!sh.roseCutAt&&!sh.recalled&&sh.persistedDone&&sh.verification?.ok&&!sh.dirty&&sh.placements.length&&!['nesting','finishing','queued'].includes(sh.status);
    const included=!!sh.setId&&!sh.draft;
    // A layout that leaves no room for another charm takes the rest of the
    // sheet, so there is no new green line to cut.
    const full=!!ready&&sheetFull(sh);
    // One action only. A full or nearly full sheet has no room for another
    // green line, so it offers nothing.
    const cuttable=!sh.roseCutAt&&(ready?!full:!!(sh.recalled?.roseStockId&&sh.rosePlan&&!sh.rosePlan.full));
    const marks=timeline(sh,history);
    host.hidden=!stockId&&!ready;
    host.classList.toggle('roseHasTimeline',!!marks);
    // The timeline comes last so it sits directly on the sheet's ruler.
    // the busy line names the step under way (it read "Loading sheet geometry…" while a cut was being recorded)
    const busyWord=sh._roseStep==='include'?'Adding the sheet to the set…':sh._roseStep==='claim'?'Reserving the physical sheet…':sh._rosePlanning?'Planning the green line…':sh._roseAction?'Recording the cut…':'Loading sheet geometry…';
    // Cut Sheet sits in the card's control row, beside the sheet tabs and Options: on a line of its own it pushed the
    // Rose Gold sheet a row below the sheets beside it (audit, 25 Sep). This panel keeps the step under way, a failure
    // and the dated green lines, and takes no room when it has none of them.
    let slot=sh.el.querySelector('.roseCut');
    if(!slot){slot=document.createElement('div');slot.className='roseCut';const row=sh.el.querySelector('.shControls');if(row)row.insertBefore(slot,row.querySelector('.shGate')||row.querySelector('.engTogHost'));else host.before(slot);}
    slot.hidden=!cuttable;
    slot.innerHTML=cuttable?`<button class="btn ghost xs" data-rose="cut" ${busy?'disabled':''}>Cut Sheet</button>`:'';
    host.innerHTML=`${busy?`<div class="help" role="status"><i class="spin"></i> ${busyWord}</div>`:''}${sh._roseError?`<p class="roseError" role="alert">${esc(sh._roseError)}</p>`:''}${marks}`;
    host.classList.toggle('roseEmpty',!host.innerHTML);
    // a failure is shown once, where it happened (the alert under the button); a pop-up used to repeat it
    const invoke=fn=>async()=>{if(sh._roseAction)return;sh._roseAction=true;sh._roseError=null;refresh(sh);try{await fn();}catch(e){sh._roseError=e.message;}finally{sh._roseAction=false;refresh(sh);C.flushManualIntake?.(sh.metal);}};
    slot.querySelector('[data-rose="cut"]')?.addEventListener('click',invoke(()=>record(sh)));
    const menu=sh.el.querySelector('.solidOptions');
    if(menu&&!menu.querySelector('[data-rose-options]')){
      const options=document.createElement('div');options.dataset.roseOptions='';options.className='roseStockOptions';
      options.innerHTML=`<h4>Cut contour</h4><label class="roseAllowance">Contour allowance <span>mm</span><input type="number" min="0.05" max="2" step="0.05" value="${sh.roseAllowanceMm||.2}" data-rose-allowance></label><p class="help">Space around the combined charm outline.</p><div data-rose-stock-choice><button type="button" class="btn ghost xs" data-rose-choose>Choose remnant or new sheet</button><span data-rose-picker></span></div>`;
      menu.append(options);
      options.querySelector('[data-rose-choose]')?.addEventListener('click',async e=>{
        e.target.disabled=true;try{const result=await api('roseList',{metal:sh.metal}),st=C.stockFor(sh.metal),pick=options.querySelector('[data-rose-picker]');
          const stocks=result.stocks.filter(s=>Math.abs(s.wPt-st.wPt)<.01&&Math.abs(s.hPt-st.hPt)<.01);
          pick.innerHTML=`<select aria-label="Physical ${esc(word(sh))} sheet">`+'<option value="">Use available remnant first</option><option value="new">New uncut sheet</option>'+stocks.map(s=>'<option value="'+esc(s.id)+'">'+esc(s.id.slice(-8).toUpperCase())+' · '+s.revision+' cuts · '+Math.round((s.wPt*s.hPt-(s.profileJson?R.area(parse(s.profileJson)):0))*R.MM*R.MM)+' mm² left</option>').join('')+'</select>';
          const select=pick.querySelector('select');select.value=sh.roseFresh?'new':sh.roseChoice||'';select.onchange=()=>{sh.roseFresh=select.value==='new';sh.roseChoice=sh.roseFresh?null:select.value||null;window.Session?.schedule();};
        }catch(error){C.toast(error.message,'bad');}finally{e.target.disabled=false;}
      });
    }
    const options=menu?.querySelector('[data-rose-options]');
    if(options){
      const input=options.querySelector('[data-rose-allowance]');
      input.disabled=!!(sh.roseCutAt||sh.recalled||busy);
      if(input!==document.activeElement&&!input._draft)input.value=sh.roseAllowanceMm||.2;
      input.oninput=()=>{input._draft=true;};
      // a value outside 0.05 to 2 mm goes back to the one in use, and the field says why (it used to snap back in silence)
      input.onchange=e=>{const n=+e.target.value;if(!Number.isFinite(n)||n<.05||n>2){e.target.value=sh.roseAllowanceMm||.2;input._draft=false;input.title='The allowance is 0.05 to 2 mm';C.toast(`Contour allowance stays ${sh.roseAllowanceMm||.2} mm: it can be 0.05 to 2 mm`,'bad');return;}input.title='';sh.roseAllowanceMm=n;input._draft=false;delete sh.rosePlanKey;window.Session?.schedule();if(ready)invoke(()=>plan(sh))();else refresh(sh);};
      options.querySelector('[data-rose-stock-choice]').hidden=!!(stockId||sh.recalled);
    }
    if(stockId&&!sh._roseLoaded&&!sh._roseLoading){observed.set(host,sh);if(observer)observer.observe(host);}
    if(ready&&included&&!busy&&!sh._roseError&&sh.rosePlanKey!==fingerprint(sh))queueMicrotask(()=>plan(sh).catch(()=>{}));
    const wait=waiting(sh);if(wait!==(sh._roseWait||0)){sh._roseWait=wait;window.RunCtl?.renderBanner?.();}   // the run's pill names it
  }
  // Lines saved before dates were kept show as one undated line, as the server reads them.
  const stagesOf=plan=>plan?.stages||(plan?.lines?.length?[{n:1,at:null,ids:(plan.shapes||plan.placements||[]).map(s=>s.id),lines:[0,plan.lines.length]}]:[]);
  // The green lines drawn now: the saved contour, or the protected lines while new charms are nested.
  function activeLines(sh){if(sh.roseCutAt)return null;return sh.rosePlan&&!sh.dirty?sh.rosePlan:sh.roseProtected||null;}
  // Uncut lines as shown: a saved line that ran along the sheet's edge is
  // tidied as the server tidies it for the next plan. Recorded cuts stay as cut.
  const views=new WeakMap();
  function shown(plan,sh){
    let view=views.get(plan);
    if(!view){const st=plan.profile||C.stockFor(sh.metal,sh);view=R.tidy(plan.lines||[],stagesOf(plan),st.wPt,st.hPt,plan.shapes,Math.max(.2,sh.roseAllowanceMm||.2)/R.MM);views.set(plan,view);}
    return view;
  }
  // Whether this layout leaves no room for another charm. Until its contour is
  // saved this is worked out here, once per layout, with the server's geometry.
  function sheetFull(sh){
    if(sh.rosePlan)return !!sh.rosePlan.full;
    const fixed=new Set((sh.roseProtected?.placements||[]).map(p=>p.id)),fresh=sh.placements.filter(p=>!fixed.has(p.id));
    if(!fresh.length)return false;
    const key=[fingerprint(sh),sh.roseStock?.id,sh.roseStock?.revision,fixed.size,sh.roseAllowanceMm||.2].join('|');
    if(sh._roseFullKey!==key){
      sh._roseFullKey=key;
      try{const st=C.stockFor(sh.metal,sh);sh._roseFull=!!R.plan(R.shapes(sh.charms.map(c=>({...c,members:[]})),fresh),st.wPt,st.hPt,sh.roseProtected?.profile||parse(sh.roseStock?.profileJson),sh.roseAllowanceMm||.2).full;}
      catch(_){sh._roseFull=false;}
    }
    return sh._roseFull;
  }
  // Whether a contour made now would add a green line: a charm outside every line the sheet has, and room left past it.
  function addsLine(sh){return unlined(sh).length>0&&!sheetFull(sh);}
  function unlined(sh){
    const lined=new Set([...(sh.roseProtected?.placements||[]).map(p=>p.id),...(sh.rosePlan&&!sh.dirty?sh.rosePlan.shapes||[]:[]).map(s=>s.id)]);
    return (sh.placements||[]).filter(p=>!lined.has(p.id));
  }
  // A sheet in the set whose charms past its last line wait for Cut Sheet: its set waits with it (no contour saved, as
  // CharmNestReadiness reads it), and says so by name; it read only "…layout checks…". How many charms wait; 0 while it
  // nests, or when it is full and plans by itself.
  function waiting(sh){
    if(!hasLine(sh)||!inSet(sh)||sh.roseCutAt||sh.recalled||!sh.roseStock?.id||sh.rosePlanHash||!sh.persistedDone||!sh.verification?.ok||sh.dirty||['nesting','finishing','queued'].includes(sh.status)||!addsLine(sh))return 0;
    return unlined(sh).length;
  }
  const waitWords=sh=>{const n=waiting(sh);return n?`${word(sh)} Sheet ${sh.page||1} has ${n} charm${n===1?'':'s'} not cut yet: press Cut Sheet`:'';};
  // The words lead to the button: the sheet on its card, the card rung, Cut Sheet focused.
  function showCut(sh){
    C.setMode('nest');const i=C.pagesOf(sh.metal).indexOf(sh);if(i>=0&&!sh.el)C.showPage(sh.metal,i);
    requestAnimationFrame(()=>{const card=sh.el;if(!card)return;card.scrollIntoView({block:'nearest',behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'auto':'smooth'});window.ringCard?.(card);card.querySelector('[data-rose="cut"]')?.focus({preventScroll:true});});
  }
  const length=path=>path.slice(1).reduce((n,p,i)=>n+Math.hypot(p[0]-path[i][0],p[1]-path[i][1]),0);
  function midpoint(path){let left=length(path)/2;for(let i=1;i<path.length;i++){const a=path[i-1],b=path[i],d=Math.hypot(b[0]-a[0],b[1]-a[1]);if(d>0&&d>=left)return [a[0]+(b[0]-a[0])*left/d,a[1]+(b[1]-a[1])*left/d];left-=d;}return path[0];}
  // Number each green line where it runs, matching its timeline entry.
  function numberLines(ctx,view,k){
    for(const s of view.stages){
      const paths=view.lines.slice(s.lines[0],s.lines[1]).filter(p=>p.length);if(!paths.length)continue;
      const [x,y]=midpoint(paths.reduce((a,b)=>length(b)>length(a)?b:a));
      badge(ctx,x*k,y*k,s.n,'#008974','#fff');
    }
  }
  // Where a line meets the top of the sheet, so its mark sits right above it.
  // A line running across the sheet is marked at its middle instead.
  function anchor(view,s,axis){
    const paths=view.lines.slice(s.lines[0],s.lines[1]).filter(p=>p.length);if(!paths.length)return null;
    if(axis==='y')return midpoint(paths.reduce((a,b)=>length(b)>length(a)?b:a))[0];
    const points=paths.flat(),top=Math.min(...points.map(p=>p[1]));
    return Math.max(...points.filter(p=>p[1]<=top+.5).map(p=>p[0]));
  }
  // The timeline above the sheet: a numbered mark and time over each green
  // line, and over each recorded cut, oldest first. Marks too close to share
  // a row move to the next row; each keeps a tick on the ruler at its line.
  const ROW=20;
  function timeline(sh,history){
    const st=C.stockFor(sh.metal,sh),marks=[];
    const add=(kind,n,at,x,about)=>{if(Number.isFinite(x))marks.push({kind,n,at,x,about});};
    for(const c of history){
      const plan=c.plan;if(!plan)continue;
      const view={lines:plan.lines||[],stages:plan.stages||[]};
      for(const s of view.stages)add('cutLine',s.n,s.at,anchor(view,s,plan.profile?.axis),`Green line ${s.n} · ${count(s.ids)} · cut ${c.revision}`);
      const pts=plan.shapes?.[0]?.paths?.[0];
      add('cut',c.revision,c.at,pts?.length?pts.reduce((n,p)=>n+p[0],0)/pts.length:null,`Cut ${c.revision} · ${count(plan.shapes)}`);
    }
    const active=activeLines(sh);
    if(active){const view=shown(active,sh);for(const s of view.stages)add('line',s.n,s.at,anchor(view,s,active.profile?.axis),`Green line ${s.n} · ${count(s.ids)}`);}
    if(!marks.length)return '';
    // Positions follow the preview's ruler, so each mark lines up with the sheet.
    const width=sh.el.querySelector('.shPreviewWrap')?.clientWidth||0,dpr=window.devicePixelRatio||1,px=Math.round(width*dpr),ruler=px?Math.round(Math.max(14*dpr,px*.042))/px:.042,span=width||600,rows=[];
    for(const m of [...marks].sort((a,b)=>a.x-b.x)){
      m.left=(ruler+(1-ruler)*Math.min(1,Math.max(0,m.x/(window.trueFrame?.(st)?.fw||st.wPt))))*100;   // the card draws the sheet at true scale in its frame
      const at=m.left/100*span,size=20+((m.kind==='cut'?'Cut ':'')+(when(m.at)||'Time not recorded')).length*5.6;
      m.flip=at-8+size>span;
      const lo=m.flip?at+8-size:at-8,hi=m.flip?at+8:at-8+size;
      let row=rows.findIndex(end=>lo>=end+6);if(row<0){row=rows.length;rows.push(0);}
      rows[row]=hi;m.row=row;
    }
    // Each mark keeps its time order in the list; a tick runs from it down to
    // the ruler, behind any label on a lower row.
    return `<div class="roseLineTimeline" role="list" aria-label="Green line and cut history" style="height:${rows.length*ROW+4}px">${marks.map(m=>`<div class="roseMark ${m.kind}${m.flip?' flip':''}" role="listitem" title="${esc(m.about)}" style="left:${m.left.toFixed(3)}%;top:${m.row*ROW}px"><span class="${m.kind==='cut'?'roseCutNumber':'roseLineNumber'}">${m.n}</span><span class="roseWhen">${m.kind==='cut'?'Cut ':''}${stamp(m.at)}</span></div><i class="roseTick ${m.kind}" aria-hidden="true" style="left:${m.left.toFixed(3)}%;top:${m.row*ROW+16}px"></i>`).join('')}</div>`;
  }
  // A numbered dot that stays the same size on screen at any pixel density.
  function badge(ctx,x,y,text,fill,ink){const d=window.devicePixelRatio||1;ctx.fillStyle=fill;ctx.beginPath();ctx.arc(x,y,7*d,0,Math.PI*2);ctx.fill();ctx.fillStyle=ink;ctx.font=`${10*d}px sans-serif`;ctx.textAlign='center';ctx.fillText(text,x,y+3*d);}
  function stroke(ctx,paths,k,color,width,dashed=false){ctx.strokeStyle=color;ctx.lineWidth=width;ctx.setLineDash(dashed?[4,3]:[]);for(const path of paths||[]){ctx.beginPath();path.forEach(([x,y],i)=>i?ctx.lineTo(x*k,y*k):ctx.moveTo(x*k,y*k));ctx.stroke();}ctx.setLineDash([]);}
  function paint(ctx,sh,k,phase){
    if(!hasLine(sh))return;
    ctx.save();const cuts=sh.roseHistory||[];
    if(phase==='history'){
      const p=parse(sh.roseStock?.profileJson);
      if(p){ctx.fillStyle='#f0eeeb';p.values.forEach((v,i)=>{if(p.axis==='x')ctx.fillRect(0,i*p.step*k,v*k,p.step*k);else ctx.fillRect(i*p.step*k,0,p.step*k,v*k);});}
      for(const c of cuts)for(const shape of c.plan?.shapes||[]){ctx.fillStyle='#d8d5d0';for(const path of shape.paths){ctx.beginPath();path.forEach(([x,y],i)=>i?ctx.lineTo(x*k,y*k):ctx.moveTo(x*k,y*k));ctx.closePath();ctx.fill();}stroke(ctx,shape.paths,k,'#aaa59c',Math.max(.6,.12*k));stroke(ctx,shape.ink,k,'#aaa59c',Math.max(.5,.1*k));}
    }else{
      if(!sh.roseCutAt&&sh.roseProtected&&(!sh.rosePlan||sh.dirty)){const view=shown(sh.roseProtected,sh);stroke(ctx,view.lines,k,'#008974',Math.max(2.5,.2*k),true);numberLines(ctx,view,k);}
      for(const c of cuts){stroke(ctx,c.plan?.lines,k,'#249c8a',Math.max(2,.2*k));const shape=c.plan?.shapes[0],pts=shape?.paths[0];if(pts?.length){const x=pts.reduce((n,p)=>n+p[0],0)/pts.length*k,y=pts.reduce((n,p)=>n+p[1],0)/pts.length*k;badge(ctx,x,y,c.revision,'#fffefb','#55746c');}}
      if(!sh.roseCutAt&&sh.rosePlan&&!sh.dirty){const view=shown(sh.rosePlan,sh);stroke(ctx,view.lines,k,'#008974',Math.max(2.5,.2*k),true);numberLines(ctx,view,k);}
    }ctx.restore();
  }
  window.RoseStock=window.CutLine={protect,prepare,nestClaim,claimLate,letGo,takeOff,seatOn,approved,plan,ensurePlan:sh=>sh.rosePlanHash && sh.rosePlanKey===fingerprint(sh) ? Promise.resolve() : plan(sh),load,restore,render,paint,record,waiting,waitWords,showCut,addsLine,unlined,full:sheetFull};   // addsLine/unlined: read-only questions for LibraryFlowRose.check
  C.allSheets().forEach(render);
})();

/* Guided rehearsal: real geometry/worker, separate sandbox-only saved state. */
(function rehearsalInit(){
  'use strict';
  if(!window.CN||!window.RoseStock){setTimeout(rehearsalInit,150);return;}
  const C=window.CN,R=window.CharmNestRose;
  if(C.S.settings.sandbox!=='on')return;
  const key='cn.roseRehearsal.sandbox.v1',esc=C.esc;
  let state=null,busy=false,error='',progress='',pending=null,worker=null,dialog;
  let demoId;try{demoId=localStorage.getItem(key);}catch(_){}
  const uid=()=> 'rgdemo-'+crypto.randomUUID();
  const call=async(action,extra={})=>{
    // Explicit sandbox flag and server guard remain mandatory even if callers change.
    const response=await C.api('charmNestLibrary',{op:'roseDemo',sandbox:true,demoId,action,...extra},{label:'Rose Gold rehearsal'});
    state=response.state;return state;
  };
  async function mutate(action,extra={}){
    pending={action,extra:{...extra,revision:state.revision,requestId:uid()}};
    await call(pending.action,pending.extra);pending=null;
  }
  function shell(){
    if(dialog)return;
    dialog=document.createElement('dialog');dialog.className='wide roseDemo';dialog.setAttribute('aria-labelledby','roseDemoTitle');
    dialog.innerHTML='<div class="dlg"><div class="dlgHead"><h3 id="roseDemoTitle">Rose Gold rehearsal</h3><div class="right"><span class="pill neutral">Sandbox only</span><button class="btn ghost xs" data-demo-close>Close</button></div></div><div class="dlgBody" data-demo-body></div><div class="dlgFoot"><div class="left"><button class="btn ghost sm" data-demo-reload>Reload saved progress</button><button class="btn ghost sm" data-demo-new>Start new rehearsal</button></div><span class="help">Sample charms · no Etsy or laser jobs</span></div></div>';
    document.body.append(dialog);
    dialog.querySelector('[data-demo-close]').onclick=()=>C.closeDlg(dialog);
    dialog.querySelector('[data-demo-reload]').onclick=()=>run(async()=>{pending=null;await call('get');});
    dialog.querySelector('[data-demo-new]').onclick=()=>{if(state&&!confirm('Start a new Rose Gold rehearsal? Your current sandbox run and physical sheets stay unchanged.'))return;run(start);};
  }
  async function start(){demoId=uid();pending=null;try{localStorage.setItem(key,demoId);}catch(_){}await call('start');}
  async function open(){shell();C.openDlg(dialog);await run(async()=>{if(demoId){await call('get');if(!state)await call('start');}else await start();});}
  async function run(fn){
    if(busy)return;busy=true;error='';progress='Loading saved rehearsal…';render();
    try{await fn();}catch(e){error=e.message||String(e);}finally{busy=false;progress='';render();}
  }
  function nest(){return run(async()=>{
    const {job}=R.demoBatch(state.batch,state.profile);
    progress='Nesting batch '+state.batch+' into '+(state.profile?'the saved remainder':'a fresh sheet')+'…';render();
    const placements=await new Promise((resolve,reject)=>{
      worker=new Worker('charm-nest-worker.js?v=20260925-fill-holes');
      const timer=setTimeout(()=>finish(new Error('Nesting took too long. Try this batch again.')),60000);
      function finish(err,result){clearTimeout(timer);worker?.terminate();worker=null;err?reject(err):resolve(result);}
      worker.onerror=e=>finish(new Error(e.message||'Could not load the nesting worker'));
      worker.onmessage=e=>{const m=e.data;if(m.type==='done')finish(null,m.result.placements);else if(m.type==='error')finish(new Error(m.message));else if(m.type==='placed'){progress='Placing sample charms · '+(m.info?.placed||'working');const el=dialog.querySelector('[data-demo-status]');if(el)el.textContent=progress;}};
      worker.postMessage({type:'solve',jobId:demoId+'-'+state.batch,job});
    });
    progress='Verifying and saving the sample layout…';render();await mutate('nest',{placements});
  });}
  function render(){
    if(!dialog)return;
    const body=dialog.querySelector('[data-demo-body]');
    dialog.querySelectorAll('[data-demo-reload],[data-demo-new]').forEach(b=>b.disabled=busy);
    if(!state){body.innerHTML=`<p class="help" role="status">${busy?'<i class="spin"></i> Loading rehearsal…':'Start a new rehearsal to try three sample batches.'}</p>${error?'<p class="roseError" role="alert">'+esc(error)+'</p>':''}`;return;}
    const locked=busy||!!pending;
    const s=state,done=s.phase==='complete',nested=s.phase==='nested',included=s.phase==='included',canNest=['empty','cut'].includes(s.phase);
    const remaining=(s.wPt*s.hPt-(s.profile?R.area(s.profile):0))*R.MM*R.MM;
    const instructions=done?'Three cuts recorded. The grey charms and dated history belong to the same sheet; the white area is the saved reusable remainder.':included?'The dashed green line is the proposed cut. Simulate the completed cut to grey these charms and save the remaining shape.':nested?'Open Options and select “Include in current set” to prepare this batch’s contour.':s.phase==='cut'?'The completed batch is grey. Nest the next seven charms into the white remainder; the previous cut areas are excluded.':'Start with seven sample charms on a fresh 100 × 50 mm sheet. Follow each batch from nesting to its recorded cut.';
    body.innerHTML=`<p class="roseDemoIntro">Rehearse three batches on one sheet. Progress is saved in the sandbox; you can close this window and return later.</p>
      <ol class="roseDemoSteps" aria-label="Rehearsal progress">${['Nest sample batch','Include in set','Simulate cut'].map((label,i)=>`<li ${((canNest&&i===0)||(nested&&i===1)||(included&&i===2))?'aria-current="step"':''}>${i+1}. ${label}</li>`).join('')}</ol>
      <p class="roseDemoGuide">${instructions}</p>
      <article class="roseDemoSheet"><div class="roseDemoSheetHead"><div><h4>RG 14/20 <span>· ${done?'Rehearsal complete':'Batch '+s.batch+' of 3'}</span></h4><small>Sample sheet ${esc(s.id.slice(-8).toUpperCase())} · 100 × 50 mm · ${Math.round(remaining)} mm² remaining</small></div>
        <details class="roseDemoOptions"><summary class="btn ghost sm">Options</summary><div><label><input type="checkbox" data-demo-include ${included?'checked':''} ${!nested||locked?'disabled':''}> Include in current set</label><span class="help">Contour allowance · 0.2 mm</span><span class="help">${included?'Contour prepared for this sample batch.':'Available after nesting the sample batch.'}</span></div></details></div>
      <div class="roseHistory"><div class="roseStockHead"><span>${s.cuts.length} simulated cut${s.cuts.length===1?'':'s'} saved</span><span class="roseLegend"><i></i> Separation cut <i class="spent"></i> Already cut</span></div>
      ${s.cuts.length?`<ol class="roseTimeline" aria-label="Physical sheet cut history">${s.cuts.map(c=>`<li style="flex-grow:${c.plan.removedPt2}"><span class="roseCutNumber">${c.revision}</span><time datetime="${new Date(c.at).toISOString()}">${esc(new Date(c.at).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}))}</time><small>${esc(c.fileBase)} · ${c.plan.shapes.length} charms</small></li>`).join('')}</ol>`:'<p class="help">Cut dates will appear here after each simulated cut.</p>'}</div>
      <canvas data-demo-canvas width="1400" height="730" role="img" aria-label="Rose Gold sample sheet: ${s.cuts.length} completed batches in grey and ${s.placements.length} current charms"></canvas>
      <div class="roseDemoActions">${canNest?`<button class="btn gold" data-demo-nest ${locked?'disabled':''}>Nest ${s.batch===1?'first':'next'} sample batch</button>`:''}${included?`<button class="btn gold" data-demo-cut ${locked?'disabled':''}>Simulate completed cut</button>`:''}${nested?'<span class="help">Next: Options → Include in current set</span>':''}${done?'<span class="roseRecorded">Rehearsal complete · remainder saved</span>':''}${s.plan?`<span class="help">After this cut: ${Math.round(s.plan.remainingPt2*R.MM*R.MM)} mm² reusable</span>`:''}</div></article>
      <p class="help" role="status" aria-live="polite">${busy?'<i class="spin"></i> ':''}<span data-demo-status>${esc(progress||(error?'Check the message below before continuing.':'Saved to sandbox · '+(done?'3 batches complete':'batch '+s.batch)))}</span></p>
      ${error?`<p class="roseError" role="alert">${esc(error)}</p>${pending?'<button class="btn ghost sm" data-demo-retry>Retry saving</button>':''}`:''}`;
    body.querySelector('[data-demo-nest]')?.addEventListener('click',nest);
    body.querySelector('[data-demo-include]')?.addEventListener('change',()=>run(()=>mutate('include')));
    body.querySelector('[data-demo-cut]')?.addEventListener('click',()=>run(()=>mutate('cut')));
    body.querySelector('[data-demo-retry]')?.addEventListener('click',()=>run(async()=>{await call(pending.action,pending.extra);pending=null;}));
    draw(body.querySelector('canvas'));
  }
  function draw(canvas){
    const ctx=canvas.getContext('2d');if(!ctx)return;
    const s=state,k=1320/s.wPt;ctx.clearRect(0,0,1400,730);ctx.fillStyle='#f5f1e9';ctx.fillRect(0,0,1400,730);
    ctx.translate(50,40);ctx.fillStyle='#fff';ctx.fillRect(0,0,s.wPt*k,s.hPt*k);
    ctx.font='16px monospace';ctx.fillStyle='#827b70';ctx.textAlign='center';
    for(let mm=0;mm<=100;mm+=10){const x=mm/R.MM*k;ctx.fillText(mm,x,-13);ctx.beginPath();ctx.moveTo(x,-7);ctx.lineTo(x,0);ctx.strokeStyle='#c1b8aa';ctx.stroke();}
    for(let mm=10;mm<=50;mm+=10){ctx.fillText(mm,-24,mm/R.MM*k+5);}
    const sh={metal:'rose',roseStock:{profileJson:s.profile?JSON.stringify(s.profile):null},roseHistory:s.cuts,rosePlan:s.plan,dirty:false};
    RoseStock.paint(ctx,sh,k,'history');
    for(const shape of s.shapes||[])for(const path of shape.paths){ctx.beginPath();path.forEach(([x,y],i)=>i?ctx.lineTo(x*k,y*k):ctx.moveTo(x*k,y*k));ctx.closePath();ctx.fillStyle='#f2d9ce';ctx.fill();ctx.strokeStyle='#a05244';ctx.lineWidth=1.6;ctx.stroke();}
    RoseStock.paint(ctx,sh,k,'lines');ctx.strokeStyle='#c08578';ctx.lineWidth=1;ctx.strokeRect(0,0,s.wPt*k,s.hPt*k);
    ctx.setTransform(1,0,0,1,0,0);
  }
  /* An entry in the Workspace menu, plus a settings entry. No auto-run. It sat beside the sandbox pill, where it
     pushed Review and Library behind the tab arrow on a laptop screen. */
  const menu=document.querySelector('#moreMenu .moreList'),pill=document.getElementById('sandboxPill');
  if(menu){const button=document.createElement('button');button.type='button';button.id='roseDemoLaunch';button.className='sandboxOnly';button.innerHTML='Rose Gold rehearsal<span class="menuHint">sandbox</span>';button.onclick=()=>{const more=document.getElementById('moreMenu');if(more)more.open=false;open();};(menu.querySelector('[data-mode="master"]')||menu.firstElementChild).after(button);}
  else if(pill){const button=document.createElement('button');button.className='btn ghost xs';button.id='roseDemoLaunch';button.textContent='Rose Gold rehearsal';button.onclick=open;pill.after(button);}
  const reset=document.getElementById('stSandboxReset');
  if(reset){const button=document.createElement('button');button.className='btn ghost sm';button.type='button';button.textContent='Rose Gold rehearsal';button.onclick=()=>{C.closeDlg(document.getElementById('dlgSettings'));open();};reset.after(button);}
  window.RoseRehearsal={open};
})();
