/* Rose Gold: physical stock, vector-derived cut plans and immutable cut history. */
(function init(){
  'use strict';
  if(!window.CN){setTimeout(init,150);return;}
  const R=window.CharmNestRose,C=window.CN,S=C.S,esc=C.esc;
  const api=(op,data={})=>C.api('charmNestLibrary',{op,...data},{label:'Rose Gold stock'});
  const fingerprint=s=>JSON.stringify((s.placements||[]).map(p=>[p.id,+p.cxPt.toFixed(3),+p.cyPt.toFixed(3),p.angle,p.scale||1,p.hash||s.charms?.find(c=>c.id===p.id)?.hash||null]));
  const refresh=sh=>{window.Session?.schedule();if(sh.el){C.renderCard(sh);C.drawPreview(sh);}};
  const parse=s=>s?JSON.parse(s):null;
  const decode=cuts=>cuts.map(c=>({...c,plan:parse(c.planJson)}));
  const stamp=at=>Number.isFinite(at)?`<time datetime="${new Date(at).toISOString()}">${esc(new Date(at).toLocaleString(undefined,{year:'numeric',month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}))}</time>`:'<span class="roseUndated">Time not recorded</span>';
  // The full detail shows on hover when the entry is too narrow for it.
  const detail=text=>`<small title="${esc(text)}">${esc(text)}</small>`;
  // The timeline lists every green line when it was first prepared, then the
  // cut that removed it, oldest first. Lines keep their dates in the saved
  // contour and, once cut, in the permanent cut history.
  const lineEntry=(s,fileBase,cut)=>`<li class="roseLineEntry"><span class="roseLineNumber">${s.n}</span>${stamp(s.at)}${detail(`Green line ${s.n} · ${s.ids.length} charm${s.ids.length===1?'':'s'}${cut?' · cut '+cut:''}${fileBase?' · '+fileBase:''}`)}</li>`;
  const cutEntry=c=>`<li style="flex-grow:${Math.max(1,c.plan?.removedPt2||1)}"><span class="roseCutNumber">${c.revision}</span>${stamp(c.at)}${detail(`Cut recorded · ${c.plan?.shapes.length||0} charms · ${c.fileBase??''}`)}</li>`;
  async function load(sh,older=false){
    const stockId=sh.roseStock?.id||sh.recalled?.roseStockId;if(!stockId)return;
    const result=await api('roseGet',{stockId,...(older?{before:Math.min(...sh.roseHistory.map(c=>c.revision))}:{})});
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
    const result=await api('roseGet',{stockId:rec.roseStockId});
    sh.roseStock=result.stock;sh.roseHistory=decode(result.cuts);sh.roseMore=result.more;sh.roseCutAt=rec.roseCutAt||null;sh.roseRevision=rec.roseRevision;sh.rosePlan=parse(rec.rosePlanJson);sh.rosePlanHash=rec.rosePlanHash;sh.roseProtected=parse(rec.roseProtectedJson);sh._roseLoaded=true;
  }
  function protect(sh){
    if(sh.metal!=='rose'||sh.roseCutAt)return;
    if(sh.roseProtected)for(const p of sh.roseProtected.placements||[]){
      const rows=sh.placements.filter(x=>x.id===p.id),q=rows[0];
      if(rows.length!==1||!sh.charms.some(c=>c.id===p.id)||['cxPt','cyPt','angle'].some(k=>!Number.isFinite(q[k])||Math.abs(q[k]-p[k])>.001)||Math.abs((q.scale||1)-(p.scale||1))>.00001)throw new Error('Reload the saved Rose Gold layout before adding charms');
    }
    if(sh.rosePlan?.profile){
      const ids=new Set(sh.rosePlan.shapes.map(s=>s.id));
      const placements=sh.placements.filter(p=>ids.has(p.id));
      if(placements.length!==ids.size)throw new Error('Reload the saved Rose Gold layout before adding charms');
      sh.roseProtected={profile:sh.rosePlan.profile,lines:sh.rosePlan.lines,placements:placements.map(p=>({...p})),shapes:sh.rosePlan.shapes,stages:sh.rosePlan.stages};
    }
  }
  async function prepare(sh,opts={}){
    if(sh.metal!=='rose')return;
    if(sh.roseCutAt)throw new Error('This layout has already been cut');
    if(!S.cloud.ok)throw new Error('Reconnect to load the physical Rose Gold sheet before nesting');
    sh.sheetId ||= 'rose-'+Date.now().toString(36)+'-'+C.uid();
    protect(sh);
    const st=C.stockFor('rose',sh);
    const r=await api('roseClaim',{sheetId:sh.sheetId,wPt:st.wPt,hPt:st.hPt,stockId:sh.roseStock?.id||sh.roseChoice,revision:sh.roseStock?.revision,fresh:!!sh.roseFresh,...opts});
    sh.roseStock=r.stock;sh.roseRevision=r.stock.revision;
    if(r.protectedJson){
      const guard=parse(r.protectedJson);
      for(const p of guard.placements){const q=sh.placements.find(x=>x.id===p.id);if(!q||['cxPt','cyPt','angle'].some(k=>Math.abs(q[k]-p[k])>.001)||Math.abs((q.scale||1)-(p.scale||1))>.00001)throw new Error('Reload the saved Rose Gold layout before adding charms');}
      sh.roseProtected=guard;
      if(opts.nesting){delete sh.rosePlan;delete sh.rosePlanHash;delete sh.rosePlanKey;}
    }
    await load(sh);window.Session?.schedule();
  }
  async function plan(sh){
    if(sh._rosePlanning)return sh._rosePlanning;
    const task=(async()=>{
      if(sh.roseCutAt)return;
      if(!sh.persistedDone||!sh.verification?.ok||sh.dirty||!sh.placements.length)throw new Error('Nest and save this layout before preparing its contour');
      const key=fingerprint(sh);
      sh._roseError=null;
      if(!sh.roseStock)await prepare(sh,{fresh:true});
      const shapes=window.CharmNestBackground ? await CharmNestBackground.run('roseShapes',{charms:sh.charms.map(c=>({id:c.id,outline:c.outline,members:c.members,centerPt:c.centerPt,bbox:c.bbox})),placements:sh.placements}) : R.shapes(sh.charms,sh.placements);
      const result=await api('rosePlan',{sheetId:sh.sheetId,stockId:sh.roseStock.id,revision:sh.roseStock.revision,fingerprint:key,shapesJson:JSON.stringify(shapes),allowanceMm:sh.roseAllowanceMm||.2});
      if(fingerprint(sh)!==key||sh.dirty)throw new Error('Layout changed while saving its contour');
      sh.rosePlan=parse(result.planJson);sh.rosePlanHash=result.planHash;sh.rosePlanKey=key;sh.roseRevision=sh.roseStock.revision;
    })();
    sh._rosePlanning=task;refresh(sh);
    try{await task;}catch(e){sh._roseError=e.message;throw e;}finally{sh._rosePlanning=null;refresh(sh);C.flushManualIntake?.('rose');}
  }
  async function record(sh){
    if(!sh.rosePlanHash)await plan(sh);
    if(!confirm('Has this exact layout AND its teal separation contour been physically cut?\n\nRecord the completed cut now. Its charms will turn grey and this material will be excluded from all future nesting.'))return;
    const r=await api('roseRecordCut',{sheetId:sh.sheetId,stockId:sh.roseStock.id,revision:sh.roseRevision,planHash:sh.rosePlanHash,by:window.B?.employee||'operator'});
    sh.roseCutAt=r.cut.at;sh.roseStock=r.stock;sh.roseHistory=[...decode([r.cut]),...(sh.roseHistory||[]).filter(c=>c.sheetId!==sh.sheetId)];
    refresh(sh);C.toast('Cut recorded · the remaining sheet is available for the next Rose Gold layout','ok');
  }
  const observed=new WeakMap();
  const observer=window.IntersectionObserver?new IntersectionObserver(entries=>{for(const e of entries)if(e.isIntersecting){observer.unobserve(e.target);const sh=observed.get(e.target);if(sh&&!sh._roseLoaded&&!sh._roseLoading){sh._roseLoading=true;load(sh).catch(error=>{sh._roseError=error.message;}).finally(()=>{sh._roseLoading=false;refresh(sh);});}}},{rootMargin:'120px'}):null;
  function render(sh){
    if(sh.metal!=='rose'||!sh.el)return;
    let host=sh.el.querySelector('.roseHistory');if(!host){host=document.createElement('div');host.className='roseHistory';sh.el.querySelector('.shPreviewWrap').before(host);}
    const stock=sh.roseStock,history=(sh.roseHistory||[]).slice().sort((a,b)=>a.revision-b.revision),busy=!!(sh._rosePlanning||sh._roseLoading||sh._roseAction);
    const stockId=stock?.id||sh.recalled?.roseStockId;
    const ready=!sh.roseCutAt&&!sh.recalled&&sh.persistedDone&&sh.verification?.ok&&!sh.dirty&&sh.placements.length&&!['nesting','finishing','queued'].includes(sh.status);
    const included=!!sh.setId&&!sh.draft;
    const timeline=[...history.flatMap(c=>[...(c.plan?.stages||[]).map(s=>lineEntry(s,c.fileBase,c.revision)),cutEntry(c)]),...stagesOf(activeLines(sh)).map(s=>lineEntry(s,sh.fileBase))];
    host.hidden=!stockId&&!ready;
    host.innerHTML=`${busy?'<div class="help" role="status"><i class="spin"></i> Loading sheet geometry…</div>':''}
      ${timeline.length?`<ol class="roseTimeline" aria-label="Green line and cut history">${timeline.join('')}</ol>`:''}
      <div class="roseActions">${sh.roseMore?'<button class="btn ghost xs" data-rose="older">Earlier cuts</button>':''}${stockId?'<button class="btn ghost xs" data-rose="refresh">Refresh history</button>':''}${ready?`<button class="btn ghost xs" data-rose="plan" ${busy?'disabled':''}>${sh.rosePlan?'Update contour':'Prepare cut contour'}</button>`:''}${(sh.rosePlan||sh.recalled?.roseStockId)&&!sh.roseCutAt?`<button class="btn ghost xs" data-rose="record" ${busy||!included||(!sh.recalled&&!ready)?'disabled':''}>Record completed cut</button>`:''}${sh.roseCutAt?'<span class="roseRecorded">Cut recorded · remainder saved</span>':''}</div>
      ${sh._roseError?`<p class="roseError" role="alert">${esc(sh._roseError)}</p>`:''}${sh.rosePlan&&!sh.roseCutAt?`<p class="roseNote">${sh.rosePlan.allowanceMm} mm contour allowance · ${Math.round(sh.rosePlan.remainingPt2*R.MM*R.MM)} mm² remaining after this cut.</p>`:''}`;
    const invoke=fn=>async()=>{if(sh._roseAction)return;sh._roseAction=true;sh._roseError=null;refresh(sh);try{await fn();}catch(e){sh._roseError=e.message;C.toast(e.message,'bad');}finally{sh._roseAction=false;refresh(sh);C.flushManualIntake?.('rose');}};
    host.querySelector('[data-rose="plan"]')?.addEventListener('click',invoke(()=>plan(sh)));
    host.querySelector('[data-rose="record"]')?.addEventListener('click',invoke(()=>record(sh)));
    host.querySelector('[data-rose="refresh"]')?.addEventListener('click',invoke(()=>load(sh)));
    host.querySelector('[data-rose="older"]')?.addEventListener('click',invoke(()=>load(sh,true)));
    const menu=sh.el.querySelector('.solidOptions');
    if(menu&&!menu.querySelector('[data-rose-options]')){
      const options=document.createElement('div');options.dataset.roseOptions='';options.className='roseStockOptions';
      options.innerHTML=`<h4>Cut contour</h4><label class="roseAllowance">Contour allowance <span>mm</span><input type="number" min="0.05" max="2" step="0.05" value="${sh.roseAllowanceMm||.2}" data-rose-allowance></label><p class="help">Space around the combined charm outline.</p><div data-rose-stock-choice><button type="button" class="btn ghost xs" data-rose-choose>Choose remnant or new sheet</button><span data-rose-picker></span></div>`;
      menu.append(options);
      options.querySelector('[data-rose-choose]')?.addEventListener('click',async e=>{
        e.target.disabled=true;try{const result=await api('roseList'),st=C.stockFor('rose'),pick=options.querySelector('[data-rose-picker]');
          const stocks=result.stocks.filter(s=>Math.abs(s.wPt-st.wPt)<.01&&Math.abs(s.hPt-st.hPt)<.01);
          pick.innerHTML='<select aria-label="Physical Rose Gold sheet"><option value="">Use available remnant first</option><option value="new">New uncut sheet</option>'+stocks.map(s=>'<option value="'+esc(s.id)+'">'+esc(s.id.slice(-8).toUpperCase())+' · '+s.revision+' cuts · '+Math.round((s.wPt*s.hPt-(s.profileJson?R.area(parse(s.profileJson)):0))*R.MM*R.MM)+' mm² left</option>').join('')+'</select>';
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
      input.onchange=e=>{const n=+e.target.value;if(!Number.isFinite(n)||n<.05||n>2){e.target.value=sh.roseAllowanceMm||.2;input._draft=false;return;}sh.roseAllowanceMm=n;input._draft=false;delete sh.rosePlanKey;window.Session?.schedule();if(ready)invoke(()=>plan(sh))();else refresh(sh);};
      options.querySelector('[data-rose-stock-choice]').hidden=!!(stockId||sh.recalled);
    }
    if(stockId&&!sh._roseLoaded&&!sh._roseLoading){observed.set(host,sh);if(observer)observer.observe(host);}
    if(ready&&included&&!busy&&!sh._roseError&&sh.rosePlanKey!==fingerprint(sh))queueMicrotask(()=>plan(sh).catch(()=>{}));
  }
  // Lines saved before dates were kept show as one undated line, as the server reads them.
  const stagesOf=plan=>plan?.stages||(plan?.lines?.length?[{n:1,at:null,ids:(plan.shapes||plan.placements||[]).map(s=>s.id),lines:[0,plan.lines.length]}]:[]);
  // The green lines drawn now: the saved contour, or the protected lines while new charms are nested.
  function activeLines(sh){if(sh.roseCutAt)return null;return sh.rosePlan&&!sh.dirty?sh.rosePlan:sh.roseProtected||null;}
  const length=path=>path.slice(1).reduce((n,p,i)=>n+Math.hypot(p[0]-path[i][0],p[1]-path[i][1]),0);
  function midpoint(path){let left=length(path)/2;for(let i=1;i<path.length;i++){const a=path[i-1],b=path[i],d=Math.hypot(b[0]-a[0],b[1]-a[1]);if(d>0&&d>=left)return [a[0]+(b[0]-a[0])*left/d,a[1]+(b[1]-a[1])*left/d];left-=d;}return path[0];}
  // Number each green line where it runs, matching its timeline entry.
  function numberLines(ctx,plan,k){
    for(const s of stagesOf(plan)){
      const paths=(plan.lines||[]).slice(s.lines[0],s.lines[1]).filter(p=>p.length);if(!paths.length)continue;
      const [x,y]=midpoint(paths.reduce((a,b)=>length(b)>length(a)?b:a));
      badge(ctx,x*k,y*k,s.n,'#008974','#fff');
    }
  }
  // A numbered dot that stays the same size on screen at any pixel density.
  function badge(ctx,x,y,text,fill,ink){const d=window.devicePixelRatio||1;ctx.fillStyle=fill;ctx.beginPath();ctx.arc(x,y,7*d,0,Math.PI*2);ctx.fill();ctx.fillStyle=ink;ctx.font=`${10*d}px sans-serif`;ctx.textAlign='center';ctx.fillText(text,x,y+3*d);}
  function stroke(ctx,paths,k,color,width,dashed=false){ctx.strokeStyle=color;ctx.lineWidth=width;ctx.setLineDash(dashed?[4,3]:[]);for(const path of paths||[]){ctx.beginPath();path.forEach(([x,y],i)=>i?ctx.lineTo(x*k,y*k):ctx.moveTo(x*k,y*k));ctx.stroke();}ctx.setLineDash([]);}
  function paint(ctx,sh,k,phase){
    if(sh.metal!=='rose')return;
    ctx.save();const cuts=sh.roseHistory||[];
    if(phase==='history'){
      const p=parse(sh.roseStock?.profileJson);
      if(p){ctx.fillStyle='#f0eeeb';p.values.forEach((v,i)=>{if(p.axis==='x')ctx.fillRect(0,i*p.step*k,v*k,p.step*k);else ctx.fillRect(i*p.step*k,0,p.step*k,v*k);});}
      for(const c of cuts)for(const shape of c.plan?.shapes||[]){ctx.fillStyle='#d8d5d0';for(const path of shape.paths){ctx.beginPath();path.forEach(([x,y],i)=>i?ctx.lineTo(x*k,y*k):ctx.moveTo(x*k,y*k));ctx.closePath();ctx.fill();}stroke(ctx,shape.paths,k,'#aaa59c',Math.max(.6,.12*k));stroke(ctx,shape.ink,k,'#aaa59c',Math.max(.5,.1*k));}
    }else{
      if(!sh.roseCutAt&&sh.roseProtected&&(!sh.rosePlan||sh.dirty)){stroke(ctx,sh.roseProtected.lines,k,'#008974',Math.max(2.5,.2*k),true);numberLines(ctx,sh.roseProtected,k);}
      for(const c of cuts){stroke(ctx,c.plan?.lines,k,'#249c8a',Math.max(2,.2*k));const shape=c.plan?.shapes[0],pts=shape?.paths[0];if(pts?.length){const x=pts.reduce((n,p)=>n+p[0],0)/pts.length*k,y=pts.reduce((n,p)=>n+p[1],0)/pts.length*k;badge(ctx,x,y,c.revision,'#fffefb','#55746c');}}
      if(!sh.roseCutAt&&sh.rosePlan&&!sh.dirty){stroke(ctx,sh.rosePlan.lines,k,'#008974',Math.max(2.5,.2*k),true);numberLines(ctx,sh.rosePlan,k);}
    }ctx.restore();
  }
  window.RoseStock={protect,prepare,plan,ensurePlan:sh=>sh.rosePlanHash && sh.rosePlanKey===fingerprint(sh) ? Promise.resolve() : plan(sh),load,restore,render,paint,record};
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
      worker=new Worker('charm-nest-worker.js?v=20260922-rehearsal');
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
    const instructions=done?'Three cuts recorded. The grey charms and dated history belong to the same sheet; the white area is the saved reusable remainder.':included?'The dashed teal line is the proposed separation cut. Simulate the completed cut to grey these charms and save the remaining shape.':nested?'Open Options and select “Include in current set” to prepare this batch’s contour.':s.phase==='cut'?'The completed batch is grey. Nest the next seven charms into the white remainder; the previous cut areas are excluded.':'Start with seven sample charms on a fresh 100 × 50 mm sheet. Follow each batch from nesting to its recorded cut.';
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
  // A visible entry beside the sandbox pill, plus a settings entry. No auto-run.
  const pill=document.getElementById('sandboxPill');
  if(pill){const button=document.createElement('button');button.className='btn ghost xs';button.id='roseDemoLaunch';button.textContent='Rose Gold rehearsal';button.onclick=open;pill.after(button);}
  const reset=document.getElementById('stSandboxReset');
  if(reset){const button=document.createElement('button');button.className='btn ghost sm';button.type='button';button.textContent='Rose Gold rehearsal';button.onclick=()=>{C.closeDlg(document.getElementById('dlgSettings'));open();};reset.after(button);}
  window.RoseRehearsal={open};
})();
