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
  async function load(sh,older=false){
    const stockId=sh.roseStock?.id||sh.recalled?.roseStockId;if(!stockId)return;
    const result=await api('roseGet',{stockId,...(older?{before:Math.min(...sh.roseHistory.map(c=>c.revision))}:{})});
    // An already cut layout can display the full physical history. An active
    // reservation must keep its original revision until prepare checks it.
    if(!sh.roseStock||sh.roseCutAt)sh.roseStock=result.stock;
    sh.roseHistory=older?[...(sh.roseHistory||[]),...decode(result.cuts)]:decode(result.cuts);
    sh.roseMore=result.more;sh._roseLoaded=true;
    const completed=sh.roseHistory.find(c=>c.sheetId===sh.sheetId);if(completed){sh.roseCutAt=completed.at;sh.roseStock=result.stock;}
    if(sh.recalled){const rec=(await api('getSheet',{id:sh.sheetId})).sheet;sh.roseCutAt=rec.roseCutAt||null;sh.rosePlan=parse(rec.rosePlanJson);sh.rosePlanHash=rec.rosePlanHash;sh.roseRevision=rec.roseRevision;}
    refresh(sh);
  }
  async function restore(sh,rec){
    const result=await api('roseGet',{stockId:rec.roseStockId});
    sh.roseStock=result.stock;sh.roseHistory=decode(result.cuts);sh.roseMore=result.more;sh.roseCutAt=rec.roseCutAt||null;sh.roseRevision=rec.roseRevision;sh.rosePlan=parse(rec.rosePlanJson);sh.rosePlanHash=rec.rosePlanHash;sh._roseLoaded=true;
  }
  async function prepare(sh,opts={}){
    if(sh.metal!=='rose')return;
    if(sh.roseCutAt)throw new Error('This layout has already been cut');
    if(!S.cloud.ok)throw new Error('Reconnect to load the physical Rose Gold sheet before nesting');
    sh.sheetId ||= 'rose-'+Date.now().toString(36)+'-'+C.uid();
    const st=C.stockFor('rose',sh);
    const r=await api('roseClaim',{sheetId:sh.sheetId,wPt:st.wPt,hPt:st.hPt,stockId:sh.roseStock?.id||sh.roseChoice,revision:sh.roseStock?.revision,fresh:!!sh.roseFresh,...opts});
    sh.roseStock=r.stock;sh.roseRevision=r.stock.revision;
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
      const shapes=R.shapes(sh.charms,sh.placements);
      const result=await api('rosePlan',{sheetId:sh.sheetId,stockId:sh.roseStock.id,revision:sh.roseStock.revision,fingerprint:key,shapesJson:JSON.stringify(shapes),allowanceMm:sh.roseAllowanceMm||.2});
      if(fingerprint(sh)!==key||sh.dirty)throw new Error('Layout changed while saving its contour');
      sh.rosePlan=parse(result.planJson);sh.rosePlanHash=result.planHash;sh.rosePlanKey=key;sh.roseRevision=sh.roseStock.revision;
    })();
    sh._rosePlanning=task;refresh(sh);
    try{await task;}catch(e){sh._roseError=e.message;throw e;}finally{sh._rosePlanning=null;refresh(sh);}
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
    if(sh.el.querySelector('[data-rose-allowance]')===document.activeElement)return;
    let host=sh.el.querySelector('.roseHistory');if(!host){host=document.createElement('div');host.className='roseHistory';sh.el.querySelector('.shPreviewWrap').before(host);}
    const stock=sh.roseStock,history=(sh.roseHistory||[]).slice().sort((a,b)=>a.revision-b.revision),busy=!!(sh._rosePlanning||sh._roseLoading||sh._roseAction);
    const stockId=stock?.id||sh.recalled?.roseStockId;
    const ready=!sh.roseCutAt&&!sh.recalled&&sh.persistedDone&&sh.verification?.ok&&!sh.dirty&&sh.placements.length&&!['nesting','finishing','queued'].includes(sh.status);
    const included=!!sh.setId&&!sh.draft;
    host.hidden=!stockId&&!ready;
    host.innerHTML=`<div class="roseStockHead"><span>${stockId?'Physical sheet <b>'+esc(stockId.slice(-8).toUpperCase())+'</b>':'Rose Gold stock'}${stock?.revision?' · '+stock.revision+' cut'+(stock.revision===1?'':'s'):''}</span><span class="roseLegend"><i></i> Separation cut <i class="spent"></i> Already cut</span></div>${busy?'<div class="help" role="status"><i class="spin"></i> Loading sheet geometry…</div>':''}
      ${history.length?`<ol class="roseTimeline" aria-label="Physical sheet cut history">${history.map(c=>`<li style="flex-grow:${Math.max(1,c.plan?.removedPt2||1)}"><span class="roseCutNumber">${c.revision}</span><time datetime="${new Date(c.at).toISOString()}">${esc(new Date(c.at).toLocaleString(undefined,{year:'numeric',month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}))}</time><small>${c.plan?.shapes.length||0} charms · ${esc(c.fileBase)}</small></li>`).join('')}</ol>`:''}
      <div class="roseActions">${sh.roseMore?'<button class="btn ghost xs" data-rose="older">Earlier cuts</button>':''}${stockId?'<button class="btn ghost xs" data-rose="refresh">Refresh history</button>':''}${ready?`<button class="btn ghost xs" data-rose="plan" ${busy?'disabled':''}>${sh.rosePlan?'Update contour':'Prepare cut contour'}</button>`:''}${(sh.rosePlan||sh.recalled?.roseStockId)&&!sh.roseCutAt?`<button class="btn ghost xs" data-rose="record" ${busy||!included||(!sh.recalled&&!ready)?'disabled':''}>Record completed cut</button>`:''}${sh.roseCutAt?'<span class="roseRecorded">Cut recorded · remainder saved</span>':''}</div>
      ${sh._roseError?`<p class="roseError" role="alert">${esc(sh._roseError)}</p>`:''}${sh.rosePlan&&!sh.roseCutAt?`<p class="roseNote">${sh.rosePlan.allowanceMm} mm contour allowance · ${Math.round(sh.rosePlan.remainingPt2*R.MM*R.MM)} mm² remaining after this cut. History updates when you record the completed cut.</p>`:''}`;
    const invoke=fn=>async()=>{if(sh._roseAction)return;sh._roseAction=true;sh._roseError=null;refresh(sh);try{await fn();}catch(e){sh._roseError=e.message;C.toast(e.message,'bad');}finally{sh._roseAction=false;refresh(sh);}};
    host.querySelector('[data-rose="plan"]')?.addEventListener('click',invoke(()=>plan(sh)));
    host.querySelector('[data-rose="record"]')?.addEventListener('click',invoke(()=>record(sh)));
    host.querySelector('[data-rose="refresh"]')?.addEventListener('click',invoke(()=>load(sh)));
    host.querySelector('[data-rose="older"]')?.addEventListener('click',invoke(()=>load(sh,true)));
    const menu=sh.el.querySelector('.solidOptions');
    if(menu&&!menu.querySelector('[data-rose-options]')){
      const options=document.createElement('div');options.dataset.roseOptions='';options.className='roseStockOptions';
      options.innerHTML=`<label>Contour allowance (mm)<input type="number" min="0.05" max="2" step="0.05" value="${sh.roseAllowanceMm||.2}" data-rose-allowance ${sh.roseCutAt||sh.recalled||busy?'disabled':''}></label><span class="help">${stockId?'Using sheet '+esc(stockId.slice(-8).toUpperCase()):'Uses an available matching remnant first; otherwise a new sheet.'}</span>${!stockId&&!sh.recalled?'<button class="btn ghost xs" data-rose-choose>Choose remnant or new sheet</button><span data-rose-picker></span>':''}${stockId&&!sh.roseCutAt&&!sh.recalled?`<button class="btn ghost xs" data-rose-release ${included||busy||['nesting','finishing','queued'].includes(sh.status)?'disabled':''}>Release stock reservation</button>`:''}`;
      menu.append(options);
      options.querySelector('[data-rose-choose]')?.addEventListener('click',async e=>{
        e.target.disabled=true;try{const result=await api('roseList'),st=C.stockFor('rose'),pick=options.querySelector('[data-rose-picker]');
          const stocks=result.stocks.filter(s=>Math.abs(s.wPt-st.wPt)<.01&&Math.abs(s.hPt-st.hPt)<.01);
          pick.innerHTML='<select aria-label="Physical Rose Gold sheet"><option value="">Use available remnant first</option><option value="new">New uncut sheet</option>'+stocks.map(s=>'<option value="'+esc(s.id)+'">'+esc(s.id.slice(-8).toUpperCase())+' · '+s.revision+' cuts · '+Math.round((s.wPt*s.hPt-(s.profileJson?R.area(parse(s.profileJson)):0))*R.MM*R.MM)+' mm² left</option>').join('')+'</select>';
          const select=pick.querySelector('select');select.value=sh.roseFresh?'new':sh.roseChoice||'';select.onchange=()=>{sh.roseFresh=select.value==='new';sh.roseChoice=sh.roseFresh?null:select.value||null;window.Session?.schedule();};
        }catch(error){C.toast(error.message,'bad');}finally{e.target.disabled=false;}
      });
      options.querySelector('input').onchange=e=>{const n=+e.target.value;if(!Number.isFinite(n)||n<.05||n>2){e.target.value=sh.roseAllowanceMm||.2;return;}sh.roseAllowanceMm=n;delete sh.rosePlanKey;if(ready)invoke(()=>plan(sh))();else refresh(sh);};
      options.querySelector('[data-rose-release]')?.addEventListener('click',invoke(async()=>{await api('roseRelease',{sheetId:sh.sheetId,stockId});delete sh.roseStock;delete sh.rosePlan;delete sh.rosePlanKey;delete sh.rosePlanHash;delete sh.roseHistory;sh._roseLoaded=false;C.sheetDirty(sh);}));
    }
    if(stockId&&!sh._roseLoaded&&!sh._roseLoading){observed.set(host,sh);if(observer)observer.observe(host);}
    if(ready&&included&&!busy&&!sh._roseError&&sh.rosePlanKey!==fingerprint(sh))queueMicrotask(()=>plan(sh).catch(()=>{}));
  }
  function stroke(ctx,paths,k,color,width,dashed=false){ctx.strokeStyle=color;ctx.lineWidth=width;ctx.setLineDash(dashed?[4,3]:[]);for(const path of paths||[]){ctx.beginPath();path.forEach(([x,y],i)=>i?ctx.lineTo(x*k,y*k):ctx.moveTo(x*k,y*k));ctx.stroke();}ctx.setLineDash([]);}
  function paint(ctx,sh,k,phase){
    if(sh.metal!=='rose')return;
    ctx.save();const cuts=sh.roseHistory||[];
    if(phase==='history'){
      const p=parse(sh.roseStock?.profileJson);
      if(p){ctx.fillStyle='#f0eeeb';p.values.forEach((v,i)=>{if(p.axis==='x')ctx.fillRect(0,i*p.step*k,v*k,p.step*k);else ctx.fillRect(i*p.step*k,0,p.step*k,v*k);});}
      for(const c of cuts)for(const shape of c.plan?.shapes||[]){ctx.fillStyle='#d8d5d0';for(const path of shape.paths){ctx.beginPath();path.forEach(([x,y],i)=>i?ctx.lineTo(x*k,y*k):ctx.moveTo(x*k,y*k));ctx.closePath();ctx.fill();}stroke(ctx,shape.paths,k,'#aaa59c',Math.max(.6,.12*k));stroke(ctx,shape.ink,k,'#aaa59c',Math.max(.5,.1*k));}
    }else{
      for(const c of cuts){stroke(ctx,c.plan?.lines,k,'#249c8a',Math.max(1,.1*k));const shape=c.plan?.shapes[0],pts=shape?.paths[0];if(pts?.length){const x=pts.reduce((n,p)=>n+p[0],0)/pts.length*k,y=pts.reduce((n,p)=>n+p[1],0)/pts.length*k;ctx.fillStyle='#fffefb';ctx.beginPath();ctx.arc(x,y,7,0,Math.PI*2);ctx.fill();ctx.fillStyle='#55746c';ctx.font='10px sans-serif';ctx.textAlign='center';ctx.fillText(c.revision,x,y+3);}}
      if(!sh.roseCutAt&&sh.rosePlan&&!sh.dirty)stroke(ctx,sh.rosePlan.lines,k,'#008974',Math.max(1.25,.1*k),true);
    }ctx.restore();
  }
  window.RoseStock={prepare,plan,ensurePlan:sh=>sh.rosePlanHash && sh.rosePlanKey===fingerprint(sh) ? Promise.resolve() : plan(sh),load,restore,render,paint,record};
  C.allSheets().forEach(render);
})();
