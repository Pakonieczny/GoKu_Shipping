/*  charm-nest-bridge.js — the Charm Sorter as master of the Design Station.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Loaded after charm-nest-1.html's own script and built on it: the same S,
 *  api(), agent log, sheets, nesting and cloud helpers. Sections follow the
 *  design document ("Charm Sorter ⇄ Design Station bridge", draft 4):
 *
 *    17 · DesignLink   the station framed and driven over postMessage (§4.7), heartbeat, session log, console
 *    18 · Orders       pull + interpret, claims, date rule, re-validation (§5, §10.3)
 *    19 · Master       master files → per-SKU designs, labels, vision fallback, overrides (§6)
 *    20 · Pool         one charm per order line and copy, keyed deterministically (§6.4)
 *    21 · Engrave      words (Claude), the checked flip, the fit, the review queue, back files (§7)
 *    22 · Sets         one set per run, one label per sheet, manifest, completion over the bridge (§8)
 *    23 · RunCtl       the two halves, the persistent run record, resume, stop, Auto/Manual (§10)
 *    24 · Review       every decision a person must make, with the problem and the quick fixes (§11)
 *    25 · boot
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
(function () {
const O = CharmNestOrders, G = CharmNestGeom, P = CharmNestPDF;
const WORKSPACE_SANDBOX = S.settings.sandbox === "on";
const MM = 25.4 / 72, PT = 72 / 25.4;
const B = window.B = { link: null, orders: { rows: [], byKey: new Map(), pulledAt: 0, stale: false, snapshot: null, filtered: 0 }, master: { entries: new Map(), files: [], loadedAt: 0, loading: null, error: null, jobs: new Map() }, maps: { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [], rows: [] }, loadedAt: 0 }, pool: { rows: new Map(), sources: new Map() }, engrave: { items: new Map(), fonts: { ok: false, Regular: null, Semibold: null, error: null, loading: null } }, review: { items: [] }, openRuns: null, run: null, sets: new Map(), employee: (localStorage.getItem("cn.employee") || "").trim() };
const SOURCE_LABEL = { personalization: "the personalisation box", personalisation: "the personalisation box", buyerMessage: "the buyer's message", staffNote: "the staff note", messages: "the staff messages", none: "", "": "" };
const el = (tag, cls, html) => { const e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
// Shared by waiting and decided engravings; choices remain read-only purchase facts.
function purchaseMarkup(row) {
  const detail=O.purchaseDetails(row.line,row.spec);
  return `<div class="purchaseType"><span class="purchaseLabel">Jewellery</span><strong>${esc(detail.type)}</strong></div><div class="purchaseChoices"><span class="purchaseLabel">Selected options</span>${detail.options.length ? `<dl>${detail.options.map(v=>`<div><dt>${esc(v.name || "Option")}</dt><dd>${esc(v.value)}</dd></div>`).join("")}</dl>` : '<span class="purchaseMissing">Selections unavailable</span>'}</div>`;
}
// Zoom/pan ported from Index(20260922-153718).html, wireTiles review viewer.
// Keep its transform order, clamp, wheel steps, drag damping and click maths.
const ListZoom = (() => {
  const bound=new WeakMap(),observed=new Set(),frames=new Map();
  const storageKey='cn.listImageFraming.'+(WORKSPACE_SANDBOX?'sandbox':'production');
  try{for(const [key,value] of JSON.parse(localStorage.getItem(storageKey)||'[]'))if(value&&Number.isFinite(value.s)&&value.s>=1&&value.s<=8&&[value.x,value.y].every(Number.isFinite))frames.set(key,value);}catch(_){}
  let saveTimer;
  function saveFrames(){clearTimeout(saveTimer);try{localStorage.setItem(storageKey,JSON.stringify([...frames]));}catch(_){}}
  window.addEventListener('pagehide',saveFrames);
  const resize=typeof ResizeObserver!=='undefined'?new ResizeObserver(entries=>{for(const e of entries)bound.get(e.target)?.apply();}):null;
  function clean(){for(const box of observed)if(!box.isConnected){resize?.unobserve(box);observed.delete(box);}}
  function observe(box){if(resize&&!observed.has(box)){observed.add(box);resize.observe(box);}}
  function detach(box){bound.get(box)?.dispose();bound.delete(box);resize?.unobserve(box);observed.delete(box);box.classList.remove('zoomReady');box.removeAttribute('title');const reset=box.closest('figure')?.querySelector('.thumbReset');if(reset){reset.hidden=true;reset.onclick=null;}}
  function bind(box,key){
    const im=box.querySelector('img,canvas');if(!im)return;
    if(bound.get(box)?.im===im){observe(box);return;}
    detach(box);const abort=new AbortController(),on=(name,fn,opts={})=>box.addEventListener(name,fn,{...opts,signal:abort.signal});
    const MAX_SCALE=8,dragThreshold=3;
    const z=frames.get(key)||{s:box.hasAttribute('data-listing')?2:1,x:0,y:0};
    const ratio=z.vw&&box.clientWidth?box.clientWidth/z.vw:1;
    im.dataset.scale=z.s;im.dataset.offsetX=z.x*ratio;im.dataset.offsetY=z.y*ratio;
    im.style.transform=`scale(${z.s}) translate(${z.x*ratio}px, ${z.y*ratio}px)`;
    im.draggable=false;box.classList.add('zoomReady');box.tabIndex=0;box.setAttribute('role','group');
    box.title='Click to zoom at a point · drag to pan · scroll to zoom · reset ↺. Keyboard: + / −, arrows, 0 to reset.';
    function persistZoom(s,x,y){
      frames.delete(key);frames.set(key,{s,x,y,vw:box.clientWidth,vh:box.clientHeight});while(frames.size>300)frames.delete(frames.keys().next().value);
      clearTimeout(saveTimer);saveTimer=setTimeout(saveFrames,150);
    }
    function clampAndApply(im,scale,offX,offY,persist){
      const viewportW=box.clientWidth,viewportH=box.clientHeight,baseW=im.clientWidth,baseH=im.clientHeight;
      if(viewportW&&viewportH&&baseW&&baseH){
        const maxX=Math.max(0,(baseW*scale-viewportW)/2/scale),maxY=Math.max(0,(baseH*scale-viewportH)/2/scale);
        offX=Math.max(-maxX,Math.min(maxX,offX));offY=Math.max(-maxY,Math.min(maxY,offY));
      }
      im.dataset.scale=scale;im.dataset.offsetX=offX;im.dataset.offsetY=offY;
      im.style.transform=`scale(${scale}) translate(${offX}px, ${offY}px)`;
      if(persist)persistZoom(scale,offX,offY);
    }
    const apply=()=>clampAndApply(im,parseFloat(im.dataset.scale)||1,parseFloat(im.dataset.offsetX)||0,parseFloat(im.dataset.offsetY)||0);
    let isDragging=false,isMouseDown=false,dragStartX=0,dragStartY=0,lastX=0,lastY=0;
    on('wheel',ev=>{
      ev.preventDefault();ev.stopPropagation();let currentScale=parseFloat(im.dataset.scale)||1,offX=parseFloat(im.dataset.offsetX)||0,offY=parseFloat(im.dataset.offsetY)||0;
      if(ev.deltaY<0)currentScale*=1.1;else{currentScale/=1.1;if(currentScale<=1){currentScale=1;offX=0;offY=0;}}
      if(currentScale>MAX_SCALE)currentScale=MAX_SCALE;clampAndApply(im,currentScale,offX,offY,true);
    },{passive:false});
    const down=ev=>{if(ev.button!=null&&ev.button!==0)return;ev.stopPropagation();if((parseFloat(im.dataset.scale)||1)<=1)return;ev.preventDefault();isMouseDown=true;isDragging=false;dragStartX=lastX=ev.clientX;dragStartY=lastY=ev.clientY;};
    const move=ev=>{if(!isMouseDown)return;ev.preventDefault();ev.stopPropagation();if(Math.abs(ev.clientX-dragStartX)>dragThreshold||Math.abs(ev.clientY-dragStartY)>dragThreshold)isDragging=true;
      clampAndApply(im,parseFloat(im.dataset.scale)||1,(parseFloat(im.dataset.offsetX)||0)+(ev.clientX-lastX)*.5,(parseFloat(im.dataset.offsetY)||0)+(ev.clientY-lastY)*.5);lastX=ev.clientX;lastY=ev.clientY;};
    const endPan=()=>{if(!isMouseDown)return;isMouseDown=false;persistZoom(parseFloat(im.dataset.scale)||1,parseFloat(im.dataset.offsetX)||0,parseFloat(im.dataset.offsetY)||0);};
    if(!window.PointerEvent){on('mousedown',down);on('mousemove',move);on('mouseup',endPan);on('mouseleave',endPan);}
    // The reference's single-pointer drag also works on touch/pen devices.
    on('pointerdown',ev=>{down(ev);if(isMouseDown)box.setPointerCapture?.(ev.pointerId);});
    on('pointermove',move);on('pointerup',endPan);on('pointercancel',endPan);on('lostpointercapture',endPan);
    on('click',ev=>{
      ev.preventDefault();ev.stopPropagation();if(isDragging){isDragging=false;return;}
      const s0=parseFloat(im.dataset.scale)||1,offX0=parseFloat(im.dataset.offsetX)||0,offY0=parseFloat(im.dataset.offsetY)||0;
      const rect=box.getBoundingClientRect(),cx=rect.left+box.clientWidth/2,cy=rect.top+box.clientHeight/2;
      const px=(ev.clientX-cx)/s0-offX0,py=(ev.clientY-cy)/s0-offY0,targetOffX=-px,targetOffY=-py;
      const halfW=(im.clientWidth||120)/2,halfH=(im.clientHeight||120)/2;
      function requiredScaleFor(delta,half){const ratio=Math.abs(delta)/half;if(ratio>=1)return Infinity;return 1/(1-ratio);}
      const DESIRED=1.33;let s1=Math.max(DESIRED,s0*1.5,requiredScaleFor(targetOffX,halfW),requiredScaleFor(targetOffY,halfH));
      if(!Number.isFinite(s1))s1=MAX_SCALE;if(s1>MAX_SCALE)s1=MAX_SCALE;
      if(s0>1.2&&Math.abs(s1-s0)<.5)s1=1;
      clampAndApply(im,s1,targetOffX,targetOffY,true);
    });
    on('keydown',ev=>{
      if(!['Enter',' ','+','=','-','0','ArrowLeft','ArrowRight','ArrowUp','ArrowDown'].includes(ev.key))return;ev.preventDefault();ev.stopPropagation();
      let s=parseFloat(im.dataset.scale)||1,x=parseFloat(im.dataset.offsetX)||0,y=parseFloat(im.dataset.offsetY)||0;
      if(ev.key==='0'){s=1;x=y=0;}else if(['Enter',' ','+','='].includes(ev.key))s=Math.min(MAX_SCALE,s*1.5);else if(ev.key==='-')s=Math.max(1,s/1.1);else{x+=ev.key==='ArrowLeft'?5:ev.key==='ArrowRight'?-5:0;y+=ev.key==='ArrowUp'?5:ev.key==='ArrowDown'?-5:0;}
      clampAndApply(im,s,x,y,true);
    });
    const reset=box.closest('figure')?.querySelector('.thumbReset');if(reset){reset.hidden=false;reset.onclick=e=>{e.stopPropagation();clampAndApply(im,1,0,0,true);};}
    bound.set(box,{im,apply,dispose:()=>abort.abort()});observe(box);
  }
  return {bind,detach,clean,observe};
})();

// One viewport-driven loader for the three work queues. A listing photo is never
// replaced by a vector: these are separate, labelled sources for comparison.
const ListMedia = (() => {
  const jobs=new WeakMap(), watched=new Set(), queue=[], photos=new Map(), pending=new Map(), wanted=new Set();
  // Firebase is the shared source; this small local index makes refresh instant.
  // Image bytes use the seven-day HTTP cache, not another Etsy metadata lookup.
  const photoStorage='cn.listingPhotos.v1',photoAttempts=new Map(),photoChecks=new Map(),photoStates=new Map(),prepareQueue=new Set(),warming=new Set(),photoLoading=new Set();
  // Reassess legacy 25-call pauses once under the batch budget. Server-side
  // quota/cooldown checks remain authoritative, including genuine Etsy 429s.
  const photoPauseStorage='cn.listingPhotoPause.v2';
  let photoPauseUntil=0;try{photoPauseUntil=Number(localStorage.getItem(photoPauseStorage))||0;}catch(_){}
  try{for(const [id,url] of JSON.parse(localStorage.getItem(photoStorage)||'[]'))if(typeof url==='string'&&url)photos.set(id,url);}catch(_){}
  let preparing=null,photoTick=0;
  function remember(id,url){
    if(!url)return;photos.delete(id);photos.set(id,url);
    while(photos.size>1500)photos.delete(photos.keys().next().value);
    try{localStorage.setItem(photoStorage,JSON.stringify([...photos].filter(([,u])=>!!u)));}catch(_){}
  }
  const photoUrl=url=>url?('/.netlify/functions/imageProxy?url='+encodeURIComponent(url)):null;
  async function warm(url){
    if(!url||warming.has(url))return;warming.add(url);
    await new Promise(resolve=>{const img=new Image(),done=()=>{clearTimeout(timer);img.onload=img.onerror=null;resolve();},timer=setTimeout(done,10000);img.onload=img.onerror=done;img.src=url;});
  }
  function photoStatus(id){
    if(!id)return 'No listing linked';
    if(photoLoading.has(id))return loading;
    const state=photoStates.get(id);
    if(['empty','cached-empty'].includes(state))return 'No listing photo';
    if(['cache-unavailable','unconfigured'].includes(state))return 'Photo temporarily unavailable';
    if(photoPauseUntil>Date.now())return '<span>Photo lookup paused<br><small>Resumes '+esc(new Date(photoPauseUntil).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}))+'</small></span>';
    return state==='loading'?'Photo being prepared':'Awaiting photo preparation';
  }
  function photoTitle(id){
    if(photos.get(id))return '';
    return photoPauseUntil>Date.now()?'New Etsy photo lookups resume '+new Date(photoPauseUntil).toLocaleString()+'. Saved photos remain available.':'Missing photos are checked with the next ten-minute update.';
  }
  function acceptPhotos(got,ids){
    if(got.retryAt>Date.now()){photoPauseUntil=Math.max(photoPauseUntil,got.retryAt);try{localStorage.setItem(photoPauseStorage,String(photoPauseUntil));}catch(_){}}
    for(const id of ids){photoChecks.set(id,Date.now());photoStates.set(id,got.states?.[id]||'cache-only');if(got.images?.[id])remember(id,photoUrl(got.images[id]));}
  }
  function refreshPhotos(ids){
    for(const host of document.querySelectorAll('[data-listing]')){const job=jobs.get(host);if(!job||!ids.includes(job.key)||host.querySelector('img'))continue;
      if(photos.get(job.key))watch(host,()=>listing(job.key),job.key,true);
      else if(job.state==='done'){host.innerHTML=photoStatus(job.key);host.setAttribute('aria-busy',String(photoLoading.has(job.key)));host.title=photoTitle(job.key);}}
  }
  async function prepare(rows){
    for(const row of rows||[]){const id=String(row.line?.listingId||'');if(id)prepareQueue.add(id);}
    if(preparing||!S.cloud.ok)return preparing;
    preparing=(async()=>{
      const warmIds=new Set();
      while(prepareQueue.size){
        const ids=[...prepareQueue];prepareQueue.clear();ids.forEach(id=>warmIds.add(id));
        const missing=ids.filter(id=>!photos.get(id)&&(!photoChecks.has(id)||Date.now()-photoChecks.get(id)>=600000||Date.now()>=photoPauseUntil&&Date.now()-(photoAttempts.get(id)||0)>=600000));
        for(let i=0;i<missing.length;i+=100){
          const batch=missing.slice(i,i+100),live=Date.now()>=photoPauseUntil;
          // Cache recovery continues during a quota pause. A live batch spends
          // at most one Etsy call for 100 distinct missing listings.
          batch.forEach(id=>{photoLoading.add(id);if(live)photoAttempts.set(id,Date.now());});refreshPhotos(batch);
          try{const got=await api('charmNestLibrary',{op:'listingPhotos',listingIds:batch,prepare:live},{quiet:true});acceptPhotos(got,batch);}
          catch(e){batch.forEach(id=>{photoChecks.set(id,Date.now());photoStates.set(id,'cache-unavailable');});console.warn('Listing photo preparation:',e.message);}
          finally{batch.forEach(id=>photoLoading.delete(id));refreshPhotos(batch);}
        }
      }
      const available=[...warmIds].map(id=>photos.get(id)).filter(Boolean);let next=0;
      await Promise.all([0,1].map(async()=>{while(next<available.length)await warm(available[next++]);}));
    })().finally(()=>{preparing=null;if(prepareQueue.size)prepare([]);});
    return preparing;
  }
  function start(){clearInterval(photoTick);prepare(Orders.rows());photoTick=setInterval(()=>{if(!document.hidden)prepare(Orders.rows());},600000);}
  // a picture that failed while the network was down (a wake, a blip) loads again when the network is back or the tab is
  // looked at again, as the sheet previews do (CharmNestAssets); it said "Unavailable · Retry" until each one was clicked
  const retryFailed=()=>{if(document.hidden)return;for(const host of document.querySelectorAll('[data-vector],[data-listing]')){const job=jobs.get(host);if(job?.state==='error')watch(host,job.load,job.key,true,job.zoomKey);}};
  window.addEventListener('online',retryFailed);document.addEventListener('visibilitychange',retryFailed);
  let running=0, photoTimer=0, photoBusy=false;
  const observer=window.IntersectionObserver ? new IntersectionObserver(entries=>{
    for(const e of entries)if(e.isIntersecting){observer.unobserve(e.target);watched.delete(e.target);queue.push(e.target);}pump();
  },{rootMargin:"160px 0px"}) : null;
  const loading='<span class="thumbLoading" role="status"><i class="spin" aria-hidden="true"></i><span class="srOnly">Loading thumbnail</span></span>';
  function pair(row) {
    return `<div class="comparePair"><figure><span class="placementThumb" data-vector aria-label="Charm vector design" aria-busy="true">${loading}</span><figcaption>Vector design<button class="thumbReset" type="button" aria-label="Reset vector image zoom" title="Reset zoom" hidden>↺</button></figcaption></figure><figure><span class="placementThumb" data-listing aria-label="First Etsy listing image" aria-busy="true">${loading}</span><figcaption>Etsy listing<button class="thumbReset" type="button" aria-label="Reset Etsy image zoom" title="Reset zoom" hidden>↺</button></figcaption></figure></div>`;
  }
  function clean() {ListZoom.clean();for(const host of watched)if(!host.isConnected){observer?.unobserve(host);watched.delete(host);}}
  function watch(host,load,key,force=false,zoomKey=key) {
    if(!host)return;
    if(window.CharmNestInteraction?.defer(host,()=>watch(host,load,key,force,zoomKey)))return;
    const old=jobs.get(host);if(old?.key===key && !force){if(old.state==="done")ListZoom.bind(host,(host.hasAttribute('data-listing')?'listing:':'vector:')+old.zoomKey);if(old.state==="waiting" && observer && !watched.has(host)){watched.add(host);observer.observe(host);}return;}
    observer?.unobserve(host);watched.delete(host);
    ListZoom.detach(host);
    const job={load,key,zoomKey,state:"waiting"};jobs.set(host,job);
    host.innerHTML=loading;host.setAttribute('aria-busy','true');host.onclick=null;host.removeAttribute('role');host.removeAttribute('tabindex');host.onkeydown=null;
    if(observer){watched.add(host);observer.observe(host);}else{queue.push(host);pump();}
  }
  function pump() {
    while(running<4 && queue.length){
      const host=queue.shift(),job=jobs.get(host);if(!host.isConnected || !job || job.state!=="waiting")continue;
      // A queued row can have left its tab while another preview was loading.
      if(host.closest('.hidden,[hidden]')){if(observer){watched.add(host);observer.observe(host);}continue;}
      job.state="loading";running++;
      setTimeout(async()=>{
        try {
          const result=await job.load();if(!host.isConnected || jobs.get(host)!==job)return;
          if(typeof result==='string' && result){
            const img=document.createElement('img');img.alt=host.getAttribute('aria-label') || '';img.decoding='async';img.loading='eager';img.referrerPolicy='no-referrer';
            await new Promise((resolve,reject)=>{const timer=setTimeout(()=>{img.onload=img.onerror=null;reject(Error('Image timed out'));},20000);img.onload=()=>{clearTimeout(timer);resolve();};img.onerror=()=>{clearTimeout(timer);reject(Error('Image unavailable'));};img.src=cors(result);host.appendChild(img);});
            if(!host.isConnected || jobs.get(host)!==job)return;host.replaceChildren(img);
          }else if(result?.nodeType)host.replaceChildren(result);
          else if(host.hasAttribute('data-listing')){host.innerHTML=photoStatus(job.key);host.title=photoTitle(job.key);}else host.textContent='No vector available';
          ListZoom.bind(host,(host.hasAttribute('data-listing')?'listing:':'vector:')+job.zoomKey);
          job.state='done';
        }catch(_){
          if(host.isConnected && jobs.get(host)===job){job.state='error';host.textContent='Unavailable · Retry';host.setAttribute('role','button');host.tabIndex=0;
            host.onclick=e=>{e.stopPropagation();watch(host,job.load,job.key,true,job.zoomKey);};host.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();e.stopPropagation();watch(host,job.load,job.key,true,job.zoomKey);}};}
        }finally{if(jobs.get(host)===job){if(job.state==='loading'){job.state='waiting';if(observer){watched.add(host);observer.observe(host);}}else host.setAttribute('aria-busy',String(host.hasAttribute('data-listing')&&photoLoading.has(job.key)));}running--;pump();}
      },0);
    }
  }
  function flushPhotos() {
    if(photoBusy || photoTimer || !wanted.size)return;
    photoTimer=setTimeout(async()=>{
      photoTimer=0;photoBusy=true;
      const ids=[...wanted].slice(0,100);ids.forEach(id=>wanted.delete(id));
      try {
        const got=await api('charmNestLibrary',{op:'listingPhotos',listingIds:ids},{quiet:true});
        acceptPhotos(got,ids);for(const id of ids)pending.get(id)?.resolve(photos.get(id)||null);
      }catch(e){for(const id of ids)pending.get(id)?.reject(e);}
      finally{ids.forEach(id=>pending.delete(id));photoBusy=false;flushPhotos();}
    },80);
  }
  function listing(id) {
    id=String(id || '');if(!id)return Promise.resolve(null);
    if(photos.get(id))return Promise.resolve(photos.get(id));
    if(photoChecks.has(id)&&Date.now()-photoChecks.get(id)<600000)return Promise.resolve(null);
    if(pending.has(id))return pending.get(id).promise;
    let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});pending.set(id,{promise,resolve,reject});wanted.add(id);flushPhotos();return promise;
  }
  const catalog=new Map();
  async function vector(row) {
    if(!row || row.spec?.noDesign)return null;
    const charm=(row.poolIds || []).map(id=>Pool.charmOf(id)).find(c=>c?.outline && c.members?.length);
    if(charm)return P.frontPreview ? P.frontPreview(charm,220) : Engrave.renderFront(charm,220);
    const sku=row.spec?.designSku || row.line?.sku;if(!sku)return null;
    let entry=Master.entryFor(sku);
    if(!entry){if(!catalog.has(sku))catalog.set(sku,Master.fetchEntry(sku).finally(()=>catalog.delete(sku)));entry=await catalog.get(sku);}
    if(!entry)return null;
    return Pool.masterPreview(entry,row.spec?.size);
  }
  function mount(node,row) {
    clean();const sku=row?.spec?.designSku || row?.line?.sku || '',lid=String(row?.line?.listingId || '');
    watch(node.querySelector('[data-vector]'),()=>vector(row),JSON.stringify([sku,row?.spec?.size,row?.poolIds,!!Master.entryFor(sku),Master.entryFor(sku)?.updatedAt,!!(row?.poolIds || []).find(id=>Pool.charmOf(id)?.outline)]),false,JSON.stringify([sku,row?.spec?.size]));
    watch(node.querySelector('[data-listing]'),()=>listing(lid),lid);
  }
  // Paged DOM construction as well as deferred image decoding. The observer
  // honours nested scroll containers, including the dock and hidden tabs.
  const pages=new Map();
  function more(host,total,shown,expand) {
    for(const [node,io] of pages)if(!node.isConnected){io.disconnect();pages.delete(node);}
    pages.get(host)?.disconnect();pages.delete(host);host.querySelector(':scope > .listMore')?.remove();
    if(shown>=total)return;
    const button=el('button','btn ghost listMore');button.type='button';button.textContent=`Show more · ${shown} of ${total}`;
    let active=true;const go=()=>{if(!active)return;active=false;io?.disconnect();button.innerHTML='<i class="spin" aria-hidden="true"></i> Loading rows…';requestAnimationFrame(expand);};
    const io=window.IntersectionObserver ? new IntersectionObserver(es=>{if(es.some(e=>e.isIntersecting))go();},{rootMargin:'160px'}) : null;
    button.onclick=go;host.appendChild(button);if(io){pages.set(host,io);io.observe(button);}
  }
  return {pair,mount,watch,more,listing,prepare,start,peek:id=>photos.get(String(id || '')) || null};
})();
const clockFormat = new Intl.DateTimeFormat([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });   // made once: a formatter costs far more to make than to use
const fmtT = t => clockFormat.format(new Date(t));
const sleep = ms => new Promise(r => setTimeout(r, ms));
// only for someone not looking at the sorter, as the sheet-complete alert does: on screen, the toast already says it
const notifyPerson = (title, body) => { if (document.hidden && S.settings.notify === "on" && "Notification" in window && Notification.permission === "granted") { try { new Notification(title, { body }); } catch (_) {} } };
const employeeName = () => B.employee || (B.link && B.link.state() && B.link.state().employee) || "";
function askEmployee() {
  const cur = employeeName();
  const v = prompt("Your name — recorded with every approval and decision:", cur || "");
  if (v && v.trim()) { B.employee = v.trim(); localStorage.setItem("cn.employee", B.employee); }
  return employeeName();
}
/* One drawing a frame. The run banner, the rail's strip and ladder and the Orders tab were each drawn in full at every
   call, and a run step, a poke, an arrival, a log line or a pool pass called them, often several times in one go. A
   request now draws once, at the next animation frame. A hidden tab gets its frames from the page clock's 16 ms timer
   (charm-nest-clock.js swaps requestAnimationFrame while the tab is hidden, which is why it is looked up at each call);
   without the clock the browser holds them until the tab is shown, and a request is kept once, not piled up. */
const CNFrame = window.CNFrame = (() => {
  const due = new Map(); let asked = false;
  // the banner draws the strip with itself, and the strip the ladder: asked for, they wait behind it, which draws them
  // (and takes them off), so each is drawn once in a frame whichever was asked for first
  const WITH = { banner: ["strip", "ladder"], strip: ["ladder"] };
  // a drawing asked for by another in the same frame is drawn in it too; one asked for again after it ran waits a frame
  function flush() { asked = false; const ran = new Set(); for (const [key, f] of due) { if (ran.has(key)) continue; ran.add(key); due.delete(key); try { f(); } catch (e) { console.error(e); } } }
  function later(key, f) {
    due.set(key, f);
    for (const k of WITH[key] || []) if (due.has(k)) { const g = due.get(k); due.delete(k); due.set(k, g); }
    if (asked) return; asked = true; if (typeof requestAnimationFrame === "function") requestAnimationFrame(flush); else setTimeout(flush, 16);
  }
  return { later, cancel: key => due.delete(key) };
})();

/* ═══ 17 · DesignLink — the Design Station as a slave ═════════════════════ */
const LiveStrip = window.LiveStrip = (() => {
  const rows = [];
  function push(ev) { rows.push({ t: ev.t || Date.now(), kind: ev.kind, text: ev.text || (ev.html ? ev.html.replace(/<[^>]+>/g, "") : "") }); if (rows.length > 40) rows.shift(); render(); }
  // every log line and state change asked for this: drawn once a frame (CNFrame); the banner draws it with itself (now)
  const render = () => CNFrame.later("strip", now);
  function now() {
    CNFrame.cancel("strip");
    const b = document.getElementById("railRecentBody");
    // newest first, and only drawn while the disclosure is open — it used to be a permanent band that clipped the newest line
    if (b && b.parentElement && b.parentElement.open) b.innerHTML = rows.length ? rows.slice().reverse().map(r => `<div><i>${fmtT(r.t)}</i><b>${esc(r.kind)}</b> ${esc(r.text).slice(0, 160)}</div>`).join("") : "<div>nothing yet</div>";
    const n = Review.count(); const rb = document.getElementById("tabReviewN"); if (rb) rb.textContent = n ? String(n) : "";
    const eb = document.getElementById("tabEngraveN"); if (eb) { const k = Engrave.pendingCount(); eb.textContent = k ? String(k) : ""; }
    if (window.Ladder) Ladder.now();
  }
  return { push, render, now, rows };
})();

/* ═══ 17b · Ladder — where the work is, on every screen ══════════════════════
   The one thing the station never said: what has been approved, what is where, what is set. It was eleven nine-pixel
   squares whose meaning lived in a title attribute, a scrolling ticker of the five most recent lines, and four badges
   spread across four tabs — so answering "where are we" meant a tour of the app and some mental arithmetic.
   The ladder is that answer, in the rail, on all eight tabs, built from state the app already holds. Seven steps a
   person would name, each with the evidence for it; the step that is waiting on someone is the only one with a
   background, and it is a button that goes to the screen that settles it. */
const Ladder = window.Ladder = (() => {
  const STEPS = [
    { id: "pull", label: "Pull", covers: ["pull", "claim"], tab: "orders",
      count: () => { const n = Orders.rows().filter(r => r.state !== "gone").length; return n ? `${n} line${n === 1 ? "" : "s"}` : ""; } },
    { id: "pool", label: "Pool", covers: ["pool", "plan"], tab: "orders",
      count: () => { const n = Orders.rows().filter(r => r.poolIds && r.poolIds.length).length; return n ? `${n} on the cards` : ""; } },
    { id: "nest", label: "Nest", covers: ["nest"], tab: "nest",
      count: () => { const sh = allSheets().filter(p => p.runId && B.run && p.runId === B.run.runId); const done = sh.filter(p => ["complete", "partial"].includes(p.status)).length; return sh.length ? `${done} of ${sh.length} sheet${sh.length === 1 ? "" : "s"}` : ""; } },
    { id: "checkpoint", label: "Check", covers: ["checkpoint"], tab: "nest",
      count: () => { const bad = allSheets().filter(p => p.runId && B.run && p.runId === B.run.runId && p.verification && !p.verification.ok).length; return bad ? `${bad} to look at` : ""; } },
    { id: "engrave", label: "Engrave", covers: ["engrave", "revalidate"], tab: "engrave",
      count: () => { const k = Engrave.pendingCount(); return k ? `${k} to settle` : (Engrave.reviewedCount() ? `${Engrave.reviewedCount()} decided` : ""); },
      waits: () => Engrave.pendingCount() },
    { id: "labels", label: "Labels", covers: ["labels"], tab: "nest",
      count: () => { const n = B.run && Sets.ofRun ? Sets.ofRun(B.run.runId).reduce((k, s2) => k + (s2.labelFiles ? s2.labelFiles.length : 0), 0) : 0; return n ? `${n} saved` : ""; } },
    { id: "commit", label: "Commit", covers: ["commit", "complete"], tab: "orders",
      count: () => { const out = (B.run && B.run.lineArchive && B.run.lineArchive.base) || {}; const n = (B.run && B.run.committed ? B.run.committed.length : 0) + (+out.committed || 0); const h = (B.run && B.run.holds ? Object.keys(B.run.holds).length : 0) + (+out.held || 0); return n || h ? `${n} committed${h ? ` · ${h} held` : ""}` : ""; } },
  ];
  const WORD = { running: ["Running", "go"], review: ["Waiting on you", "wait"], paused: ["Paused", "wait"], stopped: ["Stopped", "stop"], processed: ["Processing complete", "wait"], complete: ["Done", "go"] };
  const wordOf = r => r.status === "processed" && r.arrivalBusy ? ["Adding new orders", "go"] : WORD[r.status] || ["Running", "go"];   // as the banner says it
  const mount = () => document.getElementById("ladder");
  /** Which of the seven the run is standing on, and which of them is waiting on a person. */
  function shape() {
    const r = B.run;
    const idx = r ? O.stepIndex(r.step) : -1;
    const here = r ? STEPS.findIndex(s => s.covers.includes(r.step)) : -1;
    const revN = Review.count(), engN = Engrave.pendingCount();
    return { r, idx, here, revN, engN };
  }
  const render = () => CNFrame.later("ladder", now);   // once a frame (CNFrame); the strip draws it with itself
  function now() {
    CNFrame.cancel("ladder");
    const host = mount(); if (!host) return;
    const { r, here, revN, engN } = shape();
    if (!r) {
      // no run: the ladder still answers "what is here" — the library, the pull, and what the library is missing
      const pulled = Orders.rows().filter(x => x.state !== "gone").length;
      const miss = Master.missingCount ? Master.missingCount() : 0;
      host.innerHTML = `<div class="ldBand ldIdle"><b>No run open</b><span>${B.master.entries.size} SKU${B.master.entries.size === 1 ? "" : "s"} in the library</span></div>`
        + `<div class="ldRows">`
        + `<button class="ldRow" data-tab="orders" title="the orders on the cards"><i class="g ${pulled ? "done" : "todo"}"></i><span class="n">Orders</span><span class="c">${pulled ? `${pulled} line${pulled === 1 ? "" : "s"} pulled` : "nothing pulled"}</span></button>`
        + (miss ? `<button class="ldRow wait" data-tab="master" title="SKUs the orders want that no master file holds"><i class="g stop"></i><span class="n">Library</span><span class="c">${miss} SKU${miss === 1 ? "" : "s"} missing</span></button>` : "")
        + `</div>`;
      wire(host); return;
    }
    const [word, tone] = wordOf(r);
    const waitRow = r.status === "review" ? (engN ? "engrave" : "pull") : null;
    host.innerHTML = `<div class="ldBand ld-${tone}" title="run ${esc(r.runId)}"><b>${esc(word)}</b><span>${esc(r.setId || r.day || "")}</span></div>`
      + `<div class="ldRows">` + STEPS.map((s, i) => {
        const state = ["complete","processed"].includes(r.status) || i < here ? "done" : i === here ? (r.status === "stopped" ? "stop" : "now") : "todo";
        const waiting = (s.waits && s.waits()) || (s.id === waitRow && revN);
        const c = s.count() || "";
        return `<button class="ldRow${waiting ? " wait" : ""}${state === "now" ? " now" : ""}" data-tab="${s.tab}" title="${esc(s.label)}${c ? " — " + esc(c) : ""}"><i class="g ${state}"></i><span class="n">${esc(s.label)}</span><span class="c">${esc(c)}</span></button>`;
      }).join("") + `</div>`
      + (revN || engN ? `<div class="ldWaits">${revN ? `<button class="ldChip warn" data-tab="review" title="decisions a person must make">Review<b>${revN}</b></button>` : ""}${engN ? `<button class="ldChip info" data-tab="engrave" title="engraving still to be settled">Engraving<b>${engN}</b></button>` : ""}</div>` : "");
    wire(host);
  }
  function wire(host) {
    host.querySelectorAll("[data-tab]").forEach(b => b.onclick = () => { setMode(b.dataset.tab); if (b.dataset.tab === "engrave" && window.Engrave) Engrave.render(); });
  }
  return { render, now, STEPS, word: r => wordOf(r)[0] };
})();

const DesignLink = window.DesignLink = (() => {
  const S_ = { frame: null, nonce: null, id: 0, pending: new Map(), state: null, log: [], logBuf: [], up: false, control: false, misses: 0, hb: null, flushT: null, dropped: 0, lastHello: 0, count: 0, replies: 0, errors: 0 };
  const origin = () => (S.settings.dsOrigin || DEFAULTS.dsOrigin).replace(/\/+$/, "");
  const frameUrl = () => `${origin()}/design-1.html?bridge=1${WORKSPACE_SANDBOX ? "&sandbox=1" : "&sandbox=0"}`;
  function mount(host) {
    if (S_.frame) return S_.frame;
    // the frame lives in a fixed dock on <body>, never inside a tab: re-parenting an iframe reloads it and would end the
    // session, so the dock is laid over the Design Station tab's placeholder when that tab shows and shrinks to a
    // picture-in-picture panel on every other tab (design §5.7 live view)
    const dock = Dock.ensure();
    const f = document.createElement("iframe"); f.id = "dsFrame"; f.title = "Design Station 1"; f.allow = "clipboard-read; clipboard-write"; f.src = frameUrl();
    const veil = el("div", "veil", `<div>Design Station<br><span style="font-size:12.5px;color:var(--ink45)">press <b>Take control</b> to open the session</span></div>`);
    dock.body.append(f, veil); Dock.setHost(host);
    S_.frame = f; S_.veil = veil;
    // the first hello must wait for the frame to navigate (a message posted to the blank frame is lost); later loads (a reload
    // of the station) re-open the session by themselves
    S_.loaded = new Promise(resolve => f.addEventListener("load", () => resolve(), { once: true }));
    f.addEventListener("load", () => {
      S_.loadedAt = Date.now(); S_.loads = (S_.loads || 0) + 1; agent({ bridge: true }, "DS", `Design Station frame loaded (${frameUrl()})`);
      // A reloaded station has forgotten every command it was working on: nothing will answer them. They used to wait out
      // their own time limits (a new-orders check 20 minutes, a commit 3 minutes) with the intake or the run held behind them.
      if (S_.loads > 1) for (const [id, p] of [...S_.pending]) { if (p.type === "hello") continue; clearTimeout(p.t); S_.pending.delete(id); p.reject(new Error(`${p.type}: the Design Station reloaded before it answered — no reply`)); }
      if (S_.control && S_.loadedAt - S_.mountedAt > 500) open().catch(e => agent({ bridge: true }, "warn", `hello after reload failed: ${e.message}`));
    });
    S_.mountedAt = Date.now();
    window.addEventListener("message", onMessage);
    window.addEventListener("online", () => { if (S_.control && !S_.up) reloadFrame("the network is back"); });
    return f;
  }
  /* A frame that loaded while the network was down stays on the browser's error page: only "Reload frame" loaded it again,
     and every order check and run step waited 45 s on it and failed. It is loaded again when two hellos go unanswered,
     when the network comes back and when the heartbeat has missed for over 30 s, at most once in 45 s; its load listener
     opens the session again. A station that has said anything since it loaded is alive, maybe busy with a person's work,
     and is never reloaded under them: only one silent since its load (an error page, a page that never started) or
     silent for 5 minutes. */
  function reloadFrame(why) {
    if (!S_.frame || S_.up || navigator.onLine === false || Date.now() - (S_.reloadedAt || 0) < 45000) return false;
    if ((S_.heardAt || 0) > (S_.loadedAt || 0) && Date.now() - S_.heardAt < 300000) return false;
    S_.reloadedAt = Date.now(); agent({ bridge: true }, "warn", `Loading the Design Station frame again: ${why}`);
    S_.frame.src = frameUrl(); return true;
  }
  async function open() {
    if (!S_.frame) throw new Error("the Design Station frame is not mounted");
    if (!S_.nonce) S_.nonce = uid() + uid();
    S_.control = true;
    if (S_.loaded) await Promise.race([S_.loaded, sleep(20000)]);
    let st;
    try { st = await call("hello", { sorterClientId: S_.nonce, runId: B.run ? B.run.runId : null }, { timeoutMs: 15000 }); }
    catch (e) { if (!/answer in time/.test(e.message)) throw e; agent({ bridge: true }, "warn", "hello unanswered — trying once more");
      // (a session opened meanwhile by the frame's reload is the answer; otherwise the frame is loaded again)
      try { st = await call("hello", { sorterClientId: S_.nonce, runId: B.run ? B.run.runId : null }, { timeoutMs: 30000 }); } catch (e2) { if (S_.up && S_.state) return S_.state; reloadFrame("two hellos went unanswered"); throw e2; } }
    S_.state = st; S_.up = true; S_.misses = 0; S_.lastHello = Date.now(); if (st.etsy && st.etsy.meter) etsyReadout(st.etsy.meter);
    if (!!st.sandbox !== (WORKSPACE_SANDBOX)) { S_.control = false; S_.up = false; const why = `the station is in ${st.sandbox ? "SANDBOX" : "production"} mode but this sorter is in ${WORKSPACE_SANDBOX ? "SANDBOX" : "production"} mode`; agent({ bridge: true }, "warn", `Session refused: ${why}`); toast(`Session refused — ${why}. Reload the frame.`, "bad", 9000); throw new Error(why); }
    if (st.employee && !B.employee) { B.employee = st.employee; localStorage.setItem("cn.employee", st.employee); }
    S_.veil && S_.veil.classList.add("hidden"); Dock.layout();
    agent({ bridge: true }, "DS", `Session ${S_.nonce.slice(0, 4)} open on ${st.bench} · ${st.counts.open} open orders (${st.counts.hydrated} read) · ${st.selection.length} selected · Etsy ${st.etsy.signedIn ? "signed in" : "NOT signed in"}${st.releasedFromPreviousSession && st.releasedFromPreviousSession.length ? ` · released ${st.releasedFromPreviousSession.length} lock(s) from a previous session` : ""}`);
    if (!st.etsy.signedIn) toast("The Design Station is not signed in to Etsy — press Connect Etsy", "bad", 8000);
    startHeartbeat(); renderConsole(); if (B.run && window.RunCtl) RunCtl.renderBanner();   // a stopped run's banner says what is left to press
    api("charmNestLibrary", { op: "bridgeLog", session: S_.nonce, rows: [], meta: { sorterClientId: S_.nonce, bench: st.bench, startedAt: Date.now(), version: st.version } }).catch(() => {});
    return st;
  }
  function call(type, args = {}, { timeoutMs = 120000, onProgress, quiet = false } = {}) {
    if (!S_.frame || !S_.frame.contentWindow) return Promise.reject(new Error(`${type}: the Design Station frame is not open`));
    if (!S_.nonce) S_.nonce = uid() + uid();
    const id = ++S_.id;
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => { S_.pending.delete(id); if (!quiet) { logLine("reply", type, { error: "timeout" }, timeoutMs, true); S_.errors++; } reject(new Error(`${type}: the Design Station did not answer in time`)); }, timeoutMs);
      S_.pending.set(id, { resolve, reject, t, onProgress, type, sent: performance.now(), quiet });
      if (!quiet) { logLine("cmd", type, args); S_.count++; }
      S_.frame.contentWindow.postMessage({ source: "brites-sorter", nonce: S_.nonce, id, type, args }, origin());
    });
  }
  function onMessage(ev) {
    if (ev.origin !== origin()) { S_.dropped++; return; }
    S_.heardAt = Date.now();   // the station is alive (reloadFrame)
    const d = ev.data; if (!d || d.source !== "brites-design") return;
    if (d.type === "etsy.connected") { onEtsyConnected(d); return; }
    if (d.nonce !== S_.nonce) { S_.dropped++; renderConsole(); return; }
    if (d.id === 0) { onEvent(d); return; }
    const p = S_.pending.get(d.id); if (!p) return;
    if (d.type === "progress") { p.onProgress && p.onProgress(d); if (d.text) agentLive(`DS · ${p.type}`, d.text, d.done, d.total); return; }
    if (d.type === "ack") { p.acked = performance.now(); return; }
    clearTimeout(p.t); S_.pending.delete(d.id); if (!p.quiet) S_.replies++;
    if (!p.quiet) logLine("reply", p.type, d.type === "done" ? d.result : { error: d.error }, performance.now() - p.sent, d.type !== "done");
    if (d.type === "done") p.resolve(d.result); else { if (!p.quiet) S_.errors++; p.reject(new Error(d.error || `${p.type} failed`)); }
  }
  /* ── Connect Etsy from the sorter. Etsy refuses to load inside a frame, so the station is opened in its own popup on
        its origin (a click is needed — browsers block popups otherwise); the popup signs in, the token lands in the
        station origin's storage, the framed copy sees it, the popup reports back and closes, and a run that stopped for
        the sign-in resumes on its own. ── */
  let connectWin = null;
  async function connectEtsy() {
    if (!S_.frame) { toast("Open the Design Station tab first", "bad"); return null; }
    if (!S_.control) { try { await open(); } catch (e) { toast("Could not open the session: " + e.message, "bad", 6000); return null; } }
    const r = await call("etsy.connect", {}, { timeoutMs: 15000 });
    // signed in again by itself (its token refreshed once the network was back): a run stopped for the sign-in carries on,
    // as it does when the popup reports back; it used to answer "already signed in" and stay stopped
    if (r.signedIn && B.run && B.run.status === "stopped" && /\bsign/i.test(B.run.stoppedBy || "")) { await onEtsyConnected(r); return r; }
    if (r.signedIn) { toast("The Design Station is already signed in to Etsy", "ok"); return r; }
    connectWin = window.open(r.url, "britesEtsyConnect", "popup,width=640,height=780");
    if (!connectWin) { toast("The browser blocked the sign-in window — allow popups for this site and press Connect Etsy again", "bad", 9000); agent({ bridge: true }, "warn", "Connect Etsy: popup blocked"); return r; }
    agent({ bridge: true }, "DS", `Connect Etsy: the station opened in its own window (${r.url}) — sign in there; the sorter carries on when it reports back`);
    renderConsole();
    return r;
  }
  async function onEtsyConnected(d) {
    agent({ bridge: true }, "DS", `Etsy connected at the station${d.etsy && d.etsy.expiresAt ? ` · token to ${new Date(d.etsy.expiresAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}` : ""}`);
    toast("Design Station signed in to Etsy", "ok");
    connectWin = null;
    if (S_.control) { try { const st = await open(); if (st.etsy && st.etsy.signedIn && B.run && B.run.status === "stopped" && /\bsign/i.test(B.run.stoppedBy || "") /* \b: "Design Station" is not a sign-in */) { agent({ run: B.run.runId }, "DS", "Resuming the run now that the station is signed in"); RunCtl.resume(); } } catch (e) { agent({ bridge: true }, "warn", `re-hello after the sign-in failed: ${e.message}`); } }
    renderConsole();
  }
  let liveEv = null;
  function agentLive(label, text, done, total) {
    const html = `<b>${esc(label)}</b> ${esc(text)}${total ? ` <i>${done}/${total}</i>` : ""}`;
    if (!liveEv || !liveEv.live) liveEv = agent({ bridge: true }, "DS", text, { live: true, html });
    else agentUpdate(liveEv, { html, text });
    if (total && done >= total) { agentUpdate(liveEv, { live: false }); liveEv = null; }
  }
  function onEvent(d) {
    logLine("evt", d.type, d);
    if (d.type === "activity") agent({ bridge: true }, "DS", d.text, { fromStation: true });
    else if (d.type === "selection") S_.state = Object.assign({}, S_.state, { selection: d.selected });
    else if (d.type === "filters") S_.state = Object.assign({}, S_.state, { filters: d.filters });
    else if (d.type === "orders.changed") { Orders.markStale(); agent({ bridge: true }, "DS", `Station refreshed its order list: ${d.open} open`); }
    else if (d.type === "lock.changed") agent({ bridge: true }, "DS", `Locks changed at the station: ${Object.keys(d.locks || {}).length} locked elsewhere, ${Object.keys(d.claims || {}).length} claimed`);
    else if (d.type === "complete.done") agent({ bridge: true }, "DS", `Station completed ${(d.completed || []).length} order(s)${d.setId ? ` for ${d.setId}` : ""}`);
    else if (d.type === "ui.modal") agent({ bridge: true }, "DS", d.open ? `Station opened item ${d.transactionId} of order ${d.receiptId}` : "Station closed its dialog");
    else if (d.type === "error") agent({ bridge: true }, "warn", `Station reported: ${d.command} — ${d.error}`);
    else if (d.type === "etsy.alarm") { etsyReadout(d); }
    else if (d.type === "etsy.connected") { onEtsyConnected(d); }
    // (only a run at work is stopped: one resting between updates, or complete, used to be turned into a stopped one, and a
    // complete one stopped kept Auto from starting the next run)
    else if (d.type === "etsy.signin") { agent({ bridge: true }, "warn", "The station needs an Etsy sign-in — press Connect Etsy"); toast("Design Station: press Connect Etsy on the run banner or the Design Station tab", "bad", 8000); RunCtl.stopIfRunning("the Design Station is not signed in to Etsy", "Press Connect Etsy: the station opens in its own window to sign in, then the run resumes by itself.", "auth"); }
    else if (d.type === "state" && d.ended) { S_.up = false; S_.control = false; stopHeartbeat(); S_.veil && S_.veil.classList.remove("hidden"); agent({ bridge: true }, "warn", `Station ended the session: ${d.reason}`); if (B.run && B.run.status === "running") RunCtl.stop(`the Design Station ended the session (${d.reason})`, "Press Take control, then Resume."); }
    renderConsole();
  }
  function slimForLog(payload) {
    if (payload == null) return null;
    const o = {};
    if (Array.isArray(payload.receiptIds)) o.receiptIds = payload.receiptIds.slice(0, 40);
    for (const k of ["receiptId", "transactionId", "total", "hydrated", "runId", "on", "error", "text", "count", "open", "reason", "command"]) if (payload[k] != null) o[k] = typeof payload[k] === "string" ? payload[k].slice(0, 120) : payload[k];
    if (Array.isArray(payload.orders)) o.orders = payload.orders.length;
    if (Array.isArray(payload.selected)) o.selected = payload.selected.length;
    if (Array.isArray(payload.completed)) o.completed = payload.completed.length;
    if (Array.isArray(payload.refused)) o.refused = payload.refused.length;
    if (Array.isArray(payload.jobs)) o.jobs = payload.jobs.length;
    if (payload.labels && payload.labels.files) o.labels = payload.labels.files.length;
    if (payload.counts) o.counts = payload.counts;
    return o;
  }
  function logLine(dir, type, payload, ms, err) {
    const row = { t: Date.now(), dir, type, ms: ms == null ? null : Math.round(ms), payload: slimForLog(payload), err: !!err };
    S_.log.push(row); if (S_.log.length > 400) S_.log.shift();
    S_.logBuf.push(row);
    if (!S_.flushT) S_.flushT = setTimeout(flushLog, 2500);
    renderConsole();
  }
  // (rows wait in the buffer while the cloud is offline, the newest thousand of them, and go when it is back: they were
  // taken out first and dropped)
  function flushLog() { S_.flushT = null; if (!S.cloud.ok || !S_.nonce) { S_.logBuf.splice(0, S_.logBuf.length - 1000); return; } const rows = S_.logBuf.splice(0, 200); if (!rows.length) return; api("charmNestLibrary", { op: "bridgeLog", session: S_.nonce, rows, meta: { dropped: S_.dropped } }).catch(() => {}); if (S_.logBuf.length) S_.flushT = setTimeout(flushLog, 2500); }
  function startHeartbeat() {
    stopHeartbeat();
    const every = Math.max(2, +S.settings.heartbeatS || 5) * 1000;
    S_.hb = setInterval(async () => {
      if (!S_.control) return;
      try {
        const pong = await call("ping", {}, { timeoutMs: Math.max(1500, every - 500), quiet: true }); if (pong && pong.etsy) etsyReadout(pong.etsy); if (!S_.up) { S_.up = true; agent({ bridge: true }, "DS", "Design Station is back"); } S_.misses = 0;
        // In Auto nobody may be at the bench: a run stopped only because the station went quiet, or because the hour's
        // Etsy calls ran out, carries on by itself once the station answers again or the hour has room. It used to wait
        // for someone to press Resume while the orders piled up.
        const r = B.run, why = r?.status === "stopped" ? r.stoppedBy || "" : "";
        if (why && S.settings.runMode === "auto" && (/stopped answering the heartbeat/.test(why) || /^Etsy call budget reached/.test(why) && hourCalls() < etsyCap() * 0.9)) { agent({ run: r.runId }, "DS", `Resuming by itself: ${why} — cleared`); RunCtl.resume().catch?.(() => {}); }
        else if (why) RunCtl.autoResume();   // a passing failure, the Etsy brake lifted, a reload: Auto carries on once it has cleared
      }
      catch (_) { S_.misses++; if (S_.misses >= (+S.settings.heartbeatMiss || 3) && S_.up) { S_.up = false; S_.downAt = Date.now(); agent({ bridge: true }, "warn", `Design Station down — ${S_.misses} heartbeats missed`); if (B.run && B.run.status === "running") RunCtl.stop("the Design Station stopped answering the heartbeat", "Check the Design Station tab; when it answers again, press Resume."); }
        if (!S_.up && S_.misses >= 3 && Date.now() - (S_.downAt || 0) > 30000) reloadFrame("it has not answered the heartbeat for 30 s"); }
      renderConsole(); Dock.schedule();
    }, every);
  }
  function stopHeartbeat() { if (S_.hb) clearInterval(S_.hb); S_.hb = null; }
  /* ── the Etsy meter and budget: every reply that cost Etsy calls says so; the sorter keeps a rolling hour of them and
        refuses to start an Etsy-touching step past the cap (Settings → Etsy calls per hour). The station's own brakes
        (250 ms pacing, 429 backoff, the detail cache, the sweep cooldown) still apply underneath. ── */
  const E_ = { window: [], sessionTotal: 0, stationTotal: 0, station: null, alarmed: null };
  /** The station's ledger is the truth (only the station calls Etsy): every ping and every Etsy-touching reply carries it. */
  function etsyReadout(m) {
    if (!m) return; E_.station = m; E_.stationTotal = m.total;
    const el = document.getElementById("etsyPill"), n = document.getElementById("etsyPillN"); if (!el || !n) return;
    n.textContent = `${S_.state && S_.state.sandbox ? "emulated · " : ""}today ${m.today} · 10m ${m.last10Min} · 1m ${m.lastMinute}`;
    const hot = m.last10Min >= m.guard.per10Min * 0.7 || m.lastMinute >= m.guard.burstPerMinute * 0.7;
    const alert = document.getElementById("etsyAlert"); if (alert) { alert.classList.toggle("hidden", !m.braked && !hot); alert.title = m.braked ? "Etsy API paused — open Workspace for details" : "Etsy API nearing its limit"; }
    el.classList.toggle("alarm", !!m.braked); el.classList.toggle("warn", !m.braked && hot);
    // a rate meter is something you look at when something is wrong: in the normal state it holds no space, and the full
    // readout stays on the Design Station panel where it always was
    el.classList.toggle("quiet", !m.braked && !hot && !(S_.state && S_.state.sandbox));
    el.title = m.braked ? `Etsy watchdog at the station: ${m.alarm && m.alarm.why} — automatic Etsy work paused until ${new Date(m.brakeUntil).toLocaleTimeString()}` : `Etsy calls counted by the Design Station: ${m.total} this session · ${m.lastMinute} in the last minute · ${m.last10Min} in the current 10-minute window · ${m.lastHour} in the last hour · ${m.today} today · peak ${m.maxQps}/s · ${m.status429} rate-limit answers. Guard: ${m.guard.burstPerMinute}/min, ${m.guard.per10Min}/10 min, ${m.guard.sameOrderPer10Min} reads of one order/10 min.`;
    if (m.braked && (!E_.alarmed || E_.alarmed !== m.alarm.at)) { E_.alarmed = m.alarm.at; agent({ bridge: true }, "warn", `Etsy watchdog at the station: ${m.alarm.why} — automatic Etsy work is paused for ${Math.round(m.guard.brakeMs / 60000)} min`); if (B.run && ["running", "paused", "review"].includes(B.run.status)) RunCtl.stop(`Etsy watchdog: ${m.alarm.why}`, `The station paused automatic Etsy work until ${new Date(m.brakeUntil).toLocaleTimeString()}. Check the API meter on both apps, then Resume.`, null, "watchdog"); }
  }
  function meter(reply, what) {
    const n = reply && Number(reply.etsyCalls) || 0;
    if (reply && reply.etsy && reply.etsy.meter) etsyReadout(reply.etsy.meter);
    if (reply && reply.etsy && reply.etsy.calls != null) E_.stationTotal = reply.etsy.calls;
    if (n > 0) { E_.window.push({ t: Date.now(), n }); E_.sessionTotal += n; }
    const cut = Date.now() - 3600000; while (E_.window.length && E_.window[0].t < cut) E_.window.shift();
    agent({ bridge: true }, n > 12 ? "warn" : "DS", `Etsy: ${n} call${n === 1 ? "" : "s"} for ${what}${(reply && reply.refreshSkipped) || (reply && reply.swept === false) ? " (open list reused — swept under 90 s ago)" : ""} · ${hourCalls()} this hour · ${E_.stationTotal} this station session`);
    renderConsole();
    return n;
  }
  // the hour rolls over here as well: a budget spent stopped every Etsy step, so meter() never ran again to drop the old
  // calls, and calls from before a sleep still counted as this hour
  const hourCalls = () => { const cut = Date.now() - 3600000; while (E_.window.length && E_.window[0].t < cut) E_.window.shift(); return E_.window.reduce((a, x) => a + x.n, 0); };
  const etsyCap = () => Math.max(50, +S.settings.etsyHourlyCap || 600);
  /** Before an Etsy-touching step: false (and a stopped run) when the hour's budget is spent; a warning past 70 %. */
  function etsyBudgetOk(step) {
    const used = hourCalls(), cap = etsyCap();
    if (used >= cap) { const why = `Etsy call budget reached (${used} of ${cap} this hour) before ${step}`; agent({ bridge: true }, "warn", why); toast(why, "bad", 8000); if (B.run && B.run.status === "running") RunCtl.stop(why, `Wait for the hour to roll over or raise the cap in Settings, then Resume.`, null, "budget"); return false; }
    if (used >= cap * 0.7) agent({ bridge: true }, "warn", `Etsy calls at ${used} of ${cap} this hour — ${step} goes ahead, the run stops at the cap`);
    return true;
  }
  /* ── the sorter's side of the story, streamed to the station's banner (design §5.7): every agent line and sheet log
        line while in control, coalesced into one quiet post every half second; the station shows the last four. ── */
  const feedQ = []; let feedT = null;
  function feed(ev) {
    if (!S_.control || !S_.frame || ev.fromStation) return;
    const text = String(ev.text || (ev.html ? ev.html.replace(/<[^>]+>/g, "") : "")).trim(); if (!text) return;
    const last = feedQ[feedQ.length - 1]; if (last && last.text === text) return;
    feedQ.push({ t: ev.t || Date.now(), kind: String(ev.kind || ""), text: text.slice(0, 220) }); if (feedQ.length > 240) feedQ.shift();
    if (!feedT) feedT = setTimeout(flushFeed, 450);
  }
  /* A burst — pooling forty lines, nesting six sheets — used to lose most of itself here: the queue was capped at twelve
     and each flush sent only the last six of the twelve it took. Everything queued now reaches the station, a dozen at a
     time, and only a runaway ever drops a line. */
  function flushFeed() {
    feedT = null;
    if (!feedQ.length || !S_.control) return;
    const rows = feedQ.splice(0, 12);
    call("feed.post", { rows }, { timeoutMs: 4000, quiet: true }).catch(() => {});
    if (feedQ.length) feedT = setTimeout(flushFeed, 250);
  }
  async function release() { S_.control = false; stopHeartbeat(); try { await call("release", {}, { timeoutMs: 5000 }); } catch (_) {} S_.up = false; S_.veil && S_.veil.classList.remove("hidden"); Dock.layout(); renderConsole(); }
  function ensure() { if (S_.control && S_.up) return Promise.resolve(S_.state); if (!S_.frame) mount(document.querySelector("#designView .dsFrameHost") || Views.designHost()); return open(); }
  function renderConsole() {
    const host = document.getElementById("dsConsole"); if (!host) return;
    const st = S_.state || {};
    const kv = host.querySelector(".kv"); if (kv) kv.innerHTML = `<b>${S_.control ? (S_.up ? "Connected" : "Link down") : "Not in control"}</b> · session <span class="mono">${S_.nonce ? S_.nonce.slice(0, 6) : "—"}</span><br>${S_.count} commands · ${S_.replies} replies · ${S_.errors} errors · ${S_.dropped} dropped · ${S_.pending.size} pending<br>${st.bench ? `bench ${esc(st.bench)} · ${esc(st.version || "")} · employee ${esc(st.employee || "—")}` : ""}<br>${st.etsy ? `Etsy: ${st.etsy.signedIn ? "signed in" + (st.etsy.expiresAt ? ` · token to ${new Date(st.etsy.expiresAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}` : "") : "<span style='color:#8a3a26'>NOT signed in</span>"}` : ""}<br>${st.counts ? `${st.counts.open} open · ${st.counts.selected} selected · ${st.counts.claimed} claimed · ${st.counts.locked} locked elsewhere` : ""}<br>Etsy calls: <b>${hourCalls()}</b> this hour of ${etsyCap()} · ${E_.sessionTotal} by this sorter · ${E_.stationTotal} on the station`;
    const cb = host.querySelector("#dsConnectEtsy"); if (cb) { const signed = !!(st.etsy && st.etsy.signedIn); cb.classList.toggle("gold", !signed); cb.classList.toggle("ghost", signed); cb.textContent = signed ? "Etsy signed in ✓" : "Connect Etsy"; }
    const sw = host.querySelector("#dsControl"); if (sw) { sw.classList.toggle("on", S_.control); sw.textContent = S_.control ? "Release" : "Take control"; }
    const log = host.querySelector(".log"); if (log) { const rows = S_.log.slice(-200); log.innerHTML = rows.map(r => `<div class="row ${r.dir}${r.err ? " err" : ""}"><span class="d">${fmtT(r.t)}</span><span class="ty">${r.dir === "cmd" ? "→" : r.dir === "reply" ? "←" : "·"} ${esc(r.type)}</span><span class="pl">${esc(r.payload ? JSON.stringify(r.payload) : "")}</span><span class="ms">${r.ms != null ? r.ms + "ms" : ""}</span></div>`).join(""); log.scrollTop = log.scrollHeight; }
    const tb = document.getElementById("tabDesignN"); if (tb) tb.textContent = S_.control && !S_.up ? "!" : "";
  }
  return { mount, open, call, release, ensure, feed, meter, etsyBudgetOk, etsyReadout, connectEtsy, etsy: () => ({ hour: hourCalls(), cap: etsyCap(), session: E_.sessionTotal, station: E_.stationTotal, meter: E_.station }), state: () => S_.state, log: S_.log, up: () => S_.up, inControl: () => S_.control, nonce: () => S_.nonce, renderConsole, flushLog, origin, frameUrl, _S: S_, _E: E_ };
})();

/* ── the dock: where the station frame is kept. "full" over the Design Station tab's placeholder; "live", out of sight on
   every other tab while the sorter is in control (or a run is on): the frame stays laid out at a desktop width inside a
   dock of no size, so the station keeps hydrating and working as it did behind the old corner strip; hidden otherwise.
   The strip and its pill are gone (Paul, 24 Sep: "remove the overlay for the Design Station"): the run banner and the rail
   say what the run is doing, and the Design Station is in the Workspace menu. ── */
const Dock = window.Dock = (() => {
  const D = { el: null, body: null, host: null, mode: "hidden", virtualW: 1200, ro: null, raf: 0 };
  function ensure() {
    if (D.el) return D;
    const el = document.createElement("div"); el.id = "dsDock"; el.className = "hidden";
    el.innerHTML = `<div class="dockBody"></div>`;
    document.body.appendChild(el);
    D.el = el; D.body = el.querySelector(".dockBody");
    window.addEventListener("resize", schedule); document.addEventListener("scroll", schedule, true);
    return D;
  }
  function setHost(host) { D.host = host; if (D.ro) D.ro.disconnect(); if (host && window.ResizeObserver) { D.ro = new ResizeObserver(schedule); D.ro.observe(host); } schedule(); }
  function schedule() { if (D.raf) return; D.raf = requestAnimationFrame(() => { D.raf = 0; layout(); }); }
  /** Which mode applies now: the Design Station tab shows the frame full size; any other tab keeps it laid out out of sight while the link is in control (or a run is on). */
  function wanted() {
    if (!D.el || !document.getElementById("dsFrame")) return "hidden";
    if (S.mode === "design") return "full";
    const live = DesignLink.inControl() || (B.run && ["running", "review", "paused"].includes(B.run.status));
    return live ? "live" : "hidden";
  }
  function layout() {
    if (!D.el) return;
    const mode = wanted(); D.mode = mode;
    const f = document.getElementById("dsFrame");
    D.el.classList.toggle("hidden", mode === "hidden");
    D.el.classList.toggle("full", mode === "full"); D.el.classList.toggle("live", mode === "live");
    // the notices hang under whatever chrome the page currently has
    const stg = document.querySelector(".stage");
    if (stg) document.documentElement.style.setProperty("--chromeH", Math.round(stg.getBoundingClientRect().top + 10) + "px");
    if (mode === "hidden" || !f) return;
    if (mode === "full") {
      const host = D.host; if (!host) return; const r = host.getBoundingClientRect();
      D.el.style.left = r.left + "px"; D.el.style.top = r.top + "px"; D.el.style.width = r.width + "px"; D.el.style.height = r.height + "px"; D.el.style.right = ""; D.el.style.bottom = "";
      // The frame was always laid out at 1200 px and scaled down to fit, so on the Design Station tab the app being
      // supervised rendered at 34–61 % — its 10 px order rows at 4–7 px. It is laid out at the width it is given, down to
      // the narrowest the station itself is built for, and never scaled below 1.
      const w = r.width, h = r.height, vw = Math.max(980, Math.round(w)), k = w / vw;
      f.style.width = vw + "px"; f.style.height = Math.round(h / k) + "px"; f.style.transform = `scale(${k})`;
    } else {
      // out of sight: the dock has no size of its own (the stylesheet's .live), the frame keeps its desktop layout
      D.el.style.left = ""; D.el.style.top = ""; D.el.style.width = ""; D.el.style.height = ""; D.el.style.right = ""; D.el.style.bottom = "";
      f.style.width = D.virtualW + "px"; f.style.height = "800px"; f.style.transform = "none";
    }
  }
  return { ensure, setHost, layout, schedule, mode: () => D.mode, _D: D };
})();

/* ── the tabs' hosts ── */
const Views = window.Views = (() => {
  function designHost() {
    const v = document.getElementById("designView"); if (v.dataset.built) return v.querySelector(".dsFrameHost");
    v.dataset.built = "1";
    v.innerHTML = `<div class="dsFrameHost"></div>
      <div class="dsConsole" id="dsConsole">
        <div class="card"><div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap"><button class="switchBtn" id="dsControl" type="button">Take control</button><a class="btn ghost xs" id="dsOpenTab" target="_blank" rel="noopener">Open station in a tab</a><button class="btn gold xs" id="dsConnectEtsy" type="button" title="Signs the station in to Etsy in its own window; the framed station picks the token up">Connect Etsy</button><button class="btn ghost xs" id="dsReload" type="button">Reload frame</button></div><div class="kv">Not in control</div></div>
        <div class="card" style="gap:4px"><div class="section" style="margin:0 0 4px">Employee</div><div style="display:flex;gap:6px;align-items:center"><span id="dsEmployee" class="mono">${esc(employeeName() || "— not set —")}</span><button class="btn ghost xs" id="dsSetEmployee" type="button">Change</button></div><div class="help" style="font-size:11px;color:var(--ink45)">Recorded with every approval. Any employee may approve.</div></div>
        <div class="log"></div>
      </div>`;
    // The fold lives in the panel's own column, never over the station, and the column keeps a strip of itself when
    // folded so the way back is always on screen.
    { const console_ = v.querySelector("#dsConsole");
      const btn = document.createElement("button"); btn.type = "button"; btn.className = "dsFold";
      const paint = () => { const on = v.classList.contains("dsWide"); btn.textContent = on ? "‹ Panel" : "Hide panel ›"; btn.title = on ? "Bring the controls back" : "Fold the controls away and give the station the whole tab"; btn.setAttribute("aria-expanded", on ? "false" : "true"); };
      btn.onclick = () => { const on = !v.classList.contains("dsWide"); v.classList.toggle("dsWide", on); localStorage.setItem("cn.dsWide", on ? "1" : "0"); paint(); Dock.schedule(); };
      v.classList.toggle("dsWide", localStorage.getItem("cn.dsWide") === "1");
      paint(); console_.insertBefore(btn, console_.firstChild); }
    const host = v.querySelector(".dsFrameHost");
    // the tab is a mirror of the station being driven, not a second station: same page, following the same pointer
    v.querySelector("#dsOpenTab").href = DesignLink.frameUrl() + (DesignLink.frameUrl().includes("?") ? "&" : "?") + "mirror=1";
    v.querySelector("#dsOpenTab").textContent = "Watch full screen in a tab";
    v.querySelector("#dsOpenTab").title = "Opens the same station in a tab that follows this one, pointer and all";
    // Release in the middle of a run step stopped the run, with a red alarm, and nothing before the press said so
    v.querySelector("#dsControl").onclick = async () => { if (DesignLink.inControl()) { if (B.run && B.run.status === "running" && !confirm("The run is working through the station right now. Releasing the station stops the run until you take control again and press Resume.\n\nRelease anyway?")) return; await DesignLink.release(); } else { try { await DesignLink.ensure(); } catch (e) { toast("Could not open the session: " + e.message, "bad", 6000); } } DesignLink.renderConsole(); };
    v.querySelector("#dsConnectEtsy").onclick = () => DesignLink.connectEtsy().catch(e => toast(e.message, "bad", 6000));
    v.querySelector("#dsReload").onclick = () => { const f = document.getElementById("dsFrame"); if (f) f.src = DesignLink.frameUrl(); };
    v.querySelector("#dsSetEmployee").onclick = () => { askEmployee(); document.getElementById("dsEmployee").textContent = employeeName() || "— not set —"; };
    return host;
  }
  function onShow(mode) {
    if (mode === "design") { const host = designHost(); if (!document.getElementById("dsFrame")) DesignLink.mount(host); else Dock.setHost(host); DesignLink.renderConsole(); }
    Dock.schedule();
    if (mode === "orders") Orders.renderNow();
    if (mode === "master") Master.render();
    if (mode === "engrave") Engrave.render();
    if (mode === "review") Review.render();
  }
  return { designHost, onShow };
})();

/* ═══ 18 · Orders — pulled through the station, read deterministically ══════ */
const Orders = window.Orders = (() => {
  const rowsOf = () => B.orders.rows;
  function markStale() { B.orders.stale = true; render(); }
  async function loadMaps(force) {
    if (!S.cloud.ok) return;
    if (!force && Date.now() - B.maps.loadedAt < 60000) return;
    const [om, al, nd] = await Promise.all([api("charmNestLibrary", { op: "optionMapGet" }), api("charmNestLibrary", { op: "aliasGet" }), api("charmNestLibrary", { op: "noDesignGet" })]);
    // the maps are replaced only when what was read differs: a new map object is what makes interpretAll read lines again
    const next = [om.maps || {}, al.aliases || {}, nd.list || { patterns: [], skus: [], rows: [] }], sig = JSON.stringify(next);
    if (sig !== mapsSig || !mapsSame()) { mapsSig = sig; [B.maps.optionMaps, B.maps.aliases, B.maps.noDesign] = next; mapsRead = next; }
    B.maps.loadedAt = Date.now();
  }
  let mapsSig = "", mapsRead = [];
  const mapsSame = () => mapsRead[0] === B.maps.optionMaps && mapsRead[1] === B.maps.aliases && mapsRead[2] === B.maps.noDesign;
  const ctx = () => ({ optionMaps: B.maps.optionMaps, aliases: B.maps.aliases, noDesign: B.maps.noDesign, masterEntry: sku => Master.entryFor(sku) });
  /** The pull rule (Settings → Pull orders): every open order, those due by a date, or the N most urgent by ship-by date. */
  function applyPullRule(orders) {
    const mode = S.settings.pullMode || "all";
    let list = orders.slice().sort((a, b) => (a.shipBy || 9e12) - (b.shipBy || 9e12));
    if (mode === "dueBy" && S.settings.pullDueBy) { const [y, m, d] = S.settings.pullDueBy.split("-").map(Number); const end = new Date(y, m - 1, d, 23, 59, 59).getTime() / 1000; list = list.filter(o => o.shipBy && o.shipBy <= end); }
    if (mode === "count") list = list.slice(0, Math.max(1, +S.settings.pullCount || 40));
    return list;
  }
  /* A line reads the same until something it is read from changes, and every update read every line again. It is read
     again when its order (a new copy, update time or note), its line, a person's override, the option maps (a new object,
     see loadMaps) or what the library says of its SKUs changes; the rest of the pass (problems, overrides) runs as before. */
  const readAs = new WeakMap();
  const libFacts = sku => { const e = sku ? Master.entryFor(sku) : null; return e ? `${e.blocked ? "b:" + e.blocked : "ok"}|${e.sizes ? Object.entries(e.sizes).map(([k, v]) => (v ? "+" : "-") + k).join("\n") : ""}` : ""; };
  function inputsOf(row) {
    const o = row.order, l = row.line, m = B.maps, a = m.aliases && m.aliases[String(l.listingId)];
    return [o, +o.updateTs, o.staffNote, l, l.staffNote, row.materialOverride, row.sizeOverride, m.optionMaps, m.aliases, m.noDesign,
      libFacts(String(l.sku || "").trim().toUpperCase()), libFacts(a && a.sku ? String(a.sku).trim().toUpperCase() : "")];
  }
  function interpretAll() {
    for (const row of rowsOf()) {
      // a line cut and committed is done: a later change to the maps or the library could only raise a decision on it
      if (row.state === "gone" || (row.state === "committed" && row.spec)) continue;
      const inputs = inputsOf(row), was = readAs.get(row);
      if (!was || was.spec !== row.spec || inputs.some((v, i) => !Object.is(v, was.inputs[i]))) { row.spec = O.interpretLine(row.order, row.line, ctx()); readAs.set(row, { spec: row.spec, inputs }); }
      row.problems = row.spec.problems.slice(); if (row.spec.noDesign) row.state = row.state === "pulled" ? "noDesign" : row.state; if (row.materialOverride) { row.spec.material = row.materialOverride; row.problems = row.problems.filter(p => p.kind !== "needsMaterial"); } if (row.sizeOverride) { row.spec.size = row.sizeOverride; row.problems = row.problems.filter(p => p.kind !== "missingSize"); } row.material = row.spec.material;
    }
    Review.syncOrderItems();
  }
  async function pull(run, { silent = false, receiptIds = null } = {}) {
    await DesignLink.ensure(); await Sandbox.ready(true);   // the sandbox order stream must exist before the station sweeps
    if (!DesignLink.etsyBudgetOk("the pull")) throw new Error("Etsy call budget reached — the pull was not started");
    await Promise.all([loadMaps(), Master.load()]);
    const pullBar = window.CNProgress ? CNProgress.start("Pulling orders from Etsy") : null;
    try {
    const r = await DesignLink.call("orders.snapshot", { hydrate: true, refresh: true }, { timeoutMs: 20 * 60 * 1000, onProgress: p => { if (pullBar) { if (p.done != null && p.total) pullBar.set(p.done, p.total, p.text || ""); else if (p.text) pullBar.note(p.text); } if (p.text) agentLiveLine("Pulling orders", p.text, p.done, p.total); } });
    DesignLink.meter(r, "the pull");
    B.orders.snapshot = { total: r.total, hydrated: r.hydrated, etsy: r.etsy, at: Date.now() }; B.orders.recalled = null;
    if (r.hydrated < r.total) throw new Error(`only ${r.hydrated} of ${r.total} orders could be read from Etsy — ${r.etsy && !r.etsy.signedIn ? "the station is not signed in: press Connect Etsy" : "check the Design Station and pull again"}`);
    const wanted = receiptIds && new Set(receiptIds.map(String));
    const picked = wanted ? r.orders.filter(o => wanted.has(String(o.receiptId))) : applyPullRule(r.orders);
    B.orders.filtered = r.orders.length - picked.length;
    await Arrivals.record(picked);
    B.orders.rows = picked.flatMap(o => o.lines.map(l => ({ arrivedAt: Arrivals.at(o.receiptId), key: O.lineKey(o, l), order: o, line: l, spec: null, problems: [], state: "pulled", reason: null, claimedBy: null, poolIds: [], engrave: null, metal: null })));
    B.orders.byKey = new Map(B.orders.rows.map(r => [r.key, r]));
    Carry.adopt();
    interpretAll();
    B.orders.pulledAt = Date.now(); B.orders.stale = false;
    if (run) { run.lines = Object.fromEntries(B.orders.rows.map(lineRecord)); run.orders = picked.map(o => o.receiptId); run.step = "pull"; await RunCtl.save(run); }
    const held = B.orders.rows.filter(x => x.problems.length).length;
    agent({ bridge: true }, "DS", `Pulled ${picked.length} order(s), ${B.orders.rows.length} line(s)${B.orders.filtered ? ` (${B.orders.filtered} more open orders left out by the pull rule)` : ""} · ${held} line(s) need a decision`);
    if (!silent) toast(`${picked.length} orders · ${B.orders.rows.length} lines pulled from the Design Station`, "ok");
    render();ListMedia.prepare(B.orders.rows);
    return B.orders.rows;
    } finally { if (pullBar) pullBar.end(); }
  }
  let liveEv = null;
  function agentLiveLine(label, text, done, total) { const html = `<b>${esc(label)}</b> ${esc(text)}${total ? ` <i>${done}/${total}</i>` : ""}`; if (!liveEv || !liveEv.live) liveEv = agent({ bridge: true }, "DS", text, { live: true, html }); else agentUpdate(liveEv, { html, text }); if (total && done >= total) { agentUpdate(liveEv, { live: false }); liveEv = null; } }
  /* The record used to keep a line's state and little else, so a finished run could be listed but never opened: no
     title, no listing, no metal, no ship-by meant no card, no thumbnail and no filter. It keeps a compact copy of the
     line now — everything a row is built from, capped so four hundred of them still fit in one document. */
  const cap = (v, n) => String(v == null ? "" : v).slice(0, n);
  function lineRecord(row) {
    const l = row.line, o = row.order;
    return [row.key, { state: row.state, poolIds: row.poolIds, reason: row.reason, hold: row.hold || null, wait: row.wait || null, sku: row.spec && row.spec.designSku, material: row.material || (row.spec && row.spec.material) || null, quantity: row.spec ? row.spec.quantity : 1,
      engrave: row.engrave ? { needed: !!row.engrave.needed, state: row.engrave.state, approved: !!row.engrave.approved, text: row.engrave.text || null } : null,
      // The server reads this record, not the row, before it records a set as complete: a line with nothing to engrave
      // must read as plain there too, including before its engraving check has run.
      engraveCandidate: row.spec ? !!row.spec.engraveCandidate : null,
      changePending:!!row.changePending,repoolChanged:!!row.repoolChanged,arrivedAt: row.arrivedAt || 0, createTs: o.createTs || 0, materialOverride: row.materialOverride || null, sizeOverride: row.sizeOverride || null, problems: (row.problems || []).map(p => p.kind), updateTs: o.updateTs, orderId: o.receiptId, transactionId: l.transactionId,
      snap: { title: cap(l.title, 160), listingId: cap(l.listingId, 24), metalKey: cap(l.metalKey, 24), metalLabel: cap(l.metalLabel, 40),
        orderNumber: cap(o.orderNumber, 24), buyer: cap(o.buyer && o.buyer.name, 60), shipBy: +o.shipBy || 0, isGift: !!o.isGift,
        vars: (l.variations || []).map(v => String(v.name ?? v.formatted_name ?? "") + "\u241f" + String(v.value ?? v.formatted_value ?? "")),
        pers: (l.personalization || []).slice(0, 4).map(x => cap(x, 200)) } }];
  }
  /** The other direction: a recorded line, back to the row shape every card, list, filter and window already reads. */
  function rowFromRecord(key, l) {
    const s2 = l.snap || {};
    /* Runs recorded before a line's own copy was kept have a SKU and a material and nothing else. The master index
       holds the rest: the charm's name stands in for the listing title, and its drawing — the thing that will actually
       be cut — stands in for the shop photograph, which is arguably the better picture anyway. */
    const me = !s2.title && l.sku ? Master.entryFor(l.sku) : null;
    const order = { receiptId: String(l.orderId || ""), orderNumber: s2.orderNumber || String(l.orderId || ""), shipBy: +s2.shipBy || 0, createTs: +l.createTs || 0, updateTs: +l.updateTs || 0,
      buyer: { name: s2.buyer || "", country: "", city: "" }, buyerMessage: "", isGift: !!s2.isGift, giftMessage: "", staffNote: "", messages: [], metals: {}, lines: [] };
    const line = { transactionId: String(l.transactionId || ""), listingId: s2.listingId || "", sku: l.sku || "", title: s2.title || (me ? `${me.sku}${me.size ? " · " + me.size : ""}` : ""), quantity: +l.quantity || 1,
      metalKey: s2.metalKey || "", metalLabel: s2.metalLabel || (l.material ? labelOf(l.material) : ""), personalization: s2.pers || [], buyerMessage: "", expectedShipDate: 0,
      variations: (s2.vars || []).map(v => { const i = String(v).indexOf("\u241f"); return { name: String(v).slice(0, i < 0 ? 0 : i), value: i < 0 ? String(v) : String(v).slice(i + 1) }; }) };
    order.lines = [line];
    return { key, order, line, changePending:!!l.changePending,repoolChanged:!!l.repoolChanged, arrivedAt: +l.arrivedAt || 0, sizeOverride: l.sizeOverride || null, spec: null, problems: [], state: l.state || "pulled", reason: l.reason || null, hold: l.hold || null, wait: l.wait || null, claimedBy: null,
      poolIds: l.poolIds || [], engrave: l.engrave || null, metal: l.material || null, materialOverride: l.materialOverride || l.material || null, fromRecord: true };
  }
  /* The claim is a courtesy — a gold dot on the station's rows saying the sorter has these — never a lock. So it goes
     in batches of a hundred, each with its own time, and a batch the station does not answer is retried once and then
     let go with a warning: 357 orders in one message once ran past the two-minute reply limit and stopped the whole
     run at its first step, for a dot. */
  async function claim(ids) {
    if (!ids.length) return;
    const got = new Set(); let missed = 0;
    for (let i = 0; i < ids.length; i += 100) {
      const part = ids.slice(i, i + 100); let r = null;
      for (let attempt = 0; attempt < 2 && !r; attempt++) { try { r = await DesignLink.call("claim", { receiptIds: part, runId: B.run && B.run.runId }, { timeoutMs: 90000 }); } catch (e) { if (attempt) { missed += part.length; agent({ bridge: true }, "warn", `claim: ${e.message} — carrying on without the dot on ${part.length} order(s)`); } } }
      for (const id of (r && r.claimed) || []) got.add(id);
    }
    for (const row of rowsOf()) if (got.has(row.order.receiptId)) { row.claimedBy = "sorter"; delete row.unclaimed; }
    agent({ bridge: true }, "DS", `Claimed ${got.size} order(s) on the station (gold dot)${missed ? ` · ${missed} unanswered` : ""}`); render();
  }
  /** An order the station has already let go of is not sent again: the run passes its last step after every update, and
      each pass used to send every finished order of the day again, a list that only grows. */
  async function unclaim(ids) {
    const done = new Set(rowsOf().filter(r => r.unclaimed).map(r => String(r.order.receiptId)));
    ids = ids.filter(id => !done.has(String(id))); if (!ids.length) return;
    // a hundred per message, like the claim: a long day's list in one message could run past the reply limit. A batch the
    // station does not answer stops the rest, which stay unmarked and go again at the next pass, as one failed call did.
    const sent = new Map(); let ok = true;
    for (let i = 0; i < ids.length; i += 100) {
      const part = ids.slice(i, i + 100);
      if (ok) try { await DesignLink.call("unclaim", { receiptIds: part }); } catch (e) { ok = false; agent({ bridge: true }, "warn", `unclaim: ${e.message}`); }
      for (const id of part) if (!sent.get(String(id))) sent.set(String(id), ok);
    }
    for (const row of rowsOf()) { const k = String(row.order.receiptId); if (sent.has(k)) { row.claimedBy = null; if (sent.get(k)) row.unclaimed = true; } }
    render();
  }
  /** §10.3: every order's update_timestamp re-read through the station; changed → re-interpret; vanished → dropped. */
  async function revalidate(run, why) {
    // an order found gone stays gone: reading it again at every check cost an Etsy call each time and changed nothing.
    // A committed line is cut and its order design-complete at the station, which reports it as no longer open: every
    // check read it again (an Etsy call each, towards the hourly cap that stops the run) and a later Etsy update, such
    // as its shipping, sent a finished order back to review. It is done here.
    const settled = r => r.state === "gone" || r.state === "committed";
    const orders = [...new Set(rowsOf().filter(r => !settled(r)).map(r => r.order.receiptId))];
    const changed = [], gone = [];
    if (!DesignLink.etsyBudgetOk(`re-validation ${why}`)) throw new Error("Etsy call budget reached before order revalidation");
    // one paged list sweep tells which orders Etsy touched or closed since the pull; only those get a fresh detail read
    let chk = null;
    try { chk = await DesignLink.call("orders.check", { receiptIds: orders }, { timeoutMs: 10 * 60 * 1000, onProgress: p => { if (p.text) agentLiveLine("Re-validating orders", p.text, p.done, p.total); } }); DesignLink.meter(chk, `the open-list check ${why}`); }
    catch (e) { throw new Error(`Cannot verify Etsy open orders: ${e.message}`); }
    const first = new Map(); for (const r of rowsOf()) if (!first.has(r.order.receiptId)) first.set(r.order.receiptId, r);   // an order's first line, found once rather than a search of every line per order
    const need = orders.filter(rid => { const c = chk.orders[rid]; const cur = first.get(rid); return !c || !c.open || c.touched || (cur && c.updateTs && c.updateTs !== cur.order.updateTs); });
    agent({ bridge: true }, "DS", `Re-validation ${why}: ${orders.length} order(s) checked against the open list${chk.swept ? "" : " (list reused)"} · ${need.length} need a fresh read`);
    let done = 0;
    for (const rid of need) {
      let d = null;
      try { d = await DesignLink.call("orders.detail", { receiptId: rid, fresh: true }, { timeoutMs: 60000 }); DesignLink.meter(d, `re-reading ${rid}`); } catch (e) { throw new Error(`Cannot verify Etsy order ${rid}: ${e.message}`); }
      agentLiveLine("Re-validating orders", `order ${rid}`, ++done, need.length);
      const rows = rowsOf().filter(r => r.order.receiptId === rid && r.state !== "committed");
      if (!d || !d.order && !d.gone) throw new Error(`Etsy returned no verifiable state for order ${rid}`);
      if (d.gone) { gone.push(rid); for (const r of rows) { r.state = "gone"; r.reason = d && d.reason ? d.reason : (d && d.isShipped ? "shipped" : d && d.status ? d.status : "no longer open"); } continue; }
      if (rows.some(row=>row.order.updateTs !== d.order.updateTs)) {
        changed.push(rid);
        for (const r of rows) {
          if(r.order.updateTs===d.order.updateTs)continue;
          const nl = d.order.lines.find(l => l.transactionId === r.line.transactionId);
          const oldText = r.engrave && r.engrave.text; const oldSpec = r.spec;
          r.order = d.order; if (nl) r.line = nl; else { r.state = "gone"; r.reason = "line vanished from the order"; continue; }
          r.spec = O.interpretLine(r.order, r.line, ctx()); r.problems = r.spec.problems.slice(); r.material = r.spec.material;
          const textInputsChanged = JSON.stringify([oldSpec && oldSpec.personalization, oldSpec && oldSpec.buyerMessage, oldSpec && oldSpec.staffNote]) !== JSON.stringify([r.spec.personalization, r.spec.buyerMessage, r.spec.staffNote]);
          const materialChanged = oldSpec && oldSpec.material !== r.spec.material, skuChanged = oldSpec && oldSpec.designSku !== r.spec.designSku;
          const shapeChanged=materialChanged || skuChanged || oldSpec && (oldSpec.size!==r.spec.size || oldSpec.quantity!==r.spec.quantity);
          // An update that changes nothing this line is made from (Etsy moves an order's update time for an address, a
          // gift note, a shipping upgrade, or a change to one of its other lines) is taken as it is. Every such update
          // used to send each line of the order to Review and hold the order back until a person accepted it.
          if(!shapeChanged && !textInputsChanged)continue;
          r.changePending=true;r.repoolChanged=!!shapeChanged;
          Review.add({ kind: "orderChanged", key: "chg:" + r.key, row: r, old: { text: oldText, spec: oldSpec }, why: `Etsy updated order ${rid} after the pull (${why})${materialChanged ? " · material changed" : ""}${skuChanged ? " · SKU changed" : ""}${textInputsChanged ? " · the customer's words changed" : ""}` });
          if (textInputsChanged || shapeChanged) { if (r.engrave) { r.engrave.invalidatedBy = "order changed"; r.engrave.approved = false; r.engrave.state = "reclassify"; } Engrave.invalidate(r, "order changed"); }
        }
      }
    }
    await takeOffGone(rowsOf().filter(r => r.state === "gone" && (r.poolIds || []).length));
    if (run) { run.lines = Object.fromEntries(rowsOf().map(lineRecord)); run.revalidatedAt = Date.now(); await RunCtl.save(run); }
    agent({ bridge: true }, "DS", `Re-validated ${orders.length} order(s) ${why}: ${changed.length} changed, ${gone.length} gone`);
    Review.syncOrderItems(); render();
    return { changed, gone };
  }
  /* A charm taken off a sheet leaves the others where they are: the sheet is not arranged again, since a fresh
     arrangement loses room (Paul, 24 Sep), and the space it frees goes to the next charms placed one by one. A sheet
     with nothing left on it starts over; a Rose Gold sheet behind a saved green line keeps that line (sheetDirty). */
  function keepRest(sh) {
    if (!sh.placements.length || (sh.metal === "rose" && (sh.rosePlan || sh.roseProtected))) { sheetDirty(sh); return; }
    sh.intakeAppend = true; sh.appendOnly = true; sh.dirty = true;
    if (!["nesting", "finishing", "queued"].includes(sh.status)) sh.status = "ready";
    renderCard(sh);
  }
  /* An order that left Etsy (cancelled, refunded) is dropped from its set (§10.3). Its pieces come off every sheet that is
     still filling, so they are not cut and their room goes to the next order; the rest of that sheet stays as placed. A
     piece on a sheet that is released, cut, or fixed inside a saved Rose Gold green line stays where it is: it is cut with
     its sheet and set aside. Nothing took these pieces off before, and a cancelled order's piece held its sheet back for good. */
  async function takeOffGone(rows) {
    const filling = sh => !(window.LiveNest && LiveNest.closed(sh)) && !(sh.metal === "rose" && (sh.rosePlan || sh.roseProtected)) &&
      !["nesting", "finishing", "queued"].includes(sh.status) && !(sh.persisted && !sh.persistedDone);
    for (const row of rows) {
      // only the pages that hold its pieces are looked at: a gone order's pieces on a cut sheet stay there for good, and
      // every check used to search every charm of every sheet for them again
      const ids = new Set(row.poolIds), off = [], on = Pool.holding?.(ids);
      for (const sh of allSheets()) {
        if (on && !on.has(sh)) continue;
        const mine = sh.charms.filter(c => ids.has(c.poolId)); if (!mine.length || !filling(sh)) continue;
        sh.charms = sh.charms.filter(c => !ids.has(c.poolId)); sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id)); keepRest(sh);
        off.push(...mine.map(c => c.poolId));
        agent({ metal: sh.metal, run: sh.runId }, "POOL", `${row.order.receiptId} is no longer open on Etsy (${row.reason || "gone"}): ${mine.length} piece${mine.length === 1 ? "" : "s"} taken off ${sheetName(sh)}; the rest stay where they are`);
      }
      if (!off.length) continue;
      const offSet = new Set(off); row.poolIds = row.poolIds.filter(id => !offSet.has(id));
      try { await Pool.update(off, { state: "abandoned", sheetId: null, setId: null }); } catch (e) { agent({ bridge: true }, "warn", `pool record for ${row.order.receiptId}: ${e.message}`); }
      for (const id of off) B.pool.rows.delete(id);
    }
  }
  const STATE_PILL = { pulled: ["neutral", "pulled"], waiting: ["info", "waiting"], noDesign: ["info", "no design"], pooled: ["info", "pooled"], nested: ["ok", "nested"], written: ["ok", "written"], labelled: ["ok", "labelled"], committed: ["ok", "complete"], unmatched: ["bad", "unmatched"], held: ["bad", "held"], contended: ["warn", "other run"], skipped: ["warn", "skipped"], gone: ["bad", "gone"], oversize: ["bad", "oversize"] };
  function engravePill(r) { const e = r.engrave; if (!e) return r.spec && r.spec.engraveCandidate ? ["warn", "words?"] : ["neutral", "—"]; if (!e.needed) return ["neutral", e.state === "skipped" ? "skipped" : "no engraving"]; if (e.approved) return ["ok", "approved"]; if (e.state === "words") return ["warn", "words"]; if (e.state === "review") return ["warn", "review"]; if (e.state === "fitted") return ["info", "fitted"]; if (e.state === "blocked") return ["bad", "blocked"]; return ["info", e.state || "engrave"]; }
  const OV = { pile: null, metal: null, form: null, eng: null, q: "", view: null, sort: "arrival", desc: false, limit:48 };   // what the tab is showing right now
  const FORM_LABEL = { necklace: "Necklaces", earrings: "Earrings", "earring-single": "Single earrings", huggie: "Huggies", charm: "Charms only", bracelet: "Bracelets", anklet: "Anklets", keychain: "Keychains" };
  const viewMode = () => OV.view || S.settings.orderView || "list";
  /** The lines the filters leave, in ship-by order. */
  function visibleRows() {
    const q = OV.q.trim().toLowerCase();
    return rowsOf().filter(r => {
      if (r.state === "gone") return false;
      if (OV.metal && (r.material || "none") !== OV.metal) return false;
      if (OV.form && ((r.spec && r.spec.form) || "none") !== OV.form) return false;
      if (OV.eng) { const needs = !!(r.engrave && r.engrave.needed); if (OV.eng === "yes" ? !needs : needs) return false; }
      if (!q) return true;
      const sp = r.spec || {};
      return [r.order.receiptId, sp.designSku, r.line.sku, r.line.title, (sp.personalization || []).join(" "), sp.buyerMessage, sp.staffNote, r.reason]
        .some(x => String(x || "").toLowerCase().includes(q));
    }).sort((x, y) => {
      const k = OV.sort === "arrival" ? (O.orderPlacedAt(y) - O.orderPlacedAt(x)) : OV.sort === "order" ? String(x.order.receiptId).localeCompare(String(y.order.receiptId))
        : OV.sort === "state" ? String(x.state).localeCompare(String(y.state))
        : (x.order.shipBy || 0) - (y.order.shipBy || 0);
      return (OV.desc ? -k : k) || String(x.order.receiptId).localeCompare(String(y.order.receiptId));
    });
  }
  function imageFor(r) { return ListMedia.peek(r.line.listingId); }
  function wantImage(lid) {
    return ListMedia.listing(lid).then(url=>{
      for(const host of document.querySelectorAll('[data-lid]'))if(host.dataset.lid===String(lid))
        ListMedia.watch(host,()=>Promise.resolve(url),'listing:'+lid);
    }).catch(()=>{});
  }
  /* Thirteen state words in four colours said nothing about order. The five that are progress now carry their place in
     the run, so "3/5 nested" reads as progress; the exceptions stay unnumbered, so a problem reads differently. */
  const PROGRESS = ["pooled", "nested", "written", "labelled", "committed"];
  function stateWords(r) {
    const [k, t] = STATE_PILL[r.state] || ["neutral", r.state];
    const i = PROGRESS.indexOf(r.state);
    return [k, i < 0 ? t : `${i + 1}/${PROGRESS.length} ${t}`];
  }
  /** How the ship-by date reads today: overdue, due, or simply a date. Days are the shop's local days, the same days
   *  the date is written in: counted in UTC, from 8 pm in Toronto an order due that day showed red as overdue. */
  const shipDay = new Intl.DateTimeFormat("en-US", { month: "short", day: "2-digit" });   // made once, used for every line
  function dueOf(r) {
    const by = r.order.shipBy; if (!by) return { cls: "", txt: "\u2014", late: false, soon: false };
    const dayOf = t => { const x = new Date(t); return new Date(x.getFullYear(), x.getMonth(), x.getDate()).getTime(); };
    const now = new Date(window.SimClock?.now() ?? Date.now());   // the sandbox stream's day while it plays
    const today0 = dayOf(now.getTime()), tomorrow0 = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1).getTime(), d = dayOf(by * 1000);
    const txt = shipDay.format(new Date(by * 1000));
    return { cls: d < today0 ? "bad" : d <= tomorrow0 ? "warn" : "", txt, late: d < today0, soon: d <= tomorrow0 };
  }
  const shipTxt = r => r.order.shipBy ? shipDay.format(new Date(r.order.shipBy * 1000)) : "—";
  const wordsOf = sp => (sp.personalization || []).join(" / ") || sp.buyerMessage || "";
  /** Where this line physically is: the set and the sheet it was nested on. "What's where", answered on the line itself. */
  function placeOf(r) {
    for (const id of r.poolIds || []) { const p2 = B.pool.rows.get(id); if (p2 && (p2.sheetName || p2.sheetId)) return { set: p2.setId || "", sheet: p2.sheetName || p2.sheetId, sheetId: p2.sheetId || null }; }
    return null;
  }
  /** Everything a person needs to recognise one line, as a card or as a row: the same fields either way. */
  let listKey="";
  /* A line's day (its heading and time) depends only on its order's time, the zone being fixed: it is worked out once a
     line, not for every line at every drawing. Nodes are kept for the lines drawn lately, two hundred beyond those on
     screen: one used to be kept for every line ever drawn, until its line left the list. */
  const orderNodes=new Map(),NODES_KEPT=200;
  const dayMemo=new WeakMap(),dayOf=r=>{const at=O.orderPlacedAt(r),m=dayMemo.get(r);if(m&&m.at===at)return m.day;const day=O.orderDay(r);dayMemo.set(r,{at,day});return day;};
  function renderBody() {
    if(window.CharmNestInteraction?.defer('orders-list',renderBody))return;
    const host = document.getElementById("ordBody"); if (!host) return;
    const at = host.scrollTop;                                             // a run writing to the list must not scroll it away
    const rows = visibleRows();
    const alive=new Set(rowsOf().map(r=>r.key));for(const key of orderNodes.keys())if(!alive.has(key))orderNodes.delete(key);
    const nextKey=JSON.stringify([OV.q,OV.metal,OV.form,OV.eng,OV.sort,OV.desc,viewMode()]);
    if(nextKey!==listKey){OV.limit=48;listKey=nextKey;}
    const anchor=at>0?[...host.querySelectorAll('[data-key]')].find(n=>n.getBoundingClientRect().bottom>host.getBoundingClientRect().top):null;
    const anchorKey=anchor?.dataset.key, anchorTop=anchor?.getBoundingClientRect().top;
    const restore = () => { if (at) host.scrollTop = at; };
    if (!rowsOf().length) {
      host.innerHTML = '<div class="libEmpty"><span>Nothing pulled yet \u2014 press <b>Pull orders</b> above.</span></div>';   // one line: .libEmpty stacks its children
      return;
    }
    if (!rows.length) {
      host.innerHTML = '<div class="libEmpty">Nothing matches these filters.<br><button class="btn ghost sm" id="ordClear" style="margin-top:10px">Show everything</button></div>';
      host.querySelector("#ordClear").onclick = () => { OV.pile = null; OV.metal = null; OV.form = null; OV.eng = null; OV.q = ""; render(); };
      return;
    }
    const cards = viewMode() === "cards";
    let list=host.querySelector('#ordItems');if(!list){host.innerHTML='<div id="ordItems"></div>';list=host.firstElementChild;}list.className=cards?'ordCards':'ordList';
    const wanted=[],place=node=>{const index=wanted.length;wanted.push(node);if(list.children[index]!==node)list.insertBefore(node,list.children[index]||null);};
    let lastDay=null;
    const shown=rows.slice(0,OV.limit || 48);
    // a heading counts its day's orders in the list: counted only where headings are drawn, for the days on screen
    const dayCounts=new Map();
    if(OV.sort==='arrival'){for(const r of shown)dayCounts.set(dayOf(r).key,new Set());for(const row of rows){const ids=dayCounts.get(dayOf(row).key);if(ids)ids.add(row.order.receiptId);}}
    for (const r of shown) {
      const date=dayOf(r);
      if(OV.sort==='arrival' && date.key!==lastDay){const heading=[...list.querySelectorAll('.ordDay')].find(n=>n.dataset.day===date.key)||el('div','ordDay');heading.dataset.day=date.key;const text=`<span>${esc(date.label)}</span><small>${dayCounts.get(date.key).size} orders · Toronto time</small>`;if(heading.innerHTML!==text)heading.innerHTML=text;place(heading);lastDay=date.key;}
      const sp = r.spec || {}, m = r.material || "none";
      const st = stateWords(r), due = dueOf(r), where = placeOf(r);
      const attn = r.problems.length || ["held", "unmatched", "oversize"].includes(r.state);
      const why = attn ? (r.problems.map(x => Review.problemText(x)).join(" · ") || r.reason || "") : r.state === "waiting" ? (r.reason || "") : "";
      const gateBtn = r.state === "waiting" && r.wait ? `<button class="relHold" type="button" data-gate="${r.wait.kind === "slow" ? "release" : "cut"}" data-gm="${esc(r.wait.material)}" title="${r.wait.kind === "slow" ? "send " + esc(labelOf(r.wait.material)) + " to the laser with this set instead of waiting" : "cut the partial " + esc(labelOf(r.wait.material)) + " sheet now"}">${r.wait.kind === "slow" ? "Send now" : "Cut it anyway"}</button>` : "";
      const mail = window.CustomerMail ? CustomerMail.badgeStamp(r.order.receiptId) : "";
      const stamp=JSON.stringify([cards,r.order,r.line,r.spec,r.state,r.hold,r.wait,why,where,due,date,mail]);
      const cached=orderNodes.get(r.key);
      if(cached?.stamp===stamp){orderNodes.delete(r.key);orderNodes.set(r.key,cached);place(cached.node);ListMedia.mount(cached.node,r);continue;}
      const node = el("div", (cards ? "ocard" : "doneRow workRow orderListRow") + " hoverItem" + (attn ? " attn" : ""));
      node.setAttribute("role","button"); node.tabIndex=0; node.dataset.m = m; node.dataset.key = r.key;
      node.title = r.order.receiptId + " · " + (sp.designSku || r.line.sku || "no SKU") + " — " + r.line.title;
      const qty = sp.quantity || r.line.quantity || 1;
      const identity=`<div class="engravingIdentity"><span class="queueLabel">Order</span><div class="engravingOrder"><b class="mono onum">${esc(r.order.receiptId)}</b><span class="sku mono">${esc(sp.designSku || r.line.sku || 'No SKU')}</span></div><span class="purchaseLabel">${wordsOf(sp) ? 'Personalisation' : 'Item'}</span><span class="rowExcerpt" title="${esc(wordsOf(sp) || r.line.title || '')}">${esc(wordsOf(sp) || r.line.title || 'No title')}</span>${where ? `<span class="rowExcerpt dim">${esc(where.set)} · ${esc(where.sheet)}</span>` : ''}</div>`;
      node.innerHTML=ListMedia.pair(r)+identity+`<div class="purchaseSummary">${purchaseMarkup(r)}</div><div class="rowActions">${mail && mail !== "null" ? CustomerMail.badge(r.order.receiptId) : ""}<span class="ost ${st[0]}">${esc(st[1])}</span><span class="rowFacts">Qty ${qty} · <span class="due ${due.cls}">Ship by ${esc(due.txt)}</span></span>${why ? `<span class="rowExcerpt reviewReason" title="${esc(why)}">${esc(why)}</span>` : ''}${r.hold ? '<button class="btn ghost sm relHold" type="button">Release hold</button>' : ''}${gateBtn}</div>`;
      const number=node.querySelector('.onum');if(number){const time=el('span','orderTime');time.textContent=date.time;time.title=date.label;number.appendChild(time);}
      node.onclick = e => { if (e.target.closest("button,[role=button]") !== node && e.target.closest("button,[role=button]")) return; OrderWin.open(r.key); };
      node.onkeydown=e=>{if(e.target===node && (e.key==="Enter" || e.key===" ")){e.preventDefault();OrderWin.open(r.key);}};
      { const rh = node.querySelector(".relHold:not([data-gate])"); if (rh) rh.onclick = e => { e.stopPropagation(); Review.repool(r); }; }
      { const gb = node.querySelector("[data-gate]"); if (gb) gb.onclick = e => { e.stopPropagation(); gb.disabled = true; (gb.dataset.gate === "release" ? Gate.release(gb.dataset.gm) : Gate.cutAnyway(gb.dataset.gm)).catch(err => toast(err.message, "bad", 6000)); }; }
      // an order can be several lines on several cards: hovering one lifts all of them, the way the station does
      node.dataset.rid = String(r.order.receiptId);
      if(cached?.node){const pair=cached.node.querySelector('.comparePair');if(pair)node.querySelector('.comparePair')?.replaceWith(pair);}
      place(node);orderNodes.delete(r.key);orderNodes.set(r.key,{stamp,node});ListMedia.mount(node,r);
    }
    if(orderNodes.size>NODES_KEPT){const onList=new Set(shown.map(r=>r.key));for(const key of orderNodes.keys()){if(orderNodes.size<=NODES_KEPT)break;if(!onList.has(key))orderNodes.delete(key);}}
    const keep=new Set(wanted);for(const child of [...list.children])if(!keep.has(child))child.remove();
    ListMedia.more(list,rows.length,shown.length,()=>{OV.limit=(OV.limit || 48)+48;renderBody();});

    restore();
    if(anchorKey){const same=[...host.querySelectorAll('[data-key]')].find(n=>n.dataset.key===anchorKey);if(same)host.scrollTop+=same.getBoundingClientRect().top-anchorTop;}
  }
  /* The tab used to be one `v.innerHTML = …` on every call, and it is called from fifteen places — RunCtl.renderBanner's
     last line among them, which itself has eighteen callers, and Pool.addAll every five rows. Pooling two hundred lines
     rebuilt the whole tab forty times, and each rebuild took the scroller's position and the caret out of the search box
     with it. The head is built once, what changes is patched, and the body is the only thing ever re-emitted. */
  function orderTotals(rows) {
    const receipts=new Set(),lines=new Set();let charms=0;
    for(const r of rows) {
      if(r.state==="gone")continue;
      const receipt=String(r.order?.receiptId ?? "");if(receipt)receipts.add(receipt);
      const transaction=r.line?.transactionId,key=transaction!=null ? receipt+"/"+transaction : r.key;
      if(key && lines.has(key))continue;if(key)lines.add(key);
      if(r.spec?.noDesign || r.state==="noDesign")continue;
      const qty=Number(r.spec?.quantity ?? r.line?.quantity ?? 1);
      if(Number.isFinite(qty) && qty>0)charms+=Math.round(qty);
    }
    return {orders:receipts.size,charms};
  }
  function buildHead(v) {
    // One compact row: totals, filters/search, then the display controls.
    v.innerHTML = `<div class="ordHead">
        <div class="ordBar ordCompact" id="ordBar">
          <span class="controlGroup ordSummary"><button class="btn sm" id="ordPull">Pull orders</button><span class="chips" id="ordChips"></span><span class="charmTotal" id="ordCharmTotal"></span></span>
          <span class="controlGroup ordNarrow"><span class="chips" id="ordMetalHost"></span><input class="ordSearch" id="ordQ" placeholder="order, SKU, words…" title="search the order number, the SKU, the title and everything the customer or the shop wrote"></span>
          <span class="controlGroup ordDisplay"><select class="ordSort" id="ordSort" title="what orders the cards"><option value="arrival">newest first</option><option value="due">by ship-by</option><option value="order">by order</option><option value="state">by state</option></select><span class="viewSeg" id="ordViewSeg"></span></span>
        </div>
      </div><div class="ordBody" id="ordBody"></div>`;
    const q = v.querySelector("#ordQ");
    q.oninput = () => { OV.q = q.value; renderBody(); };                 // never rebuilt now, so the caret needs no restoring
    v.querySelector("#ordSort").onchange = e => { OV.sort = e.target.value; OV.desc = false; renderBody(); };
    v.querySelector("#ordPull").onclick = async () => { if (v.querySelector("#ordPull").disabled) return; try { await pull(null); } catch (e) { toast(e.message, "bad", 7000); agent({ bridge: true }, "warn", e.message); } };
    Sandbox.mountPanel(v);
  }
  /** Update totals and filters without rebuilding the search field. */
  function renderHead(v) {
    const all = rowsOf().filter(r=>r.state!=="gone"), totals=orderTotals(all);
    OV.pile=null; // Removed state chips must not leave an invisible saved filter.
    const running = B.run && !["complete", "stopped"].includes(B.run.status);
    const pull = v.querySelector("#ordPull");
    pull.disabled = !!running;
    // between arrivals there is no Stop to press: say what the run is doing with the lines instead
    pull.title = !running ? "refresh open orders using the pull rule in Settings" : B.run.status === "processed" ? "the open run holds these lines · new orders join it as they arrive" : "a run is open — stop it first, or its lines would be replaced under it";
    v.querySelector("#ordSort").value = OV.sort;
    const charmTotal=v.querySelector("#ordCharmTotal");
    charmTotal.innerHTML=`<b>${totals.charms}</b> charms`;
    charmTotal.title="Total charm quantity across these open orders; chain-only and packaging items are excluded";
    const byMetal = {}; for (const r of all) { const m = r.material || "none"; byMetal[m] = (byMetal[m] || 0) + 1; }
    const chip = (on, id, label, n, cls, title) => `<button class="egTab${on ? " on" : ""}" data-pile="${esc(id)}" title="${esc(title || "")}">${esc(label)}<b class="${cls}">${n}</b></button>`;
    const metals = ["gold", "silver", "rose", "gold10k", "gold14k", "none"].filter(m => byMetal[m] || OV.metal === m);
    /* Material, kind of jewellery and "does it get engraved" are the three ways a bench actually narrows a day's work.
       Each one appears only when the lines on screen give it more than one answer — a filter with one option is a
       control that can only ever do nothing. */
    const byForm = {}; for (const r of all) { const f = (r.spec && r.spec.form) || "none"; byForm[f] = (byForm[f] || 0) + 1; }
    const forms = Object.keys(byForm).filter(f => f !== "none" || OV.form === "none").sort((a2, b2) => byForm[b2] - byForm[a2]);
    const engN = all.filter(r => r.engrave && r.engrave.needed).length;
    v.querySelector("#ordChips").innerHTML = chip(true,"","Open Orders",totals.orders,"","Distinct open order numbers, across all materials");
    const sel = (id, ttl, any, opts, cur) => opts.length > 1 || cur ? `<select class="ordMetal" id="${id}" title="${esc(ttl)}">${[`<option value="">${esc(any)}</option>`].concat(opts.map(o => `<option value="${esc(o[0])}"${cur === o[0] ? " selected" : ""}>${esc(o[1])} \u00b7 ${o[2]}</option>`)).join("")}</select>` : "";
    v.querySelector("#ordMetalHost").innerHTML =
      sel("ordMetal", "narrow it to one material", "Any material", metals.map(m => [m, m === "none" ? "No material" : labelOf(m), byMetal[m] || 0]), OV.metal)
      + sel("ordForm", "narrow it to one kind of jewellery", "Any kind", forms.map(f => [f, FORM_LABEL[f] || (f === "none" ? "Kind not set" : f), byForm[f] || 0]), OV.form)
      + (engN && engN < all.length || OV.eng ? sel("ordEng", "engraved or not", "Engraved or not", [["yes", "Engraved", engN], ["no", "Not engraved", all.length - engN]], OV.eng) : "");
    v.querySelectorAll("[data-pile]").forEach(b => b.onclick = () => { OV.pile = b.dataset.pile || null; renderHead(v); renderBody(); });
    for (const [id, k] of [["ordMetal", "metal"], ["ordForm", "form"], ["ordEng", "eng"]]) { const n = v.querySelector("#" + id); if (n) n.onchange = () => { OV[k] = n.value || null; renderHead(v); renderBody(); }; }
    v.querySelector("#ordViewSeg").innerHTML = ["cards", "list"].map(k => `<button data-view="${k}"${viewMode() === k ? ' class="on"' : ""} title="${k === "cards" ? "a card for every line, with its picture" : "the same lines as rows"}">${k === "cards" ? "Cards" : "List"}</button>`).join("");
    v.querySelectorAll("[data-view]").forEach(b => b.onclick = () => { OV.view = b.dataset.view; S.settings.orderView = OV.view; saveSettings(); renderHead(v); renderBody(); });
  }
  /* Drawn once a frame (CNFrame): a run step, the banner, each arrival and the pool pass every five lines asked for it,
     many times in one go. A hidden tab draws only its count, and is drawn when shown (Views.onShow); one asked for while
     it was shown is drawn in full even if it is hidden by the frame, as it was when it was drawn at once. */
  let bodyWanted = false;
  const onScreen = () => { const v = document.getElementById("ordersView"); return !!v && !v.classList.contains("hidden"); };
  function render() {
    if (!window.CNFrame) return renderNow();
    if (onScreen()) bodyWanted = true;
    CNFrame.later("orders", renderNow);
  }
  function renderNow() {
    window.CNFrame?.cancel("orders");
    const v = document.getElementById("ordersView"), wanted = bodyWanted; bodyWanted = false;
    if (!v || (v.classList.contains("hidden") && !wanted)) { const tb = document.getElementById("tabOrdersN"); if (tb) tb.textContent = B.orders.rows.length ? String(orderTotals(B.orders.rows).orders) : ""; return; }
    if (!v.dataset.built) { v.dataset.built = "1"; buildHead(v); }
    renderHead(v);
    renderBody();
    const tb = document.getElementById("tabOrdersN"); if (tb) tb.textContent = B.orders.rows.length ? String(orderTotals(B.orders.rows).orders) : "";
  }
  return { view: () => OV, pull, claim, unclaim, revalidate, render, renderNow, renderBody, markStale, loadMaps, interpretAll, lineRecord, rowFromRecord, rows: rowsOf, visibleRows, placeOf, imageFor, wantImage, shipTxt, statePill: r => STATE_PILL[r.state] || ["neutral", r.state], applyPullRule, ctx, keepRest };
})();

/* ═══ 19 · Master — SKU labels under charms, per-SKU designs, the index ══════ */
const Master = window.Master = (() => {
  const entryFor = sku => B.master.entries.get(String(sku || "").toUpperCase()) || null;
  let showAll = false;                                              // the grid draws 600 tiles until asked for the rest
  let reindexAll = false;                                           // by default a SKU the library already holds is left alone
  /** A design drawn only in sizes keeps its picture and file under each size; the entry's own are empty. */
  const thumbOf = e => e.thumbUrl || ((Object.values(e.sizes || {}).find(s => s && s.thumbUrl) || {}).thumbUrl) || "";
  let previewObserver=null, previewQueue=[], previewRunning=0;
  function mountMasterPreviews(grid) {
    previewObserver?.disconnect();previewQueue=[];
    const paint=async host=>{
      const entry=entryFor(host.dataset.previewSku);
      const size=entry?.sizes && Object.keys(entry.sizes)[0] || null;
      if(!entry || !host.isConnected)return;
      try {
        const thumb=await Pool.masterPreview(entry,size);
        if(!host.isConnected)return;
        const img=document.createElement("img");img.alt="";img.src=thumb;
        host.replaceChildren(img);
      } catch (_) {if(host.isConnected)host.textContent="Preview unavailable";}
    };
    const pump=()=>{while(previewRunning<2 && previewQueue.length){const host=previewQueue.shift();if(!host.isConnected)continue;previewRunning++;void paint(host).finally(()=>{previewRunning--;pump();});}};
    if(typeof IntersectionObserver!=="function") {previewQueue.push(...grid.querySelectorAll('[data-preview-sku]'));pump();return;}
    previewObserver=new IntersectionObserver(entries=>{for(const entry of entries)if(entry.isIntersecting){previewObserver.unobserve(entry.target);previewQueue.push(entry.target);}pump();},{rootMargin:"150px"});
    grid.querySelectorAll('[data-preview-sku]').forEach(host=>previewObserver.observe(host));
  }
  async function load(force) {
    if (!S.cloud.ok) return;
    if (!force && Date.now() - B.master.loadedAt < 120000) return B.master.loading || null;
    if (B.master.loading) return B.master.loading;
    B.master.loading = (async () => {
      render();
      try {
        const [ix, fl] = await Promise.all([api("charmNestLibrary", { op: "masterList", limit: 3000 }, { label: "Loading the charm library" }), api("charmNestLibrary", { op: "masterListFiles" })]);
        B.master.entries = new Map((ix.entries || []).map(e => [e.sku, e])); B.master.files = fl.files || []; B.master.loadedAt = Date.now();
        B.master.error = null;
      } catch (e) { B.master.error = e.message; throw e; }   // a failed load must not look like an empty library
    })().finally(() => { B.master.loading = null; render(); });
    return B.master.loading;
  }
  async function fetchEntry(sku) { sku = String(sku || "").toUpperCase(); if (!sku) return null; const r = await api("charmNestLibrary", { op: "masterGet", sku }); if (r.entry) B.master.entries.set(sku, r.entry); return r.entry; }
  const skuRegex = () => { try { return new RegExp(S.settings.skuPattern || DEFAULTS.skuPattern); } catch (_) { return P.SKU_PATTERN_DEFAULT; } };
  /** Render the strip under a charm (for the vision fallback) → PNG data URL. */
  /** The strip a label would occupy: the outline's width (widened 30 %), from its bottom edge down by the label gap. */
  function stripBox(c, gapPt) { const b = c.outline.bbox, w = b[2] - b[0]; return [b[0] - w * 0.3, b[1] - gapPt - 6, b[2] + w * 0.3, b[1] + 2]; }
  /** Which of these charms have something drawn under them that belongs to no charm — outlined label text, most likely.
      Everything every charm owns is collected once, so a sheet of thousands is a pass over what is left, not over all of it. */
  async function strayInkUnder(parsed, group, charms, gapPt, onTick) {
    const owned = new Set();
    for (const c of group.charms) { owned.add(c.outline); for (const m of c.members) owned.add(m); }
    const cand = parsed.segments.concat(parsed.nested).filter(s => s.bbox && !owned.has(s) && (s.kind === "path" || s.kind === "text" || s.kind === "image"));
    const out = [];
    for (let i = 0; i < charms.length; i++) {
      const c = charms[i], box = stripBox(c, gapPt), top = c.outline.bbox[1] + 2, segs = [];
      let x0 = Infinity, x1 = -Infinity;
      for (const sg of cand) { const b = sg.bbox; if (b[2] < box[0] || b[0] > box[2] || b[3] < box[1] || b[1] > box[3] || b[3] >= top) continue; segs.push(sg); x0 = Math.min(x0, b[0]); x1 = Math.max(x1, b[2]); }
      // outlined text is many small shapes in a row, one or more per letter; one or two stray bits are a scrap of
      // artwork, not a label, and sending them to be read wastes a call and asks a person to confirm nothing
      const cw = c.outline.bbox[2] - c.outline.bbox[0];
      if (segs.length >= 6 && x1 - x0 >= cw * 0.4) out.push({ charm: c, segs });
      if (onTick && i % 250 === 0) { onTick(i, charms.length, cand.length); await sleep(0); }
    }
    return out;
  }
  function stripPng(parsed, c, gapPt, only) {
    const b = c.outline.bbox, w = b[2] - b[0]; const x0 = b[0] - w * 0.3, x1 = b[2] + w * 0.3, y1 = b[1] + 2, y0 = b[1] - gapPt - 6;
    const k = Math.min(6, 900 / (x1 - x0)); const cv = document.createElement("canvas"); cv.width = Math.ceil((x1 - x0) * k); cv.height = Math.ceil((y1 - y0) * k);
    const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height);
    const tx = (x, y) => [(x - x0) * k, (y1 - y) * k];
    const segs = only || parsed.segments.concat(parsed.nested).filter(s => s.bbox && s.kind !== "clip" && s.kind !== "noop" && !(s.kind === "xobj" && s.children && s.children.length) && !(s.bbox[2] < x0 || s.bbox[0] > x1 || s.bbox[3] < y0 || s.bbox[1] > y1));
    P.drawSegments(ctx, segs, tx, k);
    return cv.toDataURL("image/png");
  }
  /** Index a master file in the browser (design §6.3); the server route is used above `masterServerAbove` charms. */
  /** How many charms in this file carry a SKU written under them — is it a master library sheet, not a sheet to nest?
      Stray text elsewhere on a page does not count: the line has to sit under an outline, as a label does. */
  function looksLikeMaster(parsed, charms) {
    const pat = skuRegex(), gap = (+S.settings.labelGapMm || 6.4) * PT;
    const runs = parsed.segments.concat(parsed.nested).filter(x => x.kind === "text"), lines = [];
    for (const r of runs) { if (r.pieces && r.pieces.length > 1) lines.push(...r.pieces); else lines.push(r); }
    const live = charms.filter(c => c.mergedInto == null); if (!live.length) return 0;
    let n = 0;
    for (const t of lines) {
      if (!t.str || !t.bbox || !P.parseSkuLabel(t.str, pat)) continue;
      const cx = (t.bbox[0] + t.bbox[2]) / 2, top = t.bbox[3];
      for (const c of live) { const b = c.outline.bbox, w = b[2] - b[0], d = b[1] - top; if (d < -1 || d > gap) continue; if (cx < b[0] - w * 0.25 || cx > b[2] + w * 0.25) continue; n++; break; }
    }
    return n >= 3 && n >= live.length * 0.2 ? n : 0;
  }
  async function indexFile(file) {
    // whatever the caller had to hand — a File from the picker or a drop, bytes already read, or a plain buffer
    const bytes = await (async () => {
      // `data` is what this app hands over. A File also answers to `bytes`, but there it is a method the browser added
      // (Blob.bytes()), not the contents — reading it as data is what made an upload fail with "subarray is not a function".
      const own = file && (file.data != null ? file.data : (file.bytes != null && typeof file.bytes !== "function" ? file.bytes : null));
      const raw = own != null ? own : (file && typeof file.arrayBuffer === "function" ? await file.arrayBuffer() : file);
      if (raw instanceof Uint8Array) return raw;
      if (raw instanceof ArrayBuffer) return new Uint8Array(raw);
      if (ArrayBuffer.isView(raw)) return new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength);
      throw new Error(`could not read ${file && file.name ? file.name : "the file"} — its contents arrived as ${Object.prototype.toString.call(raw)}`);
    })();
    if (!bytes.length) throw new Error(`${file && file.name ? file.name : "the file"} is empty, or its contents were already handed to something else`);
    const masterHash = await sha256(bytes);
    const job = { name: file.name, masterHash, state: "parsing", t0: performance.now(), log: [] }; B.master.jobs.set(masterHash, job); render();
    job.bar = window.CNProgress ? CNProgress.start(`Indexing ${file.name}`, { note: `${(bytes.length / 1048576).toFixed(1)} MB · reading` }) : null;
    const say = (kind, text) => { job.log.push(text); agent({ master: masterHash }, kind, `${file.name}: ${text}`); render(); };
    try {
      say("MASTER", "reading…");
      const parsed = await P.parseSource(bytes, file.name);
      if (job.bar) job.bar.note("finding the charms");
      const g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: +S.settings.minPt || 6 });
      say("MASTER", `${g.charms.length} charm outline(s), ${parsed.counts.text} text block(s)`);
      // the server route (a background function with about 1 GB) is opt-in by charm count; 0 keeps every master in this tab
      if (+S.settings.masterServerAbove > 0 && g.charms.length > +S.settings.masterServerAbove && S.cloud.ok) {
        say("MASTER", `over ${S.settings.masterServerAbove} charms — indexing on the server`);
        const up = await uploadBytes(`charmnest/master/files/${masterHash.slice(0, 12)}-${file.name.replace(/[^\w.\-]+/g, "_")}`, bytes, "application/pdf", "Uploading master");
        const r = await api("charmNestLibrary", { op: "startMaster", path: up.path, name: file.name, opts: { skuPattern: S.settings.skuPattern, labelGapMm: S.settings.labelGapMm, minPt: S.settings.minPt, engraveMarginMm: S.settings.engraveMarginMm, replaces: B.master.files.filter(f => f.name === file.name && f.masterHash !== masterHash).map(f => f.masterHash) } });
        job.state = "server"; job.jobId = r.id;
        const t0 = Date.now(); let lastChange = Date.now(), lastSig = "";
        for (;;) { await sleep(3000); const j = await api("charmNestLibrary", { op: "getJob", id: r.id }); const d = j.job; if (!d) throw new Error("server job vanished");
          const sig = `${d.status}|${d.stage}|${d.done}`; if (sig !== lastSig) { lastSig = sig; lastChange = Date.now(); }
          const silent = Math.round((Date.now() - lastChange) / 60000);
          job.progress = `${d.done && d.total ? `${d.stage} ${d.done}/${d.total}` : d.stage} · ${Math.round((Date.now() - t0) / 60000)} min${silent >= 2 ? ` · no progress for ${silent} min` : ""}`; render();
          if (d.status === "done") { job.result = d.result; break; }
          if (d.status === "error") throw new Error(d.error || "server indexing failed");
          // a background function that stops reporting has run out of memory or time (about 1 GB and 15 min): say so instead of spinning
          if (Date.now() - lastChange > 6 * 60000) throw new Error(`the server stopped reporting at "${d.stage}" ${silent} min ago — the file is too large for the server route (about 1 GB of memory, 15 min). Index it with the local indexer: node scripts/index-master.cjs "<file>" --origin ${location.origin}`);
          if (Date.now() - t0 > 16 * 60000) throw new Error("the server route ran past its 15-minute limit — index the file with the local indexer: node scripts/index-master.cjs"); }
        await load(true);
        job.state = "done"; say("ok", `server indexed ${job.result.labelled} SKU(s), ${job.result.unlabelled.length} unlabelled, ${job.result.orphans.length} orphan label(s), ${job.result.blocked.length} blocked`);
        render(); return job;
      }
      const lab = P.labelCharms(parsed, g.charms, { pattern: skuRegex(), gapPt: (+S.settings.labelGapMm || 6.4) * PT, widen: 0.25 });
      say("MASTER", `${lab.labels.size} labelled (${lab.skuCount} SKU line(s)) · ${lab.unlabelled.length} unlabelled · ${lab.orphans.length} orphan label(s) · ${lab.duplicates.length} duplicate(s)${lab.undecodable.length ? ` · ${lab.undecodable.length} text run(s) unreadable (outlined or CID font without ToUnicode)` : ""}`);
      // Claude's grouping review merges fragments before anything is indexed
      const src = { id: "master:" + masterHash.slice(0, 8), name: file.name, bytes, parsed, group: g, charms: g.charms, metal: null, state: "ready", master: true };
      src.charms.forEach((c, i) => Object.assign(c, { id: src.id + ":" + i, sourceId: src.id, sourceName: file.name, index: c.index, name: c.sku || null, excluded: false, cloud: null }));
      job.state = "silhouettes";
      // only the charms that carry a SKU are indexed, so only they are traced: on the real master that is 1,038 of 3,408
      // a ring drawn beside the body is welded into the cut line before the charm is measured or written
      { let welded = 0, left = 0; for (const c of g.charms) { if (c.mergedInto != null || !c.sku || c.alreadyHeld) continue; const r = P.integrateRings(c); welded += r.welded; left += r.left.length; } if (welded || left) say("MASTER", `${welded} jump ring(s) welded into their charm's cut line${left ? ` · ${left} need a geometry check` : ""}`); }
      await P.buildSilhouettes(parsed, g.charms.filter(c => c.mergedInto == null && c.sku && !c.alreadyHeld), +S.settings.silhouetteRes || 6, (d, t) => { if (job.bar) job.bar.label(`Tracing charms · ${file.name}`).set(d, t); job.progress = `silhouettes ${d}/${t}`; if (d % 10 === 0) render(); });
      // The SKUs are read from the sheet as text, which is quick, so the library is consulted before any work is done:
      // a charm whose SKUs are all held already is left alone. A charm with even one new SKU is rebuilt whole, so all of
      // its SKUs keep sharing one design file, and the master they came from is superseded rather than reported as a clash.
      await load(true).catch(() => {});
      const skusOf = c => [c.sku, ...(c.extraSkus || []).map(x => x.sku)].filter(Boolean).map(x => String(x).toUpperCase());
      const known = B.master.entries;
      const labelledCharms = g.charms.filter(c => c.mergedInto == null && c.sku);
      const supersede = new Set();
      let held = 0;
      if (!reindexAll) {
        for (const c of labelledCharms) {
          const mine = skusOf(c);
          if (mine.every(sk => known.has(sk))) { c.alreadyHeld = true; held++; continue; }
          for (const sk of mine) { const e = known.get(sk); if (e && e.masterHash && e.masterHash !== masterHash) supersede.add(e.masterHash); }
        }
      }
      job.held = held; job.supersede = [...supersede];
      if (held) say("MASTER", `${held} charm(s) are already in the library and are left as they are · ${labelledCharms.length - held} to index${reindexAll ? "" : " (tick “re-index SKUs already held” to rebuild them all)"}`);
      if (held === labelledCharms.length) { await load(true); job.state = "done"; job.written = 0; say("ok", `nothing new on this sheet — all ${held} charm(s) are already in the library`); render(); return job; }
      const liveCount = g.charms.filter(c => c.mergedInto == null).length;
      if (S.settings.review !== "off" && S.cloud.ok && liveCount <= 300) { try { job.state = "review"; render(); await reviewGrouping(src); } catch (e) { say("warn", `Claude grouping review skipped: ${e.message}`); } }
      else if (S.settings.review !== "off" && S.cloud.ok) say("MASTER", `grouping review skipped: ${liveCount} charms is more than one review can hold (300) — the geometry stands, the report lists what to check`);
      // outlined labels: the strip under each unlabelled charm goes to Claude with a strict schema; a person confirms every
      // read. Only a strip with something drawn in it is sent: a charm with nothing under it is reported, not read.
      job.state = "vision"; job.progress = "looking under the unlabelled charms"; render(); await sleep(0);
      const unlAll = g.charms.filter(c => c.mergedInto == null && !c.sku);
      let found = await strayInkUnder(parsed, g, unlAll, (+S.settings.labelGapMm || 6.4) * PT, (d, t) => { if (job.bar) job.bar.label(`Looking under the unlabelled charms · ${file.name}`).set(d, t); job.progress = `looking under the unlabelled charms ${d}/${t}`; render(); });
      let unl = found.map(f => f.charm);
      job.vision = [];
      if (unlAll.length > unl.length) say("MASTER", `${unlAll.length - unl.length} unlabelled charm(s) have nothing under them — reported as unlabelled`);
      const VISION_MAX = 200;
      if (unl.length > VISION_MAX) { say("MASTER", `${unl.length} charm(s) have something under them that was not read as text — more than one pass sends to Claude (${VISION_MAX}); they are reported as unlabelled instead`); unl = []; found = []; }
      if (unl.length && S.cloud.ok) {
        say("MASTER", `asking Claude to read the strip under ${unl.length} unlabelled charm(s)`);
        const strips = found.map(f => ({ index: f.charm.index, image: stripPng(parsed, f.charm, (+S.settings.labelGapMm || 6.4) * PT, f.segs) }));
        for (let i = 0; i < strips.length; i += 40) {
          const r = await agentCall("labelRead", { sourceName: file.name, strips: strips.slice(i, i + 40) }, { label: "Claude is reading labels" });
          if (r.skipped) { say("warn", `label read skipped — ${r.skipped}`); break; }
          // a strip Claude could not read is not a label to confirm: it stays an unlabelled charm in the report
          for (const rd of r.reads || []) { if (!rd.sku) continue; const c = g.charms.find(x => x.index === rd.index); const st = strips.find(x => x.index === rd.index); if (c && st) job.vision.push({ index: rd.index, sku: rd.sku, size: rd.size, confidence: rd.confidence, image: st.image, charm: c, confirmed: false }); }
        }
      }
      if (unl.length && !job.vision.length) say("MASTER", `Claude could not read a SKU under any of the ${unl.length} charm(s) with marks beneath them — they stay unlabelled`);
      job.state = "writing"; job.parsed = parsed; job.charms = g.charms; job.lab = lab; job.src = src;
      await writeIndex(job);
      await load(true);                                                    // the index is reloaded before the job reads "done"
      job.finishedAt = Date.now(); job.state = "done"; say("ok", `indexed ${job.written} SKU(s)${job.held ? ` · ${job.held} charm(s) were already in the library` : ""}${job.vision.length ? ` · ${job.vision.length} label(s) read by Claude await confirmation` : ""}`);
      render(); return job;
    } catch (e) { job.finishedAt = Date.now(); job.state = "error"; job.error = e.message; say("warn", `indexing failed: ${e.message}`); render(); throw e; }
    finally { if (job.bar) { job.bar.end(); job.bar = null; } settleJob(job); }
  }
  /** A finished index keeps what its row and its report show. The parsed file, its traced charms and its bytes (the
      largest things this page holds: a master of thousands of charms) go once no label Claude read waits for a person,
      and only the newest twenty finished rows stay (Paul, 24 Sep: nothing may pile up on a page left open). */
  function settleJob(job) {
    if (!["done", "error"].includes(job.state) || (job.vision || []).some(v => !v.confirmed)) return;
    const l = job.lab;
    if (l && l.labels instanceof Map) job.lab = Object.assign({}, l, { labels: { size: l.labels.size } });   // each label held a piece of the parsed file
    if (job.charms) job.charmCount = job.charms.length;
    if (job.vision) job.vision = job.vision.map(v => ({ index: v.index, sku: v.sku, size: v.size, confidence: v.confidence, confirmed: v.confirmed }));
    job.parsed = job.src = job.charms = null;
    const done = [...B.master.jobs.values()].filter(j => ["done", "error"].includes(j.state) && !(j.vision || []).some(v => !v.confirmed));
    for (const old of done.slice(0, Math.max(0, done.length - 20))) B.master.jobs.delete(old.masterHash);
  }
  /** Per labelled charm: the per-SKU .ai + thumbnail, the geometry, the derived flags; then the index and file records. */
  async function writeIndex(job) {
    const { parsed, charms, lab, masterHash, name } = job; const entries = [], blocked = [], skus = [];
    const live = charms.filter(c => c.mergedInto == null && c.sku && !c.excluded && !c.alreadyHeld);
    let n = 0;
    const one = async (c) => {
      const key = c.skuSize ? `${c.sku}__${c.skuSize}` : c.sku;
      let ai = null, png = null;
      if (S.cloud.ok) { const bytes = await P.buildSingleCharm(c, parsed); [ai, png] = await Promise.all([uploadBytes(`charmnest/master/${key}.ai`, bytes, "application/illustrator", `Saving ${key}`), uploadBytes(`charmnest/master/${key}.png`, dataUrlToBytes(c.thumb), "image/png")]); }
      const reasons = []; if (c.open) reasons.push("open outline");
      let engravable = true, upAngle = null, upSource = "drawn", flipOk = true;
      try { const up = window.CharmNestBackground ? await CharmNestBackground.run('indexGeometry',{charm:{outline:c.outline,members:c.members,bbox:c.bbox,widthPt:c.widthPt,heightPt:c.heightPt}}) : G.upAngleOf(c); upAngle = up.angle; upSource = up.source; if(!window.CharmNestBackground)G.backView(c, { res: 6, upAngle }); }
      catch (e) { flipOk = false; reasons.push(e.message); }
      const wMm = c.widthPt * MM, hMm = c.heightPt * MM; const outOfRange = Math.max(wMm, hMm) > (+S.settings.sizeMaxMm || 60) || Math.max(wMm, hMm) < (+S.settings.sizeMinMm || 3);
      entries.push({ sku: c.sku, size: c.skuSize, charmHash: c.hash, widthPt: c.widthPt, heightPt: c.heightPt, areaPt2: c.areaPt2, members: c.members.length, holes: P.cutLinesOf(c).length, engravable, upAngle, upSource, aiPath: ai && ai.path, aiUrl: ai && ai.url, thumbPath: png && png.path, thumbUrl: png && png.url, open: !!c.open, labelSource: c.labelSource || "text", confidence: c.labelConfidence == null ? null : c.labelConfidence, blocked: reasons.length ? reasons.join("; ") : null, outOfRange, flipOk, backKeepOut: keepOutOf(c).length ? keepOutOf(c).map(m => ({ layer: m.layer })) : null });
      if (reasons.length) blocked.push({ sku: c.sku, reason: reasons.join("; ") }); skus.push(c.sku);
      for (const x of c.extraSkus || []) { entries.push(Object.assign({}, entries[entries.length - 1], { sku: x.sku, size: x.size })); skus.push(x.sku); if (reasons.length) blocked.push({ sku: x.sku, reason: reasons.join("; ") }); }   // every further line under the charm: the same design under another SKU
      job.progress = `written ${++n}/${live.length}`; if (job.bar) job.bar.label(`Writing the charm library · ${job.name}`).set(n, live.length); if (n % 5 === 0) render();
    };
    const queue = live.slice(); await Promise.all(Array.from({ length: 4 }, async () => { while (queue.length) await one(queue.shift()); }));
    // a small ring left loose beside a charm (not merged by grouping or review) blocks that charm
    for (const o of job.src.group.orphans || []) { const b = o.bbox; if (!b || o.kind !== "path" || !o.closed) continue; if (Math.max(b[2] - b[0], b[3] - b[1]) > 13) continue; const cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2; for (const c of live) { const ob = c.outline.bbox; if (cx < ob[0] - 4 * PT || cx > ob[2] + 4 * PT || cy < ob[1] - 4 * PT || cy > ob[3] + 4 * PT) continue; if (G.distToPolys(cx, cy, G.flatten(c.outline, 8)) <= 3 * PT) { const e = entries.find(x => x.sku === c.sku); if (e && !/detached ring/.test(e.blocked || "")) { e.blocked = (e.blocked ? e.blocked + "; " : "") + "detached ring not merged"; blocked.push({ sku: c.sku, reason: "detached ring not merged" }); } } } }
    job.entries = entries; job.blocked = blocked; job.written = entries.length;
    if (!S.cloud.ok) return;
    const replaces = [...new Set(B.master.files.filter(f => f.name === name && f.masterHash !== masterHash).map(f => f.masterHash).concat(job.supersede || []))];
    const r = await api("charmNestLibrary", { op: "masterPutIndex", entries, masterHash, masterPath: job.masterPath || null, masterName: name, hashSource: "browser", replaces }, { label: "Writing the master index" });
    job.conflicts = r.blocked || []; job.sizeMoved = r.sizeMoved || [];
    if (job.conflicts.length) agent({ master: masterHash }, "warn", `${name}: ${job.conflicts.length} SKU(s) also live in another master file — blocked until fixed: ${job.conflicts.map(b => b.sku).join(", ")}`);
    if (job.sizeMoved.length) agent({ master: masterHash }, "warn", `${name}: ${job.sizeMoved.length} SKU(s) changed size by more than 5% since the last index: ${job.sizeMoved.map(b => b.sku).join(", ")}`);
    await api("charmNestLibrary", { op: "masterPutFile", file: { masterHash, path: job.masterPath || null, name, charms: charms.filter(c => c.mergedInto == null).length, labelled: lab.labels.size, unlabelled: lab.unlabelled, orphans: lab.orphans, duplicates: lab.duplicates, undecodable: lab.undecodable.length, blocked: blocked.concat(job.conflicts.map(b => ({ sku: b.sku, reason: b.reason }))), skus, indexedBy: "browser", pageW: parsed.pageW, pageH: parsed.pageH, replaces, visionReads: (job.vision || []).map(v => ({ index: v.index, sku: v.sku, size: v.size, confidence: v.confidence, confirmed: v.confirmed })) } });
  }
  /** Members drawn on a "BACK KEEP-OUT" layer of the master are subtracted from the engraving mask. */
  const keepOutOf = c => c.members.filter(m => m.layer && /back\s*keep-?out/i.test(m.layer));
  /** A person confirms Claude's read of an outlined label: the charm gets that SKU and is written like any labelled one. */
  async function confirmVision(job, reads) {
    for (const v of reads) { const c = v.charm; c.sku = v.sku; c.skuSize = v.size || null; c.labelSource = "vision"; c.labelConfidence = v.confidence; c.name = v.sku; v.confirmed = true; job.lab.labels.set(c.index, { sku: v.sku, size: v.size, seg: null }); job.lab.unlabelled = job.lab.unlabelled.filter(i => i !== c.index); }
    job.state = "writing"; render();
    await writeIndex(job);
    for (const v of reads) await api("charmNestLibrary", { op: "masterPatch", sku: v.sku, patch: { labelSource: "vision", confirmedBy: employeeName() || "operator" } }).catch(() => {});
    job.state = "done"; await load(true); settleJob(job); render();
  }
  /** The same patch on every SKU of one charm, with a single redraw. */
  /** Everything the run found, as a file: the lists are too long to read on screen but belong somewhere. */
  function saveReport(job) {
    const l = job.lab || {};
    const rep = { file: job.name, masterHash: job.masterHash, at: new Date().toISOString(),
      charms: job.charms ? job.charms.length : job.charmCount ?? null, labelled: l.labels ? l.labels.size : null, skuLines: l.skuCount || null, written: job.written || 0,
      unlabelledCharmIndices: l.unlabelled || [], linesWithNoCharmAbove: (l.orphans || []).map(o => ({ sku: o.sku, size: o.size || null })),
      skusUnderTwoCharms: l.duplicates || [], blocked: job.blocked || [], alsoInAnotherMaster: job.conflicts || [], readByClaude: (job.vision || []).map(v => ({ index: v.index, sku: v.sku, size: v.size, confidence: v.confidence, confirmed: v.confirmed })) };
    const url = URL.createObjectURL(new Blob([JSON.stringify(rep, null, 1)], { type: "application/json" }));
    const a = document.createElement("a"); a.href = url; a.download = `${String(job.name || "master").replace(/\.[^.]+$/, "")}-index-report.json`; a.click();
    setTimeout(() => URL.revokeObjectURL(url), 8000);
    toast("report saved", "ok");
  }
  async function patchMany(skus, p) { for (const s of skus) { await api("charmNestLibrary", { op: "masterPatch", sku: s, patch: p }); const e = entryFor(s); if (e) Object.assign(e, p); } render(); }
  async function patch(sku, p) { await api("charmNestLibrary", { op: "masterPatch", sku, patch: p }); const e = entryFor(sku); if (e) Object.assign(e, p); render(); }
  /** Every SKU the pulled orders want that no master file holds, newest pull first. */
  function missingSkus() {
    const out = new Map();
    for (const r of Orders.rows()) {
      if (r.state === "gone" || r.spec.noDesign) continue;
      const p = (r.problems || []).find(x => x.kind === "unmatchedSku" && x.sku);
      if (!p) continue;
      if (!out.has(p.sku)) out.set(p.sku, { sku: p.sku, lines: 0, orders: new Set(), title: r.line.title });
      const e = out.get(p.sku); e.lines++; e.orders.add(r.order.receiptId);
    }
    return [...out.values()].sort((a, b) => b.lines - a.lines || a.sku.localeCompare(b.sku));
  }
  /* This used to open as a wall: a headline, a paragraph, sixty SKU chips and three buttons, on a tab a person opened to
     do something else, with no way to shut it. It is one line now, it closes, and it stays closed. */
  function paintMissing(v) {
    let box = v.querySelector("#mMissing");
    if (!box) { box = el("div", "missBox"); box.id = "mMissing"; const head = v.querySelector(".noteBox"); if (head) head.insertAdjacentElement("afterend", box); else v.prepend(box); }
    const miss = missingSkus();
    if (!miss.length || B.missShut) { box.className = "missBox hidden"; box.innerHTML = ""; return; }
    const lines = miss.reduce((n, m) => n + m.lines, 0);
    const orders = new Set(); miss.forEach(m => m.orders.forEach(o => orders.add(o)));
    box.className = "missBox" + (B.missOpen ? " open" : "");
    box.innerHTML = '<div class="t"><b>' + miss.length + ' SKU' + (miss.length === 1 ? "" : "s") + ' the orders want have no master file</b>' +
      '<span>' + lines + ' line' + (lines === 1 ? "" : "s") + ' \u00b7 ' + orders.size + ' order' + (orders.size === 1 ? "" : "s") + '</span>' +
      '<button class="btn ghost xs" data-a="see">' + (B.missOpen ? "Hide" : "See them") + '</button>' +
      '<button class="x" data-a="shut" title="close this \u2014 it stays closed">\u00d7</button></div>' +
      (B.missOpen ? '<div class="skus">' + miss.slice(0, 60).map(m => '<span class="s" title="' + esc(m.title) + '">' + esc(m.sku) + (m.lines > 1 ? '<i>\u00d7' + m.lines + '</i>' : "") + '</span>').join("") +
      (miss.length > 60 ? '<span class="s more">\u2026 and ' + (miss.length - 60) + ' more</span>' : "") + '</div>' +
      '<div class="acts"><button class="btn ghost xs" data-a="copy">Copy the list</button><button class="btn ghost xs" data-a="save">Save as a file</button><button class="btn gold xs" data-a="add">Add a master file</button></div>' : "");
    box.querySelector("[data-a=shut]").onclick = () => { B.missShut = true; paintMissing(v); };
    box.querySelector("[data-a=see]").onclick = () => { B.missOpen = !B.missOpen; paintMissing(v); };
    if (!B.missOpen) return;
    box.querySelector("[data-a=copy]").onclick = async () => { try { await navigator.clipboard.writeText(miss.map(m => m.sku).join("\n")); toast(miss.length + " SKU(s) copied", "ok"); } catch (_) { toast("Could not reach the clipboard", "bad"); } };
    box.querySelector("[data-a=save]").onclick = () => {
      const rows = [["sku", "lines", "orders", "title"]].concat(miss.map(m => [m.sku, m.lines, [...m.orders].join(" "), m.title]));
      const csv = rows.map(r => r.map(c => '"' + String(c).replace(/"/g, '""') + '"').join(",")).join("\n");
      const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
      a.download = "charm-library-missing-" + today() + ".csv"; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 4000);
    };
    box.querySelector("[data-a=add]").onclick = () => { const f = v.querySelector("#mFile"); if (f) f.click(); };
  }
  function render() {
    const v = document.getElementById("masterView"); if (!v || v.classList.contains("hidden")) return;
    if (!v.dataset.built) {
      v.dataset.built = "1";
      v.innerHTML = `<div class="masterHead"><input type="file" id="mFile" accept=".ai,.pdf" class="hidden"><button class="btn gold sm" id="mAdd" title="index another master .ai into the charm library">＋ Add a master file</button><input type="search" id="mSearch" placeholder="Search SKUs" style="border:1px solid var(--line);border-radius:9px;padding:7px 10px"><label style="display:flex;gap:5px;align-items:center;font-size:11px" title="Off: a SKU the library already holds is left as it is, and only new charms are built. On: every charm on the sheet is rebuilt and rewritten."><input type="checkbox" id="mAllSkus"> re-index SKUs already held</label><span class="pill neutral" id="mCount"></span></div>
        <details class="noteBox"><summary>How a master file is read</summary>Drop a master file here, or press Add. Each charm in it has its SKU as text directly under it (within ${S.settings.labelGapMm} mm, centred under the outline). A SKU is one design whatever colour it is ordered in; the material comes from the order. Labels are never part of the charm. Unlabelled charms, orphan labels and duplicates are listed in red; a SKU present in two masters is blocked until fixed.</details>
        <div id="mJobs" style="display:grid;gap:10px"></div><div id="mFiles" style="display:grid;gap:10px"></div><div class="section">Indexed SKUs</div><div class="skuGrid" id="mGrid"></div>`;
      { const cb = v.querySelector("#mAllSkus"); cb.checked = reindexAll; cb.onchange = () => { reindexAll = cb.checked; toast(reindexAll ? "every charm on the next sheet will be rebuilt" : "charms already in the library will be skipped", "ok"); }; }
      // the file goes where it is dropped: on this tab it is a master for the library
      ["dragenter", "dragover"].forEach(ev => v.addEventListener(ev, e => { if (!(e.dataTransfer && [...(e.dataTransfer.types || [])].includes("Files"))) return; e.preventDefault(); e.stopPropagation(); v.classList.add("dragOver"); e.dataTransfer.dropEffect = "copy"; }));
      ["dragleave", "drop"].forEach(ev => v.addEventListener(ev, () => v.classList.remove("dragOver")));
      v.addEventListener("drop", e => { if (!(e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length)) return; e.preventDefault(); e.stopPropagation(); for (const f of e.dataTransfer.files) indexFile(f).catch(err => toast(err.message, "bad", 7000)); });
      v.querySelector("#mFile").onchange = e => { for (const f of e.target.files) indexFile(f).catch(err => toast(err.message, "bad", 7000)); e.target.value = ""; };
      v.querySelector("#mSearch").oninput = render;
      v.querySelector("#mAdd").onclick = () => v.querySelector("#mFile").click();
      load().then(render).catch(() => {});
    }
    // What the orders on the cards are asking for that this library cannot answer. One real run wanted 133 SKUs the
    // library had never heard of, because only one master file had ever been indexed — and the only way to learn that
    // was to read 180 review cards. It is one fact, so it is said once, here, where the master files are added.
    paintMissing(v);
    const jobs = v.querySelector("#mJobs"); jobs.innerHTML = "";
    for (const job of B.master.jobs.values()) {
      const card = el("div", "masterFile");
      card.innerHTML = `<div class="fh"><b>${esc(job.name)}</b><span class="hash">${job.masterHash.slice(0, 12)}</span><span class="pill ${job.state === "done" ? "ok" : job.state === "error" ? "bad" : "warn"}">${job.state === "done" ? "indexed" : job.state === "error" ? "failed" : `<span class="spin"></span>${job.state}${job.progress ? " · " + job.progress : ""}`}</span>${job.written != null ? `<span>${job.written} SKU(s) written</span>` : ""}${job.error ? `<span style="color:#8a3a26">${esc(job.error)}</span>` : ""}</div>`;
      if (job.lab) {
        // the counts are the report; the lists behind them are for a file, not for a wall of red
        const l = job.lab, left = [], MAX = 24;
        const some = (arr, f) => arr.slice(0, MAX).map(f).join(", ") + (arr.length > MAX ? ` <i>… and ${arr.length - MAX} more</i>` : "");
        if (l.unlabelled.length) left.push(`<b>${l.unlabelled.length} charm(s) with no SKU under them</b>: ${some(l.unlabelled, i2 => "#" + i2)}`);
        if (l.orphans.length) left.push(`<b>${l.orphans.length} line(s) with no charm above</b>: ${some(l.orphans, o => esc(o.sku))}`);
        if (l.duplicates.length) left.push(`<b>${l.duplicates.length} SKU(s) written under two charms</b>: ${some(l.duplicates, d => `${esc(d.sku)} (#${d.charmIndex})`)}`);
        if (job.blocked && job.blocked.length) left.push(`<b>${job.blocked.length} blocked</b>: ${some(job.blocked, b => `${esc(b.sku)} — ${esc(b.reason)}`)}`);
        if (job.conflicts && job.conflicts.length) left.push(`<b>${job.conflicts.length} in another master too</b>: ${some(job.conflicts, b => esc(b.sku))}`);
        if (job.entries) { const oor = job.entries.filter(e => e.outOfRange); if (oor.length) left.push(`<b>${oor.length} outside the ${S.settings.sizeMinMm}–${S.settings.sizeMaxMm} mm range</b>: ${some(oor, e => esc(e.sku))}`); }
        if (left.length) {
          const box = el("div", "leftovers", left.map(x => `<div>${x}</div>`).join(""));
          const save = el("button", "btn ghost xs", "Save the full report"); save.style.marginTop = "8px"; save.onclick = () => saveReport(job);
          box.appendChild(save); card.appendChild(box);
        }
      }
      const pending = (job.vision || []).filter(x => !x.confirmed);
      if (pending.length) {
        const tray = el("div", "visionTray"); const head = el("div", "section", `Confirm ${pending.length} label(s) read by Claude from outlined text (reads under 95% are unchecked)`);
        pending.forEach((x, i) => { const t = el("div", "vt"); t.innerHTML = `<img crossorigin="anonymous" src="${cors(x.image)}" alt=""><div><label style="display:flex;gap:6px;align-items:center"><input type="checkbox" data-i="${i}" ${x.confidence >= 0.95 && x.sku ? "checked" : ""}><span class="mono">#${x.index}</span> <span class="pill ${x.confidence >= 0.95 ? "ok" : "warn"}">${Math.round(x.confidence * 100)}%</span></label><input type="text" data-sku="${i}" value="${esc(x.sku)}" placeholder="SKU as written"><input type="text" data-size="${i}" value="${esc(x.size || "")}" placeholder="size (optional)" style="margin-top:4px"><img crossorigin="anonymous" src="${cors(x.charm.thumb)}" style="width:48px;margin-top:4px;border-radius:4px" alt=""></div>`; tray.appendChild(t); });
        const btn = el("button", "btn gold sm", "Confirm checked labels"); btn.type = "button";
        btn.onclick = async () => { const reads = []; tray.querySelectorAll("input[type=checkbox]").forEach(cb => { if (!cb.checked) return; const i = +cb.dataset.i; const x = pending[i]; const sku = tray.querySelector(`input[data-sku="${i}"]`).value.trim().toUpperCase(); if (!skuRegex().test(sku)) { toast(`${sku || "(empty)"} is not a valid SKU`, "bad"); return; } x.sku = sku; x.size = tray.querySelector(`input[data-size="${i}"]`).value.trim().toUpperCase() || null; reads.push(x); }); if (!reads.length) return; if (!employeeName()) askEmployee(); await confirmVision(job, reads); toast(`${reads.length} label(s) confirmed and indexed`, "ok"); };
        card.append(head, tray, btn);
      }
      // a finished run is a line you can open; only what is still running stays open in front of you
      if (job.state === "done" || job.state === "error") {
        const d = el("details", "masterFileRow"), sum = document.createElement("summary");
        const when = new Date(job.finishedAt || Date.now()).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        sum.innerHTML = `<b>${esc(job.name)}</b><span class="hash">${when}</span><span>${job.state === "error" ? "failed" : `${job.written || 0} SKU(s) written`}</span>${job.held ? `<span class="hash">${job.held} already held</span>` : ""}`;
        d.appendChild(sum); d.appendChild(card); jobs.appendChild(d);
      } else jobs.appendChild(card);
    }
    // Indexed files pile up as the sheet is revised. One line each — what it is, when, how much — folded away by date,
    // with the full report and the remove behind a second fold. Nothing is lost, nothing is in the way.
    const files = v.querySelector("#mFiles");
    const byDay = new Map();
    for (const f of B.master.files) {
      const at = f.indexedAt ? new Date(f.indexedAt) : null;
      const day = at ? at.toLocaleDateString() : "no date";
      if (!byDay.has(day)) byDay.set(day, []);
      byDay.get(day).push(f);
    }
    const days = [...byDay.entries()].sort((a, b) => (byDay.get(b[0])[0].indexedAt || 0) - (byDay.get(a[0])[0].indexedAt || 0));
    const num = (c, l) => (c != null ? c : (l || []).length);
    files.innerHTML = days.map(([day, list], di) => {
      list.sort((a, b) => (b.indexedAt || 0) - (a.indexedAt || 0));
      const skus = list.reduce((n, f) => n + (f.skus ? f.skus.length : 0), 0);
      return `<details class="masterDay"${di === 0 ? " open" : ""}><summary>${esc(day)} · ${list.length} file${list.length === 1 ? "" : "s"} · ${skus} SKU line(s)</summary>` +
        list.map(f => {
          const at = f.indexedAt ? new Date(f.indexedAt) : null;
          const u = num(f.unlabelledCount, f.unlabelled), o = num(f.orphanCount, f.orphans), b = num(f.blockedCount, f.blocked);
          return `<details class="masterFileRow"><summary><b>${esc(f.name || f.masterHash)}</b><span class="hash">${at ? at.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : ""}</span><span>${f.labelled || 0} of ${f.charms || 0} labelled</span>${b ? `<span style="color:#8a3a26">${b} blocked</span>` : ""}</summary>` +
            `<div class="fh" style="margin:6px 0 4px"><span class="hash">${esc((f.masterHash || "").slice(0, 12))}</span>${u ? `<span style="color:#8a3a26">${u} charm(s) with no SKU under them</span>` : ""}${o ? `<span style="color:#8a3a26">${o} line(s) with no charm above</span>` : ""}<span class="hash">${esc(f.indexedBy || "")}</span></div>` +
            `<button class="btn ghost xs" data-rm="${esc(f.masterHash || "")}">Remove every SKU from this file</button></details>`;
        }).join("") + `</details>`;
    }).join("");
    files.querySelectorAll("[data-rm]").forEach(b => b.onclick = async () => { if (!confirm("Remove every SKU indexed from this master file? Pool adds for them will fail until it is re-indexed.")) return; b.disabled = true; try { await api("charmNestLibrary", { op: "masterRemoveFile", masterHash: b.dataset.rm }); } catch (e) { b.disabled = false; toast("The SKUs were not removed: " + e.message, "bad", 7000); return; } await load(true).catch(() => {}); render(); });
    const q = (v.querySelector("#mSearch").value || "").trim().toUpperCase();
    // One charm is one tile, whatever it is sold as. The same design carries several SKUs (the jewellery it goes into),
    // and they share one file in the library; the tile lists every one of them and the search matches any of them.
    const fileOf = e => e.aiPath || (Object.values(e.sizes || {}).find(s => s && s.aiPath) || {}).aiPath || "";
    const groups = new Map();
    for (const e of B.master.entries.values()) { const k = fileOf(e) || "sku:" + e.sku; if (!groups.has(k)) groups.set(k, []); groups.get(k).push(e); }
    const designs = [...groups.values()].map(list => { list.sort((a, b) => a.sku.localeCompare(b.sku)); return { list, head: list[0], skus: list.map(e => e.sku) }; });
    // a SKU can read as part of a longer one ("FROG4" inside "HUGGIE HOOPS- FROG4"): an exact SKU first, then the ones
    // that start with what was typed, then the rest — alphabetical within each
    const rank = d => d.skus.some(s => s === q) ? 0 : d.skus.some(s => s.startsWith(q)) ? 1 : 2;
    const rows = designs.filter(d => !q || d.skus.some(s => s.includes(q)))
      .sort((a, b) => (q ? rank(a) - rank(b) : 0) || a.skus[0].localeCompare(b.skus[0]));
    const cap = showAll ? rows.length : 600;
    const shown = rows.slice(0, cap);
    v.querySelector("#mCount").innerHTML = `${designs.length} charm(s) · ${B.master.entries.size} SKU(s) indexed${q ? ` · ${rows.length} match &ldquo;${esc(q)}&rdquo;` : ""}` +
      (rows.length > shown.length ? ` · <b>showing ${shown.length}</b> <button class="btn sm" id="mAll" style="margin-left:6px">Show all ${rows.length}</button>` : "");
    const all = v.querySelector("#mAll"); if (all) all.onclick = () => { showAll = true; render(); };
    const grid = v.querySelector("#mGrid");
    if (B.master.loading && !B.master.entries.size) { grid.innerHTML = `<div class="libEmpty">Loading the charm library…</div>`; return; }
    if (!shown.length) {
      // Four answers, not one: an empty library, a search that found nothing, a cloud that is down and a load that
      // failed used to be pixel-identical, because load() returns early when the cloud is off and a failed masterList
      // is swallowed. The stronger claim — "the master file has not been indexed" — is only made when the orders on the
      // cards are actually waiting on that exact SKU.
      const raw = (v.querySelector("#mSearch").value || "").trim();
      const wanted = q ? missingSkus().find(m => m.sku.toUpperCase() === q) : null;
      grid.innerHTML = !S.cloud.ok
        ? `<div class="libEmpty">Cloud offline — the charm library lives in the cloud. Nothing can be looked up until it is back.</div>`
        : B.master.error
          ? `<div class="libEmpty">Could not load the charm library: ${esc(B.master.error)}<br><button class="btn ghost sm" id="mRetry" style="margin-top:10px">Try again</button></div>`
          : q
            ? `<div class="libEmpty">No indexed SKU contains &ldquo;${esc(raw)}&rdquo;.` +
              (wanted ? ` ${wanted.lines} order line(s) are waiting on it — the master file that carries it has not been indexed yet.` : ` Check the spelling, or index the master file that carries it.`) +
              `<br><button class="btn ghost sm" id="mClear" style="margin-top:10px">Show all ${designs.length} charm(s)</button>` +
              `<button class="btn gold sm" id="mEmptyAdd" style="margin:10px 0 0 6px">＋ Add a master file</button></div>`
            : `<div class="libEmpty">No charms indexed yet — drop a master file here, or press Add.<br><button class="btn gold sm" id="mEmptyAdd" style="margin-top:10px">＋ Add a master file</button></div>`;
      const c2 = grid.querySelector("#mClear"); if (c2) c2.onclick = () => { const f = v.querySelector("#mSearch"); f.value = ""; f.focus(); render(); };
      const a2 = grid.querySelector("#mEmptyAdd"); if (a2) a2.onclick = () => v.querySelector("#mFile").click();
      const r2 = grid.querySelector("#mRetry"); if (r2) r2.onclick = () => { B.master.error = null; load(true).then(render).catch(() => render()); };
      return;
    }
    grid.innerHTML = shown.map(d => {
      const e = d.head, keys = esc(d.skus.join("|"));
      const blocked = [...new Set(d.list.map(x => x.blocked).filter(Boolean))].join("; ");
      const sizes = e.sizes ? Object.entries(e.sizes) : [];
      return `<div class="skuTile hoverItem${blocked ? " blocked" : ""}" data-sku="${esc(e.sku)}">` +
        `<div data-preview-sku="${esc(e.sku)}" style="aspect-ratio:1;background:#fff;border-radius:6px">Loading preview…</div>` +
        `<div class="sku" title="${esc(d.skus.join(", "))}">${esc(e.sku)}</div>` +
        (d.skus.length > 1 ? `<div class="meta">${d.skus.slice(1).map(s => `<div>${esc(s)}</div>`).join("")}</div>` : "") +
        `<div class="meta">${(e.widthPt * MM).toFixed(1)} × ${(e.heightPt * MM).toFixed(1)} mm · ${e.holes} hole(s)${sizes.length ? ` · sizes ${sizes.map(([k]) => k).join("/")}` : ""}${d.skus.length > 1 ? ` · ${d.skus.length} SKUs` : ""}</div>` +
        `<div class="meta">up ${e.upAngle == null ? "as drawn" : Math.round(e.upAngle) + "°"} · ${esc(e.labelSource || "text")}${e.hashSource === "server" ? " · server" : ""}</div>` +
        (blocked ? `<div class="bad">${esc(blocked)}</div>` : "") +
        `<div class="row">` +
        `<input type="number" data-up="${keys}" value="${e.upAngle == null ? "" : Math.round(e.upAngle)}" placeholder="up°" style="width:52px;border:1px solid var(--line);border-radius:6px;padding:2px 4px;font-size:11px">` +
        (blocked ? `<button class="btn ghost xs" data-unblock="${keys}">unblock</button>` : "") +
        (e.aiUrl ? `<a class="btn ghost xs" href="${e.aiUrl}" target="_blank" rel="noopener">.ai</a>`
                 : sizes.filter(([, s]) => s && s.aiUrl).map(([k, s]) => `<a class="btn ghost xs" href="${s.aiUrl}" target="_blank" rel="noopener">${esc(k)}.ai</a>`).join("")) +
        `</div></div>`;
    }).join("") + (rows.length > shown.length ? `<div class="libEmpty">${rows.length - shown.length} more — press &ldquo;Show all&rdquo;, or narrow the search</div>` : "");
    mountMasterPreviews(grid);
    // a charm's settings belong to the charm, so they are written to every SKU that shares it
    const each = (attr, fn) => grid.querySelectorAll(`[data-${attr}]`).forEach(el => fn(el, el.dataset[attr].split("|")));
    // a failed save says so (it used to fail in silence, the typed angle standing in the box as if kept)
    each("up", (inp, skus) => inp.onchange = () => { const v2 = inp.value.trim(); if (v2 === "") return; patchMany(skus, { upAngle: +v2 }).then(() => toast(`${skus.join(", ")}: up = ${+v2}° (operator)`, "ok"), e => toast(`${skus.join(", ")}: up angle not saved — ${e.message}`, "bad", 7000)); });
    each("unblock", (b, skus) => b.onclick = () => { b.disabled = true; patchMany(skus, { blocked: null }).then(() => toast(`${skus.join(", ")} unblocked`, "ok"), e => { b.disabled = false; toast(`${skus.join(", ")} not unblocked — ${e.message}`, "bad", 7000); }); });
  }
  return { entryFor, thumbOf, fetchEntry, load, indexFile, looksLikeMaster, render, patch, patchMany, keepOutOf, skuRegex, stripPng, strayInkUnder, missingSkus, missingCount: () => missingSkus().length };
})();

/* ═══ 20 · Pool — one charm per order line and copy ══════════════════════ */
const Pool = window.Pool = (() => {
  const sizeEntry = (entry, size) => (entry.sizes && Object.keys(entry.sizes).length ? (size && entry.sizes[size]) || null : entry);
  /** Fetch the per-SKU .ai once per session, parse it, trace it; every copy shares the geometry. */
  const masterLoads=new Map();
  async function masterCharm(entry, size) {
    const key=sizeEntry(entry,size)?.aiPath;
    if(!key)return loadMasterCharm(entry,size);
    if(masterLoads.has(key))return masterLoads.get(key);
    const task=loadMasterCharm(entry,size).finally(()=>masterLoads.delete(key));
    masterLoads.set(key,task);return task;
  }
  async function loadMasterCharm(entry, size) {
    const geom = sizeEntry(entry, size); if (!geom || !geom.aiPath) throw new Error(`no design file for ${entry.sku}${size ? " · " + size : ""}`);
    const key = geom.aiPath;
    if (B.pool.sources.has(key)) return B.pool.sources.get(key);
    const {url,bytes,parsed,g,charm}=await readMasterCharm(entry,size);
    // A design file indexed while the grouping lost track of the labels (22–23 Sep) kept the SKU written under its charm.
    // The text stays part of the charm, so the nest keeps its room and no neighbour is placed under it, but it still goes
    // onto the sheet and stops the sheet's .dxf. Indexing the master again writes the file without it.
    const label = charm.members.find(m => m.kind === "text" && m.bbox && m.bbox[3] <= charm.outline.bbox[1] + 1 && P.parseSkuLabel(m.str, Master.skuRegex()));
    if (label) agent({ pool: true }, "warn", `${entry.sku}: its design file still has the SKU label “${String(label.str).trim()}” under the charm, so the label goes onto every sheet with it and the sheet's .dxf fails — index its master again in the Library with “re-index SKUs already held” ticked`);
    await P.buildSilhouettes(parsed, [charm], +S.settings.silhouetteRes || 6);
    const srcId = "pool:" + key.replace(/[^\w]+/g, "_");
    const src = { id: srcId, pool: true, name: `${entry.sku}${size ? " · " + size : ""} (master)`, sku: entry.sku, bytes, hash: entry.charmHash || charm.hash, parsed, group: g, charms: [charm], metal: null, state: "ready", t0: performance.now(), cloud: { path: geom.aiPath, url }, persisting: null };
    Object.assign(charm, { id: srcId + ":0", sourceId: srcId, sourceName: src.name, index: 0, name: entry.sku, sku: entry.sku, namedBy: "master", excluded: false, cloud: { ai: url, aiPath: geom.aiPath, png: geom.thumbUrl || null, pngPath: geom.thumbPath || null }, upAngle: entry.upAngle, engravable: true, backKeepOut: Master.keepOutOf(charm) });
    S.poolSources[srcId] = src; B.pool.sources.set(key, src);
    return src;
  }
  async function readMasterCharm(entry,size) {
    const geom=sizeEntry(entry,size);
    const url = geom.aiUrl || (await api("charmNestOutput", { op: "url", path: geom.aiPath })).url;
    const bytes = await CharmNestAssets.bytes(url);
    const parsed = await P.parseSource(bytes, `${entry.sku}.ai`);
    const g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: +S.settings.minPt || 6 });
    if (!g.charms.length) throw new Error(`${entry.sku}: no outline in the master copy`);
    const charm = g.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
    if (g.charms.length > 1) { for (const c of g.charms) if (c !== charm) { for (const m of c.members) if (!charm.members.includes(m)) charm.members.push(m); charm.topIndices = [...new Set(charm.topIndices.concat(c.topIndices))]; charm.bbox = [Math.min(charm.bbox[0], c.bbox[0]), Math.min(charm.bbox[1], c.bbox[1]), Math.max(charm.bbox[2], c.bbox[2]), Math.max(charm.bbox[3], c.bbox[3])]; /* the silhouette canvas is cut to the bbox: a merged piece outside it would be drawn but never collide */ } agent({ pool: true }, "warn", `${entry.sku}: the master copy split into ${g.charms.length} pieces — folded back into one charm`); }
    // The sheet draws this charm's whole drawing, cut to its box, so whatever ink the grouping left loose inside that
    // drawing and within reach of the box is part of the charm too: the nest must pack around everything the sheet shows.
    for (let grew = true; grew;) {
      grew = false;
      for (const m of g.orphans) {
        if (!m.bbox || charm.members.includes(m) || !charm.topIndices.includes(m.parent ?? m.index)) continue;
        const pad = Math.max(charm.strokePt || 0.5, m.lwPt || 0) / 2 + 1, b = charm.bbox;
        if (m.bbox[2] < b[0] - pad || m.bbox[0] > b[2] + pad || m.bbox[3] < b[1] - pad || m.bbox[1] > b[3] + pad) continue;
        charm.members.push(m); charm.bbox = [Math.min(b[0], m.bbox[0]), Math.min(b[1], m.bbox[1]), Math.max(b[2], m.bbox[2]), Math.max(b[3], m.bbox[3])]; grew = true;
      }
    }
    { const r = P.integrateRings(charm); if (r.left.length) throw new Error(`${entry.sku}: a hoop could not join its charm — ${r.left[0]}`); }
    return {url,bytes,parsed,g,charm};
  }
  const masterPreviewCache=new Map();
  async function masterPreview(entry,size) {
    const key=sizeEntry(entry,size)?.aiPath;
    if(!key)throw new Error("No design file");
    const cached=B.pool.sources.get(key);if(cached)return cached.charms[0].thumb;
    if(masterPreviewCache.has(key))return masterPreviewCache.get(key);
    const task=readMasterCharm(entry,size).then(({charm})=>P.thumbnail(charm,168));
    masterPreviewCache.set(key,task);
    while(masterPreviewCache.size>80)masterPreviewCache.delete(masterPreviewCache.keys().next().value);
    try{return await task;}catch(e){masterPreviewCache.delete(key);throw e;}
  }
  /** Upgrade cached geometry before a recovered sheet can be used again. */
  async function repairRecoveredGeometry(d) {
    const sources=(d.sources || []).concat(Object.values(d.poolSources || {}));
    const bySource=new Map(sources.map(src=>[src.id,src]));
    const pages=(d.sheets || []).flatMap(g=>g.pages || []);
    const charms=new Set([...sources.flatMap(src=>src.charms || []),...(d.unassigned || []),...pages.flatMap(p=>p.charms || [])]);
    const protectedIds=new Set(pages.filter(p=>p.metal==='rose' && !p.roseCutAt && (p.rosePlan||p.roseProtected)).flatMap(p=>(p.placements||[]).map(pl=>pl.id)));
    const repaired=new Set(),poolIds=new Set();
    for(const c of charms) {
      if(!c.outline)continue;
      // A saved cut contour describes the original vectors exactly. A
      // recovery migration must not silently replace their geometry or clear
      // the placement list underneath that protected physical sheet.
      if(protectedIds.has(c.id))continue;
      const src=bySource.get(c.sourceId);
      let rebuilt=false;
      if(G.pathRole(c.outline)==="artwork") {
        if(!src?.parsed)throw new Error("The recovered charm needs its original CUT geometry.");
        const candidates=(await (P.groupCharmsAsync || P.groupCharms)(src.parsed,{minPt:+S.settings.minPt || 6})).charms;
        const overlap=other=>{const a=c.bbox,b=other.bbox;const intersection=Math.max(0,Math.min(a[2],b[2])-Math.max(a[0],b[0]))*Math.max(0,Math.min(a[3],b[3])-Math.max(a[1],b[1]));return intersection/Math.max(1,(a[2]-a[0])*(a[3]-a[1])+(b[2]-b[0])*(b[3]-b[1])-intersection);};
        const fresh=candidates.sort((a,b)=>overlap(b)-overlap(a))[0];
        if(!fresh || overlap(fresh)<.5 || G.pathRole(fresh.outline)!=="cut")throw new Error("The recovered charm has no matching CUT outline.");
        for(const key of ["outline","members","bbox","topIndices","strokePt","extras","dropIndices"])c[key]=fresh[key];
        rebuilt=true;
      }
      if(c.ringGeometryVersion===3 && !rebuilt)continue;
      const result=P.integrateRings(c);
      if(result.left.length)throw new Error('A recovered hoop needs a geometry check: '+result.left.join('; '));
      if(!result.welded && !rebuilt) { c.thumb=await P.thumbnail(c,168);continue; }
      await P.buildSilhouettes(src?.parsed,[c],+S.settings.silhouetteRes || 6);
      c.pinned=null;repaired.add(c);if(c.poolId)poolIds.add(c.poolId);
    }
    for(const pg of pages)if(pg.charms?.some(c=>repaired.has(c))) {
      if(pg.metal==='rose' && !pg.roseCutAt && (pg.rosePlan||pg.roseProtected)){
        // New, unplaced artwork may need a geometry migration. Re-search its
        // remainder without ever dropping the saved cut or old placements.
        pg.dirty=true;pg.status='ready';pg.intakeAppend=pg.placements.length>0;pg.appendOnly=pg.intakeAppend;
        pg.best=null;pg.bestResult=null;pg.bestInfo=null;pg.bestKey=null;
        pg.stage='New cut geometry updated — old Rose Gold contour and placements kept';
        continue;
      }
      Object.assign(pg,{dirty:true,status:'ready',placements:[],rejects:[],layout:null,outputs:null,verification:null,liveInfo:null,best:null,bestResult:null,bestInfo:null,bestKey:null,releaseFull:false,backOutputs:null});
      pg.backPool=(pg.backPool || []).filter(b=>!poolIds.has(b.poolId));
      pg.stage='Cut geometry updated — ready to re-nest';
    }
    const keys=new Set();
    for(const j of d.jobs || [])if((j.copies || []).some(id=>poolIds.has(id)) && j.state!=='skipped') {
      Object.assign(j,{state:'ready',view:null,mask:null,fit:null,writtenFit:null,verify:null,backs:[],approvedBy:null,approvedAt:null});keys.add(j.key);
      const row=d.orders?.rows?.find(r=>r.key===j.key);if(row?.engrave)Object.assign(row.engrave,{state:'ready',approved:false});
    }
    for(const j of d.jobs || [])if(!["approved","written","skipped"].includes(j.state) && j.materialVersion!==2) {
      Object.assign(j,{view:null,mask:null,fit:null,verify:null,materialVersion:2});
      if(["review","blocked","fitting"].includes(j.state))j.state="ready";
      const row=d.orders?.rows?.find(r=>r.key===j.key);if(row?.engrave)row.engrave.state=j.state;
    }
    if(keys.size)d.review=(d.review || []).filter(it=>!keys.has(it.jobKey));
    if(repaired.size && d.run && d.run.status!=='complete') {
      Object.assign(d.run,{step:'nest',status:'stopped',stoppedBy:'Recovered cut geometry updated',fix:'Resume to re-nest the repaired pieces and review their engraving.'});
    }
    return repaired.size;
  }
  function cloneCharm(c, id) { const k = Object.assign({}, c, { id, pinned: null }); return k; }
  /** §6.4 · one pooled charm per copy of the line, on the material card the ORDER says. */
  /** A line's copies, made from its traced design, ready to record; null when the line is held, has no design or does
      not fit (row.state says which). */
  async function preparePool(row, run) {
    const sp = row.spec;
    if (!sp || sp.noDesign) { row.state = "noDesign"; return null; }
    row.problems = row.problems.filter(p => !["unmatchedSku", "blockedSku", "missingSize", "oversize"].includes(p.kind));   // re-derived below on every attempt
    if (row.problems.length) { row.state = "held"; row.reason = Review.problemText(row.problems[0]); return null; }
    let entry = Master.entryFor(sp.designSku) || await Master.fetchEntry(sp.designSku);
    if (!entry) { row.state = "unmatched"; row.reason = "not in any master file"; row.problems.push({ kind: "unmatchedSku", reason: "not in any master file", sku: sp.designSku, listingId: String(row.line.listingId || ""), title: row.line.title }); agent({ pool: true }, "warn", `${row.order.receiptId} · ${sp.designSku}: not in any master file`); return null; }
    if (entry.blocked) { row.state = "held"; row.reason = `SKU blocked: ${entry.blocked}`; row.problems.push({ kind: "blockedSku", reason: entry.blocked, sku: sp.designSku }); return null; }
    if (entry.sizes && Object.keys(entry.sizes).length && !(sp.size && entry.sizes[sp.size])) { row.state = "held"; row.reason = `no design for size ${sp.size || "(none)"}`; row.problems.push({ kind: "missingSize", sku: sp.designSku, size: sp.size, available: Object.keys(entry.sizes) }); return null; }
    const src = await masterCharm(entry, sp.size);
    const base = src.charms[0];
    // oversize: the charm cannot fit the plate under the ceiling
    const st = stockFor(sp.material); const usable = (st.wPt - 2 * (+S.settings.insetPt || 0)) * (st.hPt - 2 * (+S.settings.insetPt || 0)) * (+S.settings.maxFill || 0.80);
    if (base.areaPt2 > usable || Math.min(base.widthPt, base.heightPt) > Math.max(st.wPt, st.hPt) - 2 * (+S.settings.insetPt || 0)) { row.state = "oversize"; row.reason = `charm ${(base.widthPt * MM).toFixed(1)} × ${(base.heightPt * MM).toFixed(1)} mm does not fit the ${labelOf(sp.material)} plate under the ceiling`; row.problems.push({ kind: "oversize", sku: sp.designSku, widthMm: base.widthPt * MM, heightMm: base.heightPt * MM, material: sp.material }); return null; }
    const pools = [], charms = [];
    for (let copy = 1; copy <= sp.quantity; copy++) {
      const poolId = O.poolId(row.order, row.line, copy);
      const charm = copy === 1 && !base.poolId ? base : cloneCharm(base, `${src.id}:${poolId}`);
      charm.name = `${row.order.receiptId} · ${sp.designSku}${sp.quantity > 1 ? ` · ${copy}/${sp.quantity}` : ""}`;
      charm.order = row.order.receiptId; charm.orderDate = +row.order.createTs || 0; charm.arrivedAt = row.arrivedAt || 0; charm.orderInfo = { receiptId: row.order.receiptId, transactionId: row.line.transactionId, sku: sp.designSku, copy, quantity: sp.quantity, form: sp.form, size: sp.size };
      charm.poolId = poolId; charm.metal = sp.material; charm.lineKey = row.key; charm.pinned = null; charm.excluded = false;
      pools.push({ poolId, runId: run ? run.runId : null, setId: run ? run.setId || null : null, sheetId: null, orderId: row.order.receiptId, orderDate: +row.order.createTs || 0, arrivedAt: row.arrivedAt || 0, transactionId: row.line.transactionId, sku: sp.designSku, material: sp.material, size: sp.size || null, form: sp.form || null, chain: sp.chain || null, copy, quantity: sp.quantity, charmHash: charm.hash, masterHash: entry.masterHash || null, aiPath: sizeEntry(entry, sp.size).aiPath, engrave: !!(row.engrave && row.engrave.needed), state: "ready", lineKey: row.key, updateTs: row.order.updateTs });
      charms.push(charm);
    }
    return { sp, pools, charms };
  }
  /** The recorded line joins its sheet. A line a live run already holds (a pool row the record refused) is skipped. */
  function attachPool(row, run, prep, contended) {
    const { sp, pools, charms } = prep;
    const taken = (contended || []).filter(c => pools.some(p => p.poolId === c.poolId));
    if (taken.length) { row.state = "contended"; row.reason = `claimed by run ${taken[0].runId}`; agent({ pool: true }, "warn", `${row.order.receiptId} · ${sp.designSku}: a live run (${taken[0].runId}) already holds this line — skipped`); return; }
    let page=window.LiveNest ? LiveNest.intakePage(sp.material, run) : pagesOf(sp.material).at(-1);
    if((run && page.runId && page.runId!==run.runId) || (window.LiveNest&&LiveNest.closed(page)))page=addPage(sp.material);
    S.sheets[sp.material].active=pagesOf(sp.material).indexOf(page);if(!page.el)window.CN?.showPage(sp.material,S.sheets[sp.material].active);   // the card shows the page its buttons act on
    if (run) page.runId = run.runId;
    for (const c of charms) if (!page.charms.includes(c)) page.charms.push(c);
    for (const p of pools) B.pool.rows.set(p.poolId, p);
    row.poolIds = pools.map(p => p.poolId); row.state = "pooled"; row.material = sp.material; row.reason = null; delete row.poolTry; delete row.poolError;
    if(page.placements.length){page.intakeAppend=true;page.appendOnly=true;page.dirty=true;if(!['nesting','finishing','queued'].includes(page.status))page.status='ready';renderCard(page);}else sheetDirty(page);
    agent({ metal: sp.material, pool: true }, "POOL", `${row.order.receiptId} · ${sp.designSku}${sp.quantity > 1 ? " ×" + sp.quantity : ""} → ${labelOf(sp.material)} (${row.engrave && row.engrave.needed ? "engrave" : "plain"})`);
  }
  async function poolAdd(row, run) {
    const prep = await preparePool(row, run); if (!prep) return;
    const r = S.cloud.ok ? await api("charmNestLibrary", { op: "poolPut", pools: prep.pools }, { label: "Recording the pool" }) : {};
    attachPool(row, run, prep, r.contended);
  }
  /* A line that could not go on a sheet (its SKU in no master file or blocked, a size its design lacks, too big for the
     plate, a design that would not load) is made up again only when something it depends on has changed: the line as
     read (Etsy or a person), its design in the library, the plate it would go on. One whose design would not load is
     tried again after RETRY_MS. Every update used to make up each such line again, and look up each unknown SKU again,
     so an update took longer the longer the open orders list grew (Paul, 24 Sep: "Why do we have to prepare all the
     orders every single time"). */
  const RETRY_MS = 5 * 60000;
  function trySig(row) {
    const sp = row.spec || {}, e = sp.designSku ? Master.entryFor(sp.designSku) : null, st = sp.material ? stockFor(sp.material) : null;
    return JSON.stringify([+row.order.updateTs || 0, sp.designSku || "", sp.size || "", sp.material || "", sp.quantity || 0, !!sp.noDesign, (sp.problems || []).map(p => p.kind), row.materialOverride || "", row.sizeOverride || "",
      e ? [e.masterHash || "", e.updatedAt || 0, e.blocked || "", Object.keys(e.sizes || {}).sort().join(","), sizeEntry(e, sp.size)?.aiPath || ""] : B.master.entries.size,
      st ? [Math.round(st.wPt), Math.round(st.hPt)] : 0, +S.settings.insetPt || 0, +S.settings.maxFill || 0]);
  }
  const due = row => ["pulled", "waiting"].includes(row.state) || row.poolTry !== trySig(row) || (row.poolError > 0 && Date.now() - row.poolError >= RETRY_MS);
  async function addAll(run) {
    // what goes to the laser today and what waits: full sheets for SS and GF, every other day for the slow metals, orders whole
    // (read first: whether a line is due compares what it is read as now with what it was read as when it was last tried)
    Orders.interpretAll();
    // A line a person holds waits for that person (the hold is lifted through Review.repool), and a line whose pieces
    // are all on sheets already has nothing to make. Both used to be made up again at the next intake: "Hold order" on
    // a pooled order put a second copy of each of its pieces on a sheet (same pool id, cut twice).
    const rows = Orders.rows().filter(r => ["pulled", "held", "unmatched", "oversize", "waiting"].includes(r.state) && !(r.state === "held" && r.hold) && !(onSheets(r) && settle(r)) && due(r)).sort((a, b) => (+a.order.createTs || 0) - (+b.order.createTs || 0));
    const plan = await Gate.plan(rows);
    const held = [...plan.wait.values()];
    if (held.length) agent({ pool: true }, "POOL", `${held.length} line(s) wait: ${held.filter(w => w.kind === "fill").length} for a full sheet, ${held.filter(w => w.kind === "slow").length} for a slow metal's day`);
    let n = 0;
    // the bar counts the lines made up now; lines waiting for their day are not made up
    const work = rows.filter(row => row.state !== "waiting");
    const bar = work.length && window.CNProgress ? CNProgress.start(`Preparing ${work.length} order line${work.length === 1 ? "" : "s"}`, { total: work.length }) : null;
    // The design files load side by side (each is fetched, read and traced once a session); the lines are then made up in
    // order, oldest first, and recorded in one call rather than one call a line.
    const hold = (row, e, later) => { row.state = "held"; row.reason = e.message; if (later) { row.poolError = Date.now(); row.poolTry = trySig(row); } agent({ pool: true }, "warn", `${row.order.receiptId} · ${row.spec && row.spec.designSku}: ${e.message}`); };
    await Promise.allSettled(work.filter(row => row.spec && !row.spec.noDesign).map(row => { const e = Master.entryFor(row.spec.designSku); return e && !e.blocked && sizeEntry(e, row.spec.size)?.aiPath ? masterCharm(e, row.spec.size) : null; }));
    const made = [];
    for (const row of work) {
      if (bar) bar.set(n, work.length, row.spec && row.spec.designSku ? String(row.spec.designSku) : "");
      delete row.poolError;
      try { const prep = await preparePool(row, run); if (prep) made.push([row, prep]); else row.poolTry = trySig(row); } catch (e) { hold(row, e, true); }
      if (++n % 5 === 0) { Orders.render(); }
    }
    let contended = [], failure = null;
    if (S.cloud.ok && made.length) {
      const all = made.flatMap(([, prep]) => prep.pools);
      try { for (let i = 0; i < all.length; i += 400) contended = contended.concat((await api("charmNestLibrary", { op: "poolPut", pools: all.slice(i, i + 400) }, { label: "Recording the pool" })).contended || []); }
      catch (e) { failure = e; }
    }
    for (const [row, prep] of made) { if (failure) { hold(row, failure); continue; } try { attachPool(row, run, prep, contended); } catch (e) { hold(row, e); } }
    if (bar) bar.end();
    await Gate.afterPool(run);
    Review.syncOrderItems(); Orders.render(); renderRail(); updateTopSub(); refreshAllCards();
    // the run record follows the work without holding it up (see RunCtl.loopNow); a failed save shows on the banner
    if (run) { run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); RunCtl.save(run).catch(() => {}); }
    const pooled = rows.filter(r => r.state === "pooled").length;
    if (rows.length) agent({ pool: true }, "POOL", `Pool: ${pooled} line(s) queued on the cards · ${rows.filter(r => r.state === "waiting").length} waiting · ${rows.filter(r => !["pooled", "noDesign", "waiting"].includes(r.state)).length} held`);
    return pooled;
  }
  async function update(poolIds, patch) { for (const id of poolIds) { const p = B.pool.rows.get(id); if (p) Object.assign(p, patch); } if (S.cloud.ok) for (let i = 0; i < poolIds.length; i += 400) await api("charmNestLibrary", { op: "poolUpdate", poolIds: poolIds.slice(i, i + 400), patch }); }
  /* charmOf and sheetOf walked every charm of every sheet at each call (sheetOf every placement against every charm), and
     a classify pass or a restore asks once a line, so an update took longer the more sheets the table held. One index
     answers both, first match first as the walks did; it is made again when a page comes or goes or a sheet's charms or
     placements change (a new list or a new length, the only ways they change), which costs a glance at each page. */
  let idx = null;
  function index() {
    const sheets = allSheets(), f = idx && idx.pages;
    if (f && f.length === sheets.length * 5 && sheets.every((sh, i) => f[i * 5] === sh && f[i * 5 + 1] === sh.charms && f[i * 5 + 2] === sh.charms.length && f[i * 5 + 3] === sh.placements && f[i * 5 + 4] === sh.placements.length)) return idx;
    const charm = new Map(), sheet = new Map(), where = new Map(), pages = [];
    for (const sh of sheets) {
      const byId = new Map();
      for (const c of sh.charms) { if (!charm.has(c.poolId)) charm.set(c.poolId, c); if (!byId.has(c.id)) byId.set(c.id, c); const w = where.get(c.poolId); if (!w) where.set(c.poolId, [sh]); else if (w.at(-1) !== sh) w.push(sh); }
      for (const p of sh.placements) { const c = byId.get(p.id); if (c && !sheet.has(c.poolId)) sheet.set(c.poolId, sh); }
      pages.push(sh, sh.charms, sh.charms.length, sh.placements, sh.placements.length);
    }
    return idx = { pages, charm, sheet, where };
  }
  const charmOf = poolId => index().charm.get(poolId) || null;
  const sheetOf = poolId => index().sheet.get(poolId) || null;
  /** The pages holding any of these pieces. */
  function holding(ids) { const w = index().where, out = new Set(); for (const id of ids) for (const sh of w.get(id) || []) out.add(sh); return out; }
  /** Every piece of the line already sits on a sheet. */
  function onSheets(row) { const ids = row.poolIds || []; if (!ids.length) return false; const on = index().charm; return ids.every(id => !!id && on.has(id)); }
  /** Such a line goes back to where its pieces are: committed, written into a set, or pooled on a sheet still filling. */
  function settle(row) {
    const recs = (row.poolIds || []).map(id => B.pool.rows.get(id) || {});
    row.state = recs.length && recs.every(p => p.state === "committed") ? "committed" : recs.length && recs.every(p => p.sheetId) ? "written" : "pooled";
    row.reason = null; return true;
  }
  return { poolAdd, addAll, masterCharm, masterPreview, cloneCharm, update, charmOf, sheetOf, holding, sizeEntry, repairRecoveredGeometry, onSheets, settle };
})();

/* Carry-forward is keyed by immutable order-line identity. A changed Etsy line is always re-interpreted. */
const Carry = window.Carry = (() => {
  function capture() {
    if (!B.run || B.run.status !== "complete") return;
    const rows = Orders.rows().filter(r => !["gone", "committed"].includes(r.state));
    const keys = new Set(rows.map(r => r.key));
    B.carry = Session.copy({ rows, jobs: [...Engrave.items().values()].filter(j => keys.has(j.key)), pools: B.pool.rows });
  }
  function adopt() {
    if (!B.carry) return;
    const old = new Map(B.carry.rows.map(r => [r.key, r]));
    for (let i = 0; i < B.orders.rows.length; i++) {
      const fresh = B.orders.rows[i], row = old.get(fresh.key);
      if (!row || +row.order.updateTs !== +fresh.order.updateTs || JSON.stringify(row.line) !== JSON.stringify(fresh.line)) continue;
      row.order = fresh.order;
      // A previously released line remains written: mixed orders must never cut that line twice.
      const written = row.state === "written" || row.state === "labelled";
      if (!written) { row.state = "pulled"; row.poolIds = []; row.wait = null; row.reason = null; }
      B.orders.rows[i] = row; B.orders.byKey.set(row.key, row);
      for (const id of row.poolIds) if (B.carry.pools.has(id)) B.pool.rows.set(id, B.carry.pools.get(id));
      const j = B.carry.jobs.find(j => j.key === row.key);
      if (j) { j.row = row; if (!written && j.state === "written") j.state = "approved"; if (!written) j.backs = []; B.engrave.items.set(j.key, j); }
    }
    delete B.carry;
  }
  return { capture, adopt };
})();

/* ═══ 20b · Gate — what goes to the laser today ═══════════════════════════════════════════════════════════════════
   The rules live in CharmNestOrders.planRelease and are tested there. This is the part that knows the shop: the record
   of when each slow material last went out (shop-wide, in the cloud, not in one browser), the day a person opened one
   early, the footprint of a line from its master design, and the one line on each material's card that says what is
   waiting and for what, with the button that stops the waiting. */
const Gate = window.Gate = (() => {
  const R = { lastReleased: {}, released: {}, forceFill: {}, loaded: false, plan: null };
  // a selection save in progress or just failed is this page's own business: the workspace carried "Saving selection…" and
  // the old error across a reload, where nothing was saving any more (run.membershipDirty is the lasting record of a save owed)
  for (const k of ["membershipError", "membershipPending", "membershipTask", "membershipRun"]) Object.defineProperty(R, k, { value: null, writable: true, enumerable: false });
  const O_ = window.CharmNestOrders;
  const modern = runId => (!B.run && !runId) || (!!B.run && B.run.releasePolicy === 2 && (!runId || runId === B.run.runId));
  const selected = () => B.run?.solidIncluded || R.solidIncluded || {};
  const solid = m => ["gold10k", "gold14k"].includes(m);
  const nestable = (sh, run) => !modern(run?.runId) || !solid(sh.metal) || !!(run?.solidIncluded || selected())[sh.metal] || sh.isolated;
  function policy(sh, seq, choices = selected()) {
    return O_.sheetRelease({ material: sh.metal, verified: !!sh.verification?.ok, placed: sh.placements.length,
      stopped: sh.endedBy === "stopped", dirty: sh.dirty || ["nesting", "finishing", "queued"].includes(sh.status), full: !!sh.releaseFull, topup: sh.topup && !sh.topup.closedAt ? { tried: (sh.topup.tried || []).length, of: TOPUP.orders } : false }, { seq, selected: choices });
  }
  async function upgrade(run) {
    if (!run || run.releasePolicy === 2) return;
    const prior = Sets.ofRun(run.runId);
    if (prior.some(s => s.committedAt)) return; // A partially released legacy run finishes under its recorded rules; new runs use policy 2.
    if(allSheets().some(p=>p.runId===run.runId && p.metal==='rose' && !p.roseCutAt && (p.rosePlan||p.roseProtected)))
      throw new Error('The saved Rose Gold contour must be cut before this older run can change its set rules. Its sheet and stock have been kept.');
    if (!S.cloud.ok) throw new Error("Reconnect before updating this older run's set rules");
    run.intakeRecovery ||= { retire:[], backs:[] };
    for (const set of prior) { set.status = "superseded"; await Sets.save(set); for (const [key, value] of B.sets) if (value === set) B.sets.delete(key); }
    for (const p of allSheets().filter(p => p.runId === run.runId)) {
      if (p.sheetId) run.intakeRecovery.retire.push(p.sheetId);
      run.intakeRecovery.backs.push(...p.charms.map(c => c.poolId).filter(Boolean));
      p.sheetId = null; p.fileBase = null; p.setId = null; p.seq = null; p.label = null; p.group = "dispatch"; p.draft = true;
      sheetDirty(p);
    }
    await Pool.update(allSheets().filter(p => p.runId === run.runId).flatMap(p => p.charms.map(c => c.poolId).filter(Boolean)), { state:"ready", sheetId:null, setId:null });
    for (const row of Orders.rows()) if (["written","labelled"].includes(row.state)) row.state = "pooled";
    run.releasePolicy = 2; run.solidIncluded = {}; run.setIds = []; run.setId = null; run.seq = null; run.step = "nest";
    await RunCtl.save(run);
  }
  let assemblyQueue = Promise.resolve();
  function assemble(run, context) {
    const ops = window.CharmNestOperations;
    if (ops && !context) return ops.run({key:'membership:'+run?.runId, label:'Updating set membership', resources:['production:'+run?.runId], latest:true, priority:10}, token=>assemble(run,token));
    const task = assemblyQueue.catch(() => {}).then(() => assembleNow(run, context)).finally(() => { refreshAllCards(); });
    assemblyQueue = task; return task;
  }
  async function assembleNow(run, context) {
    const choices = Object.assign({}, selected());
    const release = (sh, seq) => policy(sh, seq, choices);
    if (!modern(run?.runId) || !run) return;
    const committedSheets = new Set(Sets.ofRun(run.runId).filter(s => s.committedAt).flatMap(s => s.sheetIds));
    const pages = allSheets().filter(p => p.runId === run.runId && p.outputs && p.persistedDone && !committedSheets.has(p.sheetId));
    let set = Sets.ofRun(run.runId).find(s => s.group === "dispatch" && !s.committedAt);
    const regular = pages.filter(p => p.metal !== "rose" && release(p, 2).include);
    const roses = pages.filter(p => p.metal === "rose" && release(p, 2).include);
    if (!set && (regular.length || roses.length)) set = await Sets.ensure(run.runId, "dispatch", { roseOnly: !regular.length && choices.rose !== true });
    if (!set) { run.heldSheets = pages.length; return; }
    if (set.committedAt) return;
    for (const sh of allSheets().filter(p => p.runId === run.runId && p.setId === set.setId && !release(p, set.seq).include)) {
      const previous = {draft:sh.draft,setId:sh.setId,seq:sh.seq,sheetIndex:sh.sheetIndex,label:sh.label};
      sh.draft = true; sh.setId = null; sh.seq = null; sh.sheetIndex = null; sh.label = null;
      try {
        await Pool.update(sh.charms.map(c => c.poolId).filter(Boolean), { setId:null, sheetId:null, state:"ready" });
        if (sh.sheetId) await api("charmNestLibrary", { op:"putSheet", sheet:{ id:sh.sheetId, draft:true, setId:null, setSeq:null, sheetIndex:null, label:null, solidIncluded:solid(sh.metal) ? !!choices[sh.metal] : null } });
      } catch(e) { Object.assign(sh,previous); sh.problem="Set membership was not saved: "+e.message; throw e; }
      for (const row of Orders.rows()) if (row.poolIds.some(id => sh.charms.some(c => c.poolId === id))) row.state = "pooled";
      sh.problem=null;set.labels = null;
    }
    const included = new Set(allSheets().filter(p => p.setId === set.setId && !p.draft).map(p => p.sheetId));
    set.sheetIds = set.sheetIds.filter(id => included.has(id)); set.labelFiles = set.labelFiles.filter(f => included.has(f.sheetId));
    set.materials = [...new Set(allSheets().filter(p => included.has(p.sheetId)).map(p => p.metal))];
    for (const [rid, order] of Object.entries(set.orders)) {
      for (const [tid, line] of Object.entries(order.lines)) { line.copies = line.copies.filter(c => included.has(c.sheetId)); if (!line.copies.length) delete order.lines[tid]; }
      if (!Object.keys(order.lines).length) delete set.orders[rid];
    }
    for (const sh of pages) {
      if (!release(sh, set.seq).include) continue;
      // Membership can have been saved before a label upload was interrupted.
      // Retry the QR alone without re-nesting or publishing the sheet twice.
      if (sh.setId === set.setId) {
        if (!Sets.labelsReady(sh, set)) await Sets.onSheetSaved(sh, sh.charms, undefined, {labelsOnly:true,setOverride:set});
        continue;
      }
      const previous = { draft:sh.draft, setId:sh.setId, setDay:sh.setDay, seq:sh.seq, group:sh.group, sheetIndex:sh.sheetIndex, fileBase:sh.fileBase };
      sh.draft = false; sh.setId = set.setId; sh.setDay = set.day; sh.seq = set.seq; sh.group = "dispatch";
      // after the highest number in use: a sheet that left the set must not leave its number to be taken twice
      sh.sheetIndex = 1 + Math.max(0, ...allSheets().filter(p => p !== sh && p.setId === set.setId && p.metal === sh.metal).map(p => +p.sheetIndex || 0));
      sh.fileBase = CN.sheetFileBase(sh);
      set.labels = null;
      // Membership changes reuse verified artwork and approved backs. Only the
      // manifest metadata and QR need saving; never re-render/re-upload the AI.
      try {
        await api("charmNestLibrary", {op:"putSheet", sheet:{id:sh.sheetId,draft:false,setId:set.setId,setSeq:set.seq,sheetIndex:sh.sheetIndex,fileBase:sh.fileBase,folder:sh.fileBase,solidIncluded:solid(sh.metal) ? !!choices[sh.metal] : null}});
        await Sets.onSheetSaved(sh, sh.charms, undefined, {setOverride:set});
        if (Engrave.saveSheetBacks) await Engrave.saveSheetBacks(sh);
        sh.problem = null;
      } catch (e) { Object.assign(sh, previous); sh.problem = "Set membership was not saved: " + e.message; throw e; }
      RunCtl.onSheetDone(sh);
    }
    run.heldSheets = pages.filter(p => p.draft).length;
    await Sets.save(set); refreshAllCards();
    if (S.mode === "library") CN.loadLibrary().catch(e=>toast("Library refresh: "+e.message,"bad"));
  }
  function editable(sh) {
    // Stock dimensions are shared only by pages of this material. Independent
    // nesting, thumbnail work and cloud activity must not disable this control.
    return !sh.recalled && !pagesOf(sh.metal).some(p => ["nesting", "finishing", "queued"].includes(p.status) || p.persisted && !p.persistedDone || p._rosePlanning || p._roseLoading || p._roseAction) &&
      !(B.run && (["complete", "abandoned"].includes(B.run.status) || Sets.ofRun(B.run.runId).some(s => s.committedAt) || window.CharmNestOperations?.running('commit:'+B.run.runId)));
  }
  function membershipEditable(sh) {
    return !sh.recalled && !(B.run && (["complete","abandoned"].includes(B.run.status) || Sets.ofRun(B.run.runId).some(set=>set.committedAt) || window.CharmNestOperations?.running('commit:'+B.run.runId)));
  }
  function projectLibraryRecords(rows) {
    const run = B.run; if (!run || !modern(run.runId) || ["complete","abandoned"].includes(run.status)) return rows;
    const set = Sets.ofRun(run.runId).find(s=>s.group==='dispatch');
    return rows.map(row=>{
      if(row.runId!==run.runId || !solid(row.metal)) return row;
      const included=!!selected()[row.metal], live=allSheets().find(p=>p.sheetId===row.id);
      return {...row,solidIncluded:included,...(!included ? {draft:true,setId:null,setSeq:null,sheetIndex:null,label:null} : set && live && policy(live,set.seq).include ? {draft:false,setId:set.setId,setSeq:set.seq,sheetIndex:live.sheetIndex || row.sheetIndex} : {})};
    });
  }
  function refreshMembership() {
    for(const page of allSheets()) {const node=page.el?.querySelector('.shGate');if(node)renderRelease(page,node);}
    if (S.mode === 'library') {
      if(S.library.kind==='sets') Sets.renderLibrary(document.getElementById('libBody'),{reuse:true});
      else {S.library.rows=projectLibraryRecords(S.library.rows || []);CN.renderLibrary();}
    }
  }
  function changeMembership(m, included) {
    const run=B.run;
    if(run && (['complete','abandoned'].includes(run.status) || Sets.ofRun(run.runId).some(set=>set.committedAt) || window.CharmNestOperations?.running('commit:'+run.runId)))return Promise.reject(new Error('This set is committing or already complete'));
    const choices=run ? (run.solidIncluded ||= {}) : (R.solidIncluded ||= {});
    choices[m]=!!included;
    if(m==='rose' && included)for(const page of allSheets().filter(p=>p.metal==='rose'&&p.persistedDone&&p.verification?.ok&&!p.roseCutAt))window.RoseStock?.plan(page).catch(e=>toast('Rose Gold contour: '+e.message,'bad'));
    if (run) {
      run.membershipRevision=(run.membershipRevision||0)+1;run.commitRequested=false;run.membershipDirty=true;
      if(O_.stepIndex(run.step)>=O_.stepIndex('checkpoint'))run.membershipNext=allSheets().some(p=>p.runId===run.runId && nestable(p,run) && p.charms.length && (p.dirty || ['idle','ready','queued','nesting','finishing'].includes(p.status) || !p.outputs)) ? 'nest' : 'engrave';
    }
    R.membershipError=null;R.membershipPending=true;
    Session.schedule();refreshMembership();
    if(!run){R.membershipPending=false;refreshMembership();return Promise.resolve();}
    const revision=run.membershipRevision;
    const task=assemble(run).then(async()=>{await RunCtl.save(run);if(run.membershipRevision===revision){run.membershipDirty=false;R.membershipError=null;RunCtl.membershipUpdated?.(run);}}).catch(e=>{if(run.membershipRevision===revision){R.membershipError=e.message;toast('Set selection not saved: '+e.message+' — Retry in Options','bad');}throw e;}).finally(()=>{if(run.membershipRevision===revision){R.membershipPending=false;refreshMembership();RunCtl.poke();}});
    R.membershipTask=task;R.membershipRun=run.runId;return task;
  }
  async function flush(run) {
    // A save that failed (the network down, a 5xx, the cloud offline) is tried once more here. Its error used to be thrown
    // again at every labels and commit step, Resume after Resume, until Retry was pressed in Options.
    if(R.membershipTask && R.membershipRun===run?.runId)await Promise.resolve(R.membershipTask).catch(()=>{});
    const failed=R.membershipRun===run?.runId && R.membershipError;
    if(run?.membershipDirty || failed){
      try{await assemble(run);run.membershipDirty=false;await RunCtl.save(run);}
      catch(e){run.membershipDirty=true;if(failed){R.membershipError=e.message;refreshMembership();throw new Error('Set selection not saved: '+e.message);}throw e;}
      if(failed){R.membershipError=null;refreshMembership();}
    }
    if(window.RoseStock)for(const sh of allSheets().filter(p=>p.runId===run?.runId && p.metal==='rose' && p.setId && !p.draft && !p.roseCutAt))await RoseStock.ensurePlan(sh);
  }
  function changed() {
    Session.schedule();
    if (B.run && !["complete", "abandoned"].includes(B.run.status)) {
      RunCtl.optionsChanged();
    }
    refreshAllCards();
  }
  function renderRelease(sh, node) {
    const m = sh.metal, st = stockFor(m,sh), seq = Sets.ofRun(B.run?.runId).find(s => s.group === "dispatch" && !s.committedAt)?.seq;
    node.className = "shGate";
    if (!solid(m) && m !== "rose") {
      node.textContent = sh.setId && !sh.draft ? `In Set ${sh.seq} · ${policy(sh, sh.seq).reason}` : policy(sh, seq).reason;
      if (!sh.charms.length) node.textContent = "Full sheets only · partials carry forward";
      if(sh.el) sh.el.querySelector(".shHead").title = node.textContent;
      node.className = "shGate hidden";
      return;
    }
    const included = m === "rose" ? policy(sh,seq).include : selected()[m] === true;
    const sizeLocked=!!(sh.recalled || sh.roseCutAt || (m==='rose' && pagesOf(m).some(p=>p.roseStock)));
    // Keep the controls mounted: solver ticks, cloud replies and membership saves
    // must not replace a focused input, its draft value, or an open popup.
    if(node._sheetOptionsOwner!==sh){
      node._sheetOptionsOwner=sh;
      node.innerHTML = `<details class="sheetOptions" ${R.optionsOpen?.[m] ? "open" : ""}><summary>Options</summary><div class="solidOptions" role="group" aria-label="${esc(labelOf(m))} sheet options">
        <div class="sheetOptionsHead"><strong>${esc(labelOf(m))} options</strong><button type="button" class="sheetOptionsClose" aria-label="Close sheet options">×</button></div>
        <section class="sheetOptionSection"><label class="sheetInclude"><input type="checkbox" data-solid="include" aria-label="Include ${esc(labelOf(m))} in current set"> Include in current set</label><span class="help sheetOptionStatus" role="status" data-solid="status"></span><button type="button" class="btn ghost xs" data-solid="retry" hidden>Retry selection</button></section>
        <section class="sheetOptionSection"><h4>Sheet dimensions</h4><div class="solidSize">
          <label>Width <span>mm</span><input type="number" min="5" max="500" step="0.1" data-solid="w" value="${+(st.wIn*25.4).toFixed(2)}"></label>
          <label>Height <span>mm</span><input type="number" min="5" max="500" step="0.1" data-solid="h" value="${+(st.hIn*25.4).toFixed(2)}"></label>
        </div><div class="sheetSizeAction"><span class="help" data-solid="size-help"></span><button type="button" class="btn ghost xs" data-solid="size">Apply size</button></div></section>
      </div></details>`;
      const details=node.querySelector('.sheetOptions');
      details.ontoggle=()=>{(R.optionsOpen ||= {})[m]=details.open;};
      const close=()=>{details.open=false;(R.optionsOpen ||= {})[m]=false;node.querySelector('.sheetOptions>summary').focus();};
      node.querySelector('.sheetOptionsClose').onclick=close;
      details.onkeydown=e=>{if(e.key==='Escape'){e.preventDefault();e.stopPropagation();close();}};
      for(const axis of ['w','h'])node.querySelector('[data-solid="'+axis+'"]').oninput=e=>{e.target._draft=true;};
    }
    node.querySelector('.sheetOptions>summary').textContent='Options'+(included?' ✓':'');
    const include=node.querySelector('[data-solid="include"]');include.checked=!!included;include.disabled=!membershipEditable(sh)||!!sh.roseCutAt;
    // a locked checkbox says why, as the size control below it does
    const runNow=B.run,locked=!include.disabled?'':sh.roseCutAt?'Cut · it stays in its set':sh.recalled?'A saved sheet · its set is fixed':runNow&&['complete','abandoned'].includes(runNow.status)?'The run is finished':window.CharmNestOperations?.running('commit:'+runNow?.runId)?'The set is being committed':'A set of this run is committed · the selection is locked';
    node.querySelector('[data-solid="status"]').textContent=R.membershipError?'Selection not saved':R.membershipPending?'Saving selection…':locked;
    if(sh.el)sh.el.querySelector(".shHead").title=policy(sh,seq).reason;   // the same hover answer the Gold and Silver cards give
    const retry=node.querySelector('[data-solid="retry"]');retry.hidden=!R.membershipError;
    retry.onclick=()=>changeMembership(m,!!selected()[m]).catch(()=>{});
    include.onchange=e=>{if(!membershipEditable(sh))return renderRelease(sh,node);changeMembership(m,e.target.checked).catch(()=>{});};
    for(const [axis,value] of [['w',st.wIn],['h',st.hIn]]){
      const input=node.querySelector('[data-solid="'+axis+'"]');
      input.disabled=sizeLocked;
      if(!input._draft && input!==document.activeElement)input.value=+(value*25.4).toFixed(2);
    }
    const apply=node.querySelector('[data-solid="size"]');apply.disabled=sizeLocked||!editable(sh)||!!node._sizeApplying;
    apply.textContent=node._sizeApplying?'Applying size…':'Apply size';
    apply.setAttribute('aria-busy',String(!!node._sizeApplying));
    node.querySelector('[data-solid="size-help"]').textContent=sizeLocked?'Dimensions belong to this saved sheet.':node._sizeApplying?'Your size change is queued for saving.':!editable(sh)?'This material is in use or its set is locked.':'5–500 mm per side';
    apply.onclick=async()=>{
      const width=node.querySelector('[data-solid="w"]'),height=node.querySelector('[data-solid="h"]'),w=+width.value,h=+height.value;
      if(![w,h].every(n=>Number.isFinite(n)&&n>=5&&n<=500))return toast('Use a width and height between 5 and 500 mm','bad');
      if(node._sizeApplying||sizeLocked||!editable(sh))return;
      const run=B.run;
      const resize=()=>{
        // Recheck after any short, conflicting record write finishes. A solver
        // may have started, or the user may have opened a different run.
        if(B.run!==run || !editable(sh) || sh.roseCutAt || (m==='rose'&&pagesOf(m).some(p=>p.roseStock)))throw new Error('This material changed while saving. Apply its size again when it is ready.');
        S.settings.stock[m]=[w/25.4,h/25.4];saveSettings();
        if(+width.value===w)width._draft=false;
        if(+height.value===h)height._draft=false;
        for(const p of pagesOf(m).filter(p=>!p.recalled&&!p.roseCutAt)){for(const c of p.charms){c.pinned=null;delete c.arrivalPin;}sheetDirty(p);}
        changed();
      };
      node._sizeApplying=true;renderRelease(sh,node);
      try {
        const ops=window.CharmNestOperations;
        if(ops)await ops.run({key:'sheet-size:'+m,label:'Applying '+labelOf(m)+' sheet size',resources:['production:'+(run?.runId || sh.runId || sh.sheetId)],priority:20},resize);
        else resize();
      } catch(e){toast('Size not applied: '+e.message,'bad');}
      finally {node._sizeApplying=false;renderRelease(sh,node);}
    };
  }

  async function load() {
    if (R.loaded) return R;
    if (S.cloud.ok) { try { const r = await api("charmNestLibrary", { op: "releaseGet" }, { quiet: true }); R.lastReleased = r.lastReleased || {}; R.released = r.released || {}; } catch (e) { agent({ bridge: true }, "warn", `release record: ${e.message}`); } }
    R.loaded = true; return R;
  }
  async function put(patch) {
    if (patch.lastReleased) Object.assign(R.lastReleased, patch.lastReleased);
    if (patch.released) Object.assign(R.released, patch.released);
    if (S.cloud.ok) await api("charmNestLibrary", { op: "releasePut", lastReleased: R.lastReleased, released: R.released }, { quiet: true }).catch(e => agent({ bridge: true }, "warn", `release record: ${e.message}`));
  }
  /** A line's footprint on the plate, from its master design: the silhouette grown by half the clearance, times copies. */
  function footprint(row) {
    const sp = row.spec; if (!sp || !sp.designSku) return 0;
    const e = Master.entryFor(sp.designSku); if (!e) return 0;
    const g = Pool.sizeEntry(e, sp.size) || e; if (!(g.areaPt2 > 0)) return 0;
    return CN.inflatedArea({ areaPt2: g.areaPt2, widthPt: g.widthPt || 0, heightPt: g.heightPt || 0 }) * Math.max(1, sp.quantity || 1);
  }
  const capacity = () => Object.fromEntries(METALS.map(m => [m.key, CN.usableArea(S.sheets[m.key]) * (+S.settings.maxFill || 0.80)]));
  /** Plan the lines that could pool now, and mark the ones that wait. Returns the plan. */
  async function plan(rows) {
    await load();
    if (modern()) {
      // Pool all valid arrivals for visibility and draft nesting. Release is a separate, post-verification step.
      const take = new Set();
      for (const r of rows) { if (r.state === "waiting") { r.state = "pulled"; r.wait = null; r.reason = null; } take.add(r.key); }
      return R.plan = { take, wait: new Map(), materials: {} };
    }
    const ready = rows.filter(r => r.spec && !r.spec.noDesign && !r.problems.length && r.spec.material);
    for (const r of ready) if (r.spec.designSku && !Master.entryFor(r.spec.designSku)) await Master.fetchEntry(r.spec.designSku).catch(() => {});
    const lines = ready.map(r => ({ key: r.key, orderId: String(r.order.receiptId), material: r.spec.material, areaPt2: footprint(r), createTs: +r.order.createTs || 0, shipBy: +r.order.shipBy || 0 }));
    R.plan = O_.planRelease(lines, { today: today(), capacity: capacity(), lastReleased: R.lastReleased, released: R.released, forceFill: R.forceFill, cadenceDays: +S.settings.cadenceDays || 2, lateDays: S.settings.lateDays == null ? 2 : +S.settings.lateDays });
    for (const r of ready) {
      const w = R.plan.wait.get(r.key);
      if (w) { r.state = "waiting"; r.wait = w; r.reason = waitWords(w); }
      else if (r.state === "waiting") { r.state = "pulled"; r.wait = null; r.reason = null; }
    }
    return R.plan;
  }
  const dayWord = d => d ? new Date(d + "T12:00:00").toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" }) : "";
  function daysUntil(d) { return Math.round((Date.parse(d + "T12:00:00") - Date.parse(today() + "T12:00:00")) / 86400000); }
  function waitWords(w) {
    if (w.kind === "slow") { const n = daysUntil(w.until); return `${labelOf(w.material)} goes to the laser ${n <= 0 ? "today" : n === 1 ? "tomorrow" : dayWord(w.until)} — waits for it`; }
    return `waits for a full ${labelOf(w.material)} sheet · ${w.pct}% so far`;
  }
  /** After pooling: the slow materials that went out today are recorded, and every card learns its kin group. */
  async function afterPool(run) {
    if (modern(run?.runId)) { if (run) run.groups = Object.fromEntries(METALS.map(m => [m.key, "dispatch"])); for (const p of allSheets()) if (p.runId === run?.runId && !p.setId) p.group = "dispatch"; refreshAllCards(); return; }
    const pooled = Orders.rows().filter(r => ["pooled", "written"].includes(r.state) && r.material);
    const went = {}; for (const r of pooled) if (O_.SLOW_MATERIALS.has(r.material) && R.lastReleased[r.material] !== today()) went[r.material] = today();
    if (Object.keys(went).length) { await put({ lastReleased: went }); agent({ bridge: true }, "POOL", `${Object.keys(went).map(m => labelOf(m)).join(", ")} released to the laser today — next in ${+S.settings.cadenceDays || 2} days`); }
    const groups = O_.kinGroups(pooled.map(r => ({ orderId: String(r.order.receiptId), material: r.material })));
    for (const m of METALS) for (const pg of pagesOf(m.key)) if (!pg.fileBase && (!run || pg.runId === run.runId)) pg.group = groups[m.key] || m.key;
    if (run) {
      run.groups = groups;
      for (const group of new Set(Object.values(groups))) { const set = await Sets.ensure(run.runId, group); set.materials = group.split("+"); await Sets.save(set); }
    }
    refreshAllCards();
  }
  /** A person opens a slow material today, or cuts a fast material's partial sheet: the waiting lines pool at once. */
  async function release(material) { await put({ released: { [material]: today() } }); await repoolWaiting(material, `${labelOf(material)} released to the laser by hand`); }
  async function cutAnyway(material) { R.forceFill[material] = true; await repoolWaiting(material, `${labelOf(material)}: partial sheet cut by hand`); }
  async function repoolWaiting(material, why) {
    const who = employeeName() || askEmployee(); if (!who) return;
    agent({ bridge: true, metal: material }, "POOL", `${why} (${who})`);
    const rows = Orders.rows().filter(r => r.state === "waiting" && r.wait && r.wait.material === material);
    for (const r of rows) { r.state = "pulled"; r.wait = null; r.reason = null; }
    if (B.run && O.stepIndex(B.run.step) >= O.stepIndex("pool")) await Pool.addAll(B.run); else await plan(Orders.rows());
    Orders.render(); RunCtl.poke();
  }
  /** The one line under a card's head. Slow: the next day it goes, and how much is waiting. Fast: how full the waiting
   *  sheet has got. Nothing at all when there is nothing to say. */
  function renderCard(sh) {
    const el2 = sh.el && sh.el.querySelector('[data-r="gate"]'); if (!el2) return;
    if (modern(sh.runId) || sh.metal === "rose") { renderRelease(sh, el2); return; }
    // the line belongs to an open run: what waits, waits for that run's next sheet. With no run open it says nothing.
    if (!B.run || ["complete", "stopped", "abandoned"].includes(B.run.status)) { el2.classList.add("hidden"); return; }
    const m = sh.metal, p = R.plan && R.plan.materials[m];
    const waiting = Orders.rows().filter(r => r.state === "waiting" && r.wait && r.wait.material === m);
    const pieces = waiting.reduce((n, r) => n + Math.max(1, (r.spec && r.spec.quantity) || 1), 0);
    let html = "", cls = "shGate";
    if (O_.SLOW_MATERIALS.has(m)) {
      const last = R.lastReleased[m], open = !last || (R.released[m] === today()) || daysUntil(last) <= -(+S.settings.cadenceDays || 2);
      const next = open ? today() : new Date(Date.parse(last + "T12:00:00") + (+S.settings.cadenceDays || 2) * 86400000).toISOString().slice(0, 10);
      const n = daysUntil(next);
      if (open) { if (!pieces && !(p && p.taken)) { el2.classList.add("hidden"); return; } cls += " open"; html = `<span class="t"><b>Goes to the laser today</b>${R.released[m] === today() ? " · opened by hand" : ""}</span><span class="n">${p && p.taken ? `${p.taken} piece${p.taken === 1 ? "" : "s"} on the sheet` : ""}</span>`; }
      else html = `<span class="t"><b>Next to the laser ${n === 1 ? "tomorrow" : dayWord(next)}</b> · every ${+S.settings.cadenceDays || 2} days</span><span class="n">${pieces ? `${pieces} piece${pieces === 1 ? "" : "s"} waiting` : "nothing waiting"}</span>${pieces ? `<button class="btn ghost xs" data-gate="release" title="send the ${pieces} waiting piece${pieces === 1 ? "" : "s"} with this set instead of waiting for ${dayWord(next)}">Send now</button>` : ""}`;
    } else {
      if (!pieces) { el2.classList.add("hidden"); return; }
      const pct = (waiting[0].wait && waiting[0].wait.pct) || 0;
      html = `<span class="t"><b>${pct}% of a sheet</b> waiting for more</span><span class="n">${pieces} piece${pieces === 1 ? "" : "s"}</span><button class="btn ghost xs" data-gate="cut" title="cut the partial sheet now instead of waiting for it to fill">Cut it anyway</button>`;
    }
    el2.className = cls; el2.classList.remove("hidden"); el2.innerHTML = html;
    const b = el2.querySelector("[data-gate]"); if (b) b.onclick = () => { b.disabled = true; (b.dataset.gate === "release" ? release(m) : cutAnyway(m)).catch(e => toast(e.message, "bad", 6000)); };
  }
  return { solidSelected:m => selected()[m] === true, changeMembership, flush, projectLibraryRecords, refreshMembership, load, plan, afterPool, release, cutAnyway, renderCard, footprint, modern, policy, assemble, upgrade, selected, nestable, renderRelease, state: () => R };
})();

/* ═══ 21 · Engrave — the words, the checked flip, the fit, the review, the back files ═══ */
const Engrave = window.Engrave = (() => {
  const F_ = B.engrave.fonts;
  // Source Sans 3 (Adobe, SIL Open Font License): a humanist sans drawn in the same tradition as Myriad Pro, shipped with the app
  const FONT_FILES = { Regular: "vendor/fonts/SourceSans3-Regular.otf", Semibold: "vendor/fonts/SourceSans3-Semibold.otf" };
  async function loadFonts(force) {
    // An emoji map or font that did not load (a flaky network after a wake) is tried again, at most once a minute and when
    // the network is back: every engraving with an emoji went to Review as unsupported for the rest of the session.
    if (F_.loading || F_.ok && (!F_.emojiError || !force && Date.now() - (F_.emojiTriedAt || 0) < 60000)) return F_.loading || F_;
    const late = F_.ok; F_.emojiTriedAt = Date.now();
    F_.loading = (async () => {
      for (const [w, path] of Object.entries(FONT_FILES)) {
        if (late) break;
        try { const r = await fetch(path, { cache: "force-cache" }); if (!r.ok) throw new Error(`HTTP ${r.status}`); const buf = await r.arrayBuffer(); if (buf.byteLength < 1000) throw new Error("empty file"); F_[w] = opentype.parse(buf); (F_.workerFonts ||= {})[w] = buf; }
        catch (e) { if (w === "Regular") F_.error = `${path}: ${e.message}`; else F_.semiboldMissing = `${path}: ${e.message}`; }
      }
      if (F_.Regular) try {
        const mapResponse = await fetch("vendor/fonts/emoji-sequences.json", {cache:"no-cache"});
        if (!mapResponse.ok) throw new Error("Emoji map unavailable");
        const map = await mapResponse.json();
        const fontResponse = await fetch("vendor/fonts/NotoEmoji-Regular.ttf?v="+map.fontSha256, {cache:"force-cache"});
        if (!fontResponse.ok) throw new Error("Emoji font unavailable");
        const bytes = await fontResponse.arrayBuffer();
        if (await CN.sha256(new Uint8Array(bytes)) !== map.fontSha256) throw new Error("Emoji font version differs from its shape map");
        const emoji = opentype.parse(bytes);
        for (const weight of ["Regular", "Semibold"]) if (F_[weight]) F_[weight] = window.CharmNestText.withEmoji(F_[weight], emoji, map, opentype.Path);
        F_.workerFonts.emoji = bytes; F_.workerFonts.emojiMap = map; F_.emoji = true;
        // (loaded late: the fitting worker takes it at its next start, and words held for their emoji are looked at again)
        if (late) { delete F_.emojiError; workerClient = null; agent({ engrave: true }, "ENGRAVE", "Emoji font loaded"); for (const job of items().values()) if (job.state === "words" && job.missing && G.glyphCoverage(F_.Regular, job.lines.join("\n")).ok) setReady(job); return; }
      } catch (e) { F_.emojiError = e.message; if (!late) agent({engrave:true}, "warn", "Emoji font could not load: " + e.message); }
      if (late) return;
      F_.ok = !!F_.Regular; if (!F_.ok) agent({ engrave: true }, "warn", `Source Sans 3 is not available (${F_.error}) — engraving cannot be set exactly; the .otf files belong in vendor/fonts/`);
      else agent({ engrave: true }, "ENGRAVE", `Engraving fonts loaded: ${F_.Regular.names.fullName ? Object.values(F_.Regular.names.fullName)[0] : "Regular"}${F_.Semibold ? " + Semibold" : " (Semibold missing — Regular used at every size)"}`);
      const h = document.getElementById("stFontsHelp"); if (h) h.innerHTML = F_.ok ? `Loaded: ${esc(Object.values(F_.Regular.names.fullName || {})[0] || "Source Sans 3 Regular")}${F_.Semibold ? ", " + esc(Object.values(F_.Semibold.names.fullName || {})[0] || "Semibold") : " · Semibold missing"}` : `<span style="color:#8a3a26">Not found: ${esc(F_.error)}</span> — SourceSans3-Regular.otf and SourceSans3-Semibold.otf belong in vendor/fonts/`;
    })().finally(() => { F_.loading = null; });
    return F_.loading;
  }
  const fontFor = weight => (weight === "Semibold" && F_.Semibold) || F_.Regular;
  const fitOpts = job => ({ minCapMm: +S.settings.engraveMinCapMm || 1.6, maxHeightFrac: +S.settings.engraveMaxHeightFrac || 0.4, lineGap: job?.lineGap ?? 0.216, minStrokeMm: +S.settings.engraveMinStrokeMm || 0, minGapMm: +S.settings.engraveMinGapMm || 0, tryRotated: S.settings.engraveTryRotated !== "off" });
  const items = () => B.engrave.items;
  const charmFor = job => job.editCharm || Pool.charmOf(job.copies[0]);
  const sheetFor = (job, poolId) => job.editingBack ? (allSheets().find(p=>p.sheetId === job.editSheet.sheetId && p.charms.some(c=>c.poolId === poolId)) || job.editSheet) : Pool.sheetOf(poolId);
  let openingBack = null, openingBackId = null;
  async function openBack(poolId, sheetId) {
    // a press on another back while one is opening says so (it used to be dropped without a word)
    if (openingBack) { if (poolId !== openingBackId) toast("Another back is still opening · press again in a moment", "", 3500); return openingBack; }
    openingBackId = poolId;
    openingBack = (async()=>{
      if (!poolId || !sheetId) throw new Error("This preview is missing its saved charm identity");
      const {sheet:d}=await api("charmNestLibrary",{op:"getSheet",id:sheetId},{label:"Opening back engraving"});
      const saved=(d?.backPool || []).find(b=>b.poolId===poolId);
      if (!saved || !(d.poolIds || []).includes(poolId)) throw new Error("This charm moved or its engraving changed. Refresh the Library and reopen it.");
      const existing=[...items().values()].find(j=>!j.editingBack && j.copies.includes(poolId));
      let charm=Pool.charmOf(poolId);
      const savedCharm=(d.charms || []).find(c=>c.poolId===poolId);
      if (!charm) {
        const source=(d.sources || []).find(s=>s.id===savedCharm?.sourceId || s.name===savedCharm?.sourceName);
        if (!source?.pool) throw new Error("Restore this sheet to the Nest workspace before editing backs from a combined source file.");
        if (!source?.url) throw new Error("The original front design is unavailable. Restore this sheet's source file before editing its back.");
        const src=await Pool.masterCharm({sku:saved.sku,aiPath:source.path || source.url,aiUrl:source.url,upAngle:saved.upAngle},null);
        charm=Pool.cloneCharm(src.charms[0],poolId); charm.poolId=poolId;
        if(savedCharm?.bbox && charm.bbox.some((v,i)=>Math.abs(v-savedCharm.bbox[i])>.5)) throw new Error("The saved source geometry has changed. Rebuild this sheet before editing its back.");
      }
      const sheet=Object.assign({},d,{sheetId:d.id,seq:d.setSeq,backPool:d.backPool || [],fileBase:d.fileBase || d.folder,
        folderPath:d.outputs?.ai?.path?.replace(/\/[^/]+$/,"")});
      if(!sheet.folderPath) throw new Error("The saved sheet folder is missing");
      const row=existing ? {...existing.row,engrave:{...existing.row.engrave},poolIds:[poolId]} : {key:"back:"+poolId,state:"written",poolIds:[poolId],order:{receiptId:saved.order},line:{transactionId:saved.transactionId,sku:saved.sku},spec:{designSku:saved.sku,personalization:[saved.text || ""]},engrave:{needed:true,state:"review"}};
      const job={key:"back:"+poolId,row,copies:[poolId],editingBack:true,editSheet:sheet,editCharm:charm,editOriginal:saved,expectedApprovedAt:saved.approvedAt,
        lineGap:saved.lineGap ?? .18,lineMode:saved.lineMode || "preserve",lineInput:saved.lineInput || saved.lines, state:"review",text:saved.text || "",lines:saved.lines?.length ? saved.lines.slice() : String(saved.text || "").split("\n"),
        solidBack:!!saved.solidBack,source:saved.source || "personalization",quote:saved.sourceQuote || null,confidence:1,questions:[],requests:{side:"back"},backs:[],t:Date.now(),materialVersion:2};
      await loadFonts(); if(!F_.ok) throw new Error("Engraving font is unavailable");
      job.view=G.backView(charm,{res:6,upAngle:saved.upAngle ?? charm.upAngle,solidBack:job.solidBack});
      job.mask=G.engraveMask(job.view,{marginMm:+S.settings.engraveMarginMm || .8,keepOut:charm.backKeepOut || []});
      const font=fontFor(saved.weight),layout=G.layoutLines(job.lines,font,saved.sizePt,job.lineGap,saved.angle || 0,saved.centre || [job.mask.cx,job.mask.cy]);
      const check=G.verifyInk(layout.cmds,job.mask);
      if(check.ok) {
        const ceiling=G.refitAt(job.lines,font,job.mask,fitOpts(job),{centre:layout.centre,angle:layout.angle});
        job.fit={ok:true,size:saved.sizePt,fittedMax:Math.max(saved.sizePt,ceiling.ok?ceiling.size:0),weight:saved.weight || "Regular",centre:layout.centre,angle:layout.angle,layout,glyphs:layout.glyphs,cmds:layout.cmds,capMm:saved.capMm || saved.sizePt*G.capPerEm(font)*MM,metrics:saved.metrics,small:!!saved.small,thin:!!saved.thin};
        job.verify={geometry:check,at:Date.now()};
      }
      items().set(job.key,job);
      for(const dlg of document.querySelectorAll('dialog[open]')) dlg.close();
      Object.assign(EG,{tab:"place",focus:job.key,list:false,chosen:true,q:"",card:null,cardKey:null});
      setMode("engrave");
      if(!job.fit) await fitJob(job); else render();
    })().catch(e=>toast(e.message,"bad",7000)).finally(()=>{openingBack=null;openingBackId=null;});
    return openingBack;
  }
  async function syncEditedBack(job) {
    const {sheet:d}=await api("charmNestLibrary",{op:"getSheet",id:job.editSheet.sheetId});
    if(!d) return;
    job.editSheet.backPool=d.backPool || [];
    for(const p of allSheets()) if(p.sheetId===d.id) {p.backPool=d.backPool || [];p.backOutputs=d.backOutputs || null;if(p.recalled) Object.assign(p.recalled,{backPool:p.backPool,backs:p.backPool});}
    for(const r of S.library.rows) if(r.id===d.id) Object.assign(r,{backPool:d.backPool || [],backs:d.backPool || [],backCount:(d.backPool || []).length});
    if(["written","skipped"].includes(job.state)) {
      for(const other of items().values()) if(other!==job && other.copies.includes(job.copies[0])) {
        other.copyOverrides=[...new Set([...(other.copyOverrides || []),job.copies[0]])];
        other.copies=other.copies.filter(id=>id!==job.copies[0]);other.backs=(other.backs || []).filter(b=>b.poolId!==job.copies[0]);
        if(!other.copies.length) {other.state="written";Review.remove("eng:"+other.key);}
      }
      const row=Orders.rows().find(r=>r.poolIds.includes(job.copies[0]));
      if(row && row.poolIds.length===1) row.engrave={...(row.engrave || {}),text:job.text,state:job.state,needed:job.state!=="skipped",approved:true};
    }
    refreshBacks();Orders.render();Review.render();Session.schedule();
    if(S.mode === "library") await CN.loadLibrary();
  }

  const jobOf = row => items().get(row.key) || null;
  function ensureJob(row) { let j = items().get(row.key); if (!j) { j = { key: row.key, row, lineGap:.216, state: "classify", text: null, lines: [], source: null, quote: null, confidence: null, requests: null, questions: [], decision: null, fit: null, view: null, mask: null, claude: null, approvedBy: null, approvedAt: null, backs: [], copies: [], reason: null, t: Date.now() }; items().set(row.key, j); } j.copies = row.poolIds.filter(id=>!(j.copyOverrides || []).includes(id)); return j; }
  const pendingCount = () => [...items().values()].filter(j => !j.editingBack && ["words", "review", "fitting", "ready", "classify"].includes(j.state) && j.row.state !== "gone").length;
  /** How many placements have already been settled in this run — the numerator of "3 of 9" on the card. */
  const DECIDED = ["approved", "written", "skipped"];                    // the same set the Decided tab lists
  const decidedJobs = () => [...items().values()].filter(j => DECIDED.includes(j.state) && j.row.state !== "gone" && j.copies.length);
  /** A recalled set's engraving: each back written on its sheets becomes a decided job on the line it belongs to, with
   *  its words, who approved it and the file, so the Decided list reads the same for a set from March as for today's. */
  function fromRecall() {
    items().clear();
    const rows = Orders.rows();
    for (const pg of allSheets()) {
      if (!pg.recalled) continue;
      for (const bk of pg.backPool || []) {
        const row = rows.find(r => (bk.poolId && r.poolIds.includes(bk.poolId)) || (String(r.order.receiptId) === String(bk.order) && (!bk.sku || (r.spec && r.spec.designSku) === bk.sku || r.line.sku === bk.sku)));
        if (!row) continue;
        const j = ensureJob(row);
        const lines = bk.lines && bk.lines.length ? bk.lines : String(bk.text || "").split("\n").filter(Boolean);
        Object.assign(j, { state: "written", text: lines.join("\n"), lines, lineGap:bk.lineGap ?? .18, approvedBy: bk.approvedBy || null, approvedAt: pg.recalled.updatedAt || null, backs: [{ poolId: bk.poolId, sheet: pg.fileBase, png: bk.outputs && bk.outputs.png && bk.outputs.png.url, ai: bk.outputs && bk.outputs.ai && bk.outputs.ai.url, capMm: bk.capMm }], recalledFrom: pg });
        row.engrave = { needed: true, state: "written", approved: true, text: j.text };
      }
    }
    render();
  }
  const reviewedCount = () => decidedJobs().length;

  /* ── 7.1 · which lines are engraved, and what the text is ── */
  const classifyTasks = new WeakMap();
  function classify(row) {
    const job = ensureJob(row);
    if (classifyTasks.has(job)) return classifyTasks.get(job);
    const task = Promise.resolve().then(() => classifyOnce(row)).finally(() => { classifyTasks.delete(job); render(); });
    classifyTasks.set(job, task); render();
    return task;
  }
  async function classifyOnce(row) {
    const job = ensureJob(row); const sp = row.spec; const owner = items();
    if (!sp.engraveCandidate) { setNone(job, "no personalisation, message or note"); return job; }
    // Etsy offers engraving on these designs; legacy catalog estimates are not eligibility rules.
    const engravable = true;
    job.state = "classify"; row.engrave = { needed: false, state: "classify" };
    // A fresh reading replaces the line split, size and decision kept for earlier words. The fit restored the first words
    // it had seen, so an order whose words changed on Etsy showed its new words but was engraved with the old ones.
    job.lineInput = null; job.lineMode = "auto"; job.wantSize = null; job.decision = null;
    let r = null;
    // the Claude job already asked about these very words is taken up again (its poll cut off by the network, a reload)
    // rather than paid for a second time; one that failed is replaced once
    const ask = { order: row.order.receiptId, sku: sp.designSku, title: row.line.title, form: sp.form, quantity: sp.quantity, engravable, personalization: sp.personalization, buyerMessage: sp.buyerMessage, staffNote: sp.staffNote, messages: sp.messages };
    let key = 2166136261; for (const ch of JSON.stringify(ask)) key = Math.imul(key ^ ch.charCodeAt(0), 16777619); key = (key >>> 0).toString(36);
    const prior = job.claudeJob && job.claudeJob.ask === key ? job.claudeJob.id : null;
    try { r = await agentCall("engraveIntent", ask, { label: `Claude reads the words of ${row.order.receiptId}`, background: true, existingId: prior, retryFailed: !!prior, onStarted: id => { job.claudeJob = { id, ask: key }; } }); }
    catch (e) { r = { skipped: e.message }; }
    if (items() !== owner || items().get(job.key) !== job || row.state === "gone" || job.state !== "classify") return job;
    if (!r || r.skipped || r.error) {
      // Keep the verbatim proposal reviewable even when the classifier is unavailable.
      job.text = sp.personalization.join("\n"); job.lines = sp.personalization.slice(); job.source = "personalization"; job.confidence = 0; job.questions = [`Claude was unavailable (${(r && (r.skipped || r.error)) || "no answer"}) — confirm the words`]; job.requests = { side: "back", font: null, handwriting: false, image: false };
      return job.lines.length ? setReady(job) : toWords(job, "Enter the requested inscription");
    }
    job.text = r.text || ""; job.lines = job.text.split(/\r?\n/).map(s => s.trim()).filter(Boolean); job.source = r.source; job.quote = r.sourceQuote; job.confidence = r.confidence; job.requests = r.requests; job.questions = r.questions || []; job.claudeReasoning = r.reasoning || null;
    const originalLines=[(sp.personalization || []).join("\n"),sp.buyerMessage,sp.staffNote,job.quote].filter(Boolean)
      .map(t=>String(t).split(/\r?\n/).map(t=>t.trim()).filter(Boolean))
      .find(lines=>lines.length && lines.join(" ").replace(/\s+/g," ")===job.text.replace(/\s+/g," ").trim());
    if(originalLines) {job.lines=originalLines;job.text=originalLines.join("\n");}
    agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}: Claude reads ${r.engrave ? `"${job.text.replace(/\n/g, " / ")}" from ${r.source} (${Math.round(r.confidence * 100)}%)` : "no engraving"}${job.questions.length ? ` · ${job.questions.length} question(s)` : ""}`, { reason: r.reasoning || null });
    // Confidence describes the interpretation, not whether a preview can be drawn.
    // Nothing here approves or exports a proposed inscription.
    if (!r.engrave && !job.lines.length) {
      // Settings' engraving confidence: 0 is a setting (trust every reading); it used to be read as 0.8, like an empty field
      const trust = S.settings.engraveConfidence;
      if (job.confidence < (trust != null && trust !== "" && Number.isFinite(+trust) ? +trust : 0.65) || job.questions.length)
        return toWords(job, "Check the requested inscription");
      setNone(job, "No engraving requested"); return job;
    }
    if (!job.lines.length) return toWords(job, "Enter the requested inscription");
    return setReady(job);
  }
  function setNone(job, why) { job.state = "none"; job.reason = why; job.row.engrave = { needed: false, state: "none", reason: why, approved: true }; Review.remove("eng:" + job.key); RunCtl.poke(); return job; }
  function toWords(job, why) { job.state = "words"; job.reason = why; job.row.engrave = { needed: true, state: "words", text: job.text, approved: false, reason: why }; Review.add({ kind: "engraveWords", key: "eng:" + job.key, row: job.row, job, why }); RunCtl.poke(); return job; }
  async function setReady(job, wake = true) {
    await loadFonts();
    if (!F_.ok) { job.state = "blocked"; job.reason = "Source Sans 3 font files are missing"; job.row.engrave = { needed: true, state: "blocked", text: job.text, approved: false, reason: job.reason }; Review.add({ kind: "fontMissing", key: "eng:" + job.key, row: job.row, job, why: F_.error }); return job; }
    const cov = G.glyphCoverage(F_.Regular, job.lines.join("\n"));
    if (!cov.ok) { job.state = "words"; job.reason = `Unsupported engraving characters: ${cov.missing.map(c => c + " (" + [...c].map(x=>"U+"+x.codePointAt(0).toString(16).toUpperCase()).join(" ") + ")").join(", ")}${F_.emojiError ? " — " + F_.emojiError : ""}`; job.missing = cov.missing; job.row.engrave = { needed: true, state: "words", text: job.text, approved: false, reason: job.reason }; Review.add({ kind: "notRepresentable", key: "eng:" + job.key, row: job.row, job, why: job.reason }); return job; }
    job.state = "ready"; job.reason = null; job.missing = null; job.row.engrave = { needed: true, state: "ready", text: job.text, approved: false }; Review.remove("eng:" + job.key);
    if(!job.editingBack) {Pool.update(job.copies, { engrave: true }).catch(() => {});if (wake) RunCtl.poke();}                                                        // a waiting run fits it now (the classifier answers asynchronously)
    return job;
  }
  /** A person's decision on the words (confirm / edit / no engraving), recorded with the name. */
  async function decideWords(job, { text, none, by, note }) {
    by = by || employeeName() || askEmployee(); if (!by) { toast("Set your name first", "bad"); return; }
    if (none) { setNone(job, `no engraving — decided by ${by}`); job.decision = { by, at: Date.now(), none: true }; Review.remove("eng:" + job.key); agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId}: no engraving (${by})`); Orders.render(); RunCtl.poke(); return job; }
    job.lineInput = null; job.lineMode = "auto"; job.text = String(text || "").trim(); job.lines = job.text.split(/\r?\n/).map(s => s.trim()).filter(Boolean); job.decision = { by, at: Date.now(), text: job.text, note: note || null }; job.questions = []; job.requests = { side: "back", font: null, handwriting: false, image: false }; job.confidence = 1;
    if (!job.editingBack && job.text && job.text !== (job.row.spec.personalization || []).join("\n")) { try { await DesignLink.call("notes.set", { receiptId: job.row.order.receiptId, text: `${job.row.spec.staffNote ? job.row.spec.staffNote + "\n" : ""}Engrave (${by}): ${job.text.replace(/\n/g, " / ")}` }); } catch (e) { agent({ engrave: true }, "warn", `staff note not saved: ${e.message}`); } }
    agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId}: words decided by ${by}: "${job.text.replace(/\n/g, " / ")}"`);
    Review.remove("eng:" + job.key);
    await setReady(job);
    if (job.state === "ready") { const ch = job.copies.length && sheetFor(job,job.copies[0]); if (ch && ch.fileBase) await fitJob(job); }
    Orders.render(); if(!job.editingBack) RunCtl.poke(); return job;
  }
  let previewRecovery = false, previewRecoveryTimer = null, preparingJob = null;
  const isWorking = job => classifyTasks.has(job) || fitTasks.has(job) || preparingJob === job;
  const canFit = job => !!(job.copies?.length && charmFor(job)?.outline && sheetFor(job, job.copies[0])?.fileBase);
  const needsPreview = job => job.row.state !== "gone" && !isWorking(job) && job.lines?.length && !job.missing?.length &&
    (job.state === "words" || (["ready", "fitting"].includes(job.state) && canFit(job)));
  const queuedJobs = jobs => jobs.filter(j => ["review", "words", "blocked", "classify", "ready", "fitting"].includes(j.state));
  async function prepareWaitingPreviews() {
    if (previewRecovery) return;
    previewRecovery = true;
    try {
      for (const job of items().values()) {
        if (!needsPreview(job)) continue;
        // Let the tab paint and handle navigation between recovered placements.
        await new Promise(resolve => setTimeout(resolve, 0));
        if (items().get(job.key) !== job || !needsPreview(job)) continue;
        preparingJob = job;
        job.questions = (job.questions || []).filter(q => !/not engravable|cannot (?:be |take )engrav|design.*engrav/i.test(q));
        try {
          if (job.state === "words") await setReady(job, false);
          if (["ready", "fitting"].includes(job.state) && canFit(job)) await fitJob(job);
        } catch (e) {
          job.state = "blocked"; job.reason = e.message;
          job.row.engrave = { needed:true, state:"blocked", text:job.text, approved:false, reason:e.message };
        } finally { preparingJob = null; }
      }
    } finally { previewRecovery = false; RunCtl.poke(); render(); }
  }
  let classifyPass = null, classifyOwner = null;
  function classifyAll(run) {
    const owner=items();
    if (classifyPass && classifyOwner === owner) return classifyPass;
    classifyOwner=owner;
    const pass=classifyAllOnce(run,owner).finally(() => { if(classifyPass === pass)classifyPass = null; });
    classifyPass=pass;return pass;
  }
  async function classifyAllOnce(run,owner) {
    await loadFonts();
    if(items() !== owner)return 0;
    // the line's own fields first: most lines have nothing to read, and those need not be looked for on the sheets
    const rows = Orders.rows().filter(r => ["pooled", "written"].includes(r.state) && r.spec && r.spec.engraveCandidate && (!r.engrave || r.engrave.state === "reclassify" || r.engrave.state === "classify") && (!Gate.modern() || Pool.sheetOf(r.poolIds[0])));
    const q = rows.slice(); let done = 0;
    // one bar for the whole pass, not one per order: what a person needs to know is how far along the reading is
    const bar = rows.length && window.CNProgress ? CNProgress.start(`Reading the words of ${rows.length} order line${rows.length === 1 ? "" : "s"}`, { total: rows.length }) : null;
    try {
      await Promise.all(Array.from({ length: 3 }, async () => { while (q.length && items() === owner) { const r = q.shift(); try { await classify(r); } catch (e) { if(items() !== owner)break; agent({ engrave: true }, "warn", `${r.order.receiptId}: classifier failed — ${e.message}`); toWords(ensureJob(r), e.message); } done++; if (bar) bar.set(done, rows.length, r.order.receiptId); render(); } }));
    } finally { if (bar) bar.end(); }
    if(items() !== owner)return done;
    if(Orders.rows().some(r=>["pooled","written"].includes(r.state) && r.spec?.engraveCandidate && (!r.engrave || r.engrave.state === "reclassify") && (!Gate.modern() || Pool.sheetOf(r.poolIds[0]))))
      return done + await classifyAllOnce(run,owner);
    let plain = 0;
    for (const r of Orders.rows()) if (r.state === "pooled" && r.spec && !r.spec.engraveCandidate && !r.engrave) { r.engrave = { needed: false, state: "none", approved: true }; plain++; }
    // a pass with nothing to read changes nothing: the lists and the run record are left as they are
    if (!done && !plain) return 0;
    done += plain;
    Orders.render(); render();
    // the run record follows the work without holding it up (see RunCtl.loopNow); a failed save shows on the banner
    if (run) { run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); RunCtl.save(run).catch(() => {}); }
    return done;
  }

  /* ── 7.2 / 7.3 · the flip and the fit, once per distinct placement (identical copies share it) ── */
  let workerClient = null;
  function fitClient() {
    if (!workerClient) {
      if (!window.Worker || !F_.workerFonts?.Regular) throw new Error("Background engraving could not start. Reload and retry this placement.");
      workerClient = window.CharmNestEngraveFit.createClient({WorkerClass:window.Worker,url:"charm-nest-engrave-worker.js?v=20260921-material",fonts:F_.workerFonts});
    }
    return workerClient;
  }
  function fitInput(job, charm, entry) {
    // Keep path identity (outline may also be a member), but exclude UI/cache
    // objects and any DOM references from the structured-clone payload.
    return {charm:charm && {outline:charm.outline,members:charm.members,bbox:charm.bbox,widthPt:charm.widthPt,heightPt:charm.heightPt,upAngle:charm.upAngle},
      lines:(job.lineInput || job.lines).slice(),lineMode:job.lineMode || "auto",opts:fitOpts(job),
      viewOptions:{res:6,upAngle:job.editingBack ? job.editOriginal.upAngle ?? charm?.upAngle : entry.upAngle == null ? undefined : +entry.upAngle,materialVersion:2},
      maskOptions:{marginMm:+S.settings.engraveMarginMm || .8,keepOut:charm?.backKeepOut || []}};
  }
  const fitStamp = ({charm, ...options}) => JSON.stringify(options);
  const fitTasks = new WeakMap();
  function fitJob(job) {
    if (fitTasks.has(job)) return fitTasks.get(job);
    const task = Promise.resolve().then(() => fitJobOnce(job)).finally(() => { fitTasks.delete(job); render(); });
    fitTasks.set(job, task);
    return task;
  }
  async function fitJobOnce(job) {
    if (EG.cardKey === job.key) { EG.card = null; EG.cardKey = null; }
    job.reason = null; job.verify = null; job.fit = null; delete job.writtenFit;
    await loadFonts();
    job.lineInput ||= job.lines.slice();
    job.lines = job.lineInput.slice();
    if (!F_.ok || !G.glyphCoverage(F_.Regular, job.lines.join("\n")).ok) return setReady(job);
    const poolId = job.copies[0]; const charm = charmFor(job); if (!charm) { job.state = "ready"; return job; }
    job.state = "fitting"; job.row.engrave.state = "fitting"; render();
    const entry = Master.entryFor(job.row.spec.designSku) || {};
    const input = fitInput(job, charm, entry), stamp = fitStamp(input), currentItems = items();
    let result;
    try { result = await fitClient().run(input); }
    catch (e) {
      if (items() !== currentItems || items().get(job.key) !== job || job.state !== "fitting") return job;
      job.state = "blocked"; job.reason = e.message; job.flipError = e; job.row.engrave.state = "blocked"; job.row.engrave.reason = job.reason;
      Review.add({ kind: e.stage === "flip" ? "flipFailed" : "placement", key: "eng:" + job.key, row: job.row, job, why: job.reason, checks:e.checks, images:e.images });
      agent({engrave:true}, "warn", `${job.row.order.receiptId}: ${job.reason}`);
      // A failed back check holds this engraving; other jobs and sheets continue.
      render(); return job;
    }
    // A result belongs to the exact job/input that requested it. A changed set,
    // skipped job or edit made during calculation cannot be overwritten.
    if (items() !== currentItems || items().get(job.key) !== job || job.row.state === "gone" || job.state !== "fitting") return job;
    if (charmFor(job) !== charm || charm.outline !== input.charm.outline || charm.members !== input.charm.members || stamp !== fitStamp(fitInput(job, charmFor(job), Master.entryFor(job.row.spec.designSku) || {}))) return fitJobOnce(job);
    const {view,mask,fit,lines,check}=result;
    // materialVersion marks a fit made with the current material model; Pool.repairRecoveredGeometry refits only older
    // ones on restore. Without the mark every fit (and every nudge, turn and resize on it) was thrown away on reload.
    job.view=view; job.mask=mask; job.lines=lines; job.text=lines.join("\n"); job.fit=fit; job.fitAt=Date.now(); job.materialVersion=2;
    if(!fit) {
      job.state="review";job.reason=result.reason;job.row.engrave.state="review";
      if(!job.editingBack)Review.add({kind:"placement",key:"eng:"+job.key,row:job.row,job,why:result.reason});
      render();return job;
    }
    job.wantSize=fit.size; job.verify={geometry:check,at:Date.now()};
    job.state = "review"; job.row.engrave.state = "review"; job.claude = null;
    agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId} · ${job.row.spec.designSku}: "${job.lines.join(" / ")}" fits at ${fit.size.toFixed(2)} pt (cap ${fit.capMm.toFixed(2)} mm, ${fit.weight}${fit.angle ? `, ${fit.angle}°` : ""}${fit.small ? ", SMALL" : ""}${fit.thin ? ", strokes under the engraver limit" : ""}) — awaiting a person`);
    if (!job.editingBack) Review.add({ kind: "placement", key: "eng:" + job.key, row: job.row, job });
    render(); return job;
  }
  async function claudeRead(job) {
    if (!S.cloud.ok || !job.fit) return;
    const png = renderBack(job, 700, { grid: true }).toDataURL("image/png");
    const r = await agentCall("engraveReview", { image: png, order: job.row.order.receiptId, sku: job.row.spec.designSku, text: job.lines.join("\n"), capMm: job.fit.capMm, font: "Source Sans 3", weight: job.fit.weight, angle: job.fit.angle, small: job.fit.small }, { label: `Claude looks at the back of ${job.row.order.receiptId}`, background: true });
    if (r.skipped) { job.claude = { skipped: r.skipped }; render(); return; }
    job.claude = { legible: !!r.legible, notes: r.notes || "", concerns: r.concerns || [] };
    agent({ engrave: true }, r.legible ? "ENGRAVE" : "warn", `${job.row.order.receiptId}: Claude ${r.legible ? "reads it fine" : "finds it hard to read"} — ${r.notes}`);
    render();
  }
  /** Fits every job whose words are read and whose sheet is written; returns how many it fitted. */
  async function fitAll(run) {
    const jobs = [...items().values()].filter(j => !j.editingBack && j.state === "ready" && j.row.state !== "gone" && canFit(j) && !isWorking(j));
    let fitted = 0;
    // a job counts only when the fit moved it on: one left "ready" (its charm not found on the sheet yet) changed nothing
    for (const j of jobs) { if (j.state !== "ready") continue; const sh = j.copies.length && Pool.sheetOf(j.copies[0]); if (!sh || !sh.fileBase) continue; try { await fitJob(j); } catch (e) { j.state = "blocked"; j.reason = e.message; j.row.engrave.state = "blocked"; agent({ engrave: true }, "warn", `${j.row.order.receiptId}: ${e.message}`); Review.add({ kind: "flipFailed", key: "eng:" + j.key, row: j.row, job: j, why: e.message }); } if (j.state !== "ready") fitted++; }
    if (!fitted) return 0;
    // the run record follows the work without holding it up (see RunCtl.loopNow); a failed save shows on the banner
    if (run) { run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); RunCtl.save(run).catch(() => {}); }
    render(); return fitted;
  }
  /* Reading the words and fitting them run beside the placement, never in front of it (Paul, 24 Sep: "it stops the next
     charm from being placed every time there's a back engraving"). New orders went onto their sheets, and then the
     next update waited while Claude read every new order's words, about 7 s a line, and the run waited on them at
     Engraving. Now both only start this pass: the charms keep coming, and a set still waits for its words to be read,
     fitted and approved before it goes to the laser (Sets.validateRelease). A pass asked for while one runs is run
     once more when it ends, so a line that arrives meanwhile is read too; when a pass has read or fitted anything the
     run is told (RunCtl.backgroundSettled), since a line that needs no engraving can let its set go on.            */
  let backgroundPass = null, backgroundAgain = null, settledPasses = 0;
  function background(run) {
    if (backgroundPass) { backgroundAgain = run || backgroundAgain || B.run; return backgroundPass; }
    backgroundPass = (async () => {
      let did = 0;
      try {
        for (let r = run || B.run; ;) {
          did += (await classifyAll(r)) || 0;
          did += (await fitAll(r)) || 0;
          if (!backgroundAgain) break;
          r = backgroundAgain; backgroundAgain = null;
        }
      } catch (e) { agent({ engrave: true }, "warn", `engraving: ${e.message}`); }
      finally { backgroundPass = null; if (did) { settledPasses++; RunCtl.backgroundSettled(); } }
    })();
    return backgroundPass;
  }
  function revokeBacks(job) {
    job.approvedAt = null; job.backs = []; delete job._backPreview; delete job.writtenFit; delete job._shelveTried;
    for (const sh of allSheets()) sh.backPool = (sh.backPool || []).filter(b=>!job.copies.includes(b.poolId));
    refreshBacks();
    const invalidate=()=>api("charmNestLibrary", {op:"backInvalidate", poolIds:job.copies});
    const task = window.CharmNestOperations ? window.CharmNestOperations.run({key:'back-remove:'+job.key,label:'Updating engraving',resources:['production:'+(job.editSheet?.runId || B.run?.runId || job.editSheet?.sheetId || 'manual')]},invalidate) : backQueue.catch(()=>{}).then(invalidate);
    backQueue = task; task.catch(e=>{ job.reason = "Back removal not saved: " + e.message; RunCtl.stopIfRunning(job.reason, "Reconnect and reopen this engraving before continuing."); toast(job.reason,"bad"); });
  }
  function invalidate(row, why) { const j = items().get(row.key); if (!j) return; if (j.state === "approved" || j.state === "written" || j.state === "review" || j.state === "ready") { revokeBacks(j); j.previous = { text: j.text, fit: j.fit && (({ layout, glyphs, cmds, ...f }) => f)(j.fit), approvedBy: j.approvedBy }; j.state = "classify"; j.fit = null; j.approvedBy = null; j.approvedAt = null; j.reason = why; Review.remove("eng:" + j.key); } }

  /* ── 7.5 · the review controls ── */
  /* One fitting policy for dragging, rotation and manual size: change the
     wrapping before reducing the requested size, and restore it when room
     returns. The original words remain separate from generated line breaks. */
  function refit(job, place) {
    const font=fontFor(job.fit.weight),want=place.size ?? job.wantSize ?? job.fit.size;
    const f=G.reflowAt(job.lineInput || job.lines,font,job.mask,{...fitOpts(job),measure:place.measure},{...place,size:want},job.lineMode || 'auto');
    if(!f.ok)return false;
    f.fittedMax=Math.max(f.size,job.fit.fittedMax || 0);f.weight=job.fit.weight;f.rect=job.fit.rect;
    job.wantSize=want;job.lines=f.lines.slice();job.text=job.lines.join("\n");
    job.fit=f;job.verify={geometry:G.verifyInk(f.cmds,job.mask),at:Date.now()};job.nudged=true;job.claude=null;
    return true;
  }
  /** New words on a placement someone moved or turned: they are fitted afresh, then put back where that person had
   *  them, at the size the fresh fit found (smaller there if it must be). They used to go back to the automatic spot,
   *  and the move and turn were lost. False when they had to be placed again somewhere else. */
  async function fitNewWords(job) {
    const keep = job.nudged && job.fit ? { centre: job.fit.centre.slice(), angle: job.fit.angle } : null;
    job.wantSize = null; await fitJob(job);
    if (!keep || !job.fit || job.state !== "review") return true;
    if (refit(job, keep)) { Session.schedule(); refresh(job); return true; }
    job.nudged = false; toast("The new words do not fit where they had been moved, so they were placed again", "", 6000); return false;
  }
  function nudge(job, dxMm, dyMm) { if (!job.fit) return; const c = [job.fit.centre[0] + dxMm * PT, job.fit.centre[1] + dyMm * PT]; if (!refit(job, { centre: c, angle: job.fit.angle })) toast("No room there", "bad"); else Session.schedule(); refresh(job); }   // an arrow key raises no input/pointerup to checkpoint on
  /** The middle of the area the text may use: the centre of gravity of the solid pixels, not of the bounding box, so a
      cat's head with ears puts the name where the metal actually is. */
  function maskCentroid(m) {
    let sx = 0, sy = 0, n = 0;
    for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) if (m.bits[y * m.w + x]) { sx += x; sy += y; n++; }
    if (!n) return [m.cx, m.cy];
    return [m.ox + (sx / n + 0.5) / m.res, m.oy + (sy / n + 0.5) / m.res];
  }
  function centreText(job) { if (!job.fit) return; if (!moveTo(job, maskCentroid(job.mask))) toast("The text does not fit in the middle — left where it was", "bad"); }
  function moveTo(job, centre) { if (!job.fit) return false; const ok = refit(job, { centre, angle: job.fit.angle }); if (ok) refresh(job); return ok; }
  /** Turn the text about its centre. Within three degrees of straight or upright it snaps there. */
  function rotateTo(job, angle) { if (!job.fit) return false; angle = ((angle % 360) + 360) % 360; for (const snap of [0, 90, 180, 270, 360]) if (Math.abs(angle - snap) < 3) angle = snap % 360; const ok = refit(job, { centre: job.fit.centre, angle }); if (!ok) toast("The text does not fit at that angle", "bad"); else Session.schedule(); refresh(job); return ok; }
  /** The box around the text: its centre, its width and height in the text's own frame, and its angle. */
  function textBox(glyphs, centre, angleDeg) {
    const a = -(angleDeg || 0) * Math.PI / 180, ca = Math.cos(a), sa = Math.sin(a);
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const g of glyphs) for (const c of g.cmds) { if (c.type === "Z") continue; for (const [px, py] of [[c.x, c.y], c.x1 != null ? [c.x1, c.y1] : null, c.x2 != null ? [c.x2, c.y2] : null].filter(Boolean)) { const dx = px - centre[0], dy = py - centre[1]; const lx = dx * ca - dy * sa, ly = dx * sa + dy * ca; if (lx < x0) x0 = lx; if (lx > x1) x1 = lx; if (ly < y0) y0 = ly; if (ly > y1) y1 = ly; } }
    if (!isFinite(x0)) return null;
    return { cx: centre[0], cy: centre[1], lx0: x0, ly0: y0, lx1: x1, ly1: y1, angle: angleDeg || 0 };
  }
  function resize(job, size) {
    if(!job.fit || !Number.isFinite(size))return;
    size=Math.min(fitOpts(job).maxHeightFrac*(job.mask.hPt || job.mask.h/job.mask.res),Math.max(.01,size));
    if(!refit(job,{centre:job.fit.centre,angle:job.fit.angle,size})){toast("No room at that size","bad");return;}
    reRead(job);refresh(job);
  }
  function setLineSpacing(job, percent, measure = true) {
    if(!job.fit || !Number.isFinite(percent) || job.backSaving || job.approvalPreparing)return false;
    const previous=job.lineGap;
    job.lineGap=.18*Math.max(0,Math.min(300,percent))/100;
    if(!refit(job,{centre:job.fit.centre,angle:job.fit.angle,measure})) {job.lineGap=previous;return false;}
    refresh(job);if(measure)Session.schedule();return true;
  }
  async function resplit(job) { const vars = G.splitVariants(job.lines); const i = (job.splitIndex || 0) + 1; const pick = vars[i % vars.length]; job.splitIndex = i; job.lineInput=pick.slice(); job.lineMode="preserve"; job.lines = pick; job.text = pick.join("\n"); agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId}: re-split as "${pick.join(" / ")}"`); await fitJob(job); }
  async function skip(job, by) { by = by || employeeName() || askEmployee(); if (!by) return; revokeBacks(job); job.state = "skipped"; job.approvedBy = null; job.row.engrave = { needed: false, state: "skipped", text: job.text, approved: true, reason: `cut plain — skipped by ${by}` }; job.row.flag = `engraving skipped by ${by}`; Review.remove("eng:" + job.key); agent({ engrave: true }, "warn", `${job.row.order.receiptId} · ${job.row.spec.designSku}: engraving skipped by ${by} — cut plain, order flagged`); await Pool.update(job.copies, { engrave: false, engraveSkippedBy: by }); if(job.editingBack) {await backQueue;await syncEditedBack(job);} Orders.render(); render(); if(!job.editingBack) RunCtl.poke(); }
  function sendBack(job, why) { revokeBacks(job); job.state = "words"; job.reason = why || "sent back from the placement review — a decision on the words is needed"; job.row.engrave.state = "words"; job.row.engrave.approved = false; Review.remove("eng:" + job.key); Review.add({ kind: "engraveWords", key: "eng:" + job.key, row: job.row, job, why: job.reason }); render(); Orders.render(); }
  async function approve(job, by) {
    if(job.backSaving || job.approvalPreparing) return;
    if(EG.cardKey === job.key && EG.card?._previewFailed) { toast("Refit the words to restore the preview before approving.", "bad"); return; }
    if(EG.cardKey===job.key) EG.card?._flushSpacing?.();
    job.approvalPreparing=true;
    try {
    if(EG.cardKey===job.key) {
      clearTimeout(EG.card?._wordsTimer);
      const text=EG.card?.querySelector('[data-f="words"]')?.value.trim();
      if(text === "") { toast("Type the words first", "bad"); return; }
      if(text != null) delete EG.drafts?.[job.key];
      if(text != null && text!==(job.lineInput || job.lines).join("\n")) {
        job.text=text;job.lineInput=text.split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
        // placed somewhere new, the words are shown before anyone approves them
        job.lines=job.lineInput.slice();if(!(await fitNewWords(job)))return;
      }
    }
    } finally {job.approvalPreparing=false;}
    if (job.backSaving) return;
    by = by || employeeName() || askEmployee(); if (!by) { toast("An employee name is required to approve", "bad"); return; }
    // says which step is missing (it read "Nothing verified to approve" whatever the reason)
    if (!job.fit || !job.verify || !job.verify.geometry.ok) { toast(!job.fit ? "Not approved: the words are not placed on the charm yet" : !job.verify ? "Not approved: the placement is still being checked" : "Not approved: the placement failed its check · move or resize the words first", "bad"); return; }
    job.state = "approved"; job.approvedBy = by; job.approvedAt = Date.now(); job.row.engrave = Object.assign(job.row.engrave || {}, { needed: true, state: "approved", approved: true, text: job.text, approvedBy: by });
    Review.remove("eng:" + job.key);
    agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId} · ${job.row.spec.designSku}: placement approved by ${by} (${job.fit.size.toFixed(2)} pt, cap ${job.fit.capMm.toFixed(2)} mm${job.nudged ? ", nudged" : ""})`);
    await saveBacks(job);
  }
  /** An approval's back files are written; one that cannot be written sends the job back to placement review. */
  async function saveBacks(job) {
    job.backSaving = true; const approval = job.approvedAt, was = job.state, had = (job.backs || []).length;
    render(); Orders.render(); refreshBacks();
    try { await writeBacks(job); if (job.approvedAt === approval && job.backPending) { delete job.backPending; backRetry.n = 0; } } catch (e) { if (job.approvedAt !== approval) { job.backSaving = false; return; }
      // The network or the cloud gone for a while is not the placement's fault: the approval stands and its back file waits
      // for the cloud, written when it is back (resumeBacks). It used to undo the approval, and after a wake a person
      // approved the same words again. Only a file that fails its own checks goes back to review.
      if (/answer in time|timed out|network|Failed to fetch|HTTP 5\d\d|link is down|no reply|closed|offline|Reconnect to save|PUT failed/i.test(e.message || "") || globalThis.navigator?.onLine === false) {
        job.backPending = e.message; agent({ engrave: true }, "warn", `${job.row.order.receiptId}: the approved back file waits for the cloud (${e.message})`); retryBacksLater(); render(); }
      else { job.state = "review"; job.row.engrave.state = "review"; job.row.engrave.approved = false; job.reason = "back file failed: " + e.message; refreshBacks(); agent({ engrave: true }, "warn", `${job.row.order.receiptId}: ${job.reason}`); if (!job.editingBack) Review.add({ kind: "placement", key: "eng:" + job.key, row: job.row, job, why: job.reason }); render(); } }
    job.backSaving = false; Session.schedule();
    // the run looks again only when the save changed something (a save with nothing to write would start it over and over)
    if(!job.editingBack && (job.state !== was || (job.backs || []).length !== had)) RunCtl.backgroundSettled();
  }
  // back files waiting for the cloud are written when it is back (cn-cloud-back, "online") and, failing that, 30 s, 1, 2
  // and 5 minutes on, then every 10 minutes
  const backRetry = { t: 0, n: 0 };
  function retryBacksLater() { if (backRetry.t) return; backRetry.t = setTimeout(() => { backRetry.t = 0; resumeBacks(true); }, [30000, 60000, 120000, 300000, 600000][Math.min(backRetry.n++, 4)]); }
  /** A reload while an approval's back files were being written left it approved with none, or only some, of them:
      its sheet showed "Saving…" until the run next passed Engraving, and not at all while the run stayed stopped. After the
      workspace is restored those writes run again, as the approval ran them (saveBacks). */
  function resumeBacks(pendingOnly) {
    let waiting = 0;
    for (const job of items().values()) {
      if (job.state !== "approved" || job.backSaving || !job.approvedAt || !(job.fit && job.view || job.writtenFit && job.writtenFit.approvedAt === job.approvedAt) || !(job.copies || []).length) continue;
      if (job.copies.every(id => (job.backs || []).some(b => b.poolId === id && b.approvedAt === job.approvedAt))) continue;
      if (pendingOnly && !job.backPending) continue;
      // (with the cloud still away it waits on: each try builds and checks the file before it finds that out)
      if (job.backPending && !S.cloud.ok) { waiting++; continue; }
      agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId} · ${job.row.spec.designSku}: writing the approved back file ${job.backPending ? "that waited for the cloud" : "again, cut short by the reload"}`);
      saveBacks(job).catch(e => agent({ engrave: true }, "warn", `${job.row.order.receiptId}: ${e.message}`));
    }
    if (waiting) retryBacksLater();
  }
  window.addEventListener("online", () => { resumeBacks(true); if (F_.emojiError) loadFonts(true).catch(() => {}); });

  /* ── 7.6 · back files, one per piece, only after approval ── */
  function renderBack(job, px, { grid = false, hatch = true, editable = false } = {}) {
    const view = job.view, mask = job.mask, fit = job.fit; const cv = document.createElement("canvas"); cv._editable = editable;
    const bb = view.members.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
    const pad = 3 * PT; const w = bb[2] - bb[0] + 2 * pad, h = bb[3] - bb[1] + 2 * pad; const k = px / Math.max(w, h);
    cv.width = Math.round(w * k); cv.height = Math.round(h * k); cv._sizePt = {w,h}; const ctx = cv.getContext("2d");
    ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height);
    const tx = (x, y) => [(x - bb[0] + pad) * k, (bb[3] + pad - y) * k];
    if (grid) { ctx.strokeStyle = "rgba(0,0,0,.09)"; ctx.lineWidth = 1; const x0 = Math.floor((bb[0] - pad) * MM), x1 = Math.ceil((bb[2] + pad) * MM); for (let mm = x0; mm <= x1; mm++) { const p = tx(mm * PT, 0); ctx.beginPath(); ctx.moveTo(p[0], 0); ctx.lineTo(p[0], cv.height); ctx.stroke(); } const y0 = Math.floor((bb[1] - pad) * MM), y1 = Math.ceil((bb[3] + pad) * MM); for (let mm = y0; mm <= y1; mm++) { const p = tx(0, mm * PT); ctx.beginPath(); ctx.moveTo(0, p[1]); ctx.lineTo(cv.width, p[1]); ctx.stroke(); } ctx.save(); ctx.fillStyle = "rgba(0,0,0,.4)"; ctx.font = `${Math.max(9, k * 2)}px sans-serif`; ctx.textBaseline = "top"; ctx.fillText("1 mm grid", 4, 4); ctx.restore(); }   // hung from the top edge: a large preview's label was cut off
    if (hatch && mask) { // the eroded mask as a light tint: where text may go
      const img = ctx.createImageData(cv.width, cv.height); const d = img.data;
      for (let py = 0; py < cv.height; py++) for (let pxx = 0; pxx < cv.width; pxx++) { const x = bb[0] - pad + pxx / k, y = bb[3] + pad - py / k; if (G.at(mask, x, y)) { const i = (py * cv.width + pxx) * 4; d[i] = 231; d[i + 1] = 237; d[i + 2] = 223; d[i + 3] = 255; } }
      const off = document.createElement("canvas"); off.width = cv.width; off.height = cv.height; off.getContext("2d").putImageData(img, 0, 0); ctx.globalCompositeOperation = "multiply"; ctx.drawImage(off, 0, 0); ctx.globalCompositeOperation = "source-over";
    }
    const base = document.createElement("canvas"); base.width = cv.width; base.height = cv.height;   // grid + mask tint, drawn once
    base.getContext("2d").drawImage(cv, 0, 0);
    for (const m of view.members) { ctx.beginPath(); P.pathToCanvas(ctx, m, tx); ctx.strokeStyle = "#000"; ctx.lineWidth = Math.max(1, 0.5 * k); ctx.stroke(); }
    if (false && fit) { ctx.fillStyle = "#111"; for (const g of fit.glyphs) { ctx.beginPath(); let cur = null; for (const c of g.cmds) { if (c.type === "M") { const p = tx(c.x, c.y); ctx.moveTo(p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "L") { const p = tx(c.x, c.y); ctx.lineTo(p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "C") { const a = tx(c.x1, c.y1), b = tx(c.x2, c.y2), p = tx(c.x, c.y); ctx.bezierCurveTo(a[0], a[1], b[0], b[1], p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "Q") { const a = tx(c.x1, c.y1), p = tx(c.x, c.y); ctx.quadraticCurveTo(a[0], a[1], p[0], p[1]); cur = [c.x, c.y]; } else ctx.closePath(); } ctx.fill("nonzero"); } }
    const outline = document.createElement("canvas"); outline.width = cv.width; outline.height = cv.height;   // …and the charm itself
    outline.getContext("2d").drawImage(cv, 0, 0);
    /** Repaint: the static layers, then the text — the fitted one, or a provisional one while a hand is moving it. */
    const glyphsOf = g => { ctx.fillStyle = "#111"; for (const gl of g) { ctx.beginPath(); for (const c of gl.cmds) { if (c.type === "M") { const p = tx(c.x, c.y); ctx.moveTo(p[0], p[1]); } else if (c.type === "L") { const p = tx(c.x, c.y); ctx.lineTo(p[0], p[1]); } else if (c.type === "C") { const a = tx(c.x1, c.y1), b2 = tx(c.x2, c.y2), d2 = tx(c.x, c.y); ctx.bezierCurveTo(a[0], a[1], b2[0], b2[1], d2[0], d2[1]); } else if (c.type === "Q") { const a = tx(c.x1, c.y1), d2 = tx(c.x, c.y); ctx.quadraticCurveTo(a[0], a[1], d2[0], d2[1]); } else if (c.type === "Z") ctx.closePath(); } ctx.fill("nonzero"); } };
    cv._paint = (prov) => {
      ctx.clearRect(0, 0, cv.width, cv.height); ctx.drawImage(outline, 0, 0);
      const gl = prov && prov.glyphs ? prov.glyphs : (job.fit ? job.fit.glyphs : []);
      const centre = prov && prov.centre ? prov.centre : (job.fit ? job.fit.centre : null);
      const angle = prov && prov.angle != null ? prov.angle : (job.fit ? job.fit.angle || 0 : 0);
      if (gl.length) glyphsOf(gl);
      if(cv._editable && job._spacingActive && job.fit?.layout?.rows.length>1) {
        const layout=job.fit.layout,a=angle*Math.PI/180,ca=Math.cos(a),sa=Math.sin(a);
        const at=(x,y)=>tx(centre[0]+x*ca-y*sa,centre[1]+x*sa+y*ca);
        ctx.save();ctx.strokeStyle="rgba(169,130,63,.65)";ctx.lineWidth=1;ctx.setLineDash([3,3]);
        for(const row of layout.rows) {const left=at(layout.local[0]-2,row.y),right=at(layout.local[2]+2,row.y);ctx.beginPath();ctx.moveTo(...left);ctx.lineTo(...right);ctx.stroke();}
        ctx.restore();
      }
      if (prov && prov.centre && prov.mode === "move") {                     // guides: the charm's own centre lines, lit when the text is on them
        const c = prov.centre, snapX = Math.abs(c[0] - mask.cx) < 0.35 * PT, snapY = Math.abs(c[1] - mask.cy) < 0.35 * PT;
        ctx.save(); ctx.setLineDash([4, 4]); ctx.lineWidth = 1;
        ctx.strokeStyle = snapX ? "rgba(160,110,30,.9)" : "rgba(0,0,0,.18)"; const px1 = tx(mask.cx, bb[1]), px2 = tx(mask.cx, bb[3]); ctx.beginPath(); ctx.moveTo(px1[0], px1[1]); ctx.lineTo(px2[0], px2[1]); ctx.stroke();
        ctx.strokeStyle = snapY ? "rgba(160,110,30,.9)" : "rgba(0,0,0,.18)"; const py1 = tx(bb[0], mask.cy), py2 = tx(bb[2], mask.cy); ctx.beginPath(); ctx.moveTo(py1[0], py1[1]); ctx.lineTo(py2[0], py2[1]); ctx.stroke();
        ctx.restore();
      }
      /* The box around the text is the whole editor: drag inside it to move, drag a corner to resize, drag the handle
         above it to turn. It is drawn in screen pixels so it reads the same at every zoom. */
      cv._box = null;
      if (gl.length && centre && cv._editable) {
        const box = textBox(gl, centre, angle); if (!box) return;
        const m = 3 * PT;                                                     // a little air around the letters
        const corners = [[box.lx0 - m, box.ly0 - m], [box.lx1 + m, box.ly0 - m], [box.lx1 + m, box.ly1 + m], [box.lx0 - m, box.ly1 + m]];
        const a = (box.angle || 0) * Math.PI / 180, ca = Math.cos(a), sa = Math.sin(a);
        const world = ([lx, ly]) => [box.cx + lx * ca - ly * sa, box.cy + lx * sa + ly * ca];
        const pts = corners.map(world).map(p => tx(p[0], p[1]));
        const topMid = world([(box.lx0 + box.lx1) / 2, box.ly1 + m]); const tm = tx(topMid[0], topMid[1]);
        const up = [-sa, ca];                                                 // the text's own "up", in pt
        const hp = tx(topMid[0] + up[0] * 2 * PT, topMid[1] + up[1] * 2 * PT);
        ctx.save(); ctx.lineWidth = 1; ctx.strokeStyle = "rgba(38,110,190,.9)"; ctx.setLineDash([]);
        ctx.beginPath(); pts.forEach((p, i) => i ? ctx.lineTo(p[0], p[1]) : ctx.moveTo(p[0], p[1])); ctx.closePath(); ctx.stroke();
        ctx.beginPath(); ctx.moveTo(tm[0], tm[1]); ctx.lineTo(hp[0], hp[1]); ctx.stroke();
        ctx.fillStyle = "#fff";
        for (const p of pts) { ctx.beginPath(); ctx.rect(p[0] - 4, p[1] - 4, 8, 8); ctx.fill(); ctx.stroke(); }
        ctx.beginPath(); ctx.arc(hp[0], hp[1], 5, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
        ctx.restore();
        cv._box = { box, corners: pts, rotate: hp, centrePx: tx(centre[0], centre[1]) };
      }
    };
    cv._map = { bb, pad, k, tx, base, outline, inv: (px, py) => [bb[0] - pad + px / k, bb[3] + pad - py / k] };
    cv._paint();
    return cv;
  }
  function renderFront(charm, px) {
    if (!charm?.outline || !Array.isArray(charm.bbox) || charm.bbox.length !== 4 || !charm.bbox.every(Number.isFinite))
      return el("div", "noPic", "The charm preview is still loading.");
    const cv = document.createElement("canvas"); const b = charm.bbox, pad = 3 * PT; const w = b[2] - b[0] + 2 * pad, h = b[3] - b[1] + 2 * pad, k = px / Math.max(w, h); cv.width = Math.round(w * k); cv.height = Math.round(h * k); const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height); const tx = (x, y) => [(x - b[0] + pad) * k, (b[3] + pad - y) * k]; P.drawCharm(ctx, charm, tx, k); return cv;
  }
  async function mountPlacementThumbnail(host,job) {
    if(!host || !job)return;
    const revision=host._thumbRevision=(host._thumbRevision || 0)+1;
    const current=()=>host.isConnected && host._thumbRevision === revision;
    const charm=(job.copies || job.row.poolIds || []).map(id=>Pool.charmOf(id)).find(Boolean);
    const paint=c=>{if(current())host.replaceChildren(renderFront(c,220));};
    if(charm?.outline && charm.members?.length){paint(charm);return;}
    const sku=job.row.spec.designSku || job.row.line.sku;
    const entry=Master.entryFor(sku) || await Master.fetchEntry(sku).catch(()=>null);
    if(!current())return;
    const geom=entry && Pool.sizeEntry(entry,job.row.spec.size);
    if(!entry || !geom?.aiPath){host.textContent="Preview unavailable";return;}
    try { const src=await Pool.masterCharm(entry,job.row.spec.size);if(current())paint(src.charms[0]); }
    catch(_){if(current())host.textContent="Preview unavailable";}

  }
  // Preview ownership is always derived from placements, never from SKU, order number or a cached sheet name.
  function sheetBacks(sheet) {
    const id = sheet.sheetId || sheet.id;
    const live = id && allSheets().find(p => p.sheetId === id);
    const target = live || sheet;
    const owned = window.CharmNestBacks.placedIds(target);
    const records = [...(sheet.backPool || sheet.backs || []), ...allSheets().flatMap(p => p.backPool || [])];
    const invalid = new Set();
    for (const j of items().values()) {
      if (j.editingBack && j.state !== "written") continue;
      if (!j.approvedAt || !["approved", "written"].includes(j.state)) { j.copies.forEach(id => invalid.add(id)); continue; }
      records.push(...(j.backs || []));
      if (!(j.view && j.fit) && F_.ok && hasPlacement(j) && j.copies.some(id=>owned.has(id) && !(j.backs || []).some(b=>b.poolId===id && b.approvedAt===j.approvedAt))) { try { restoreWritten(j); } catch (_) { /* written when it can be (writeBacks says why) */ } }
      if (j.view && j.fit && j.copies.some(id=>owned.has(id) && !(j.backs || []).some(b=>b.poolId===id && b.approvedAt===j.approvedAt))) {
        if (!j._backPreview || j._previewAt !== j.approvedAt) { const cv = renderBack(j, 360, {hatch:false}); j._backPreview = cv.toDataURL("image/png"); j._previewSize = cv._sizePt; j._previewAt = j.approvedAt; }
        for (const poolId of j.copies) if (!(j.backs || []).some(b => b.poolId === poolId && b.approvedAt === j.approvedAt)) records.push({poolId, order:j.row.order.receiptId, sku:j.row.spec.designSku, copy:B.pool.rows.get(poolId)?.copy, text:j.text, approvedAt:j.approvedAt, preview:j._backPreview, previewWPt:j._previewSize?.w, previewHPt:j._previewSize?.h, pending:true});
      }
    }
    // Older compact history records did not include placements; their saved back membership is authoritative.
    const normalized = target.recalled && !target.charms?.length ? Object.assign({}, target, {poolIds:target.recalled.poolIds?.length ? target.recalled.poolIds : (target.backPool || []).map(b=>b.poolId)}) : target;
    const fallback = !live && !Array.isArray(sheet.poolIds) && !sheet.placements ? Object.assign({}, sheet, {poolIds:(sheet.backPool || sheet.backs || []).map(b=>b.poolId)}) : normalized;
    return window.CharmNestBacks.forSheet(fallback, records.filter(b => !invalid.has(b.poolId)));
  }
  const backsMarkup = sheet => {
    const st = sheet.stock || sheet.recalled?.stock || sheet.outputs?.report?.stock || (sheet.metal ? stockFor(sheet.metal) : {});
    return window.CharmNestBacks.markup(sheetBacks(sheet), {wPt:st.wPt || st.wIn * PT_PER_IN, hPt:st.hPt || st.hIn * PT_PER_IN});
  };
  function refreshBacks() {
    refreshAllCards();
    for (const el of document.querySelectorAll("[data-back-sheet]")) {
      const id = el.dataset.backSheet, sh = allSheets().find(p=>p.sheetId === id) || S.library.rows.find(p=>p.id === id);
      if (sh) el.innerHTML = backsMarkup(sh);
    }
    Session.schedule();
  }
  function reconcileSheet(sh) {
    sh.backPool = sheetBacks(sh).filter(b=>!b.pending).map(b => { const out = Object.assign({}, b, {sheetId:sh.sheetId, setId:sh.setId || null, runId:sh.runId || null}); delete out.preview; return out; });
    return sh.backPool;
  }
  async function saveSheetBacks(sh) {
    // the sheet's approved backs go in one call (the server takes up to 400), and one already saved as it stands is not
    // sent again: each back used to be its own round trip at every save of its sheet, so a sheet with more engraved
    // charms held the next charm back longer
    const records = reconcileSheet(sh), sent = sh._backsSent || (sh._backsSent = new Map());
    const due = records.filter(b => sent.get(b.poolId) !== JSON.stringify(b));
    for (let i = 0; i < due.length; i += 400) {
      const chunk = due.slice(i, i + 400);
      await api("charmNestLibrary", {op:"backPut", backs:chunk});
      for (const b of chunk) sent.set(b.poolId, JSON.stringify(b));
    }
    refreshBacks();
  }
  let backQueue = Promise.resolve();
  function writeBacks(job) {
    const write = async()=>{
      try{return await writeBacksNow(job);}
      catch(e){
        if(!e.engravingPending)throw e;
        job.state="blocked";job.reason=e.message;
        job.row.engrave={...job.row.engrave,state:"blocked",approved:false,reason:e.message};
        Review.add({kind:"placement",key:"eng:"+job.key,row:job.row,job,why:e.message});
        agent({engrave:true},"warn",`${job.row.order.receiptId}: ${e.message}`);render();return false;
      }
    }, ops=window.CharmNestOperations;
    // One engraving's files are written one save at a time; the production lock is held only while each back is
    // recorded on its sheet (recordBack), so a back being built, checked and uploaded never holds up the next charm
    // (Paul, 24 Sep). It used to hold the lock the whole time, and the next intake and sheet waited for every back.
    const task = ops ? ops.run({key:'back-save:'+job.key,label:'Saving engraving',resources:['back:'+job.key]},write) : backQueue.catch(()=>{}).then(write);
    backQueue=task;return task;
  }
  const recordBack = (job, fn) => { const ops = window.CharmNestOperations; return ops ? ops.run({ key: "back-record:" + job.key, label: "Recording engraving", resources: ["production:" + (job.editSheet?.runId || B.run?.runId || job.editSheet?.sheetId || "manual")] }, fn) : fn(); };
  async function writeBacksNow(job) {
    if (!["approved", "written"].includes(job.state) || !hasPlacement(job)) return;
    // counted while it runs, so the upkeep never lets go of a placement a write is using (shelveWritten)
    job._writingBacks = (job._writingBacks || 0) + 1;
    try { if (!job.fit || !job.view) { await loadFonts(); restoreWritten(job); } return await writeBacksOnce(job); }
    finally { job._writingBacks--; }
  }
  async function writeBacksOnce(job) {
    if (!["approved", "written"].includes(job.state) || !job.fit || !job.view) return;
    const approval = job.approvedAt;
    const current = () => approval === job.approvedAt && ["approved", "written"].includes(job.state);

    const charm0 = charmFor(job); if(!charm0) throw Object.assign(new Error("The charm is being moved between sheets; retry saving its engraving after nesting finishes"),{engravingPending:true}); const src = sourceOf(charm0.sourceId); const view = job.view, fit = job.fit;
    const rel = fit.glyphs.map(g => ({ cmds: g.cmds.map(c => { const o = { type: c.type }; if (c.type !== "Z") { o.x = c.x - view.cx; o.y = c.y - view.cy; } if (c.type === "C" || c.type === "Q") { o.x1 = c.x1 - view.cx; o.y1 = c.y1 - view.cy; } if (c.type === "C") { o.x2 = c.x2 - view.cx; o.y2 = c.y2 - view.cy; } return o; }) }));
    const bySheet = new Map();
    for (const poolId of job.copies) { const sh = sheetFor(job,poolId); if (!sh) continue; if (!bySheet.has(sh)) bySheet.set(sh, []); bySheet.get(sh).push(poolId); }
    if (!bySheet.size) throw Object.assign(new Error("no sheet holds these pieces yet"),{engravingPending:true});
    const png = renderBack(job, 500, { grid: false, hatch: false }); const pngBlob = await new Promise(r => png.toBlob(r, "image/png"));
    job.backs = [];
    // the cards and back lists are drawn again once, after the copies are recorded (or one fails), not once a copy
    let shown = false;
    try {
    for (const [sh, poolIds] of bySheet) {
      sh.backPool = sh.backPool || [];
      for (const poolId of poolIds) {
        const p = B.pool.rows.get(poolId) || {}; const copy = job.editingBack ? job.editOriginal.copy || 1 : p.copy || 1;
        const built = await P.buildBackFile({ charm: charm0, parsed: src.parsed, cutMembers: view.cutMembers, cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 * PT, glyphs: rel, view: S.settings.backFileView || "asSeenFromBack", title: `${job.row.order.receiptId} · ${job.row.spec.designSku} · back`, meta: { poolId, order: job.row.order.receiptId, sku: job.row.spec.designSku, copy, text: job.text, font: "Source Sans 3", weight: fit.weight, sizePt: fit.size, capMm: fit.capMm, lineGap:fitOpts(job).lineGap, angle: fit.angle, approvedBy: job.approvedBy, approvedAt: job.approvedAt, upAngle: view.upAngle, flipChecks: view.checks } });
        const verified = await verifyBackFile(built.bytes, job);                 // 7.4 · flip integrity re-run on the written, re-parsed file
        if (!verified.ok) throw Object.assign(new Error(`the written back file did not re-verify (${verified.why})`),{engravingPending:true});
        const name = `${sh.fileBase}_back_${poolId}_${approval}`;
        let ai = null, pngUp = null;
        if (S.cloud.ok && sh.folderPath) { ai = await uploadBytes(`${sh.folderPath}/back/${name}.ai`, built.bytes, "application/illustrator", `Saving back ${copy}`); pngUp = await uploadBytes(`${sh.folderPath}/back/${name}.png`, pngBlob, "image/png"); }
        const rec = { lineGap:fitOpts(job).lineGap, lineMode:job.lineMode || "auto", lineInput:job.lineInput || job.lines, materialVersion:2, upAngle:view.upAngle, poolId, sheetId: sh.sheetId, setId: sh.setId || null, runId: sh.runId || null, order: job.row.order.receiptId, transactionId: job.row.line.transactionId, sku: job.row.spec.designSku, copy, text: job.text, lines: job.lines, font: "Source Sans 3", weight: fit.weight, sizePt: +fit.size.toFixed(3), capMm: +fit.capMm.toFixed(3), box: fit.rect ? [fit.rect.x0, fit.rect.y0, fit.rect.x1, fit.rect.y1].map(v => +v.toFixed(2)) : null, centre: fit.centre.map(v => +v.toFixed(2)), angle: fit.angle, small: !!fit.small, thin: !!fit.thin, metrics: fit.metrics, flipChecks: view.checks, flipDetail: view.detail, verified: { geometry: job.verify.geometry, file: verified }, review: job.claude, approvedBy: job.approvedBy, approvedAt: job.approvedAt, nudged: !!job.nudged, decision: job.decision || null, source: job.source, sourceQuote: job.quote, confidence: job.confidence, view: S.settings.backFileView || "asSeenFromBack", reference: built.reference, outputs: { ai: ai && { path: ai.path, url: ai.url }, png: pngUp && { path: pngUp.path, url: pngUp.url } }, name, previewWPt:png._sizePt.w, previewHPt:png._sizePt.h, pageWPt: built.wPt, pageHPt: built.hPt };
        // the sheet's saves write its list of backs too: recording one waits its turn with them, and nothing else does
        const recorded = await recordBack(job, async () => {
          if (!current()) return false;
          if (sheetFor(job,poolId) !== sh) throw Object.assign(new Error("The charm moved during approval. Retry on its current sheet."),{engravingPending:true});
          if (!S.cloud.ok || !ai || !pngUp) throw new Error("Reconnect to save the approved back files");
          await api("charmNestLibrary", { op: "backPut", back: rec, ...(job.editingBack ? {expectedApprovedAt:job.expectedApprovedAt} : {}) });
          for (const page of allSheets()) page.backPool = (page.backPool || []).filter(b => b.poolId !== poolId);
          if (!current()) return false;
          sh.backPool = (sh.backPool || []).filter(b=>b.poolId!==poolId); sh.backPool.push(rec); job.backs.push(rec);
          if(job.editingBack) job.expectedApprovedAt=rec.approvedAt; window.LaserReview?.saved(sh); shown = true;
          return true;
        });
        if (!recorded) return;
        agent({ metal: sh.metal, engrave: true }, "ENGRAVE", `Back file written and re-verified: ${name}.ai (${built.reference.redrawn ? "cut reference redrawn from the exact transformed paths" : "original cut bytes under the mirror matrix"})`);
      }
      if(job.editingBack) {const {sheet:latest}=await api("charmNestLibrary",{op:"getSheet",id:sh.sheetId});sh.backPool=latest.backPool || [];}
      scheduleBackOutputs(sh);
    }
    } finally { if (shown) refreshBacks(); }
    if (!current()) return;
    job.state = "written"; job.row.engrave.state = "written";
    delete job._backPreview; delete job._previewSize; delete job._previewAt;   // the saved picture stands for it from now on
    await Pool.update(job.copies, { engrave: true, engraveApprovedBy: job.approvedBy, ...(job.editingBack ? {} : {state:"engraved"}) });
    if(job.editingBack) {await syncEditedBack(job);toast("Back engraving updated","ok");}
    render();
  }
  /* Paul, 24 Sep: "nothing may accumulate". A written engraving whose every copy has its back saved in the cloud (the .ai
     and its picture) keeps its placement as numbers (writtenFit) and lets go of the back geometry, the mask and the letter
     outlines, in memory and in the checkpoint: the Decided list shows the saved picture and file, and writing its backs
     again (a piece moved to another sheet, a carried line) builds them again from the charm, as they were fitted
     (restoreWritten). A placement this page could not build again exactly is kept whole. */
  const hasPlacement = job => !!(job.fit && job.view) || !!(job.writtenFit && job.approvedAt && job.writtenFit.approvedAt === job.approvedAt);
  const maskKey = m => { let h = 2166136261; for (let i = 0; i < m.bits.length; i++) h = Math.imul(h ^ m.bits[i], 16777619) >>> 0; return [m.w, m.h, m.res, m.ox, m.oy, h].join(":"); };
  function restoreWritten(job) {
    const w = job.writtenFit, again = why => Object.assign(new Error(why), { engravingPending: true });
    if (!w || w.approvedAt !== job.approvedAt) throw again("This engraving's placement is not kept — fit the words again");
    if (!F_.ok) throw new Error("Engraving font is unavailable");
    const charm = charmFor(job); if (!charm) throw again("The charm is being moved between sheets; retry saving its engraving after nesting finishes");
    let view; try { view = G.backView(charm, { res: 6, upAngle: w.upAngle }); } catch (e) { throw again(`This charm's back could not be read again (${e.message}) — fit the words again`); }
    const mask = G.engraveMask(view, { marginMm: w.marginMm, keepOut: charm.backKeepOut || [] });
    // the charm's back is not what the words were approved on (its drawing or keep-out changed): a person fits them again
    if (maskKey(mask) !== w.maskKey) throw again("This charm's back changed since the words were approved — fit the words again");
    const layout = G.layoutLines(w.lines, fontFor(w.weight), w.size, w.lineGap, w.angle || 0, w.centre), geometry = G.verifyInk(layout.cmds, mask);
    if (!geometry.ok) throw again("The approved words no longer fit this charm's back — fit the words again");
    const { approvedAt, upAngle, marginMm, lineGap, filledArtwork, maskKey: k, ...fit } = w;
    job.view = view; job.mask = mask; job.fit = Object.assign(fit, { layout, glyphs: layout.glyphs, cmds: layout.cmds }); job.verify = { geometry, at: Date.now() };
  }
  function shelveWritten() {
    if (!F_.ok) return 0;
    let n = 0;
    for (const job of items().values()) {
      if (job.state !== "written" || job.editingBack || job.backSaving || job.approvalPreparing || job._writingBacks > 0 || isWorking(job) || EG.cardKey === job.key) continue;
      if (!job.fit || !job.view || !job.mask || !job.approvedAt || job._shelveTried === job.approvedAt || !(job.copies || []).length) continue;
      if (!job.copies.every(id => (job.backs || []).some(b => b.poolId === id && b.approvedAt === job.approvedAt && b.outputs?.ai?.url && b.outputs?.png?.url))) continue;
      const fit = job.fit, lines = (fit.lines || job.lines).slice(), lineGap = fitOpts(job).lineGap;
      let same = false;
      try { same = JSON.stringify(G.layoutLines(lines, fontFor(fit.weight), fit.size, lineGap, fit.angle || 0, fit.centre).cmds) === JSON.stringify(fit.cmds); } catch (_) {}
      if (!same) { job._shelveTried = job.approvedAt; continue; }
      const { layout, glyphs, cmds, ...rest } = fit;
      job.writtenFit = Object.assign({}, rest, { lines, approvedAt: job.approvedAt, upAngle: job.view.upAngle, marginMm: job.mask.marginMm, lineGap, filledArtwork: !!job.view.detail?.filledArtwork, maskKey: maskKey(job.mask) });
      job.view = job.mask = job.fit = job.verify = null; delete job.flipError;
      delete job._backPreview; delete job._previewSize; delete job._previewAt; n++;
    }
    return n;
  }
  /** Parse the written back file and compare its cut geometry with the verified view (mirrored back for the front-coordinates variant). */
  async function verifyBackFile(bytes, job) {
    try {
      const parsed = await P.parseSource(bytes, "back.ai");
      // A back file has one explicit cut-reference layer. Do not mistake a
      // rectangular charm containing holes for an artboard around many charms.
      const references=parsed.segments.concat(parsed.nested).filter(m=>m.kind==="path" && m.closed && (m.stroke || m.fill) && /CUT OUTLINE/.test(m.layer || ""));
      const larger=(a,b)=>(b.bbox[2]-b.bbox[0])*(b.bbox[3]-b.bbox[1])>(a.bbox[2]-a.bbox[0])*(a.bbox[3]-a.bbox[1])?b:a;
      const g=references.length ? null : await (P.groupCharmsAsync || P.groupCharms)(parsed,{minPt:4});
      if(!references.length && !g.charms.length)return {ok:false,why:"no cut outline found in the written file"};
      const c=references.length ? {outline:references.reduce(larger),members:references} : g.charms.reduce(larger);
      const cut = c.members.filter(m => m === c.outline || G.isCutLine(m));
      const bbW = cut.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const bbV = job.view.members.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const res = 6; const fw = G.makeFrame([0, 0, bbW[2] - bbW[0], bbW[3] - bbW[1]], res, (bbW[2] - bbW[0]) / 2, 1), fv = G.makeFrame([0, 0, bbV[2] - bbV[0], bbV[3] - bbV[1]], res, (bbV[2] - bbV[0]) / 2, 1);
      if (Math.abs(fw.w - fv.w) > 2 || Math.abs(fw.h - fv.h) > 2) return { ok: false, why: `written extent ${fw.w}×${fw.h} px differs from the verified back ${fv.w}×${fv.h} px` };
      const frame=fv,wc=cut.map(m=>G.transformSeg(m,G.translate(-bbW[0],-bbW[1]))),vc=job.view.members.map(m=>G.transformSeg(m,G.translate(-bbV[0],-bbV[1])));
      const outlineIndex=job.view.outline ? job.view.members.indexOf(job.view.outline) : job.view.members.reduce((best,m,i,all)=>(m.bbox[2]-m.bbox[0])*(m.bbox[3]-m.bbox[1])>(all[best].bbox[2]-all[best].bbox[0])*(all[best].bbox[3]-all[best].bbox[1])?i:best,0);
      const W=G.materialMask(wc,wc[cut.indexOf(c.outline)],frame),V=G.materialMask(vc,vc[outlineIndex],frame);
      const front = (S.settings.backFileView || "asSeenFromBack") === "frontCoordinates";
      const diff = G.diffFraction(W, front ? G.flipX(V) : V);
      const textPaths=parsed.segments.concat(parsed.nested).filter(s=>s.kind==="path" && s.fill && !s.stroke && !/CUT OUTLINE/.test(s.layer || "")),textOk=textPaths.length>=1;
      const textCmds=textPaths.flatMap(s=>G.flatten(G.transformSeg(s,G.translate(-bbW[0],-bbW[1])),16).flatMap(poly=>poly.map((p,i)=>({type:i?'L':'M',x:p[0],y:p[1]})).concat({type:'Z'})));
      const ink=textOk && G.verifyInk(textCmds,G.engraveMask({mask:W},{marginMm:job.mask?.marginMm || 0}));
      return {ok:diff<=.01 && textOk && ink.ok,why:diff>.01?`cut geometry differs by ${(diff*100).toFixed(2)}%`:!textOk?'no engraving paths in the file':!ink.ok?'engraving intersects a cut-out or cut-edge clearance':null,pixelDiff:diff,textPaths:textOk,ink};
    } catch (e) { return { ok: false, why: e.message }; }
  }
  /** back/back-index.pdf and back/back-report.json for a sheet: a summary for people, drawn once after a run of
   *  approvals has settled, in the background worker, after the approval's save has let go of the run. It used to be
   *  drawn again inside every approval, on the page's own thread (0.1 to 0.6 s, longer with every back on the sheet),
   *  while the run's nesting and sheet saves waited for the approval to finish. */
  function scheduleBackOutputs(sh) {
    clearTimeout(sh._backIndexTimer);
    sh._backIndexTimer = setTimeout(() => {
      sh._backIndexTimer = null;
      if (sh._backIndexRun) { sh._backIndexAgain = true; return; }
      sh._backIndexRun = sheetBackOutputs(sh).catch(e => agent({ metal: sh.metal }, "warn", `back index: ${e.message}`))
        .finally(() => { sh._backIndexRun = null; if (sh._backIndexAgain) { sh._backIndexAgain = false; scheduleBackOutputs(sh); } });
    }, 2500);
  }
  /** A back index that was still to be drawn when the page was left (a reload within those seconds) is drawn now. */
  function refreshBackIndexes() {
    for (const sh of allSheets()) if (!sh.recalled && (sh.backPool || []).length && (!sh.backOutputs || sh.backOutputs.count !== sh.backPool.length) && !sh._backIndexTimer && !sh._backIndexRun) scheduleBackOutputs(sh);
  }
  async function sheetBackOutputs(sh) {
    if (!S.cloud.ok || !sh.folderPath || !(sh.backPool || []).length) return;
    const backs = sh.backPool.slice(), when = at => at ? new Date(at).toLocaleString() : "";
    const items = await Promise.all(backs.map(async b => ({
      png: b.outputs && b.outputs.png && b.outputs.png.url ? await CharmNestAssets.bytes(b.outputs.png.url).catch(() => null) : null,
      lines: [`${b.order} · ${b.sku} · copy ${b.copy}`, `"${String(b.text).replace(/\n/g, " / ")}"`, `${b.font} ${b.weight} · ${b.sizePt} pt · cap ${b.capMm} mm${b.angle ? ` · ${b.angle}°` : ""}${b.small ? " · SMALL" : ""}`, `approved by ${b.approvedBy || "—"} ${when(b.approvedAt)}`]
    })));
    const pdf = await CharmNestExport.backIndexPdf({ title: sh.fileBase, backs: items });
    const report = backs.map(b => ({ poolId: b.poolId, order: b.order, sku: b.sku, copy: b.copy, text: b.text, font: b.font, weight: b.weight, sizePt: b.sizePt, capMm: b.capMm, box: b.box, centre: b.centre, angle: b.angle, flipChecks: b.flipChecks, verified: b.verified, review: b.review, approvedBy: b.approvedBy, approvedAt: b.approvedAt, file: b.outputs && b.outputs.ai && b.outputs.ai.path }));
    const [idx, rep] = await Promise.all([uploadBytes(`${sh.folderPath}/back/back-index.pdf`, pdf, "application/pdf", "Saving back index"), uploadBytes(`${sh.folderPath}/back/back-report.json`, new TextEncoder().encode(JSON.stringify(report, null, 1)), "application/json")]);
    sh.backOutputs = { index: { path: idx.path, url: idx.url }, report: { path: rep.path, url: rep.url }, count: backs.length };
    // the record is written in turn with the run's own sheet saves, which carry backOutputs too
    if (sh.sheetId) { const put = () => api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, backOutputs: sh.backOutputs } }), ops = window.CharmNestOperations; await (ops ? ops.run({ key: "back-index:" + sh.sheetId, label: "Saving back index", resources: ["production:" + (sh.runId || sh.sheetId)] }, put) : put()).catch(() => {}); }
  }

  /* ── the Engraving tab ── */
  // drafts: words typed on a card and not yet applied, per job. A card is rebuilt whenever its job changes state (an
  // auto-refit, a background render, a tab switch, a reload); the typing it held must come back with it.
  const EG = { tab: null, focus: null, chosen: false, card: null, cardKey: null, reread: 0, q: "", drafts: {} };
  // a draft outlives its card only while its job waits in the list: one left by a job decided, removed or gone was kept,
  // and saved with every checkpoint, for as long as the tab stayed open (Paul, 24 Sep: nothing may accumulate)
  function pruneDrafts() { for (const k of Object.keys(EG.drafts || {})) { const j = items().get(k); if (!j || j.row?.state === "gone" || !queuedJobs([j]).length) delete EG.drafts[k]; } return EG.drafts; }
  function disposeCards() {
    const v = document.getElementById("engraveView");
    v?.querySelectorAll(".rvItem").forEach(c => { c._dispose?.(); clearTimeout(c._wordsTimer); c._ro?.disconnect(); });
    EG.card = null; EG.cardKey = null;
  }
  function renderWorking(v, jobs) {
    const host = v.querySelector("[data-eg-working]"); if (!host) return;
    const n = jobs.filter(isWorking).length;
    host.hidden = !n;
    if(n) {
      if(!host.firstElementChild)host.innerHTML='<span class="egTab working" title="Actively reading or fitting engraving previews"><span class="spin"></span>Working<b></b></span>';
      const count=host.querySelector('b');if(count.textContent!==String(n))count.textContent=String(n);
    }
  }
  const waitingReason = job => ["ready", "fitting"].includes(job.state)
    ? (canFit(job) ? "Preparing this engraving preview…" : "Waiting for this charm’s sheet and outline. Its preview will resume when they are ready.")
    : job.state === "classify" ? "Processing stopped before these words were read. Confirm the inscription below or resume the run." : "the words need a decision";
  const matchesQ = j => { const q = (EG.q || "").trim().toLowerCase(); if (!q) return true; return [j.row.order.receiptId, j.row.spec && j.row.spec.designSku, j.row.line.sku, j.text, (j.lines || []).join(" "), j.row.line.title].some(x => String(x || "").toLowerCase().includes(q)); };
  /** Which run's engraving this is. A tab that showed a queue and no run left nobody able to say whose queue it was. */
  function runWord() {
    // a recalled set names itself, even while a finished run is still remembered underneath
    const rc = B.orders.recalled && window.Recall && Recall.on() ? Object.assign({ runId: B.orders.recalled.runId || "" }, B.orders.recalled) : null;
    const r = rc || B.run || (B.orders.recalled ? Object.assign({ runId: B.orders.recalled.runId || "" }, B.orders.recalled) : null); if (!r) return "no run — earlier runs…";
    return (r.seq ? `Set ${r.seq}` : r.runId.slice(-8)) + (r.day ? " · " + new Date(r.day + "T12:00:00").toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "");
  }
  /** An empty queue has four different causes, and only one of them means "you are done". Say which one it is. */
  function emptyWhy(jobs, nWords, nDone) {
    const r = B.run;
    const line = (t, btn) => `<div class="libEmpty"><b>${t}</b>${btn || ""}</div>`;
    if (!r && window.Recall && Recall.on()) return nDone
      ? line(`Everything engraved in this set is under Decided (${nDone}). Nothing else in it was engraved.`, `<button class="btn ghost sm" data-go="done">Open Decided</button>`)
      : line("Nothing in this set was engraved.", `<button class="btn ghost sm" data-go="hist">Another set…</button>`);
    if (!r && !jobs.length) return line("No run is open, so there is nothing to engrave yet.", `<button class="btn gold sm" data-go="hist">Earlier sets…</button>`);
    if (r && r.status === "stopped") return line(`This run stopped before the engraving step${r.stoppedBy ? ` — ${esc(r.stoppedBy)}` : ""}.`, `<button class="btn ghost sm" data-go="orders">Back to the run</button>`);
    if (r && O.stepIndex(r.step) < O.stepIndex("engrave")) return line(`Nothing to engrave yet — this run is still at ${esc(STEP_WORDS_EG[r.step] || r.step)}.`, `<button class="btn ghost sm" data-go="orders">Back to the run</button>`);
    const rv = Review.count();
    if (rv) return line(`${rv} line${rv === 1 ? " needs" : "s need"} a decision before anything can be engraved.`, `<button class="btn gold sm" data-go="review">Open Review</button>`);
    if (nDone) return line("Every placement in this run is decided.", `<button class="btn ghost sm" data-go="hist">Another run…</button>`);
    return line("Nothing in this run needs engraving.", `<button class="btn ghost sm" data-go="hist">Another run…</button>`);
  }
  const STEP_WORDS_EG = { pull: "pulling orders", claim: "claiming", pool: "pooling", plan: "planning", nest: "nesting", checkpoint: "checking the sheets", labels: "writing labels", commit: "committing" };
  /** Bring the counts and the up-next rail up to date without touching the card a person is working on. */
  function renderChrome(v, queue) {
    const jobs = [...items().values()].filter(j => j.row.state !== "gone").sort((a, b) => (b.row.arrivedAt || 0) - (a.row.arrivedAt || 0) || (+b.row.order.createTs || 0) - (+a.row.order.createTs || 0));
    renderWorking(v, jobs);
    const n = { place: queue.length, done: decidedJobs().length };
    v.querySelectorAll(".egTab[data-tab]").forEach(b => {
      const k = n[b.dataset.tab]; let t = b.querySelector("b");
      if (!k) { if (t) t.remove(); return; }
      if (!t) { t = el("b", b.dataset.tab === "words" ? "warn" : b.dataset.tab === "place" ? "info" : "ok"); b.appendChild(t); }
      t.textContent = String(k);
    });
    const rail = v.querySelector("#egNext"); if (!rail) return;
    const rest = queue.filter(j2 => j2.key !== EG.cardKey);
    rail.innerHTML = "";
    rail.querySelectorAll(".egChip").forEach(b => b.onclick = () => { EG.focus = b.dataset.key; render(); });
    const card = EG.card; if (card) { const kind = card.querySelector(".rh .kind"); if (kind) kind.textContent = `${decidedJobs().length + 1} of ${decidedJobs().length + queue.length} · ${decidedJobs().length} done`; }
  }
  // Keyed rows keep decoded thumbnails, focus and scroll position while each
  // classifier/worker result arrives. Navigation away also retains the cache.
  const placementRows = new WeakMap();
  let placementLimit=40, placementQuery=null, doneLimit=40, doneQuery=null;
  function renderPlacementRows(list, queue) {
    const wanted = new Set();
    if(placementQuery!==EG.q){placementLimit=40;placementQuery=EG.q;}
    queue.slice(0,placementLimit).forEach((job,index) => {
      let row = placementRows.get(job);
      if (!row) {
        row = el("div", "doneRow placementRow hoverItem");
        row.setAttribute("role","button"); row.tabIndex=0; row.dataset.open=job.key;
        row.innerHTML=ListMedia.pair(job.row)+'<div class="engravingIdentity"><span class="queueLabel">Engraving</span><div class="engravingOrder"><b class="mono" data-order></b><span class="sku mono"></span></div><span class="purchaseLabel">Engraving</span><span class="w"></span><span class="dim" data-stage></span></div><div class="purchaseSummary" data-purchase></div>';
        const open=()=>{if(isWorking(job))return;EG.focus=job.key;EG.list=false;render();};
        row.onclick=e=>{if(!e.target.closest('[data-vector][role=button],[data-listing][role=button]'))open();}; row.onkeydown=e=>{if(e.target===row && (e.key==="Enter" || e.key===" ")){e.preventDefault();open();}};
        placementRows.set(job,row);
      }
      wanted.add(row);
      const busy=isWorking(job), receipt=String(job.row.order.receiptId), sku=job.row.spec.designSku || "";
      row.dataset.rid=receipt; row.setAttribute("aria-busy",String(busy)); row.setAttribute("aria-disabled",String(busy));
      row.setAttribute("aria-label",`${busy ? "Preparing" : "Open"} engraving for order ${receipt} · ${sku}`);
      const write=(selector,value)=>{const node=row.querySelector(selector);if(node.textContent!==value)node.textContent=value;};
      write('[data-order]',receipt);write('.sku',sku);write('.w',(job.lines || []).join(' / '));
      const purchase=purchaseMarkup(job.row);
      if(row._purchase!==purchase){row.querySelector('[data-purchase]').innerHTML=purchase;row._purchase=purchase;}
      write('[data-stage]',busy ? (job.state === "classify" ? "Reading words…" : "Preparing preview…") : "");
      if(list.children[index] !== row) list.insertBefore(row,list.children[index] || null);
      ListMedia.mount(row,job.row);
    });
    [...list.children].forEach(row=>{if(!wanted.has(row))row.remove();});
    ListMedia.more(list,queue.length,Math.min(placementLimit,queue.length),()=>{placementLimit+=40;render();});
  }
  /** Repaint the card in place: the picture, the numbers, the chips. The pane is only rebuilt when what it holds changes. */
  function refresh(job) {
    const c = EG.card;
    if (!c || !c.isConnected || EG.cardKey !== job.key) { render(); return; }
    const f = job.fit; if (!f) { render(); return; }
    const bc = c.querySelector(".backHost canvas"); if (bc && bc._paint) bc._paint();
    const cap = c.querySelector("[data-cap]"); if (cap) cap.textContent = f.capMm.toFixed(2) + " mm";
    const an = c.querySelector('input[data-a="angle"]'); if (an && document.activeElement !== an) an.value = String(Math.round(f.angle || 0));
    const sl = c.querySelector('input[data-a="resize"]');
    if (sl && document.activeElement !== sl) { sl.max = f.fittedMax.toFixed(2); sl.min = (0.5 * f.fittedMax).toFixed(2); sl.value = f.size.toFixed(2); }
    else if (sl) { sl.max = f.fittedMax.toFixed(2); sl.min = (0.5 * f.fittedMax).toFixed(2); }
    const spacing=c.querySelector('[data-a="spacing"]');
    // line spacing means something only with two lines or more: it shows when a refit wraps the words, and hides again
    if(spacing){const pct=Math.round(fitOpts(job).lineGap/.18*100);spacing.value=pct;c.querySelector('[data-spacing]').textContent=pct+'%';const group=spacing.closest('.spacingControl');group.style.setProperty('--spacing',pct/100);group.hidden=(job.lines || []).length<2;}
    const nums = c.querySelector(".pvNums dd"); if (nums) nums.textContent = `${f.size.toFixed(2)} pt · cap ${f.capMm.toFixed(2)} mm · ${f.weight}${f.angle ? ` · ${f.angle}°` : ""}`;
    const rh = c.querySelector(".reviewIdentity") || c.querySelector(".rh"); if (rh) {
      rh.querySelectorAll(".small").forEach(n => n.remove());
      if (f.small) rh.insertAdjacentHTML("beforeend", `<span class="small" title="the cap height is under the engraver minimum in Settings">SMALL · cap ${f.capMm.toFixed(2)} mm</span>`);
      if (f.thin) rh.insertAdjacentHTML("beforeend", `<span class="small" title="the thinnest stroke is under the engraver limit">THIN STROKES</span>`);
    }
    const cl = c.querySelector(".claude"); if (cl) cl.remove();
    LiveStrip.render();
  }
  /** What Claude last said about the rendered back — and, once it has been moved, that nothing has looked at it since. */
  function claudeBlock(job) {
    if (job.claude) return `<div class="claude">${job.claude.skipped ? `Claude could not look at the render: ${esc(job.claude.skipped)}` : `<b>${job.claude.legible ? "Reads clearly" : "Hard to read"}</b> — ${esc(job.claude.notes)}`}</div>`;
    if (job.rereading) return `<div class="claude" style="opacity:.6">Claude is looking at the rendered back…</div>`;
    return `<div class="claude" style="opacity:.75">Nothing has looked at this since you moved it.</div>`;
  }
  /** A placement that has been moved is exactly the one worth looking at again, so it is looked at again. */
  function reRead(job) {
    clearTimeout(EG.reread);
    EG.reread = setTimeout(() => {
      if (!job.fit || job.state !== "review") return;
      job.rereading = true; refresh(job);
      claudeRead(job).catch(() => {}).then(() => { job.rereading = false; refresh(job); });
    }, 700);
  }                                   // which tab is open and which placement is in front
  let rendering = false;
  function render() {
    if(window.CharmNestInteraction?.defer('engraving-view',render))return;
    if (rendering) return;
    rendering = true;
    try { renderView(); window.LaserReview?.changed(); }
    catch (error) {
      // A failed preview is local to this tab. In particular, do not let it
      // reject Session.restore() and prevent the rest of startup from finishing.
      const v = document.getElementById("engraveView");
      if (v && !v.classList.contains("hidden")) {
        disposeCards();
        v.innerHTML = `<div class="libEmpty" role="alert"><b>This engraving preview could not open.</b><p>Your sheets and decisions are kept.</p><p>${esc(error.message)}</p><button class="btn ghost sm" data-eg-retry>Retry preview</button><button class="btn ghost sm" data-eg-list>Placements</button></div>`;
        v.querySelector("[data-eg-retry]").onclick = render;
        v.querySelector("[data-eg-list]").onclick = () => { EG.list = true; EG.tab = "place"; EG.chosen = true; render(); };
      }
      console.error("Engrave preview could not render", error);
    } finally { rendering = false; }
  }
  function renderView() {
    LiveStrip.render();
    const v = document.getElementById("engraveView");
    if (!previewRecovery && previewRecoveryTimer === null && [...items().values()].some(needsPreview))
      previewRecoveryTimer = setTimeout(() => {
        previewRecoveryTimer = null;
        void prepareWaitingPreviews().catch(error => console.error("Engraving preview recovery failed", error));
      }, 32);
    if (!v || v.classList.contains("hidden")) return;
    // a background fit finishing must not tear down the card someone is judging: when nothing about what this pane holds
    // has changed, only the counts and the rail are brought up to date
    if (EG.card && EG.card.isConnected && EG.card.dataset.state === "review" && !EG.card._previewFailed && EG.tab === "place") {
      const q2 = queuedJobs([...items().values()].filter(j => j.row.state !== "gone")).filter(matchesQ);
      const f2 = q2.find(j => j.key === EG.focus) || q2[0];
      if (f2 && f2.key === EG.cardKey) { renderChrome(v, q2); return; }
    }
    const jobs = [...items().values()].filter(j => j.row.state !== "gone").sort((a, b) => (b.row.arrivedAt || 0) - (a.row.arrivedAt || 0) || (+b.row.order.createTs || 0) - (+a.row.order.createTs || 0));
    // the words to settle and the placements to approve are one queue, one card each: the card carries the words as an
    // editable field, so nothing needs a second tab
    const words = jobs.filter(matchesQ).filter(j => j.state === "words" || j.state === "blocked"), queue = queuedJobs(jobs.filter(matchesQ)), done = jobs.filter(matchesQ).filter(j => ["approved", "written", "skipped"].includes(j.state));
    // One screen, three tabs, one thing in front of you at a time: the words a person has to settle, the placements to
    // approve, and what has already been decided. The counts are the tabs, so what is left is never more than a glance.
    // Until a person picks a tab, the screen follows the work: it used to settle on Decided while the run was still
    // classifying and then stay there as placements arrived behind it, so someone watching this tab saw an empty pane
    // and no sign that anything was waiting. Once a tab is picked by hand it stays picked, empty or not.
    if (!EG.tab || !EG.chosen || EG.tab === "words") EG.tab = queue.length ? "place" : "done";
    if (EG.list == null && jobs.some(isWorking)) EG.list = true;
    const tab = EG.tab;
    const focus = queue.find(j2 => j2.key === EG.focus) || queue[0] || null;
    if(doneQuery!==EG.q){doneLimit=40;doneQuery=EG.q;}
    const doneStamp=tab === "done" ? JSON.stringify([EG.q,EG.openDone,doneLimit,done.map(j=>[j.key,j.state,j.lines,j.approvedAt,j.approvedBy,j.backs,j.backPending,O.purchaseDetails(j.row.line,j.row.spec)])]) : null;
    if(tab === "done" && v.dataset.egTab === "done" && v._doneStamp === doneStamp && v.querySelector('#egBacks')) {renderChrome(v,queue);return;}
    v._doneStamp=doneStamp;
    const liveList = v.querySelector('.egPlacementList');
    if(tab === "place" && EG.list && liveList && v.dataset.egTab === "place") {
      renderChrome(v,queue);renderPlacementRows(liveList,queue);return;
    }
    const oldDoneScroll=v.dataset.egTab===tab ? (v.querySelector(".egPane.scroll")?.scrollTop || 0) : 0;
    v.dataset.egTab=tab;
    const tabBtn = (id, label, n, cls) => `<button class="egTab${tab === id ? " on" : ""}" data-tab="${id}" title="${esc(label)}">${label}${n ? `<b class="${cls}">${n}</b>` : ""}</button>`;
    // the words field someone is typing in keeps its focus and caret through the rebuild (its text is kept as a draft)
    const typing = (() => { const a = document.activeElement, c = a && v.contains(a) && a.matches('[data-f="words"]') && a.closest(".rvItem"); return c ? { key: c.dataset.key, start: a.selectionStart, end: a.selectionEnd } : null; })();
    // so does the search box: a rebuild used to drop its focus, the new card took it, and the next letters typed into the
    // search were read as the card's shortcuts (A approves, S skips)
    const searching = (() => { const a = document.activeElement; return a && a.id === "egQ" && v.contains(a) ? { start: a.selectionStart, end: a.selectionEnd } : null; })();
    disposeCards();
    v.innerHTML = `<div class="ordBar egBar">
        <span class="controlGroup">${tabBtn("place", "Placements", queue.length, "info")}${tabBtn("done", "Decided", done.length, "ok")}
        <span data-eg-working hidden></span>
        </span><span class="controlGroup"><input class="ordSearch" id="egQ" placeholder="order, SKU, words…" value="${esc(EG.q || "")}" title="search the placements, the words and what has been decided by order number, SKU or the engraved words"></span>
        </div>
      <div class="egPane grow"${tab === "place" ? "" : " hidden"}><div class="rvList" id="egQueue"></div>
        <div class="egNext" id="egNext"></div></div>
      <div class="egPane grow scroll"${tab === "done" ? "" : " hidden"}><div id="egBacks"></div></div>`;
    renderWorking(v, jobs);
    v.querySelectorAll(".egTab[data-tab]").forEach(b => b.onclick = () => { EG.tab = b.dataset.tab; EG.chosen = true; render(); });
    { const q = v.querySelector("#egQ"); q.oninput = () => { EG.q = q.value; render(); }; if (searching) { q.focus({ preventScroll: true }); q.setSelectionRange(searching.start, searching.end); } }
    if (tab === "place") {
      const q = v.querySelector("#egQueue");
      if (EG.list) {
        EG.card = null; EG.cardKey = null;
        q.innerHTML = '<div class="rvList egPlacementList"></div>';
        renderPlacementRows(q.firstElementChild,queue);
      }
      else if (focus) {
        const c = placementCard(focus, queue.length); c.classList.add("full"); q.appendChild(c); EG.card = c; EG.cardKey = focus.key;
        const ta = typing && typing.key === focus.key && c.querySelector('[data-f="words"]');
        if (ta) { const n = ta.value.length; ta.focus({ preventScroll: true }); ta.setSelectionRange(Math.min(typing.start, n), Math.min(typing.end, n)); }
      }
      else { EG.card = null; EG.cardKey = null; q.innerHTML = emptyWhy(jobs, words.length, done.length); q.querySelectorAll("[data-go]").forEach(b => b.onclick = () => { const g = b.dataset.go; if (g === "hist") RunHistory.show(); else if (g === "words") { EG.tab = "words"; EG.chosen = true; render(); } else { setMode(g); if (g === "review") Review.render(); } }); }
      // what is coming: the order and the words, so the list and the picture are the same thing
      const rail = v.querySelector("#egNext");
      const rest = queue.filter(j2 => j2 !== focus);
    rail.innerHTML = "";
      rail.querySelectorAll(".egChip").forEach(b => b.onclick = () => { EG.focus = b.dataset.key; render(); });
    }
    if (tab === "done") {
      const bk = v.querySelector("#egBacks");
      const decided = decidedJobs().filter(matchesQ).sort((a, b) => (b.row.arrivedAt || 0) - (a.row.arrivedAt || 0) || (b.approvedAt || 0) - (a.approvedAt || 0));
      const stateWord = j2 => j2.state === "written" ? "Saved to sheet" : j2.state === "skipped" ? "No engraving" : j2.backPending ? "Approved · back file waits for the cloud" : "Approved";
      const stateWhy = j2 => j2.state === "written" ? "the back file is saved with the sheet" : j2.state === "skipped" ? "cut plain, nothing on the back" : j2.backPending ? "approved — the back file is written when the cloud answers again (" + esc(j2.backPending) + ")" : "approved — the back file is written when the sheet is";
      const fmtT = t => t ? new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "";
      /* One row per decision; the row opens into everything there is to know about it — the back as it was written,
         the front it belongs to, the words, the size, who decided and when, which sheet, the file — and Reopen. The
         separate "back files written" grid said the same things a second time, smaller, and is gone. */
      bk.innerHTML = decided.length
        ? `<div class="section" style="margin-top:2px">Decided · ${decided.length}</div><div class="rvList" id="egDone">` + decided.slice(0,doneLimit).map(j2 => {
            const w = j2.state === "skipped" ? "cut plain" : esc(j2.lines.join(" / "));
            const who = j2.approvedBy || (j2.decision && j2.decision.by) || "";
            const b0 = (j2.backs || [])[0] || {}; const png = b0.png || (b0.outputs && b0.outputs.png && b0.outputs.png.url) || ""; const ai = b0.ai || (b0.outputs && b0.outputs.ai && b0.outputs.ai.url) || "";
            const open = EG.openDone === j2.key;
            const detail = !open ? "" : `<div class="doneDetail">
                <div class="dd back">${png ? `<img crossorigin="anonymous" src="${esc(cors(png))}" alt="the back as written" referrerpolicy="no-referrer" data-retry="1">` : `<div class="noPic">${j2.state === "skipped" ? "cut plain — nothing on the back" : "the back picture is written with the sheet"}</div>`}<span class="cap">back${b0.sheet ? " · " + esc(b0.sheet) : ""}</span></div>
                <div class="dd front">${j2.view?.detail?.filledArtwork || j2.writtenFit?.filledArtwork ? `<div class="why">Filled artwork: inspect the back outline and cut-outs before approving.</div>` : ""}<div class="frontHost"></div><span class="cap">front</span></div>
                <dl class="meta">
                  <dt>Words</dt><dd class="serif">${w}</dd>
                  ${j2.fit || j2.writtenFit || b0.capMm ? `<dt>Size</dt><dd>cap ${(+(b0.capMm || (j2.fit || j2.writtenFit || {}).capMm || 0)).toFixed(2)} mm · Source Sans 3${(j2.fit || j2.writtenFit || b0).weight ? " " + esc((j2.fit || j2.writtenFit || b0).weight) : ""}</dd>` : ""}
                  <dt>Decided</dt><dd>${esc(who || "—")}${j2.approvedAt ? " · " + esc(new Date(j2.approvedAt).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })) : ""}${j2.decision && j2.decision.why ? " · " + esc(j2.decision.why) : ""}</dd>
                  ${(j2.backs || []).length ? `<dt>File</dt><dd>${(j2.backs || []).map(b => { const u = b.ai || (b.outputs && b.outputs.ai && b.outputs.ai.url); return u ? `<a href="${esc(u)}" target="_blank" rel="noopener" title="the back file, as it went to the laser">${esc((b.name || "back") + ".ai")}</a>` : "not saved to the cloud"; }).join(" · ")}</dd>` : ""}
                  ${j2.copies && j2.copies.length ? `<dt>Pieces</dt><dd>${j2.copies.length} on ${[...new Set(j2.copies.map(pid => (Pool.sheetOf(pid) || {}).fileBase).filter(Boolean))].map(esc).join(", ") || "the sheet"}</dd>` : ""}
                </dl>
                <div class="ctl">${j2.recalledFrom ? `<span class="hint">this set is recalled — reopening rebuilds its sheet from the master files first</span>` : ""}</div>
              </div>`;
            return `<div class="doneRow decidedRow hoverItem${open ? " open" : ""}" tabindex="0" aria-expanded="${open}" data-rid="${esc(j2.row.order.receiptId)}" data-key="${esc(j2.key)}" title="View engraving details">${ListMedia.pair(j2.row)}<div class="engravingIdentity"><span class="queueLabel">Engraving · decided</span><div class="engravingOrder"><b class="mono">${esc(j2.row.order.receiptId)}</b><span class="sku mono">${esc(j2.row.spec.designSku || "")}</span></div><span class="purchaseLabel">Engraving</span><span class="w">${w}</span></div><div class="purchaseSummary">${purchaseMarkup(j2.row)}</div><div class="decisionActions"><div class="decisionStatus"><span class="ost ${j2.state === "skipped" ? "warn" : "ok"}" title="${stateWhy(j2)}">${stateWord(j2)}</span><span class="by">${esc(who || "Decision recorded")}${j2.approvedAt ? " · " + fmtT(j2.approvedAt) : ""}</span></div><button class="btn ghost sm" data-a="reopen" title="Reopen this engraving for changes">Reopen</button></div>${detail}</div>`;
          }).join("") + `</div>`
        : `<div class="libEmpty">${window.Recall && Recall.on() ? "Nothing in this set was engraved." : "nothing decided yet"}</div>`;
      bk.querySelectorAll(".doneRow").forEach(rw => {
        const toggle=()=>{EG.openDone=EG.openDone===rw.dataset.key?null:rw.dataset.key;render();};
        rw.addEventListener("click",e=>{if(!e.target.closest("button, a, [role=button], .doneDetail"))toggle();});
        rw.addEventListener("keydown",e=>{if(e.target===rw && (e.key==="Enter" || e.key===" ")){e.preventDefault();toggle();}});
        ListMedia.mount(rw,items().get(rw.dataset.key)?.row);
      });
      bk.closest(".egPane.scroll").scrollTop=oldDoneScroll;
      const doneHost=bk.querySelector("#egDone");if(doneHost)ListMedia.more(doneHost,decided.length,Math.min(doneLimit,decided.length),()=>{doneLimit+=40;render();});
      { const rw = bk.querySelector(".doneRow.open"); const j2 = rw && items().get(rw.dataset.key); const host = rw && rw.querySelector(".frontHost");
        if (j2 && host) void mountPlacementThumbnail(host,j2); }

      // a picture that will not load is retried once with a fresh request, then says so instead of a broken icon
      bk.querySelectorAll("img").forEach(im => im.addEventListener("error", () => { if (im.dataset.retry) { im.dataset.retry = ""; im.src = im.src.replace(/([?&])_r=\d+/, "$1").replace(/[?&]$/, "") + (im.src.includes("?") ? "&" : "?") + "_r=" + Date.now(); return; } const d = document.createElement("div"); d.className = im.classList.contains("mini") ? "mini" : "noPic"; d.textContent = im.classList.contains("mini") ? "" : "the picture did not load — the .ai file is still there"; im.replaceWith(d); }));
      bk.querySelectorAll("[data-a=reopen]").forEach(b => b.onclick = () => {
        const j2 = items().get(b.closest(".doneRow").dataset.key); if (!j2) return;
        const who = employeeName() || askEmployee(); if (!who) return;
        if (!confirm(`Reopen ${j2.row.order.receiptId}? It goes back to the words step, and any back file already written for it is superseded.`)) return;
        const go = () => { sendBack(j2, `reopened by ${who}`); EG.tab = "place"; EG.focus = j2.key; EG.list = false; EG.chosen = true; render(); };
        // a recalled set's sheet is rebuilt from the master files first: the button says so while it runs (it used to
        // sit there as if nothing had been pressed)
        if (j2.recalledFrom && j2.recalledFrom.recalled) { if (b.disabled) return; b.disabled = true; b.textContent = "Rebuilding the sheet…"; Recall.rebuild(j2.recalledFrom).then(() => { const pool = B.pool.rows; for (const [pid, p] of pool) if (String(p.orderId) === String(j2.row.order.receiptId) && (p.sku === (j2.row.spec && j2.row.spec.designSku) || p.sku === j2.row.line.sku) && !j2.row.poolIds.includes(pid)) j2.row.poolIds.push(pid); go(); }).catch(e => { if (b.isConnected) { b.disabled = false; b.textContent = "Reopen"; } toast(`Could not rebuild the sheet: ${e.message}`, "bad", 7000); }); return; }
        go();
      });
    }
  }
  /** The placement review card: front and back side by side, the mask hatch, the text as it will be cut, the controls. */
  function placementCard(job, remaining) {
    const it = Review.items().find(i => i.key === "eng:" + job.key) || { kind: "placement", key: "eng:" + job.key, row: job.row, job };
    const card = el("div", "rvItem"); card.dataset.kind = "placement"; card.dataset.state = job.state; card.dataset.key = job.key; card.tabIndex = 0;
    const r = job.row, sp = r.spec, f = job.fit;
    const decided = decidedJobs().length;
    // One card, one order, one screen: the back is the work and the right column is everything you need to judge it.
    const pct = Math.round((job.confidence != null ? job.confidence : 0) * 100);
    const conf = job.source ? `<span class="conf ${pct >= 80 ? "" : pct >= 60 ? "mid" : "low"}" title="how sure Claude is that these are the words to cut, read from ${esc(SOURCE_LABEL[job.source] || job.source)}${job.quote ? ` — “${esc(job.quote)}”` : ""}">${pct}% sure</span>` : "";
    const row2 = (t, v) => v && v !== "—" ? `<dt>${t}</dt><dd>${esc(v)}</dd>` : "";
    const wordsJob = job.state !== "review";
    const requests = job.requests || {};
    const reviewNotes = (job.questions || []).filter(q => !/not engravable|cannot (?:be |take )engrav|design.*engrav/i.test(q));
    if (requests.side && !["back", "unspecified"].includes(requests.side)) reviewNotes.push(`Requested side: ${requests.side}`);
    if (requests.font) reviewNotes.push(`Requested font: ${requests.font}`);
    if (requests.handwriting) reviewNotes.push("Customer requested handwriting");
    if (requests.image) reviewNotes.push("Customer requested an image");
    card.innerHTML = `<div class="rh"><div class="reviewProgress"><span class="kind" title="this placement's place in the queue · how many are decided">${decided + 1} of ${decided + remaining} · ${decided} done</span><span class="nav"><button class="btn ghost xs" data-a="prev" title="the previous placement in the queue">‹ Back</button><button class="btn ghost xs" data-a="next" title="the next placement in the queue">Next ›</button></span></div><div class="reviewIdentity"><span class="ttl">${esc(r.order.receiptId)}</span><span class="sub">${esc(sp.designSku)}${sp.form ? " · " + esc(sp.form) : ""}${sp.size ? " · " + esc(sp.size) : ""}${job.copies.length > 1 ? ` · ${job.copies.length} copies` : ""}</span>${conf}${f && f.small ? `<span class="small" title="the cap height is under the engraver minimum in Settings">SMALL · cap ${f.capMm.toFixed(2)} mm</span>` : ""}${f && f.thin ? `<span class="small" title="the thinnest stroke is under the engraver limit">THIN STROKES</span>` : ""}</div><button class="x" data-a="close" title="back to the list of placements" aria-label="close">×</button></div>
      <div class="placeView">
        <div class="pvMain"><div class="backHost"></div>
          <div class="ctl">${f && !wordsJob ? `<button class="btn sage sm" data-a="approve" title="this placement is right — write the back file">${job.editingBack ? "Save changes" : "Approve"} <b class="k">A</b></button><button class="btn ghost sm" data-a="centre" title="put the text in the middle of the metal it may use">Centre</button><label class="lineControl">Lines <select data-a="linecount" aria-label="Engraving line count">${["auto","preserve",1,2,3,4,5,6].map(n=>`<option value="${n}" ${String(job.lineMode || "auto")===String(n)?"selected":""}>${n==="auto"?"Auto":n==="preserve"?"As typed":n}</option>`).join("")}</select></label><span class="mono dim" data-cap title="cap height of the lettering">${f.capMm.toFixed(2)} mm</span><label class="spacingControl" ${(job.lines || []).length > 1 ? "" : "hidden"} title="Scroll here to change line spacing; Shift scroll for fine adjustment. 100% is the original gap."><span class="spacingIcon" aria-hidden="true"><i></i><i></i><i></i></span><span>Line spacing</span><input type="range" data-a="spacing" aria-label="Line spacing" min="0" max="300" step="1" value="${Math.round(fitOpts(job).lineGap/.18*100)}"><output data-spacing>${Math.round(fitOpts(job).lineGap/.18*100)}%</output></label><span class="quarterTurns" role="group" aria-label="Rotate text"><button class="btn ghost sm" data-a="turnLeft" title="Rotate text 90° counterclockwise">↶ +90°</button><button class="btn ghost sm" data-a="turnRight" title="Rotate text 90° clockwise">↷ −90°</button></span><label class="angle" title="the angle of the text, in degrees — type one, or drag the handle above the text"><input type="number" data-a="angle" min="-359" max="359" step="1" value="${Math.round(f.angle || 0)}">°</label>` : ""}
            <span class="rest"><button class="btn ghost sm" data-a="skip" title="cut this charm plain — nothing engraved on its back">No engraving <b class="k">S</b></button></span></div>
</div>
        <div class="pvSide">
          <div class="pvWords"><span class="lbl">Words on the back</span><textarea data-f="words" rows="${Math.max(1, Math.min(4, (job.lines || []).length || 1))}" title="Line breaks are preserved. The preview updates after typing.">${esc((job.lineInput || job.lines || []).join("\n"))}</textarea>
            <div class="wordsActs"><button class="btn gold xs" data-a="usewords" title="${wordsJob ? "settle the words and draw the placement" : "re-fit the placement with these words"}">${wordsJob ? "Engrave these words" : "Use these words"}</button>${wordsJob ? `<button class="btn ghost xs" data-a="skip" title="cut this charm plain — nothing engraved on its back">No engraving</button>` : ""}</div>
            ${wordsJob || !f ? `<div class="why">${esc(job.reason || waitingReason(job))}${(job.questions || []).length ? ` — ${esc(job.questions.join(" · "))}` : ""}</div>` : ""}</div>
          ${!wordsJob && reviewNotes.length ? `<div class="reviewNotes">${esc([...new Set(reviewNotes)].join(" · "))}</div>` : ""}
          ${job.view?.detail?.filledArtwork ? `<div class="why">Filled artwork: inspect the back outline and cut-outs before approving.</div>` : ""}<div class="frontHost"></div>
          <dl class="meta">${row2("Customer", (sp.personalization || []).join(" / "))}${row2("Buyer msg", sp.buyerMessage)}${row2("Staff note", sp.staffNote)}${job.decision ? `<dt>Decided by</dt><dd>${esc(job.decision.by)}</dd>` : ""}</dl>
        </div></div>`;
    const charm = job.copies.length ? charmFor(job) : null;

    if (charm) card.querySelector(".frontHost").appendChild(renderFront(charm, 420));
    else void mountPlacementThumbnail(card.querySelector(".frontHost"),job);
    // the customer, one question away: the same box follows the placement through every rebuild of this card, so what
    // was typed stays (charm-nest-mail.js)
    try { const mb = window.CustomerMail?.lineBox(job); if (mb) card.querySelector(".pvSide").insertBefore(mb, card.querySelector(".frontHost")); } catch (e) { console.warn("customer mail:", e); }
    if (wordsJob) { const bh = card.querySelector(".backHost"); bh.innerHTML = `<div class="noBack">${esc(job.state === "blocked" ? (job.reason || "This preview needs attention — use the words to retry.") : waitingReason(job))}</div>`; }
    const ta = card.querySelector('[data-f="words"]'), use = card.querySelector('[data-a="usewords"]');
    const applyWords = async (keepFocus = false) => {
      clearTimeout(card._wordsTimer);
      if (!card.isConnected || job.backSaving || job.approvalPreparing) return;
      const text = ta.value.trim(), start = ta.selectionStart, end = ta.selectionEnd;
      const focused = keepFocus && document.activeElement === ta;
      if (!text) { if (!keepFocus) toast("Type the words first", "bad"); return; }
      delete EG.drafts?.[job.key];
      use.disabled = true;
      try {
        if (wordsJob) await decideWords(job, { text, note: text !== (job.text || "").trim() ? "edited" : "confirmed" });
        else {
          job.lineInput = text.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
          job.lines = job.lineInput.slice(); job.text = text; job.edited = true;
          await fitNewWords(job);
        }
      } catch (e) { toast(e.message, "bad"); }
      use.disabled = false; render();
      if (focused && EG.cardKey === job.key) {
        // back to the words, unless the person has moved on to another control meanwhile. When the rebuild already put
        // focus back (the person kept typing through the fit), its caret is the current one: the one from before the fit
        // would jump back into the middle of what was typed since.
        const next = EG.card?.querySelector('[data-f="words"]'), now = document.activeElement, live = now === next;
        if (next && (live || !now || now === document.body)) { next.focus(); next.setSelectionRange(live ? next.selectionStart : start, live ? next.selectionEnd : end); }
      }
    };
    use.onclick = () => applyWords();
    const keepDraft = () => { EG.drafts ||= {}; if (ta.value === ta.defaultValue) delete EG.drafts[job.key]; else EG.drafts[job.key] = { value: ta.value, base: ta.defaultValue }; };
    ta.addEventListener("input", () => {
      keepDraft();
      clearTimeout(card._wordsTimer);
      card.querySelectorAll('[data-a="approve"]').forEach(b => { b.disabled = true; });
      if (!wordsJob && ta.value.trim()) card._wordsTimer = setTimeout(() => applyWords(true), 350);
    });
    // Words typed on an earlier build of this card and not yet applied come back, and carry on as if typing continued.
    // If the job's own words changed underneath (the order changed on Etsy), the new words win and the person is told.
    { const draft = EG.drafts?.[job.key];
      if (draft && draft.base !== ta.defaultValue) { delete EG.drafts[job.key]; if (draft.value.trim() !== ta.value.trim()) toast(`${r.order.receiptId}: the words changed while you were editing them — your unapplied edit was replaced`, "", 6000); }
      else if (draft && draft.value !== ta.value) { ta.value = draft.value; ta.dispatchEvent(new Event("input")); } }
    ta.addEventListener("keydown", e => {
      if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) { e.preventDefault(); applyWords(); }
      e.stopPropagation();
    });
    // the back preview is drawn to the box it is actually given, and redrawn when that box changes: no fixed number,
    // nothing cut off on a short laptop screen, nothing left blurry after the window is resized
    const backHost = card.querySelector(".backHost");
    let mounted = 0, raf = 0;
    if (wordsJob) { /* no back to draw */ }
    const wire = bc => {
      let drag = null, flowFrame = 0;
      const paintFlow = () => {
        flowFrame=0;if(!drag)return;
        const centre=drag.pending || drag.c,angle=drag.pendingAngle ?? drag.angle,size=drag.pendingSize ?? drag.want;
        const f=G.reflowAt(job.lineInput || job.lines,fontFor(job.fit.weight),job.mask,{...fitOpts(job),measure:false},{centre,angle,size},job.lineMode || "auto");
        if(f.ok){bc._paint({glyphs:f.glyphs,centre:f.centre,angle:f.angle,mode:drag.mode});const cap=card.querySelector('[data-cap]');if(cap)cap.textContent=f.capMm.toFixed(2)+" mm";}
        else bc._paint();
      };
      const queueFlow = () => {if(!flowFrame)flowFrame=requestAnimationFrame(paintFlow);};
      // the pointer is captured by the canvas, so the handlers live and die with this canvas: every card used to add
      // another pair of listeners to the window and none of them was ever removed
      const near = (p, q2, r) => Math.hypot(p[0] - q2[0], p[1] - q2[1]) <= r;
      const local = e => { const rect = bc.getBoundingClientRect(); return [(e.clientX - rect.left) * bc.width / rect.width, (e.clientY - rect.top) * bc.height / rect.height]; };
      bc.addEventListener("pointermove", e => { if (drag || !bc._box) return; const p = local(e); const b = bc._box; bc.style.cursor = near(p, b.rotate, 9) ? "alias" : b.corners.some(c => near(p, c, 8)) ? "nwse-resize" : "grab"; });
      bc.addEventListener("pointerdown", e => {
        if (!job.fit) return; bc.setPointerCapture(e.pointerId); e.preventDefault();
        const p = local(e), b = bc._box;
        const mode = b && near(p, b.rotate, 10) ? "rotate" : (b && b.corners.some(c => near(p, c, 9))) || e.shiftKey ? "resize" : "move";
        const c0 = job.fit.centre.slice(); const cpx = b ? b.centrePx : bc._map.tx(c0[0], c0[1]);
        drag = { mode, x: e.clientX, y: e.clientY, c: c0, size: job.fit.size, want:job.wantSize ?? job.fit.size, angle: job.fit.angle || 0, cpx, r0: Math.max(4, Math.hypot(p[0] - cpx[0], p[1] - cpx[1])), a0: Math.atan2(p[1] - cpx[1], p[0] - cpx[0]) };
        bc.classList.add("drag");
      });
      bc.addEventListener("pointermove", e => {
        if (!drag) return;
        const rect = bc.getBoundingClientRect(), kx = bc.width / rect.width;
        const dx = (e.clientX - drag.x) * kx / bc._map.k, dy = -(e.clientY - drag.y) * kx / bc._map.k;
        const p = local(e);
        if (drag.mode === "resize") {
          // a corner pulled away from the centre grows the text, pulled in shrinks it: the size follows the distance
          const r = Math.hypot(p[0] - drag.cpx[0], p[1] - drag.cpx[1]);
          drag.pendingSize = Math.min(fitOpts(job).maxHeightFrac*(job.mask.hPt || job.mask.h/job.mask.res),Math.max(0.01, drag.size * r / drag.r0));
          queueFlow();
        } else if (drag.mode === "rotate") {
          const a = Math.atan2(p[1] - drag.cpx[1], p[0] - drag.cpx[0]);
          let ang = drag.angle - (a - drag.a0) * 180 / Math.PI;               // screen y points down, so the sign flips
          ang = ((ang % 360) + 360) % 360; for (const snap of [0, 90, 180, 270, 360]) if (Math.abs(ang - snap) < 3) ang = snap % 360;
          drag.pendingAngle = ang;
          queueFlow();
          const an = card.querySelector('input[data-a="angle"]'); if (an) an.value = String(Math.round(ang));
        } else {
          const c = [drag.c[0] + dx, drag.c[1] + dy];
          // within a third of a millimetre of the charm's own centre line, the text takes it
          if (Math.abs(c[0] - job.mask.cx) < 0.35 * PT) c[0] = job.mask.cx;
          if (Math.abs(c[1] - job.mask.cy) < 0.35 * PT) c[1] = job.mask.cy;
          drag.pending = c;
          queueFlow();
        }
      });
      bc.addEventListener("pointerup", () => {
        if (!drag) return;
        cancelAnimationFrame(flowFrame);flowFrame=0;
        const d = drag; drag = null; bc.classList.remove("drag");
        if (d.mode === "resize") { if (d.pendingSize != null) resize(job, d.pendingSize); else bc._paint(); }
        else if (d.mode === "rotate") { if (d.pendingAngle != null) rotateTo(job, d.pendingAngle); else bc._paint(); }
        else if (d.pending) { if (!moveTo(job, d.pending)) { toast("No room there — kept the previous position", "bad"); bc._paint(); } }
        else bc._paint();
      });
      bc.addEventListener("pointercancel", () => { cancelAnimationFrame(flowFrame);flowFrame=0;drag = null; bc.classList.remove("drag"); refresh(job); });
    };
    let disposed = false;
    const mountBack = () => {
      if (disposed) return;
      try {
      if (wordsJob || !job.view || !backHost.isConnected) return;
      const r = backHost.getBoundingClientRect();
      // side by side, the box says how big; stacked on a narrow screen the box has no height of its own, so half
      // the window is the ceiling and the charm keeps its shape either way
      const stacked = getComputedStyle(backHost).flexGrow === "0";
      const room = stacked ? Math.min(r.width || 320, (window.innerHeight || 700) * 0.46) : Math.min(r.width || 320, r.height || 320);
      const px = Math.round(Math.min(1400, Math.max(200, room - 4)));
      if (!px || Math.abs(px - mounted) < 12) return;
      mounted = px; backHost.textContent = "";
      const bc = renderBack(job, px, { grid: true, editable: true }); bc.title = "drag the words to move them · drag a corner to resize · drag the handle above to turn · arrow keys nudge 0.25 mm, with shift they turn 1° · cut-outs and holes stay clear"; backHost.appendChild(bc); wire(bc);
      } catch (error) {
        card._previewFailed = true;
        backHost.textContent = "This back preview could not be drawn. Refit the words to retry.";
        card.querySelectorAll('[data-a="approve"]').forEach(b => { b.disabled = true; });
        card._ro?.disconnect();
        console.error("Engraving back preview failed", error);
      }
    };
    raf = requestAnimationFrame(() => { raf = 0; mountBack(); });
    card._dispose = () => { disposed = true; cancelAnimationFrame(raf); card._ro?.disconnect(); };
    if (window.ResizeObserver) { const ro = new ResizeObserver(() => { if (raf) return; raf = requestAnimationFrame(() => { raf = 0; mountBack(); }); }); ro.observe(backHost); card._ro = ro; }
    const spacing = card.querySelector('[data-a="spacing"]');
    if(spacing) {
      const group=spacing.closest('.spacingControl');let pending=+spacing.value,frame=0,settle=0,dirty=false;
      const apply=measure=>{
        frame=0;if(!card.isConnected || job.state!=="review")return;
        if(!setLineSpacing(job,pending,measure)) {pending=Math.round(fitOpts(job).lineGap/.18*100);spacing.value=pending;}
        const out=group.querySelector('output');out.textContent=pending+'%';
        out.title=(pending-100>=0?'+':'')+(pending-100)+'% from the original gap';
        group.style.setProperty('--spacing',pending/100);
      };
      const queue=()=>{
        pending=+spacing.value;dirty=true;job._spacingActive=true;
        if(!frame)frame=requestAnimationFrame(()=>apply(false));
        clearTimeout(settle);settle=setTimeout(()=>{card._flushSpacing();job._spacingActive=false;refresh(job);},180);
      };
      card._flushSpacing=()=>{if(frame)cancelAnimationFrame(frame);frame=0;clearTimeout(settle);if(dirty){apply(true);dirty=false;}job._spacingActive=false;refresh(job);};
      spacing.addEventListener('input',queue);
      spacing.addEventListener('change',()=>card._flushSpacing());
      group.addEventListener('wheel',e=>{if(!e.deltaY)return;e.preventDefault();spacing.value=Math.max(0,Math.min(300,(frame?pending:Math.round(fitOpts(job).lineGap/.18*100))+(e.deltaY<0?1:-1)*(e.shiftKey?1:5)));queue();},{passive:false});
      group.style.setProperty('--spacing',pending/100);
    }
    const capOut = card.querySelector("[data-cap]");
    card.querySelectorAll("[data-a]").forEach(b => { const a = b.dataset.a; if (a === "usewords" || a === "linecount" || a === "spacing") return; if (a === "angle") { b.onchange = () => { card._flushSpacing?.(); const v = +b.value; if (Number.isFinite(v)) rotateTo(job, v); }; b.addEventListener("keydown", e => e.stopPropagation()); return; } b.onclick = () => { card._flushSpacing?.(); if (a === "approve") approve(job); else if (a === "centre") centreText(job); else if (a === "turnLeft" || a === "turnRight") {rotateTo(job,(job.fit?.angle || 0)+(a === "turnLeft" ? 90 : -90));} else if (a === "close") { if(job.backSaving) return; if(job.editingBack) {items().delete(job.key);Review.remove("eng:"+job.key);} EG.list = true; EG.card = null; EG.cardKey = null; render(); } else if (a === "prev" || a === "next") { const q = queuedJobs([...items().values()].filter(matchesQ).filter(j2 => j2.row.state !== "gone")); const i = q.findIndex(j2 => j2.key === job.key); const j3 = q[(i + (a === "next" ? 1 : q.length - 1)) % q.length]; if (j3) { EG.focus = j3.key; EG.card = null; EG.cardKey = null; render(); } }
      else if (a === "resplit") resplit(job); else if (a === "skip") skip(job); else if (a === "back") sendBack(job); }; });
    const lineControl=card.querySelector('[data-a="linecount"]');
    if(lineControl) lineControl.onchange=async()=>{
      card._flushSpacing?.();
      job.lineMode=lineControl.value;
      await applyWords();
    };
    void capOut;
    card.addEventListener("keydown", e => { if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA" || e.target.tagName === "SELECT" || e.repeat) return; const k = e.key.toLowerCase(); if (["a","s","escape","arrowleft","arrowright","arrowup","arrowdown"].includes(k)) card._flushSpacing?.(); if (k === "a") { e.preventDefault(); approve(job); } else if (k === "s") { e.preventDefault(); skip(job); } else if (e.key === "Escape") { if(job.editingBack && !job.backSaving) {items().delete(job.key);Review.remove("eng:"+job.key);} EG.list = true; EG.card = null; EG.cardKey = null; render(); } else if (e.shiftKey && (e.key === "ArrowLeft" || e.key === "ArrowRight")) { e.preventDefault(); rotateTo(job, (job.fit ? job.fit.angle || 0 : 0) + (e.key === "ArrowLeft" ? 1 : -1)); } else if (e.key === "ArrowLeft") { e.preventDefault(); nudge(job, -0.25, 0); } else if (e.key === "ArrowRight") { e.preventDefault(); nudge(job, 0.25, 0); } else if (e.key === "ArrowUp") { e.preventDefault(); nudge(job, 0, 0.25); } else if (e.key === "ArrowDown") { e.preventDefault(); nudge(job, 0, -0.25); } });
    // the next card takes focus only when the person was already working in this pane, so a held key cannot run the queue.
    // It never takes focus from a field someone is typing in: the card's single-key shortcuts (A approve, S no engraving,
    // arrows nudge) would otherwise receive the rest of what they type.
    const wasHere = document.activeElement && document.activeElement.closest && document.activeElement.closest("#egQueue");
    if (wasHere || !document.activeElement || document.activeElement === document.body) setTimeout(() => { if (card.isConnected && !document.activeElement?.matches?.("input, textarea, select, [contenteditable]")) card.focus(); }, 30);
    void it;
    return card;
  }
  return { loadBackPreview: identity => api("charmNestLibrary", {op:"backPreview", ...identity}, {quiet:true}), openBack, sheetBacks, backsMarkup, refreshBacks, reconcileSheet, saveSheetBacks, refreshBackIndexes, view: () => ({ tab: EG.tab, focus: EG.focus, chosen: EG.chosen, q: EG.q, list: EG.list, drafts: pruneDrafts() }), restoreView: v => Object.assign(EG, v || {}, { card: null, cardKey: null, reread: 0, drafts: Object.assign({}, v?.drafts || EG.drafts || {}) }), loadFonts, classify, classifyAll, background, settled: () => settledPasses, canFit, isWorking, fitJob, fitAll, approve, nudge, hasPlacement, shelveWritten, resize, rotateTo, setLineSpacing, resplit, skip, sendBack, decideWords, invalidate, render, fromRecall, placementCard, renderBack, renderFront, pendingCount, reviewedCount, items, jobOf, ensureJob, setReady, writeBacks, saveBacks, resumeBacks, verifyBackFile, sheetBackOutputs, fonts: F_ };
})();

/* ═══ 22 · Sets — production evidence and release ═══ */
const LaserReview = window.LaserReview = (()=>{
  const R=window.CharmNestReadiness, records=new Map();let frame=0,polling=false,lastPoll=0;
  function record(s){const id=s.id || s.sheetId,old=records.get(id);if(id && (!old || (s.updatedAt || 0)>=(old.updatedAt || 0)))records.set(id,s);return s;}
  function projected(s){
    const id=s.id || s.sheetId,base=records.get(id) || s;
    const live=allSheets().find(x=>x.sheetId===id && !x.recalled);
    const ids=base.poolIds || (live?.placements || []).map(p=>live.charms.find(c=>c.id===p.id)?.poolId).filter(Boolean);
    const d={...base,poolIds:ids,engraving:{...base.engraving},backPool:(base.backPool || base.backs || []).slice()};
    const rows=R.decisions(Orders.rows());
    for(const pid of ids)if(rows[pid] && !d.engraving[pid])d.engraving[pid]=rows[pid];
    for(const j of Engrave.items().values())for(const pid of j.copies || [])if(ids.includes(pid)) {
      if(j.state==='written' && !j.backSaving)continue; // cloud evidence wins once saved
      d.engraving[pid]={needed:!['none','skipped'].includes(j.state),state:j.state,approved:['none','skipped','approved'].includes(j.state)};
      d.backPool=d.backPool.filter(b=>b.poolId!==pid);
      if(j.state==='approved')d.backPool.push(...(j.backs || []).filter(b=>b.poolId===pid));
    }
    if(live?.dirty || live?.runHold || live?.saving || ['nesting','finishing'].includes(live?.status))d.dirty=true;
    return d;
  }
  const sheet=s=>R.sheet(projected(s));
  const group=(st,sheets)=>R.set(st,sheets.map(projected));
  function labels(s,files){
    const id=s.id || s.sheetId, fs=files?.length?files:s.label?.files || [];
    return `<div class="productionRow" data-laser-sheet="${esc(id)}"><div class="sheetQR">${fs.length?fs.map(f=>`<img data-big title="Sheet QR label" data-label-sheet="${esc(id)}" data-label-path="${esc(f.path || '')}" data-label-part="${+f.part || 1}" crossorigin="anonymous"${f.url?` src="${esc(cors(f.url))}"`:''} alt="Sheet QR code">`).join(''):'<span class="qrWaiting">QR label pending</span>'}</div></div>`;
  }
  function sections(body){
    body.innerHTML='<section class="laserSection readyArea" data-laser-area="ready"><h2>Laser cutting <span>Ready sheets and sets</span></h2><p class="laserEmpty">No sheets are ready for cutting yet.</p><div class="laserAreaItems"></div></section><section class="laserSection" data-laser-area="pending"><h2>In progress</h2><div class="laserAreaItems"></div></section>';
  }
  function place(card,ready,body){body.querySelector(`[data-laser-area="${ready?'ready':'pending'}"] .laserAreaItems`).appendChild(card);}
  function refresh(){
    frame=0;
    if(S.mode!=='library')return;   // its cards are the Library's: a closed Library has let its records go (below)
    document.querySelectorAll('[data-laser-card]').forEach(card=>{
      const sheets=(card._laserSheets || []).map(id=>records.get(id)).filter(Boolean),report=card._laserSet?group(card._laserSet,sheets):sheet(sheets[0] || {});
      const seal=card.querySelector('[data-laser-seal]');if(seal){const html=R.seal(report,card._laserSet?'Set':'Sheet');if(seal.innerHTML!==html)seal.innerHTML=html;}
      const title=card.querySelector('[data-set-title]');if(title)title.textContent=O.setLabel(card._laserSet.seq)+(card._laserSet.day?' · '+card._laserSet.day:'');
      card.querySelectorAll('[data-sheet-status]').forEach(n=>{const s=records.get(n.dataset.sheetStatus);if(s){const html=R.counter(sheet(s));if(n.innerHTML!==html)n.innerHTML=html;}});
      const body=card.closest('#libBody');if(body && card.parentElement!==body.querySelector(`[data-laser-area="${report.ready?'ready':'pending'}"] .laserAreaItems`))place(card,report.ready,body);
    });
    document.querySelectorAll('[data-laser-area]').forEach(area=>{const hasItems=!!area.querySelector('.laserAreaItems')?.children.length;area.hidden=area.dataset.laserArea!=='ready' && !hasItems;const empty=area.querySelector('.laserEmpty');if(empty)empty.hidden=hasItems;});
  }
  function changed(){if(!frame)frame=requestAnimationFrame(refresh);}
  async function poll(force=false){
    if(polling || S.mode!=='library' || document.hidden || !S.cloud.ok || (Date.now()-lastPoll<(force?5000:60000)))return;
    const cards=[...document.querySelectorAll('[data-laser-card]')].filter(x=>{const r=x.getBoundingClientRect();return r.width>0&&r.height>0&&r.bottom>0&&r.top<innerHeight;});
    const ids=[...new Set(cards.flatMap(x=>(x._laserSheets || []).concat([...x.querySelectorAll('[data-laser-sheet]')].map(n=>n.dataset.laserSheet))))];if(!ids.length)return;
    polling=true;lastPoll=Date.now();
    try{const response=await api('charmNestLibrary',{op:'laserStatus',sheetIds:ids,setIds:cards.map(x=>x._laserSet?.setId).filter(Boolean)},{quiet:true});for(const set of response.sets || [])for(const card of document.querySelectorAll('[data-laser-card]'))if(card._laserSet?.setId===set.setId)card._laserSet={...card._laserSet,sheetIds:set.sheetIds};const found=new Set();for(const s of response.sheets || []){found.add(s.id);records.set(s.id,s);}for(const id of ids)if(!found.has(id) && records.has(id))records.set(id,{...records.get(id),archived:true});changed();}
    catch(e){console.warn('Production readiness refresh',e);}
    finally{polling=false;}
  }
  function saved(sh){
    const old=records.get(sh.sheetId);if(old)record({...old,backPool:(sh.backPool || []).slice(),engraving:{...old.engraving,...R.decisions(Orders.rows())}});
    changed();
  }
  // Paul, 24 Sep: nothing may pile up on a page left open. The records are the Library's, read again each time it opens
  // (renderLibrary, openLibrarySheet); while it is closed they are let go at the minute's check.
  setInterval(()=>{if(S.mode!=='library')records.clear();poll();},60000);
  document.addEventListener('visibilitychange',()=>{if(!document.hidden)poll(true);});
  return {record,sheet,group,labels,sections,place,changed,saved,poll,projected};
})();

/* ═══ 22 · Sets — one run, one date, one folder, one numbering across materials ═══ */
const Sets = window.Sets = (() => {
  /* ofRun and setOfSheet searched every set the session holds at each call, and a page's closed() asks once a page. The
     sets are grouped by run and by id once, and again only when the map is replaced or changed (its set, delete and
     clear count the changes; a set's run and id never change after it is made). */
  let groups = null, groupsOf = null, groupsAt = -1, changes = 0;
  function watched(m) {
    if (m && !Object.prototype.hasOwnProperty.call(m, "set")) for (const k of ["set", "delete", "clear"]) { const f = Map.prototype[k]; Object.defineProperty(m, k, { value(...a) { changes++; return f.apply(this, a); }, configurable: true, writable: true }); }
    return m;
  }
  function grouped() {
    const m = watched(B.sets);
    if (groupsOf !== m || groupsAt !== changes) {
      const runs = new Map(), ids = new Map();
      for (const x of m.values()) { const l = runs.get(x.runId); if (l) l.push(x); else runs.set(x.runId, [x]); if (!ids.has(x.setId)) ids.set(x.setId, x); }
      groups = { runs, ids }; groupsOf = m; groupsAt = changes;
    }
    return groups;
  }
  const byRun = () => watched(B.sets);                // keyed `${runId}|${group}` — a run has one set per kin group
  const keyOf = (runId, group) => `${runId}|${group || "all"}`;
  /* A set used to be "the run": every sheet of every material a run made, under one number. But the only thing that
     ties two sheets together is an order with a piece on each, and the shop said so: a material no order ties to
     another is a set of its own. So a run has one set per kin group (Orders.kinGroups), each numbered on the day's
     counter in its own transaction, and a second run later the same day carries the numbering on. */
  const allocating = new Map();
  function ensure(runId, group, opts = {}) {
    const key = keyOf(runId, group);
    if (!allocating.has(key)) allocating.set(key, allocate(runId, group, opts).finally(() => allocating.delete(key)));
    return allocating.get(key);
  }
  async function allocate(runId, group, opts) {
    const k = keyOf(runId, group);
    const prior=byRun().get(k);
    if(group === "dispatch" && prior?.committedAt){byRun().set(keyOf(runId,"committed:"+prior.setId),prior);byRun().delete(k);}
    if (byRun().has(k) && !byRun().get(k).offline) return byRun().get(k);
    if (!S.cloud.ok) throw new Error("Reconnect to the cloud before numbering a new set; the layout is kept locally");
    const day = today();
    // A run keeps releasing after a commit: its next dispatch set follows the newest committed one and gets a number of
    // its own, where asking again for the run's dispatch set would hand back the set already cut.
    const followed = group === "dispatch" ? ofRun(runId).filter(s => s.group === "dispatch" && s.committedAt).sort((a, b) => (+b.committedAt || 0) - (+a.committedAt || 0))[0] || null : null;
    const after = followed?.setId || null;
    // The server numbers a follow-on set only once the set it follows is recorded as committed. A commit whose record
    // did not save is saved again first: a refusal then names its cause, and every later set used to be refused.
    // A commit whose record did save is not saved again: the server checks its readiness against the run's current lines,
    // and an order in it that changed on Etsy after the cut (its line back in review) refused that save and stopped every
    // later intake for good.
    if (followed && followed.savedCommit !== followed.committedAt) await save(followed);
    let set;
    if (S.cloud.ok) { const r = await api("charmNestLibrary", { op: "setAllocate", day, runId, group: group || "", roseOnly: !!opts.roseOnly, after }, { label: "Numbering the set" }); if (r.deferred) return null; set = { setId: r.setId, seq: r.seq, day: r.day || day, runId, group: group || null, name: O.setLabel(r.seq), folder: O.setFolder(r.day || day, r.seq), orders: {}, sheetIds: [], materials: [], labelFiles: [], status: "open" }; }

    byRun().set(k, set);
    if (B.run && B.run.runId === runId) { B.run.setIds = [...new Set((B.run.setIds || []).concat([set.setId]))]; B.run.setId = B.run.setId || set.setId; B.run.day = set.day; B.run.seq = B.run.seq || set.seq; RunCtl.renderBanner(); }
    agent({ run: runId }, "cloud", `${set.name} allocated for ${set.day}${group ? ` · ${group.split("+").map(m => labelOf(m)).join(" + ")}` : ""} → ${set.folder}`);
    return set;
  }
  /** Every set of a run, in set order. */
  const ofRun = runId => (grouped().runs.get(runId) || []).slice().sort((a, b) => (a.seq || 0) - (b.seq || 0));
  const setOfSheet = sh => sh.draft || (["gold10k","gold14k"].includes(sh.metal) && !Gate.solidSelected(sh.metal)) ? null : sh.runId ? ((sh.setId ? grouped().ids.get(sh.setId) : null) || byRun().get(keyOf(sh.runId, sh.group)) || null) : null;
  /** The QR label, 145 × 145 pt, the print page's exact geometry (QR 85 pt at 3,3 · label 9 pt bold at 1,93 · "Notes:" at 92,0.5), ECC M. */
  async function renderLabelPng(payload, label, scale) {
    const k = scale || 8; const cv = document.createElement("canvas"); cv.width = Math.round(145 * k); cv.height = Math.round(145 * k);
    const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height);
    const holder = document.createElement("div"); holder.style.cssText = "position:absolute;left:-9999px;top:0"; document.body.appendChild(holder);
    let ecc = "M";
    try {
      const make = level => { holder.innerHTML = ""; return new QRCode(holder, { text: payload, width: Math.round(85 * k), height: Math.round(85 * k), correctLevel: level }); };
      try { make(QRCode.CorrectLevel.M); } catch (e) { make(QRCode.CorrectLevel.L); ecc = "L"; }
      await new Promise(r => setTimeout(r, 30));
      const qcv = holder.querySelector("canvas"); const qimg = holder.querySelector("img");
      if (qcv?.width && qcv?.height) ctx.drawImage(qcv, 3 * k, 3 * k, 85 * k, 85 * k);
      else if (qimg) { await qimg.decode(); if(!qimg.naturalWidth)throw new Error("QR image did not render"); ctx.drawImage(qimg, 3 * k, 3 * k, 85 * k, 85 * k); }
      else throw new Error("QR code did not render — retry the label");
    } finally { holder.remove(); }
    ctx.fillStyle = "#000"; ctx.textBaseline = "top"; ctx.font = `bold ${9 * k}px Helvetica, Arial, sans-serif`; ctx.fillText(label, 1 * k, 93 * k, 143 * k); ctx.fillText("Notes:", 92 * k, 0.5 * k);
    const blob = await new Promise(r => cv.toBlob(r, "image/png")); if(!blob)throw new Error("QR preview could not be saved"); return { blob, dataUrl: cv.toDataURL("image/png"), ecc };
  }
  /** After a sheet is saved: its label(s) beside it, the sheet record and the set record kept current, pool rows → written. */
  function labelsReady(sh, set) {
    const byId = new Map(sh.charms.map(c => [c.id, c]));
    const ids = [...new Set(sh.placements.map(p => byId.get(p.id)).filter(Boolean).map(c => String(c.order || c.id).split("/")[0]))];
    const parts = O.safeChunks(ids, O.CARD_TO_METAL[sh.metal] || sh.metal, 1000, 500, 8);
    const files = sh.label?.files || [], indexed = (set.labelFiles || []).filter(f => f.sheetId === sh.sheetId);
    return sh.setId === set.setId && !sh.draft && parts.length > 0 && files.length === parts.length && indexed.length === files.length &&
      files.every((f, i) => f.url && f.path && f.sheet === sh.fileBase && f.payload === O.encodeOrderList(parts[i], O.CARD_TO_METAL[sh.metal] || sh.metal) &&
        indexed.some(g => g.path === f.path && g.url === f.url && g.part === f.part));
  }
  async function onSheetSaved(sh, items, outputs, {labelsOnly = false, setOverride = null} = {}) {
    const set = setOverride || setOfSheet(sh); if (!set) return;
    const byId = new Map(items.map(c => [c.id, c]));
    const placed = sh.placements.map(p => byId.get(p.id)).filter(Boolean);
    const ids = [...new Set(placed.map(c => String(c.order || c.id).split("/")[0]))];
    const metalDS = O.CARD_TO_METAL[sh.metal] || sh.metal;
    // one code per sheet: a QR holds a thousand characters (about ninety orders) before it has to split; it used to split at fifty
    const parts = O.safeChunks(ids, metalDS, 1000, 500, 8);
    const files = [];
    for (const [i, slice] of parts.entries()) {
      const payload = O.encodeOrderList(slice, metalDS);
      const label = `${METAL_TAG[sh.metal]} · ${set.name} · Sheet ${sh.sheetIndex || sh.page}${parts.length > 1 ? ` [${i + 1}/${parts.length}]` : ""} · ${slice.length} order${slice.length === 1 ? "" : "s"}`;
      const png = await renderLabelPng(payload, label);
      let up = null; if (S.cloud.ok && sh.folderPath) up = await uploadBytes(`${sh.folderPath}/${sh.fileBase}_label${parts.length > 1 ? `_${i + 1}of${parts.length}` : ""}.png`, png.blob, "image/png", "Saving the sheet label");
      files.push({ path: up && up.path, url: up && up.url, dataUrl: up ? null : png.dataUrl, sheet: sh.fileBase, sheetId: sh.sheetId, metal: metalDS, part: i + 1, parts: parts.length, orders: slice, payload, ecc: png.ecc, label });
    }
    sh.label = { files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload, ecc: f.ecc, label: f.label })), orders: ids };
    sh.setId = set.setId; sh.setSeq = set.seq;
    if (S.cloud.ok && sh.sheetId) await api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, label: sh.label, setId: set.setId, setSeq: set.seq, sheetIndex: sh.sheetIndex, orders: ids, runId: sh.runId, poolIds: placed.map(c => c.poolId).filter(Boolean) } });
    // pool rows and order lines
    const poolIds = placed.map(c => c.poolId).filter(Boolean);
    if (!labelsOnly && poolIds.length) await Pool.update(poolIds, { sheetId: sh.sheetId, setId: set.setId, state: "written", sheetName: sh.fileBase });
    for (const row of labelsOnly ? [] : Orders.rows()) { if (!row.poolIds.length) continue; const allPlaced = row.poolIds.every(pid => { const p = B.pool.rows.get(pid); return p && p.sheetId; }); if (allPlaced && row.state === "pooled") row.state = "written"; }
    // the set record
    if (!set.sheetIds.includes(sh.sheetId)) set.sheetIds.push(sh.sheetId);
    if (!set.materials.includes(sh.metal)) set.materials.push(sh.metal);
    for (const c of placed) { const rid = String(c.order || "").split("/")[0]; if (!rid || !c.orderInfo) continue; const o = set.orders[rid] = set.orders[rid] || { lines: {}, held: null }; const ln = o.lines[c.orderInfo.transactionId] = o.lines[c.orderInfo.transactionId] || { transactionId: c.orderInfo.transactionId, sku: c.orderInfo.sku, copies: [] }; if (!ln.copies.some(x => x.poolId === c.poolId)) ln.copies.push({ copy: c.orderInfo.copy, sheetId: sh.sheetId, sheet: sh.fileBase, poolId: c.poolId, backPoolId: null }); }
    set.labelFiles = set.labelFiles.filter(f => f.sheetId !== sh.sheetId).concat(sh.label.files.map(f => Object.assign({ sheetId: sh.sheetId }, f)));
    set.labels = null; // A previously collected PDF/manifest no longer describes these sheet labels.
    if (!labelsOnly) set.status = "nesting";
    await save(set);
    if (window.RunHistory) RunHistory.refreshIfOpen();
    agent({ metal: sh.metal, run: sh.runId }, "cloud", `${sh.fileBase}: ${files.length} label${files.length === 1 ? "" : "s"} saved (${ids.length} order${ids.length === 1 ? "" : "s"}) · set ${set.name} now ${set.sheetIds.length} sheet(s)`);
    Orders.render();
  }
  async function save(set, context) { if(window.CharmNestOperations && !context)return window.CharmNestOperations.run({key:'set-save:'+set.setId,label:'Saving set record',resources:['set-record:'+set.setId],latest:true},token=>save(set,token)); if (!S.cloud.ok || set.offline) return; const orders = {}; for (const [rid, o] of Object.entries(set.orders)) orders[rid] = { held: o.held || null, lines: Object.values(o.lines) }; await api("charmNestLibrary", { op: "setUpdate", setId: set.setId, patch: { runId: set.runId, day: set.day, seq: set.seq, name: set.name, folder: set.folder, materials: set.materials, sheetIds: set.sheetIds, orders, status: set.status, labels: set.labels || null, labelFiles: set.labelFiles.map(f => ({ path: f.path, url: f.url, sheet: f.sheet, sheetId: f.sheetId, part: f.part, parts: f.parts, orders: f.orders, payload:f.payload || null, metal:f.metal || null, ecc:f.ecc || null, label: f.label })), committed: set.committed || null, refused: set.refused || null, completedAt: set.completedAt || null, completionDay: set.completionDay || null, committedAt: set.committedAt || null, backCount: set.backCount || 0 } }); if (set.committedAt) set.savedCommit = set.committedAt; }
  const sheetsOf = set => allSheets().filter(sh => sh.setId === set.setId);
  /** Which orders of the set travel, which are held and why (design §5.4, §8.4). */
  function evaluate(set) {
    const rows = Orders.rows(); const byOrder = new Map();
    for (const r of rows) { if (!byOrder.has(r.order.receiptId)) byOrder.set(r.order.receiptId, []); byOrder.get(r.order.receiptId).push(r); }
    const out = { committable: [], held: {}, gone: [] };
    for (const [rid, lines] of byOrder) {
      if (lines.every(l => l.state === "gone")) { out.gone.push(rid); continue; }
      // an order with an Etsy change still in Review waits, whichever sheet its changed line is on: the change used to be
      // left out here, and an order whose changed line sat on an earlier set was committed with its other line
      const ev = O.evaluateOrder(lines.map(l => ({ key: l.key, spec: l.spec, state: l.state, reason: l.reason, problems: l.problems, engrave: l.engrave, changePending: l.changePending })));
      const holdByPerson = lines.find(l => l.hold);
      if (ev.committable && !holdByPerson) out.committable.push(rid); else out.held[rid] = holdByPerson ? { line: holdByPerson.key, why: holdByPerson.hold } : ev.held;
      if (set.orders[rid]) set.orders[rid].held = out.held[rid] || null;
    }
    return out;
  }
  /** labels/Set-K_labels.pdf (one page per sheet label), Set-K_manifest.pdf, set.json. */
  function validateRelease(set) {
    const pendingRelease = message => Object.assign(new Error(message), {releasePending:true});
    const sheets = sheetsOf(set);
    if (sheets.some(sh => sh.runHold)) throw pendingRelease("A sheet in this set still needs attention");
    const sheetPools=new Set(sheets.flatMap(sh=>sh.charms.filter(c=>sh.placements.some(p=>p.id===c.id)).map(c=>c.poolId)));
    // A cancelled order is dropped from the set, not waited for: a piece of it that could not come off its sheet is cut with it and set aside
    if(Orders.rows().some(row=>row.state !== "gone" && (row.changePending || row.hold) && row.poolIds.some(id=>sheetPools.has(id))))throw pendingRelease("An order on this sheet changed or needs review");
    const reports=sheets.map(sh=>({...sh,roseStockId:sh.roseStock?.id,id:sh.sheetId,poolIds:(sh.placements || []).map(p=>sh.charms.find(c=>c.id===p.id)?.poolId).filter(Boolean),placedCount:sh.placements.length,outputs:sh.cloud || {},engraving:window.CharmNestReadiness.decisions(Orders.rows())}));
    if(!window.CharmNestReadiness.set(set,reports).ready)throw pendingRelease("Set is not ready for laser: finish every sheet's engraving approvals, saved back files, layout checks and QR labels");
    if (!Gate.modern(set.runId)) return;
    if (!set.sheetIds.length || sheets.length !== set.sheetIds.length || set.sheetIds.some(id => !sheets.some(sh => sh.sheetId === id))) throw pendingRelease("The set's sheets are not all loaded");
    for (const sh of sheets) {
      const policy = Gate.policy(sh, set.seq);
      if (!policy.include) throw pendingRelease(`${labelOf(sh.metal)}: ${policy.reason}`);
      if (!sh.persistedDone || !sh.outputs) throw pendingRelease(`${sh.fileBase}: sheet files are not saved`);
      if (!labelsReady(sh, set)) throw pendingRelease(`${sh.fileBase}: QR labels are missing or out of date`);
      const placed = new Set(sh.placements.map(p => sh.charms.find(c => c.id === p.id)?.poolId).filter(Boolean));
      for (const job of Engrave.items().values()) {
        if (job.row?.state !== "gone" && job.copies.some(id => placed.has(id)) && !["none", "skipped", "written"].includes(job.state)) throw pendingRelease(`${sh.fileBase}: back engraving still needs to be finished`);
      }
      for (const row of Orders.rows()) {
        if (!row.engrave?.needed || row.state === "gone") continue;
        for (const poolId of row.poolIds.filter(id => placed.has(id))) {
          const back = (sh.backPool || []).find(b => b.poolId === poolId);
          if (!row.engrave.approved || row.engrave.state !== "written" || !back?.approvedAt || !back.verified?.file?.ok || !back.outputs?.ai?.path || !back.outputs.ai.url) throw pendingRelease(`${row.order.receiptId}: back engraving is not approved and saved`);
        }
      }
    }
  }
  function releaseIssue(set) { try { validateRelease(set); return null; } catch(e) { if(e.releasePending)return e.message; throw e; } }
  async function finalize(set, context) {
    const ops=window.CharmNestOperations;
    if(ops && !context){await Gate.flush(B.run);return ops.run({key:'labels:'+set.setId,label:'Saving set labels',resources:['production:'+set.runId]},token=>finalize(set,token));}
    if(B.run?.runId===set.runId)await RunCtl.save(B.run);
    validateRelease(set);
    const { PDFDocument, StandardFonts, rgb } = PDFLib;
    const files = set.labelFiles.slice().sort((a, b) => a.sheet.localeCompare(b.sheet) || a.part - b.part);
    const labels = await PDFDocument.create();
    for (const f of files) { const bytes = f.url ? await CharmNestAssets.bytes(f.url) : dataUrlToBytes(f.dataUrl); const img = await labels.embedPng(bytes); const page = labels.addPage([145, 145]); page.drawImage(img, { x: 0, y: 0, width: 145, height: 145 }); }
    const ev = evaluate(set);
    const man = await PDFDocument.create(); const font = await man.embedFont(StandardFonts.Helvetica), bold = await man.embedFont(StandardFonts.HelveticaBold);
    let page = man.addPage([612, 792]), y = 756;
    const ansi = t => String(t).replace(/→/g, "->").replace(/↔/g, "<->").replace(/·/g, "-").replace(/[^\x20-\x7E\xA0-\xFF]/g, "?");   // Helvetica (WinAnsi) only
    const line = (t, opts = {}) => { if (y < 48) { page = man.addPage([612, 792]); y = 756; } page.drawText(ansi(t).slice(0, 110), Object.assign({ x: 36, y, size: 9.5, font }, opts)); y -= opts.size ? opts.size + 4 : 13; };
    line(`${set.name} · ${set.day} · run ${set.runId}`, { size: 15, font: bold }); line(`${set.sheetIds.length} sheet(s) · materials ${set.materials.map(m => labelOf(m)).join(", ")} · ${Object.keys(set.orders).length} order(s) · ${ev.committable.length} committable · ${Object.keys(ev.held).length} held · ${ev.gone.length} gone`); y -= 6;
    line("Orders and sheets", { font: bold, size: 11 });
    for (const [rid, o] of Object.entries(set.orders).sort()) { const copies = Object.values(o.lines).flatMap(l => l.copies.map(c => `${l.sku}${l.copies.length > 1 ? "#" + c.copy : ""}→${c.sheet}`)); line(`${rid}  ${ev.held[rid] ? "HELD: " + (ev.held[rid].why || "") + "  " : ""}${copies.join("  ")}`); }
    y -= 6; line("Engraving", { font: bold, size: 11 });
    const backs = sheetsOf(set).flatMap(sh => (sh.backPool || []).map(b => `${sh.fileBase}: ${b.order} ${b.sku} #${b.copy} "${String(b.text).replace(/\n/g, " / ")}" ${b.sizePt} pt · ${b.approvedBy || "?"}`));
    if (backs.length) backs.forEach(b => line(b)); else line("no engraving in this set");
    y -= 6; line("Labels", { font: bold, size: 11 }); files.forEach(f => line(`${f.label}  ${f.path || "(not uploaded)"}`));
    // released for labels: a sheet without its .pdf yet gets it now, copied from its .ai inside the bucket (op_sheetPdf)
    const noPdf = sheetsOf(set).filter(sh => sh.sheetId && sh.cloud && sh.cloud.ai && !sh.cloud.pdf);
    if (S.cloud.ok && !set.offline && noPdf.length) await api("charmNestLibrary", { op: "sheetPdf", ids: noPdf.map(sh => sh.sheetId) }, { label: "Saving the sheets' .pdf", quiet: true })
      .then(r => { for (const sh of noPdf) if (r && r.urls && r.urls[sh.sheetId]) sh.cloud.pdf = r.urls[sh.sheetId]; }).catch(e => console.warn("sheet .pdf", e));
    const json = { setId: set.setId, runId: set.runId, day: set.day, seq: set.seq, name: set.name, folder: set.folder, materials: set.materials, sheets: sheetsOf(set).map(sh => ({ sheetId: sh.sheetId, name: sh.fileBase, metal: sh.metal, sheetIndex: sh.sheetIndex, folder: sh.folderPath, orders: sh.label ? sh.label.orders : [], placements: sh.placements.length, backs: (sh.backPool || []).map(b => ({ poolId: b.poolId, order: b.order, sku: b.sku, copy: b.copy, text: b.text, approvedBy: b.approvedBy, file: b.outputs && b.outputs.ai && b.outputs.ai.path })), verification: sh.verification && { ok: sh.verification.ok }, outputs: sh.cloud || null })), orders: Object.fromEntries(Object.entries(set.orders).map(([rid, o]) => [rid, { held: ev.held[rid] || null, lines: Object.values(o.lines) }])), held: ev.held, gone: ev.gone, labels: files.map(f => ({ sheet: f.sheet, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload, path: f.path })), approvals: backs.length, generatedAt: new Date().toISOString() };
    set.backCount = sheetsOf(set).reduce((n, sh) => n + (sh.backPool || []).length, 0);
    if (S.cloud.ok && !set.offline) {
      const [lp, mp, jp] = await Promise.all([uploadBytes(`${set.folder}/labels/${set.name}_labels.pdf`, await labels.save({ useObjectStreams: false }), "application/pdf", "Saving the set's labels PDF"), uploadBytes(`${set.folder}/${set.name}_manifest.pdf`, await man.save({ useObjectStreams: false }), "application/pdf", "Saving the manifest"), uploadBytes(`${set.folder}/set.json`, new TextEncoder().encode(JSON.stringify(json, null, 1)), "application/json")]);
      set.labels = { pdf: { path: lp.path, url: lp.url }, manifest: { path: mp.path, url: mp.url }, json: { path: jp.path, url: jp.url }, files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet })) };
    } else set.labels = { files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet })) };
    set.status = "labelled"; await save(set);
    for (const id of set.sheetIds || []) window.Session?.dropBest?.(id);   // released: its sheets' best-layout records go
    agent({ run: set.runId }, "cloud", `${set.name}: ${files.length} label(s) collected in labels/${set.name}_labels.pdf · manifest and set.json saved`);
    return set;
  }
  /** §8.4 · the real lock, the preview, the commit — held and refused orders stay open on the station. */
  async function commit(set, run, context) {
    const ops=window.CharmNestOperations;
    if(ops && !context){await Gate.flush(run);return ops.run({key:'commit:'+run.runId,label:'Committing set',resources:['production:'+run.runId]},async token=>{if(run.membershipNext || run.membershipDirty)return {membershipChanged:true};Gate.refreshMembership();try{return await commit(set,run,token);}finally{requestAnimationFrame(()=>Gate.refreshMembership());}});}
    validateRelease(set);
    const ev = evaluate(set);
    const committable = ev.committable.filter(rid => set.orders[rid]);
    for (const [rid, h] of Object.entries(ev.held)) Review.add({ kind: "heldOrder", key: "held:" + rid, rid, why: `${rid} held — ${h.why || "unresolved line"}`, line: h.line });
    if (!committable.length) { agent({ run: run.runId }, "warn", `${set.name}: no committable order — ${Object.keys(ev.held).length} held, ${ev.gone.length} gone`); set.status = "complete-with-holds"; set.completedAt = Date.now(); set.completionDay = today(); set.committed = []; set.refused = Object.entries(ev.held).map(([id, h]) => ({ id, reason: h.why })); await save(set); return { completed: [], refused: set.refused }; }
    if (!set.labels || !(set.labels.files || []).length) throw new Error("no saved labels for the set");
    await DesignLink.ensure();
    const sel = await DesignLink.call("ui.select", { receiptIds: committable }, { timeoutMs: 120000 });
    const refused = (sel.refused || []).slice();
    const ids = sel.selected.filter(id => committable.includes(id));
    if (!ids.length) { set.status = "complete-with-holds"; set.committed = []; set.refused = refused; await save(set); agent({ run: run.runId }, "warn", `${set.name}: the station refused every selection — ${refused.map(r => r.id + " (" + r.reason + ")").join(", ")}`); return { completed: [], refused }; }
    const preview = await DesignLink.call("complete.preview", { receiptIds: ids }, { timeoutMs: 120000 });
    agent({ run: run.runId }, "DS", `Station preview: ${preview.jobs.length} label job(s) for ${ids.length} order(s)${preview.notes && preview.notes.skipped && preview.notes.skipped.length ? ` · ${preview.notes.skipped.length} with no recognised metal` : ""}`);
    const labels = { setId: set.setId, folder: `${set.folder}/labels`, files: set.labels.files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet })), pdf: set.labels.pdf ? set.labels.pdf.url : null };
    validateRelease(set); // Recheck after the station selection/preview awaits.
    // The server checks the set against the run's saved lines before it records the set as complete. Save them first,
    // so it sees what was checked here: a set refused after the station commit leaves its orders done at the station
    // and the set open, and the run stopped at every later check.
    await RunCtl.save(run);
    const r = await DesignLink.call("complete.commit", { receiptIds: ids, labels, runId: run.runId, setId: set.setId, sheetIds: set.sheetIds, backCount: set.backCount || 0, completedBy: `Charm Sorter (${employeeName() || "operator"})` }, { timeoutMs: 180000 });
    set.committed = r.completed; set.refused = refused.concat(r.refused || []); set.committedAt = Date.now(); set.completedAt = set.committedAt; set.completionDay = today(); set.status = set.refused.length || Object.keys(ev.held).length ? "complete-with-holds" : "complete";
    for (const row of Orders.rows()) if (r.completed.includes(row.order.receiptId) && (row.state === "written" || row.state === "labelled" || row.state === "noDesign")) row.state = "committed";
    await Pool.update([...B.pool.rows.keys()].filter(id => r.completed.includes(B.pool.rows.get(id).orderId)), { state: "committed", committedAt: Date.now() });
    await save(set);
    for (const id of set.sheetIds || []) window.Session?.dropBest?.(id);
    agent({ run: run.runId }, "DS", `${set.name} committed: ${r.completed.length} order(s) marked design-complete and sent DESIGNED :) internally · ${set.refused.length} refused · ${Object.keys(ev.held).length} held`);
    return { completed: r.completed, refused: set.refused, held: ev.held };
  }
  async function undo(set) {
    // a set that committed nothing (every order held or refused) has nothing to reopen at the station
    const reopened = (set.committed || []).map(String);
    if (reopened.length) { await DesignLink.ensure(); await DesignLink.call("complete.undo", { receiptIds: reopened }, { timeoutMs: 180000 }); }
    set.status = "awaiting review"; set.committed = []; set.committedAt = null; set.completedAt = null; set.completionDay = null; await save(set);
    await Pool.update([...B.pool.rows.keys()].filter(id => (B.pool.rows.get(id) || {}).setId === set.setId), { state: "written" });
    for (const row of Orders.rows()) if (row.state === "committed" && reopened.includes(String(row.order.receiptId))) row.state = "written";
    if (B.run && (B.run.setId === set.setId || (B.run.setIds || []).includes(set.setId))) { B.run.status = "review"; B.run.step = "engrave"; await RunCtl.save(B.run); RunCtl.renderBanner(); }
    agent({ run: set.runId }, "DS", `${set.name}: completion undone on the station — set back to awaiting review, every file kept`);
    Orders.render();
  }
  /** The Library's Sets view: one card per set, its sheets side by side, held orders, engraving count, label thumbnails. */
  function libraryGroups(sets, sheets) {
    const groups = new Map(sets.filter(s=>s.status !== "superseded").map(s=>["set:"+s.setId, Object.assign({},s,{sheets:[]})]));
    for (const sheet of sheets) {
      const meta=O.libraryGroup(sheet, {combineSolids:true}), key=meta.key;
      if (!groups.has(key)) groups.set(key, {...meta, runId:sheet.runId, day:sheet.day, status:meta.standalone ? "not included in a set" : meta.working ? "held for a later set" : "saved", sheets:[], materials:[], orders:{}});
      const group=groups.get(key); group.sheets.push(sheet);
      group.materials=[...new Set([...(group.materials || []),sheet.metal])];
      group.updatedAt=Math.max(+group.updatedAt || 0,+sheet.updatedAt || 0);
      for (const id of sheet.orders || []) if (!group.orders?.[id]) (group.orders ||= {})[id]={};
    }
    return [...groups.values()].sort((a,b)=>a.setId && b.setId ? O.compareCompleted(a,b) : a.setId ? -1 : b.setId ? 1 : String(b.day || "").localeCompare(String(a.day || "")));
  }
  let _cache = null, libraryRequest = 0;
  async function renderLibrary(body, opts) {
    const request = ++libraryRequest;
    const reuse = !!(opts && opts.reuse && _cache);
    if (!reuse && !_cache) body.innerHTML = `<div class="libEmpty">Loading sets…</div>`;
    try {
      if (!reuse) {
        const [ss, sh] = await Promise.all([api("charmNestLibrary", {op:"setList", includeSheets:true, limit:200}), api("charmNestLibrary", {op:"listSheets", limit:500})]);
        if (request !== libraryRequest || S.library.kind !== "sets") return;
        const records = [...new Map([...(sh.sheets || []), ...(ss.sheets || [])].map(r=>[r.id,r])).values()];
        _cache = {rawSets:ss.sets || [], rawSheets:records, sets:[], sheets:records};
      }
      if (S.library.kind !== "sets") return;
      _cache.sheets=Gate.projectLibraryRecords(_cache.rawSheets || _cache.sheets);
      _cache.sheets.forEach(LaserReview.record);
      _cache.sets=libraryGroups(_cache.rawSets || [],_cache.sheets);
      const sheets = _cache.sheets;
      // the metal chips and the search used to light up and change nothing here: this view read neither
      const metal = S.library.metal && S.library.metal !== "all" ? S.library.metal : null;
      const q = (document.getElementById("libSearch").value || "").trim().toLowerCase();
      let sets = _cache.sets;
      if (metal) sets = sets.filter(st => (st.materials || []).includes(metal));
      if (q) sets = sets.filter(st => `${st.setId ? O.completedTitle(st) : st.name || ""} ${st.setId || ""} ${st.setId ? O.completionDay(st) : st.day || ""} ${st.runId || ""} ${Object.keys(st.orders || {}).join(" ")} ${(st.sheets || []).flatMap(r=>[r.fileBase,r.names,...(r.backs || []).map(b=>b.text)]).join(" ")}`.toLowerCase().includes(q));
      if (!sets.length) { body.innerHTML = `<div class="libEmpty">${q || metal ? "No sets match this filter." : "No sets yet."}</div>`; return; }
      LaserReview.sections(body);
      for (const st of sets) {
        const all = st.sheets.slice().sort((a, b) => (a.metal || "").localeCompare(b.metal || "") || (a.sheetIndex || 0) - (b.sheetIndex || 0));
        const mine = metal ? all.filter(x => x.metal === metal) : all;
        const held = Object.entries(st.orders || {}).filter(([, o]) => o.held);
        const card = el("div", "setCard"); card.dataset.laserCard="set";card._laserSet=st;card._laserSheets=all.map(r=>r.id);
        card.innerHTML = `${st.standalone || st.working ? `<div class="sh"><span class="nm">${st.standalone ? "14K / 10K Solid Sheets" : "Sheets"}</span></div>` : `<div class="sh"><span class="nm" data-set-title>${esc(O.setLabel(st.seq))}${st.day?" · "+esc(st.day):""}</span>${st.labels?.pdf || st.labels?.manifest || st.labels?.json || /complete/.test(st.status) ? `<details class="setActions"><summary aria-label="Set file menu">⋯</summary><div>${st.labels?.pdf ? `<a href="${st.labels.pdf.url}" target="_blank" rel="noopener">Labels PDF</a>` : ""}${st.labels?.manifest ? `<a href="${st.labels.manifest.url}" target="_blank" rel="noopener">Manifest</a>` : ""}${st.labels?.json ? `<a href="${st.labels.json.url}" target="_blank" rel="noopener">Set data</a>` : ""}${/complete/.test(st.status) ? `<button class="btn ghost xs" data-undo="${esc(st.setId)}">Undo set</button>` : ""}</div></details>` : ""}</div>`}
          <div class="sheetsRow">${mine.map(r => `<article class="librarySheet"><div class="libCard hoverItem" data-m="${r.metal}" data-id="${r.id}" title="${esc(r.folder || r.id)}">${window.sheetHead ? sheetHead(r, { inFan: true }) : `<div class="h"><span class="nm">${esc(r.folder || r.id)}</span></div>`}<div data-back-sheet="${esc(r.id)}">${Engrave.backsMarkup(r)}</div><img class="pv" data-sheet-preview="${esc(r.id)}" crossorigin="anonymous"${r.preview ? ` src="${esc(cors(r.preview))}"` : ""} loading="lazy" alt="Sheet preview"><div class="m"><span><b>${r.placedCount}</b>/${r.charmCount}</span><span><b>${Math.round((r.density || 0) * 100)}%</b></span><span>${(r.orders || []).length} orders</span><span class="sheetBackStatus" data-sheet-status="${esc(r.id)}" aria-live="polite">${window.CharmNestReadiness.counter(LaserReview.sheet(r))}</span></div></div>${LaserReview.labels(r,(st.labelFiles || []).filter(f=>f.sheetId===r.id))}</article>`).join("") || "<div class='libEmpty'>no sheets recorded</div>"}</div>
          ${held.length ? `<div class="holds"><b>Held:</b> ${held.map(([rid, o]) => `${esc(rid)} — ${esc(o.held.why || "")}`).join(" · ")}</div>` : ""}
          ${st.refused && st.refused.length ? `<div class="holds"><b>Refused by the station:</b> ${st.refused.map(r => `${esc(r.id)} — ${esc(r.reason)}`).join(" · ")}</div>` : ""}
          `;
        card.querySelectorAll(".libCard").forEach(x => x.onclick = () => openLibrarySheet(x.dataset.id));
        card.querySelectorAll("[data-big]").forEach(img => img.onclick = () => { const d = document.createElement("dialog"); d.className = "wide"; d.innerHTML = `<div class="dlg"><div class="dlgHead"><h3>${esc(img.title)}</h3><div class="right"><button class="btn ghost xs">Close</button></div></div><div class="dlgBody" style="display:grid;place-items:center"><img crossorigin="anonymous" class="labelBig" src="${esc(img.src)}" alt=""></div></div>`; d.querySelector("button").onclick = () => d.close(); d.addEventListener("close", () => d.remove()); document.body.appendChild(d); d.showModal(); });
        const ub = card.querySelector("[data-undo]"); if (ub) ub.onclick = async () => { if (!confirm(`Undo the completion of ${st.name}? The orders return to the station's list; every file is kept.`)) return; const local = [...byRun().values()].find(x => x.setId === st.setId) || Object.assign({ orders: {}, sheetIds: st.sheetIds || [], materials: st.materials || [], labelFiles: st.labelFiles || [] }, st); byRun().set(local.runId || st.setId, local); try { await undo(local); toast(`${st.name} undone`, "ok"); renderLibrary(body); } catch (e) { toast(e.message, "bad", 6000); } };
        LaserReview.place(card,LaserReview.group(st,all).ready,body);
      }
      LaserReview.changed();
    } catch (e) { if (request === libraryRequest && S.library.kind === "sets") body.innerHTML = `<div class="libEmpty">Could not load sets: ${esc(e.message)}</div>`; }
  }
  return { releaseIssue, ensure, ofRun, keyOf, labelsReady, onSheetSaved, finalize, commit, undo, evaluate, save, renderLibrary, renderLabelPng, sheetsOf, setOfSheet, byRun };
})();

/* ═══ 23 · RunCtl — the two halves, the record, resume, stop, Auto/Manual ═══ */
const RunCtl = window.RunCtl = (() => {
  /* A run used to halt after five of its own steps and ask, with a button named after the source code, whether to do
     the next one. Nobody could answer that question usefully, and nothing was gained by asking it: a run that has been
     started is a run that should finish. Processing continues past pending sheets, review and engraving decisions.
     Release checks remain strict; only an operator stop or a critical failure interrupts the run.
     Manual and Auto still differ, in the one place the difference means anything: Auto starts the next run when this
     one is done, Manual does not. */
  const PAUSE_AFTER = new Set();
  let waiter = null, autoTimer = null;
  let settling = null;   // a stopped run whose Resume waits for its new orders' step to wind down (resume)
  const run = () => B.run;
  let saveQueue = Promise.resolve();
  /* The record is one Firestore document: 1 MiB and 40,000 index entries at most. It kept every line the run ever took,
     and a run left in Auto for days (new arrivals join the open run) stopped saving at 1,100–1,400 lines and stopped
     with it. The lines of an order the run is done with (O.closedOrders: cut and committed, or gone from Etsy) are now
     written once to the run's line archive and left out of the record, with that order's committed id and hold; the
     oldest sheet notes are left out too, and lineArchive counts what is outside. The server reads the archive under the
     record wherever a finished line is still wanted: laser readiness, a set's completion, a Rose Gold cut, history, a
     recalled set's orders. A resume takes the orders still in progress, which are all in the record. This page keeps
     every line in memory and in the browser workspace, as before. A record that still nears the limit is said once. */
  const warned = new Set();
  const finishedHashes = lines => { const out = new Map(); for (const keys of O.closedOrders(lines).values()) for (const k of keys) out.set(k, O.textHash(JSON.stringify(lines[k]))); return out; };
  /** Each finished line the archive does not hold in its current form goes there, in parts of at most 256 KB. */
  async function archiveFinished(r, lines, hashes) {
    const done = r.archivedLines || (r.archivedLines = {});
    // a mark is read only for a line in the run's lines (here and in recordOf): one whose line and row are gone goes too
    for (const k of Object.keys(done)) if (!(k in lines) && !B.orders?.byKey?.has(k)) delete done[k];
    const due = [...hashes].filter(([k, h]) => done[k] !== h).map(([k]) => [k, lines[k]]);
    if (!due.length) return;
    const parts = O.archiveParts(due);
    try {
      for (let i = 0; i < parts.length; i += 4) {
        const batch = parts.slice(i, i + 4);
        await api("charmNestLibrary", { op: "runArchive", runId: r.runId, parts: batch.map(p => ({ json: p.json })) }, { quiet: true });
        for (const p of batch) for (const k of p.keys) done[k] = hashes.get(k);
        const la = r.lineArchive || (r.lineArchive = {}); la.parts = (la.parts || 0) + batch.length; la.at = Date.now();
      }
    } catch (e) {
      // what was not archived stays in the record, as it always did, and is tried again at the next save
      if (!warned.has("archive:" + r.runId)) { warned.add("archive:" + r.runId); agent({ run: r.runId }, "warn", `Finished orders could not be moved out of the run record (${e.message}). They stay in it and are tried again at the next save.`); }
    }
  }
  /** The record as it is stored online: the lines of the orders still in progress (and any finished line the archive does
      not hold yet), and lineArchive, which counts what was left out. */
  function recordOf(r, hashes) {
    const lines = r.lines || {}, done = r.archivedLines || {}, la = r.lineArchive || null;
    /* The record goes out as JSON the moment it is made (api writes the text before anything else runs), so it shares the
       run's own lines and lists; only what is changed below is copied (holds, sheet notes). Two deep copies of the
       whole run used to be made at every save. */
    const rec = Object.assign({}, r, { errors: (r.errors || []).slice(-50) }); delete rec.lines; delete rec.archivedLines; delete rec.lineArchive;
    delete rec._wait; delete rec.saveError;
    // an order leaves the record only when the archive holds each of its lines exactly as they are now
    const out = new Set(); hashes = hashes || finishedHashes(lines);
    for (const [id, keys] of O.closedOrders(lines)) if (keys.every(k => done[k] && done[k] === hashes.get(k))) out.add(id);
    const kept = {}; let left = 0;
    for (const [k, l] of Object.entries(lines)) { if (l && out.has(String(l.orderId))) left++; else kept[k] = l; }
    rec.lines = kept;
    if (rec.holds && typeof rec.holds === "object") rec.holds = Object.assign({}, rec.holds);
    if (rec.sheets && typeof rec.sheets === "object") rec.sheets = Object.assign({}, rec.sheets);
    const isOut = id => out.has(String(id));
    let committed = 0, held = 0, sheets = 0;
    if (Array.isArray(rec.orders)) rec.orders = rec.orders.filter(id => !isOut(id));
    if (Array.isArray(rec.committed)) { const n = rec.committed.length; rec.committed = rec.committed.filter(id => !isOut(id)); committed = n - rec.committed.length; }
    if (rec.holds && typeof rec.holds === "object") for (const id of Object.keys(rec.holds)) if (isOut(id)) { delete rec.holds[id]; held++; }
    // one note per sheet the run wrote, read only for counts: the newest hundred stay
    const notes = Object.keys(rec.sheets || {}), keep = O.RUN_RECORD.keepSheets;
    if (notes.length > keep) { for (const k of notes.slice(0, notes.length - keep)) delete rec.sheets[k]; sheets = notes.length - keep; }
    // a record written before the work signature was a hash carries the whole text of it
    if (typeof rec.processingSignature === "string" && rec.processingSignature.length > 64) rec.processingSignature = O.textHash(rec.processingSignature);
    const base = (la && la.base) || {};
    if (la || left || committed || held || sheets) rec.lineArchive = { lines: (+base.lines || 0) + left, committed: (+base.committed || 0) + committed, held: (+base.held || 0) + held, sheets: (+base.sheets || 0) + sheets, parts: (la && la.parts) || 0, at: (la && la.at) || null };
    return rec;
  }
  /** Said once a run: a record past 70% of what one document holds. The run stops the day it is full. */
  function sizeCheck(r, rec) {
    if (warned.has(r.runId)) return;
    const bytes = O.utf8Bytes(JSON.stringify(rec)), entries = O.indexEntries(rec), L = O.RUN_RECORD;
    const share = Math.max(bytes / L.bytes, entries / L.entries);
    if (share < L.warn) return;
    warned.add(r.runId);
    const text = `The online record of run ${r.runId} is ${Math.round(share * 100)}% full (${Math.round(bytes / 1024).toLocaleString()} KB of 1,024 KB; ${entries.toLocaleString()} of 40,000 index entries), with ${Object.keys(rec.lines || {}).length} order lines still in progress in it. Finish or skip the orders waiting in this run, or give it up and start a new one, before it fills: a full record cannot be saved and the run stops.`;
    agent({ run: r.runId }, "warn", text); toast(text, "bad", 15000);
  }
  async function save(r) {
    r = r || B.run; if (!r) return;
    r.updatedAt = Date.now();
    if (r === B.run && Orders.rows().length) { const ids = new Set((r.orders || []).map(String)); r.lines = Object.fromEntries(Orders.rows().filter(row => ids.has(String(row.order.receiptId))).map(Orders.lineRecord)); }
    Session.schedule();
    if (!S.cloud.ok) { r.saveError = "Cloud offline — work is saved on this browser; reconnect to save the run online"; renderBanner(); return; }
    const persist=async()=>{
      const lines=r.lines || {},hashes=finishedHashes(lines),done=r.archivedLines || {};
      // the finished lines are hashed once a save: only a save that writes to the archive (and so waits) hashes them again
      const moving=[...hashes].some(([k,h])=>done[k]!==h);
      if(moving)await archiveFinished(r,lines,hashes);
      const rec=recordOf(r,moving?null:hashes);
      sizeCheck(r,rec);
      return api("charmNestLibrary",{op:"runPut",run:rec});
    };
    const write=window.CharmNestOperations ? window.CharmNestOperations.run({key:'run-save:'+r.runId,label:'Saving run',resources:['run-record:'+r.runId],latest:true},persist) : saveQueue.catch(()=>{}).then(persist);
    saveQueue = write;
    try { await write; r.cloudSavedAt = Date.now(); delete r.saveError; }
    catch (e) { r.saveError = e.message; agent({ run: r.runId }, "warn", `Run not saved online: ${e.message}`); throw e; }
    finally { renderBanner(); if (window.RunHistory) RunHistory.refreshIfOpen(); }
  }
  function newRun(mode) { const day = today(); return { runId: `run-${day}-${uid()}`, day, setId: null, releasePolicy: 2, solidIncluded: Object.assign({}, Gate.state().solidIncluded || {}), step: "pull", status: "running", mode: mode || S.settings.runMode || "manual", startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; }
  async function start(opts = {}) {
    if (B.run && ["running", "review", "paused", "processed", "stopped"].includes(B.run.status)) { toast("A run is already open — resume, finish, or abandon it before starting another", "bad"); return B.run; }
    if (B.run) await save(B.run);
    if ((B.run || Recall.on()) && clearRunState()===false) return B.run;
    const r = newRun(opts.mode); Gate.state().solidIncluded = {}; B.run = r;
    agent({ run: r.runId }, "DS", `Run ${r.runId} started (${r.mode} mode)`);
    await save(r); renderBanner();
    loop().catch(e => { stop(e.message, "Fix the cause and press Resume."); });
    return r;
  }
  let loopTask=null,loopAgain=false;
  function loop(){
    if(loopTask){loopAgain=true;return loopTask;}
    loopTask=loopNow().finally(()=>{loopTask=null;const again=loopAgain;loopAgain=false;if(again && B.run?.status==='running')queueMicrotask(()=>loop().catch(e=>stop(e.message,'Fix the cause and press Resume.')));});return loopTask;
  }
  async function loopNow() {
    const r = B.run;
    while (r && r===B.run && r.status === "running") {
      if(r.membershipNext){await Gate.flush(r);r.step=r.membershipNext;delete r.membershipNext;}
      const step = r.step, membershipRevision=r.membershipRevision || 0;
      renderBanner();
      /* A step that fails for a passing reason — the station did not answer, the network dropped, a function answered
         5xx — is tried again, twice, with a breath in between, before the run stops. The stages know what to expect
         of each other; a slow reply is not a reason to leave a whole day's orders standing. */
      let outcome = null;
      for (let attempt = 0; ; attempt++) {
        try { outcome = await doStep(r, step); break; }
        catch (e) {
          const passing = /answer in time|timed out|network|Failed to fetch|HTTP 5\d\d|link is down|no reply|closed/i.test(e.message || "");
          if (!passing || attempt >= 2 || r.status !== "running") throw e;
          agent({ run: r.runId }, "warn", `${step}: ${e.message} — trying again (${attempt + 1} of 2)`);
          await new Promise(res => setTimeout(res, 4000 * (attempt + 1)));
        }
      }
      if (r!==B.run || r.status !== "running") break;
      if ((r.membershipRevision || 0)!==membershipRevision && O.stepIndex(step)>=O.stepIndex('checkpoint')) {
        await Gate.flush(r);r.step=r.membershipNext || 'engrave';delete r.membershipNext;await save(r);continue;
      }
      if (outcome && outcome.done) break;
      if (outcome && outcome.goto) { r.step = outcome.goto; await save(r); continue; }
      const next = O.nextStep(step);
      // engraving finished beside the run after it passed Engraving: the steps after it run again before the run rests
      if (!next && seenAtEngrave !== backgroundDone) { r.step = "engrave"; continue; }
      if (!next) { updatePending(r); r.status = hasPending(r) ? "processed" : "complete"; r.processingComplete = true; r.finishedAt = Date.now(); r.processingSignature=workSignature(r); await save(r); renderBanner(); onComplete(r); break; }
      r.step = next;
      if (r.mode === "manual" && PAUSE_AFTER.has(step)) { r.status = "paused"; await save(r); renderBanner(); agent({ run: r.runId }, "DS", `Paused after ${step} — press Next step (${next})`); break; }
      // The record follows the steps without holding them up: step saves coalesce into the latest one, and the run waits
      // for its record only where it comes to rest (processed, complete, paused, stopped). Every step can be run again,
      // so a record one step behind after a crash only repeats a step. A save that fails says so on the banner.
      save(r).catch(() => {});
    }
  }
  async function next() { const r = B.run; if (!r || r.status !== "paused") return; r.status = "running"; await save(r); loop().catch(e => stop(e.message, "Fix the cause and press Resume.")); }
  async function doStep(r, step) {
    if (["labels","commit"].includes(step)) await Gate.flush(r);
    switch (step) {
      case "pull": { const rows = await Orders.pull(r, { silent: true }); if (!rows.length) { r.nothingToCut=true; return {goto:"complete"}; } return; }
      case "claim": { Orders.claim([...new Set(Orders.rows().map(x => x.order.receiptId))]).catch(() => {}); r.claimedAt = Date.now(); return; }
      case "pool": { const n=await Pool.addAll(r); if(!n){r.nothingToCut=true;updatePending(r);agent({run:r.runId},"info","No eligible pieces to nest; unresolved items remain available for follow-up.");return {goto:"complete"};} Engrave.background(r);return; }
      case "plan": { for (const m of METALS) { const pg = activePage(m.key); if (!pg.charms.some(c => c.poolId)) continue; const sat = computeSaturation(pg); agent({ metal: m.key, run: r.runId }, "info", `${labelOf(m.key)}: ${sat.count} piece(s) need ${fmt.pct(sat.totalNeeded / sat.usable)} of the plate — ${sat.recommend ? "over the " + fmt.pct(sat.rho.cap) + " ceiling, the overflow goes to a second sheet" : "under the " + fmt.pct(sat.rho.cap) + " ceiling"}`); } return; }
      case "nest": { await nestAll(r); return; }
      case "checkpoint": { await Arrivals.processPending?.({checkpoint:true}); if(r.status!=="running")return; await LiveNest.finish(r); await Gate.assemble(r); const sets = Sets.ofRun(r.runId); for (const set of sets.filter(s=>!s.committedAt)) { set.status = "awaiting review"; await Sets.save(set); } r.setIds = sets.map(x => x.setId); r.setId = r.setIds[0] || r.setId || null; r.status = "running"; await save(r); return; }
      case "engrave": {
        // Nothing here waits for engraving (Paul, 24 Sep): approved backs not written yet are written, and new lines'
        // words read and fitted, beside the run. A set still waits for them before it goes to the laser, and the run
        // looks again when they are done (backgroundSettled).
        seenAtEngrave = backgroundDone;
        for (const j of Engrave.items().values()) if (!j.editingBack && j.state === "approved" && !j.backSaving && (j.fit && j.view || j.writtenFit && j.writtenFit.approvedAt === j.approvedAt) && j.copies.length && Pool.sheetOf(j.copies[0])) { j.copies = j.row.poolIds.filter(id=>!(j.copyOverrides || []).includes(id)); Engrave.saveBacks(j).catch(e => agent({ engrave: true }, "warn", `${j.row.order.receiptId}: ${e.message}`)); }
        Engrave.background(r); updatePending(r); return;
      }
      case "revalidate": {
        // Etsy is read again right before labels are made. With no set ready for its labels (sheets still filling, the
        // usual state between arrivals), there is nothing to protect yet and the sweep only held the next orders back.
        if (!Sets.ofRun(r.runId).some(s => { try { return s.sheetIds.length && !s.committedAt && !Sets.releaseIssue(s); } catch (_) { return true; } })) return;
        const v = await Orders.revalidate(r, "before labels"); if (v.changed.length && [...Engrave.items().values()].some(j => j.state === "classify")) { agent({ run: r.runId }, "warn", `${v.changed.length} order(s) changed — back to engraving`); return { goto: "engrave" }; } return; }
      case "labels": {
        r.deferredSets={};
        for(const set of Sets.ofRun(r.runId).filter(s=>s.sheetIds.length&&!s.committedAt)){
          const issue=Sets.releaseIssue(set);
          if(issue){await deferSet(r,set,issue);continue;}
          try{await Sets.finalize(set);}catch(e){if(!e.releasePending)throw e;await deferSet(r,set,e.message);}
        }
        return;
      }
      case "commit": {
        if (Gate.modern(r.runId) && !Sets.ofRun(r.runId).some(s => s.sheetIds.length)) { r.nothingToCut = true; return; }
        const eligible=[];
        for(const set of Sets.ofRun(r.runId).filter(s=>s.sheetIds.length&&!s.committedAt)){
          const issue=Sets.releaseIssue(set);if(issue)await deferSet(r,set,issue);else eligible.push(set);
        }
        if(!eligible.length){r.awaitCommit=false;return;}
        if (S.settings.autoCommit === "off" && !r.commitRequested) { r.awaitCommit = true; agent({ run: r.runId }, "DS", "Processing finished. Ready sets are saved; Commit set remains available."); return; }
        r.awaitCommit=false;
        const v = await Orders.revalidate(r, "before commit");
        if (v.changed.length && [...Engrave.items().values()].some(j => j.state === "classify")) { agent({ run: r.runId }, "warn", `${v.changed.length} order(s) changed before the commit — back to engraving`); r.commitRequested = false; return { goto: "engrave" }; }
        const all = { completed: [], refused: [], held: {} };
        for (const set of eligible) { try { const res = await Sets.commit(set, r); if(res.membershipChanged)return {goto:r.membershipNext || 'engrave'}; all.completed.push(...res.completed); all.refused.push(...res.refused); Object.assign(all.held, res.held || {}); } catch(e){if(!e.releasePending)throw e;await deferSet(r,set,e.message);} }
        r.committed = [...new Set([...(r.committed||[]),...all.completed])]; r.refused = all.refused; r.holds = {...r.holds,...all.held}; return;
      }
      case "complete": {
        // The gold dot says "in a Charm Sorter run": it comes off an order once the run is done with it (cut and committed,
        // gone from Etsy, skipped, or nothing to make). In Auto the run passes this step after every update while orders
        // still wait on partial sheets; lifting every dot here left those orders unmarked at the station, free to be made
        // by hand a second time, and a contended order lost the dot of the run that holds it. A run that ends with nothing
        // left to do lets go of every order, as before.
        const done = new Set(["committed", "gone", "skipped", "noDesign"]);
        updatePending(r);
        const open = hasPending(r) ? new Set(Orders.rows().filter(x => !done.has(x.state)).map(x => x.order.receiptId)) : new Set();
        Orders.unclaim([...new Set(Orders.rows().map(x => x.order.receiptId))].filter(id => !open.has(id))).catch(() => {});
        return;
      }
      default: return;
    }
  }
  /** Nest every card that holds this run's pool charms; wait until every sheet of the run (overflow included) is written, verified and saved. */
  function nestAll(r) {
    return new Promise((resolve, reject) => {
      const pages = allSheets().filter(pg => pg.runId === r.runId && !pg.runHold && Gate.nestable(pg, r) && pg.charms.some(c => c.poolId && !c.excluded));
      if (!pages.length) return resolve();
      waiter = { r, resolve, reject };
      for (const pg of pages) {
        if (["complete","partial"].includes(pg.status) && pg.verification && !pg.verification.ok) holdSheet(r,pg,"Verification flagged — open the report");
        else if(pg.status === "error") holdSheet(r,pg,pg.problem || "Sheet needs review");
        else if (pg.status === "ready" || pg.status === "idle") startNest(pg);
        else if (["complete", "partial"].includes(pg.status) && pg.persisted) pg.persisted.then(() => onSheetDone(pg));
      }
      onSheetDone(null);
    });
  }
  function onSheetDone(sh, err) {
    const r = B.run; if (!r) return;
    if (sh && sh.runId === r.runId) {
      r.sheets[sh.sheetId || sh.metal + "-" + sh.page] = { metal: sh.metal, page: sh.page, status: sh.status, verified: !!(sh.verification && sh.verification.ok), placed: sh.placements.length, rejects: sh.rejects.length, fileBase: sh.fileBase || null, error: err ? err.message : null };
      // the notes are read only for counts: memory keeps the newest hundred, as the record does, and counts the rest with
      // what a resumed run's record left out (lineArchive.base), so a run left on for weeks does not grow (Paul, 24 Sep)
      { const notes = Object.keys(r.sheets), over = notes.length - ((O.RUN_RECORD && O.RUN_RECORD.keepSheets) || 100); if (over > 0) { for (const k of notes.slice(0, over)) delete r.sheets[k]; const la = r.lineArchive || (r.lineArchive = {}), base = la.base || (la.base = {}); base.sheets = (+base.sheets || 0) + over; } }
      /* A sheet's trouble belongs on that sheet's card. The banner used to carry the whole message — "GF 14/20 · sheet 3:
         bad row — Fix the cause (see the card's log), then Resume" — across every tab, above every list, for the rest of
         the session. Now the banner names the sheet and offers the way to it; the reason is written where the sheet is. */
      if (err && (err.critical || err.stage === "persistence")) {
        sh.problem = err.message; CN.renderCard(sh);
        stop(`${sheetName(sh)} could not be saved`, "Reconnect and Resume; existing work is retained.", { metal: sh.metal, page: sh.page }, /answer in time|timed out|network|Failed to fetch|HTTP 5\d\d|link is down|no reply|closed|offline/i.test(err.message || "") ? "transient" : null);
        return;
      }
      if (err) holdSheet(r,sh,err.message);
      if (err) { /* Keep the local hold while other sheets finish. */ }
      else if (["complete", "partial"].includes(sh.status) && sh.verification && !sh.verification.ok) holdSheet(r,sh,"Verification flagged — open the report");
      // (one the run's own Stop ended waits for Resume instead, which carries on with it: see finishNest)
      else if (sh.endedBy === "stopped" && !sh.resumeWait) holdSheet(r,sh,"This sheet was stopped; retry it when ready");
      else if(!sh.runHold && sh.verification?.ok && sh.fileBase){delete (r.sheetHolds||{})[sh.sheetId || sh.metal+"-"+sh.page];}
      save(r).catch(() => {});
    }
    if (!waiter || waiter.r !== r) { poke(); return; }
    const pages = allSheets().filter(pg => pg.runId === r.runId && !pg.runHold && Gate.nestable(pg, r) && pg.charms.some(c => !c.excluded));
    const writes = allSheets().filter(pg => pg.runId === r.runId && pg.persisted && !pg.persistedDone);
    // A sheet that took new pieces while it was nesting (a Review decision re-pooled a line onto it) finished on the
    // layout it started with and stayed dirty. Nothing arranged it again, and this step waited on it for good: the run
    // sat at "nest" and every later update's orders queued behind it.
    for (const pg of pages) if (pg.dirty && ["complete", "partial"].includes(pg.status) && !(pg.persisted && !pg.persistedDone) && !pg._operationStarting && !pg._learnedStarting) { pg.status = "ready"; startNest(pg); }
    const busy = writes.length || pages.some(pg => ["nesting", "finishing", "queued", "ready", "idle"].includes(pg.status) || pg.dirty);
    for (const pg of writes) if (pg.persisted && !pg.persistedDone) pg.persisted.then(() => { pg.persistedDone = true; onSheetDone(null); }, () => { pg.persistedDone = true; onSheetDone(null); });
    if (busy) return;
    const notWritten = pages.filter(pg => !pg.fileBase);
    for(const pg of notWritten)holdSheet(r,pg,"Sheet unfinished — retry Nest when ready");
    const w = waiter; waiter = null; w.resolve();
  }
  function holdSheet(r,sh,why){sh.runHold=why;sh.problem=why;(r.sheetHolds||={})[sh.sheetId || sh.metal+"-"+sh.page]=why;CN.renderCard(sh);}
  // a set that already waits for the same reason is not saved again at every pass (engraving read beside the run passes often)
  async function deferSet(r,set,why){(r.deferredSets||={})[set.setId]=why;if(set.status==="awaiting review"&&set._deferredFor===why)return;set.status="awaiting review";await Sets.save(set);set._deferredFor=why;}
  function updatePending(r){
    const pages=allSheets().filter(p=>p.runId===r.runId&&p.charms.length);
    r.pendingWork={orders:Orders.rows().filter(row=>["held","waiting","pulled","pooled"].includes(row.state)).length,sheets:pages.filter(p=>p.runHold || !p.outputs || !p.verification?.ok || p.dirty).length,review:Review.count(),engraving:Engrave.pendingCount(),sets:Object.keys(r.deferredSets||{}).length,commit:!!r.awaitCommit};
    return r.pendingWork;
  }
  function hasPending(r){return Object.values(r.pendingWork||{}).some(Boolean);}
  /* The work still to do: the lines not finished, their engraving (and any back being edited), the run's sheets not cut.
     A finished line (committed, gone, skipped, no design) gives the run work only by leaving that state, which changes
     the text as well; the text used to hold every line and job the run ever took, and grew with its history. */
  function workText(r){const done=new Set(),rows=[];for(const x of Orders.rows()){if(O.FINISHED_LINE.has(x.state))done.add(x.key);else rows.push([x.key,x.state,x.hold,x.changePending,x.engrave?.state,x.engrave?.approved]);}const cut=new Set(Sets.ofRun(r.runId).filter(s=>s.committedAt).flatMap(s=>s.sheetIds||[]));return JSON.stringify([r.membershipRevision||0,rows,[...Engrave.items().values()].filter(j=>j.editingBack||!done.has(j.key)).map(j=>[j.key,j.state,j.approvedAt]),allSheets().filter(p=>p.runId===r.runId&&!(p.sheetId&&cut.has(p.sheetId))).map(p=>[p.sheetId,p.metal,p.page,p.status,p.dirty,p.runHold,p.placements.length,p.persistedDone])]);}
  // the state of the work as a short hash: its whole text, kept in the record, grew with every order the run took
  function workSignature(r){return O.textHash(workText(r));}
  // a signature saved before it was a hash is the whole text, and still compares
  function workChanged(r){const text=workText(r);return r.processingSignature!==O.textHash(text) && r.processingSignature!==text;}
  function continueProcessing(r,step){
    if(r!==B.run || r.status==='stopped' || r.status==='abandoned')return;
    r.processingComplete=false;r.status='running';r.step=step;window.CN?.resumeQueueChanged?.();
    // the steps start at once and the record follows them, as between steps (loopNow); a record that cannot be saved still stops the run
    save(r).catch(e=>stop(e.message,'Fix the cause and press Resume.'));
    loop().catch(e=>stop(e.message,'Fix the cause and press Resume.'));
  }
  function optionsChanged(){
    const r=B.run;if(!r || ['complete','abandoned'].includes(r.status))return;
    r.membershipRevision=(r.membershipRevision||0)+1;r.membershipNext='nest';r.membershipDirty=true;r.commitRequested=false;
    if(r.status==='stopped' && !['Sheet options changed','Sheet settings changed'].includes(r.stoppedBy))return;
    if(r.status==='running'){membershipUpdated(r);loop().catch(e=>stop(e.message,'Fix the cause and press Resume.'));}
    else {r.status='running';r.stoppedBy=null;r.fix=null;continueProcessing(r,'nest');}
  }
  /* Work that runs beside the run (Engrave.background, Engrave.saveBacks) tells it when it is done: a resting run takes
     up its steps again at once (its record of the work may already hold the change, so it does not wait for one), and
     a run going through its steps goes through them again before it rests (loopNow). */
  let backgroundDone = 0, seenAtEngrave = 0;
  function backgroundSettled() { backgroundDone++; poke(true); }
  function poke(force) {
    const r=B.run;
    if(r?.status==='processed' && !r.arrivalBusy && (force === true || workChanged(r))){
      r.processingSignature=workSignature(r);
      const needsNest=allSheets().some(p=>p.runId===r.runId&&!p.runHold&&Gate.nestable(p,r)&&p.charms.length&&(p.dirty||['ready','idle','queued','nesting','finishing'].includes(p.status)));
      continueProcessing(r,needsNest?'nest':'engrave');
    }
    renderBanner();LiveStrip.render();
  }
  /* Auto used to finish a set, blank the screen and refill it minutes later with no countdown anywhere — a person came
     back from the bench to an empty app and could not tell whether the shift was done or something had crashed. */
  const NEXT = { at: 0, t: 0 };
  function armNext(ms) { NEXT.at = Date.now() + ms; clearInterval(NEXT.t); NEXT.t = setInterval(() => { if (!NEXT.at) { clearInterval(NEXT.t); return; } renderBanner(); }, 1000); }
  function cancelNext() { NEXT.at = 0; clearInterval(NEXT.t); clearTimeout(autoTimer); renderBanner(); }
  /** A run that is given up lets go of its lines: its pool rows are marked abandoned and the station's claims are lifted,
      so the next run can take them without waiting a day for the rows to go stale. */
  function releaseRun(r) {
    // Giving up a run gives up its unfinished work only: a piece already cut in a committed set stays on record as cut,
    // where it used to be marked abandoned with the rest.
    const cutSets = new Set(Sets.ofRun(r.runId).filter(s => s.committedAt).map(s => s.setId));
    const cut = id => { const p = B.pool.rows.get(id) || {}; return p.state === "committed" || cutSets.has(p.setId); };
    const open = Orders.rows().filter(x => x.state !== "committed" && !(x.poolIds || []).some(cut));
    const ids = [...new Set(open.filter(x => x.poolIds && x.poolIds.length).flatMap(x => x.poolIds))];
    if (ids.length && S.cloud.ok) api("charmNestLibrary", { op: "poolUpdate", poolIds: ids, patch: { state: "abandoned" } }, { quiet: true }).catch(() => {});
    Orders.unclaim([...new Set(open.map(x => x.order.receiptId))]).catch(() => {});
    r.status = "abandoned"; save(r).catch(() => {});
  }
  // a stop the person pressed is answered by the banner; the red toast and the desktop alert are for stops nobody asked for
  // (a passing failure again while Auto retries it is said on the banner only, not with a toast every few minutes all night)
  function stop(why, fix, at, kind) { const r = B.run; if (!r) return; kind = kind || kindOf(why); const retrying = auto.runId === r.runId && auto.step === r.step, again = retrying && auto.n > 0 && ["transient", "watchdog"].includes(kind); r.status = "stopped"; r.stoppedBy = why; r.fix = fix || null; r.at = at || null; r.stopKind = kind; r.errors.push({ t: Date.now(), why }); if (retrying) auto.next = Date.now() + AUTO_WAIT[Math.min(auto.n, AUTO_WAIT.length - 1)]; if (waiter && waiter.r === r) { const w = waiter; waiter = null; w.resolve(); } agent({ run: r.runId }, "warn", `Run stopped: ${why}${fix ? " — " + fix : ""}`);
    if (r.errors.length > 50) r.errors.splice(0, r.errors.length - 50);   // memory keeps the last fifty, as the record does (recordOf): a run left on for weeks kept every stop
    if (why !== "stopped by the operator" && !again) { toast(`Run stopped: ${why}`, "bad", 8000); notifyPerson("Charm Sorter run stopped", why); }
    window.CN?.resumeQueueChanged?.();   // the run's queued sheets start on Resume
    save(r).catch(() => {}); renderBanner(); }
  /** Stop on the banner. While new orders are going on (Adding new orders) the run's charm searches end at once as well,
      as the card's own Stop ends one: every charm already placed stays where it is, and the orders not placed yet wait on
      their sheets for Resume (finishNest). Otherwise the sheets already nesting finish and save, as before. */
  function operatorStop() {
    const r = B.run; if (!r) return;
    const intake = !!r.arrivalBusy;
    stop("stopped by the operator", "Press Resume to carry on from the recorded step.", null, "operator");
    // (a sheet Claude was planning stops on the spot, back to ready: it takes its place in line for Resume too)
    if (intake) for (const sh of allSheets()) if (sh.runId === r.runId && sh.status === "nesting") { sh.resumeWait = true; stopNest(sh); if (sh.status === "ready") startNest(sh); }
  }
  function stopIfRunning(why, fix, kind) { if (B.run && B.run.status === "running") stop(why, fix, null, kind); }
  function reviewStop(r) { return r?.status === "stopped" && /needs a look|shape analysis failed|a back flip failed its checks|^Sheet options changed|^Sheet settings changed/i.test(r.stoppedBy || ""); }
  function preservePendingSheets(r) {
    for(const pg of allSheets().filter(p=>p.runId===r.runId)) {
      const selected=r.at && pg.metal===r.at.metal && pg.page===r.at.page;
      if(pg.verification && !pg.verification.ok || selected && pg.problem)holdSheet(r,pg,pg.problem || "Verification flagged — open the report");
    }
  }
  async function recoverReviewStop() {
    const r=B.run;if(!reviewStop(r))return false;
    await resume();return true;
  }
  async function resume() {
    const r = B.run; if (!r || !["stopped", "paused", "processed"].includes(r.status)) return;
    // Stopped while new orders went on: that step winds down first (a few tenths of a second) so it cannot carry on
    // beside the resumed run. Resume takes Stop's place on the banner, where a double click lands.
    if (r.status === "stopped" && r.arrivalBusy) {
      if (settling === r) return;
      settling = r; renderBanner();
      try { for (const t = Date.now(); r.arrivalBusy && Date.now() - t < 60000;) await new Promise(res => setTimeout(res, 100)); }
      finally { settling = null; }
      if (r !== B.run || r.status !== "stopped") { renderBanner(); return; }
    }
    const pendingStop=reviewStop(r);
    if(pendingStop)preservePendingSheets(r);
    await Gate.upgrade(r);
    if (r.status === "processed") { r.step = "engrave"; r.processingComplete = false; }
    // a sheet the stop left queued (waiting for Resume) is nested again as well
    if (allSheets().some(pg => pg.runId === r.runId && !(pendingStop && pg.runHold) && (pg.problem || pg.dirty || ["ready", "idle", "queued"].includes(pg.status)) && pg.charms.length)) {
      r.step = "nest"; for (const pg of allSheets()) if (pg.runId === r.runId && pg.problem && !(pendingStop && pg.runHold)) sheetDirty(pg);
    }
    r.status = "running"; r.stoppedBy = null; r.fix = null; delete r.stopKind; window.CN?.resumeQueueChanged?.(); await save(r);
    if (["nest"].includes(r.step)) { for (const pg of allSheets()) if (pg.runId === r.runId && !pg.runHold && ["complete", "partial"].includes(pg.status) && pg.verification && !pg.verification.ok) sheetDirty(pg); }
    if (!r.workspaceRestored && (r.step === "pool" || r.step === "pull" || r.step === "claim")) r.step = "pull";
    loop().catch(e => stop(e.message, "Fix the cause and press Resume."));
  }
  function membershipUpdated(r) {
    if(r!==B.run)return;
    if(r.step==='nest' && r.status==='running'){
      for(const pg of allSheets().filter(p=>p.runId===r.runId && !p.runHold && Gate.nestable(p,r) && p.charms.length && ['ready','idle'].includes(p.status)))startNest(pg);
      onSheetDone(null);
    }
    if(r.status==='processed' || (r.status==='paused' && r.awaitCommit) || (r.status==='stopped' && r.stoppedBy==='Sheet options changed')){
      r.awaitCommit=false;r.status='running';r.stoppedBy=null;r.fix=null;r.step=r.membershipNext || 'engrave';delete r.membershipNext;window.CN?.resumeQueueChanged?.();
      save(r).then(()=>loop()).catch(e=>stop(e.message,'Retry after fixing the cause.'));
    }
    poke();
  }
  async function commitNow() { const r = B.run; if (!r) return; r.commitRequested = true; r.awaitCommit = false; if (["paused","processed"].includes(r.status)) { r.status = "running"; r.processingComplete=false;r.step="labels"; await save(r); loop().catch(e => stop(e.message, "Fix the cause and press Resume.")); } }
  /** Resume a run from its record after a reload or a crash: at or before pool → start over from pull (pool ids are deterministic); later → the set's sheets are restored from their records first. */
  async function pickResume() {
    if (!S.cloud.ok) { toast("Cloud offline — nothing to resume", "bad"); return; }
    const r = await api("charmNestLibrary", { op: "runList", limit: 30 });
    const open = (r.runs || []).filter(x => !["complete", "abandoned"].includes(x.status));
    if (!open.length) { toast("No open run to resume", ""); return; }
    const pick = prompt(`Open runs:\n${open.map((x, i) => `${i + 1}. ${x.runId} · ${x.step} · ${x.status} · ${x.lines} line(s) · ${new Date(x.updatedAt).toLocaleString()}${x.stoppedBy ? " · " + x.stoppedBy : ""}`).join("\n")}\n\nNumber to resume (or a to abandon one):`, "1");
    if (!pick) return;
    const m = /^a\s*(\d+)/i.exec(pick.trim());
    if (m) {
      const x = open[+m[1] - 1]; if (!x) return;
      if (!confirm(`Give up run ${x.runId.slice(-8)}?\n\n${x.lines} line(s) at step ${x.step}. Anything decided but not written is lost.\n\nThe sheets and files already saved are kept.`)) return;
      await api("charmNestLibrary", { op: "runPut", run: { runId: x.runId, status: "abandoned", abandonedAt: Date.now() }, merge: true });
      toast(`${x.runId} abandoned`, "ok");
      return;
    }
    const x = open[+pick - 1]; if (!x) return;
    await resumeRun(x.runId);
  }
  async function resumeRun(runId) {
    const rr = await api("charmNestLibrary", { op: "runGet", runId }); const rec = rr.run; if (!rec) { toast("Run record not found", "bad"); return; }
    // the orders the run is done with are in its line archive, outside the record: still counted, never taken in again
    if (rec.lineArchive) { const la = rec.lineArchive; rec.lineArchive = { parts: +la.parts || 0, at: la.at || null, base: { lines: +la.lines || 0, committed: +la.committed || 0, held: +la.held || 0, sheets: +la.sheets || 0 } }; }
    if (rec.status === "processed") { rec.step = "engrave"; rec.processingComplete = false; }
    const pendingStop=reviewStop(rec);
    B.run = rec; rec.status = "running"; rec.stoppedBy = null; rec.fix = null;
    agent({ run: runId }, "DS", `Resuming ${runId} at step ${rec.step}`);
    if (O.stepIndex(rec.step) <= O.stepIndex("pool")) { rec.step = "pull"; }
    else {
      const savedStep = rec.step, savedLines = rec.lines || {};
      // A recovery uses this run's orders, regardless of the current pull limit. Pulling
      // into the run itself used to erase its checkpoint and all saved pool identities.
      await Orders.pull(null, { silent: true, receiptIds: rec.orders || Object.values(savedLines).map(l => l.orderId) });
      for (const row of Orders.rows()) {
        const old = savedLines[row.key];
        if (!old || +old.updateTs !== +row.order.updateTs) continue;
        for (const key of ["state", "poolIds", "reason", "hold", "wait", "engrave", "materialOverride", "sizeOverride", "changePending", "repoolChanged"]) if (old[key] != null) row[key] = old[key];
      }
      Orders.interpretAll();
      await restoreRunSheets(rec);
      if(pendingStop)preservePendingSheets(rec);
      await Pool.addAll(rec);                                              // idempotent: existing pool rows are the same ids; only unplaced lines get charms
      rec.step = allSheets().some(p => p.runId === runId && p.dirty) || ["nest", "checkpoint"].includes(savedStep) ? "nest" : savedStep;
    }
    await save(rec); renderBanner();
    loop().catch(e => stop(e.message, "Fix the cause and press Resume."));
  }
  async function restoreRunSheets(rec) {
    const setIds = [...new Set((rec.setIds || []).concat(rec.setId ? [rec.setId] : []))];
    const sets = [];
    for (const sid of setIds) {
      const sr = await api("charmNestLibrary", { op: "setGet", setId: sid }); const sd = sr.set; if (!sd) continue;
      const set = { setId: sd.setId, seq: sd.seq, day: sd.day, runId: rec.runId, group: sd.group || null, name: sd.name || O.setLabel(sd.seq), folder: sd.folder || O.setFolder(sd.day, sd.seq), orders: Object.fromEntries(Object.entries(sd.orders || {}).map(([rid, o]) => [rid, { held: o.held || null, lines: Object.fromEntries((o.lines || []).map(l => [l.transactionId, l])) }])), sheetIds: sd.sheetIds || [], materials: sd.materials || [], labelFiles: sd.labelFiles || [], labels: sd.labels || null, status: sd.status || "open", committedAt: sd.committedAt || null, completedAt: sd.completedAt || null, completionDay: sd.completionDay || null, committed: sd.committed || null, refused: sd.refused || null, backCount: sd.backCount || 0 };
      set.committedAt = sd.committedAt || null; set.completedAt = sd.completedAt || null; set.completionDay = sd.completionDay || null; set.savedCommit = set.committedAt;
      Sets.byRun().set(Sets.keyOf(rec.runId, set.committedAt ? "committed:" + set.setId : set.group), set); sets.push(set);
    }
    const bySet = new Map(sets.map(x => [x.setId, x]));
    const ls = await api("charmNestLibrary", { op: "listSheets", limit: 500, runId: rec.runId });

    for (const slim of ls.sheets || []) {
      const d = (await api("charmNestLibrary", { op: "getSheet", id: slim.id })).sheet; if (!d) continue;
      const prim = S.sheets[d.metal]; let pg = prim.pages.find(p => p.sheetId === d.id) || (prim.pages[0].charms.length ? addPage(d.metal) : prim.pages[0]);
      const set = d.draft ? null : bySet.get(d.setId);
      pg.draft = !!d.draft || !set; pg.releaseFull = !!d.releaseFull; pg.intakeFinalized = !!d.intakeFinalized; pg.intakeOptimized=!!d.intakeOptimized; pg.intakeOptimizedCount=+d.intakeOptimizedCount||0; pg.missRearranged=!!d.missRearranged; pg.topup=d.topup||null;
      pg.sheetId = d.id; pg.runId = rec.runId; pg.group = set ? set.group || null : "dispatch"; pg.setId = set ? set.setId : null; pg.seq = set ? d.setSeq || set.seq : null; pg.setDay = d.day; pg.cardStartedAt = d.cardStartedAt || d.createdAt || null; pg.sheetIndex = set ? d.sheetIndex : null; pg.fileBase = d.fileBase; pg.folderPath = d.outputs?.ai?.path?.replace(/\/[^/]+$/, "") || (set ? `${set.folder}/${d.fileBase}` : `charmnest/sheets/${d.day}/${d.fileBase}`); pg.label = set ? d.label || null : null; pg.backPool = d.backPool || []; pg.backOutputs = d.backOutputs || null; pg.cloud = d.outputs ? { ai: d.outputs.ai && d.outputs.ai.url, pdf: d.outputs.pdf && d.outputs.pdf.url, labelled: d.outputs.labelled && d.outputs.labelled.url, report: d.outputs.report && d.outputs.report.url, preview: d.outputs.preview && d.outputs.preview.url } : null;
      pg.restored = true; pg.persistedDone = true;
      if(d.metal==='rose' && d.roseStockId && window.RoseStock)await RoseStock.restore(pg,d);
      // the pieces: each placement's pool charm from the master copy, pinned at its cut position
      for (const p of d.placements || []) {
        const rc = (d.charms || []).find(c => c.id === p.id); if (!rc || !rc.poolId) continue;
        if (!Orders.rows().some(row => row.poolIds.includes(rc.poolId))) continue;
        const pool = B.pool.rows.get(rc.poolId) || (await api("charmNestLibrary", { op: "poolGet", poolIds: [rc.poolId] })).pools[rc.poolId]; if (!pool) continue; B.pool.rows.set(rc.poolId, pool);
        const entry = Master.entryFor(pool.sku) || await Master.fetchEntry(pool.sku); if (!entry) continue;
        const src = await Pool.masterCharm(entry, pool.size); const base = src.charms[0];
        const charm = Pool.cloneCharm(base, `${src.id}:${rc.poolId}`); charm.name = rc.name; charm.order = pool.orderId; charm.orderDate = pool.orderDate || 0; charm.arrivedAt = pool.arrivedAt || 0; charm.poolId = rc.poolId; charm.metal = d.metal; charm.lineKey = pool.lineKey; charm.orderInfo = { receiptId: pool.orderId, transactionId: pool.transactionId, sku: pool.sku, copy: pool.copy, quantity: pool.quantity, form: pool.form, size: pool.size }; charm.pinned = { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle };
        if (!pg.charms.some(c => c.poolId === rc.poolId)) pg.charms.push(charm);
        pg.placements = pg.placements.filter(x => x.id !== charm.id).concat([{ id: charm.id, angle: p.angle, cxPt: p.cxPt, cyPt: p.cyPt, wPt: p.wPt, hPt: p.hPt, layerName: p.layer, scale: 0.975 }]);
      }
      pg.status = d.status === "complete" ? "complete" : "partial"; pg.dirty = false; pg.verification = d.verification ? Object.assign({ ok: !!d.verification.ok, geom: { ok: !!d.verification.ok, res: 6, overlapPx: 0, outsidePx: 0, overlappingPairs: [] }, render: d.verification.render || null }, d.verification) : null; pg.endedBy = d.endedBy; pg.density = d.density; pg.liveInfo = { freePt2: d.freePt2, usablePt2: d.usablePt2, placedPt2: (d.usablePt2 || 0) - (d.freePt2 || 0), pocket: d.pocket, placed: pg.placements.length, total: pg.charms.length };
      pg.persisted = Promise.resolve();
      computeSaturation(pg); renderCard(pg);
      for (const row of Orders.rows()) { if (!row.poolIds.length) continue; if (row.poolIds.every(pid => { const pool = B.pool.rows.get(pid); return pool && pool.setId && ["written", "engraved", "labelled", "committed"].includes(pool.state); })) row.state = "written"; else if (row.poolIds.some(pid => (d.poolIds || []).includes(pid))) row.state = "pooled"; }
      for (const b of pg.backPool) { const row = Orders.rows().find(x => x.poolIds.includes(b.poolId)); if (row) { const j = Engrave.ensureJob(row); j.backSaving = false; j.state = "written"; j.text = b.text; j.lines = b.lines || String(b.text).split("\n"); j.approvedBy = b.approvedBy; j.approvedAt = b.approvedAt; j.backs.push(b); row.engrave = { needed: true, state: "written", approved: true, text: b.text, approvedBy: b.approvedBy }; } }
      // Cloud-only recovery has file links, not the editable output bytes. A working
      // sheet must be rebuilt and verified before it can acquire set membership.
      if (pg.draft || pg.placements.length !== (d.placements || []).length) {
        for (const c of pg.charms) c.pinned = null;
        sheetDirty(pg);
        for (const j of Engrave.items().values()) if (j.copies.some(id => (d.poolIds || []).includes(id))) {
          j.state = "ready"; j.backs = []; j.row.engrave.state = "ready"; j.row.engrave.approved = false;
        }
      }
    }
    // Saved rows without a recovered placement (for example, an unselected solid
    // material) still need their charms recreated on the queue.
    for (const row of Orders.rows()) if (row.poolIds.length && !row.poolIds.every(id => Pool.charmOf(id))) { row.state = "pulled"; row.poolIds = []; }
    agent({ run: rec.runId }, "cloud", `Restored ${(ls.sheets || []).length} sheet(s) of ${sets.map(x => x.name).join(", ") || "the working run"} from their records`);
    return sets[0];
  }
  function onComplete(r) {
    // In Auto this ends every arrival: the banner already reads "Processing complete · …", and a toast and a chime
    // every ten minutes said it again. The log line said "complete" of a run that stays open.
    const processed = r.status === "processed";
    const out = (r.lineArchive && r.lineArchive.base) || {}, committed = (r.committed || []).length + (+out.committed || 0);   // with those in the line archive after a resume
    agent({ run: r.runId }, "ok", `Run ${r.runId} ${processed ? "processed" : "complete"}: ${committed} order(s) committed · ${Object.keys(r.holds || {}).length + (+out.held || 0)} held · ${(r.refused || []).length} refused`);
    if (!processed) { toast(r.nothingToCut ? "Working sheets saved — held until eligible for a set" : `Set complete — ${committed} order(s) marked design-complete`, "ok", 7000); ding && ding(); }
    Arrivals.start();
  }
  /** Clear finished order state while keeping unfinished physical layouts on their material cards.
      drop "released": the run's orders were let go (a run given up, records purged), so a half-filled sheet holding
      them leaves its card and only an uncut Rose Gold contour, the physical plate, stays. drop "all": every record went
      with it (sandbox reset), so the cards start empty. A half-filled sheet kept from a run that is gone is never
      filled again: the next run opened sheet 2 beside it, one charm left on sheet 1 for good (23 Sep). */
  function clearRunState(beforeClear, { drop = null } = {}) {
    // (a sheet waiting for the stopped run's Resume is not at work: it does not keep the run from being put down)
    if(allSheets().some(p=>p.metal==='rose' && !p.roseCutAt && (p.rosePlan||p.roseProtected) &&
      (['nesting','finishing','queued'].includes(p.status) && !window.CN?.heldForResume?.(p) || p.persisted&&!p.persistedDone || p._rosePlanning || p._roseAction || p._operationStarting))){
      toast('The protected Rose Gold sheet is still being nested or saved. Wait until it finishes before clearing this run.','bad');
      return false;
    }
    window.CN?.dropResumeQueue?.();
    beforeClear?.();
    Carry.capture();
    // nothing waits for a run that is gone: the rows go back to plain pulled lines, and the gate forgets its plan
    for (const r of Orders.rows()) if (r.state === "waiting") { r.state = "pulled"; r.wait = null; r.reason = null; }
    if (window.Gate) { const g = Gate.state(); g.plan = null; g.forceFill = {}; }
    for (const m of METALS) {
      const prim=S.sheets[m.key];
      if (!prim.pages.some(p => p.runId || p.recalled || p.charms.some(c => c.poolId))) continue;
      // Clearing an order run is not a physical cut. Keep an uncut Rose
      // contour, its reservation and original orders together, even if its
      // set has already been committed. Keep other unfinished partial sheets.
      const retained=drop==='all'?[]:prim.pages.filter(p=>p.status!=='nesting' && !p.roseCutAt &&
        ((p.metal==='rose' && (p.rosePlan || p.roseProtected)) ||
          (!drop && p.placements.length && !p.releaseFull && !p.recalled && !Sets.ofRun(p.runId).some(set=>set.committedAt && set.sheetIds.includes(p.sheetId)))));
      for(const pg of prim.pages.slice())if(!retained.includes(pg)){
        window.Session?.dropBest?.(pg.sheetId);   // put down with the run: its best-layout record is not needed again
        delete pg.resumeWait;
        if(pg.status==='nesting')stopNest(pg);
        if(pg!==prim){(pg.workers||[]).forEach(w=>w.terminate());pg.workers=[];continue;}
        pg.charms=pg.charms.filter(c=>!c.poolId);pg.sheetId=null;pg.fileBase=null;pg.setId=null;pg.runId=null;pg.seq=null;pg.setDay=null;pg.cardStartedAt=null;pg.sheetIndex=null;pg.group=null;pg.draft=false;pg.releaseFull=false;pg.isolated=false;pg.backPool=[];pg.backOutputs=null;pg.label=null;pg.cloud=null;pg.persisted=null;pg.recalled=null;
        delete pg.roseStock;delete pg.roseProtected;delete pg.roseHistory;delete pg.rosePlan;delete pg.roseCutAt;delete pg.rosePlanHash;delete pg.rosePlanKey;delete pg.roseFresh;delete pg.roseChoice;pg._roseLoaded=false;
        sheetDirty(pg);
      }
      prim.pages=retained.includes(prim)?retained:[prim,...retained];
      prim.pages.forEach((p,i)=>{p.page=i+1;p.el=null;});
      prim.active=retained.length?prim.pages.indexOf(retained.at(-1)):0;
      prim.pages[prim.active].el=prim.cardEl;
    }
    B.orders.rows=[];B.orders.byKey=new Map();B.engrave.items=new Map();B.review.items=[];B.pool.rows=new Map();B.run=null;B.orders.recalled=null;B.orders.pulledAt=null;B.orders.filtered=0;B.orders.stale=false;
    Object.assign(Recall.state(),{runId:null,setId:null,live:null});
    for(const m of METALS)if(S.sheets[m.key].pages.length>1 || S.sheets[m.key].pages[0].placements.length)showPage(m.key,S.sheets[m.key].active);
    Orders.render();Engrave.render();Review.render();renderBanner();renderRail();updateTopSub();
    return true;
  }
  function setRunMode(mode) {
    S.settings.runMode = mode === "auto" ? "auto" : "manual"; saveSettings(); renderModeBtn();
    if (mode === "auto") { agent({ bridge: true }, "DS", "Auto mode on: the sorter pulls the latest orders by the date rule and runs the whole process, continuing past pending approvals"); if (!B.run || B.run.status === "complete") { if (B.run && B.run.status === "complete" && clearRunState()===false)return; start({ mode: "auto" }).catch(e => toast(e.message, "bad")); } else if (B.run.status === "stopped") { B.run.mode = "auto"; resume().catch(e => toast(e.message, "bad")); } else if (B.run.status === "paused") { B.run.mode = "auto"; next(); } else B.run.mode = "auto"; }
    else { clearTimeout(autoTimer); if (B.run) B.run.mode = "manual"; agent({ bridge: true }, "DS", "Manual mode: finish processing; start the next run manually"); }
    renderBanner();
  }
  function renderModeBtn() { const b = document.getElementById("btnRunMode"); if (!b) return; const auto = S.settings.runMode === "auto"; b.classList.toggle("auto", auto); document.getElementById("runModeText").textContent = auto ? "Auto" : "Manual"; }
  // the banner drawn at once: a caller outside the run may read it straight after (the run's own calls wait for the frame)
  let bannerDrawing = false;
  const bannerNow = () => { bannerDrawing = true; try { renderBanner(); } finally { bannerDrawing = false; } };
  const STEP_WORDS = { pull: "Pulling orders", claim: "Claiming", pool: "Pooling", plan: "Planning", nest: "Nesting", checkpoint: "Checking the sheets", engrave: "Engraving", revalidate: "Re-checking the orders", labels: "Writing labels", commit: "Committing", complete: "Complete" };
  function stepDetail(r) {
    const sh = allSheets().filter(p => p.runId === r.runId);
    if (r.step === "nest" && sh.length) return ` · sheet ${Math.min(sh.filter(p => ["complete", "partial"].includes(p.status)).length + 1, sh.length)} of ${sh.length}`;
    if (r.step === "pool") return ` · ${Orders.rows().filter(x => x.poolIds && x.poolIds.length).length} of ${Orders.rows().filter(x => x.state !== "gone").length} lines`;
    if (r.step === "pull" || r.step === "claim") return ` · ${Orders.rows().filter(x => x.state !== "gone").length} lines`;
    return "";
  }
  /* In Auto nobody may be at the bench. A run stopped by a passing failure (the network after a wake, a function's 5xx, a
     station that did not answer in time), by the Etsy watchdog's brake or by a reload waited for Resume however long the
     cause lasted, while the orders piled up. Each stop now has a kind, and Auto takes up the passing ones by itself once
     the network and the cloud answer: 1, 2, 5 and 10 minutes after the stop, then every 10 minutes, and at once when the
     network comes back. The operator's Stop, a decision waiting for a person, a sign-in and a spent Etsy budget (the
     heartbeat lifts that one) are left alone. */
  const PASSING = /answer in time|timed out|network|Failed to fetch|HTTP 5\d\d|link is down|no reply|closed/i, AUTO_WAIT = [60000, 120000, 300000, 600000];
  const auto = { runId: null, step: null, n: 0, next: 0, busy: false, t: 0 };   // the retries of one run at one step: a step done starts them afresh
  function kindOf(why) {
    why = why || "";
    return why === "stopped by the operator" ? "operator" : why === "Workspace restored after refresh" ? "restored" : reviewStop({ status: "stopped", stoppedBy: why }) ? "review"
      : /^Etsy watchdog/.test(why) ? "watchdog" : /^Etsy call budget reached/.test(why) ? "budget" : /\bsign/i.test(why) ? "auth"
      : PASSING.test(why) || /stopped answering the heartbeat/.test(why) ? "transient" : "error";
  }
  // (a run stopped by a reload, or one saved before stops had kinds, is read from its words)
  function stopKindOf(r) { return !r || r.status !== "stopped" ? null : r.stoppedBy === "Workspace restored after refresh" ? "restored" : r.stopKind || kindOf(r.stoppedBy); }
  function autoResumable(r) {
    if (!r || r !== B.run || r.status !== "stopped" || S.settings.runMode !== "auto" || settling === r) return false;
    const k = stopKindOf(r), m = window.DesignLink?.etsy?.().meter;
    return k === "transient" || k === "restored" || k === "watchdog" && Date.now() >= ((m && m.braked && m.brakeUntil) || 0);
  }
  // two sorter tabs brought back from the same workspace would both carry on with the one run: the first to take its lock does
  const claimed = new Set();
  function claimRun(runId) {
    const locks = globalThis.navigator?.locks; if (claimed.has(runId) || !locks?.request) return Promise.resolve(true);
    return new Promise(res => locks.request("charm-sorter-run:" + runId, { ifAvailable: true }, lock => { if (!lock) { res(false); return; } claimed.add(runId); res(true); return new Promise(() => {}); }).catch(() => res(true)));
  }
  /** Called by the heartbeat, the boot after a reload (now: true) and the network coming back. */
  async function autoResume(now) {
    const r = B.run; if (auto.busy || !autoResumable(r)) return false;
    if (auto.runId !== r.runId || auto.step !== r.step) { Object.assign(auto, { runId: r.runId, step: r.step, n: 0, next: ((r.errors || []).at(-1)?.t || Date.now()) + AUTO_WAIT[0] }); renderBanner(); }
    clearTimeout(auto.t);
    if (!now && Date.now() < auto.next) { auto.t = setTimeout(() => autoResume(), Math.max(1000, auto.next - Date.now())); return false; }
    auto.busy = true; auto.n++; auto.next = Date.now() + AUTO_WAIT[Math.min(auto.n, AUTO_WAIT.length - 1)]; renderBanner();
    try {
      if (globalThis.navigator?.onLine === false) throw new Error("the network is down");
      if (!await claimRun(r.runId)) throw new Error("another sorter tab is carrying on with this run");
      await api("charmNestLibrary", { op: "ping", calibration: false }, { quiet: true });
      await DesignLink.ensure();
      if (!autoResumable(r)) return false;
      agent({ run: r.runId }, "DS", `Resuming by itself (try ${auto.n}): ${r.stoppedBy}`);
      try { await resume(); } catch (e) { if (r === B.run && r.status === "running") stop(e.message, "Fix the cause and press Resume."); throw e; }
      auto.step = r.step; return true;
    } catch (e) {
      agent({ run: r.runId }, "info", `Not resumed yet (try ${auto.n}): ${e.message} — next try at ${new Date(auto.next).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`);
      if (autoResumable(r)) auto.t = setTimeout(() => autoResume(), auto.next - Date.now());
      return false;
    } finally { auto.busy = false; renderBanner(); }
  }
  // what the banner says in place of "press Resume" while Auto will carry on by itself
  function autoNote(r) {
    if (!autoResumable(r)) return "";
    if (auto.busy) return "Auto is resuming it now…";
    if (globalThis.navigator?.onLine === false) return "Auto carries on by itself when the network is back.";
    if (auto.runId !== r.runId || auto.step !== r.step) return "Auto carries on by itself shortly.";
    return `Auto carries on by itself: next try ${new Date(auto.next).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}${auto.n ? ` (tried ${auto.n}×)` : ""}, or press Resume.`;
  }
  function renderBanner() {
    // what the banner keeps up besides its drawing (the workspace checkpoint, the bench buttons) is done at every call;
    // the drawing is done once a frame (CNFrame), or at once for a caller outside the run (RunCtl.renderBanner)
    if (window.Session) Session.schedule();
    if (window.guardBench) guardBench();
    if (!bannerDrawing && window.CNFrame) { CNFrame.later("banner", bannerNow); return; }
    window.CNFrame?.cancel("banner");
    const previousMenu = document.getElementById("runMenu");
    const menuWasOpen = !!previousMenu?.open;
    const focusId = previousMenu?.contains(document.activeElement) ? document.activeElement.id : null;
    const h = document.getElementById("runBanner"); if (!h) return; const r = B.run;
    LiveStrip.now();                                        // one path: the banner and the ladder can never disagree
    if (!r) {
      // a set has finished and Auto will start another: the banner stays, so nobody comes back to a blank app
      if (NEXT.at > Date.now()) {
        const left = Math.max(0, NEXT.at - Date.now()), mm = Math.floor(left / 60000), ss = Math.floor(left % 60000 / 1000);
        h.classList.remove("hidden"); h.className = "runBanner done";
        h.innerHTML = `<span class="why"><b>Set finished</b> · the next run starts in ${mm}:${String(ss).padStart(2, "0")}</span><span class="spacer"></span><span class="acts"><button class="btn gold sm" id="rbNow" title="start the next run now instead of waiting">Run now</button><button class="btn ghost sm" id="rbCancelNext" title="do not start another run on its own">Cancel</button></span>`;
        h.querySelector("#rbNow").onclick = () => { cancelNext(); if(clearRunState()===false)return; start({ mode: "auto" }).catch(e => toast(e.message, "bad")); };
        h.querySelector("#rbCancelNext").onclick = () => cancelNext();
        Dock.schedule(); return;
      }
      // a run left open by a reload or a closed tab is offered here, not in a toast pointing at another tab's button
      if (B.openRuns && B.openRuns.length) {
        h.classList.remove("hidden"); h.className = "runBanner review";
        h.innerHTML = `<span class="why"><b>${B.openRuns.length} open run${B.openRuns.length === 1 ? "" : "s"}</b> · left from an earlier session</span><span class="spacer"></span><span class="acts">${B.openRuns.slice(0, 3).map(x => `<button class="btn gold sm" data-rbres="${esc(x.runId)}" title="carry on from ${esc(x.step)} · ${x.lines} line(s) · ${new Date(x.updatedAt).toLocaleString()}">Resume ${esc(x.runId.slice(-8))} · ${esc(x.step)}</button>`).join("")}<button class="btn ghost sm" id="rbLater" title="leave these for later — they stay on record">Not now</button></span>`;
        h.querySelectorAll("[data-rbres]").forEach(b => b.onclick = () => { B.openRuns = null; resumeRun(b.dataset.rbres).catch(e => toast(e.message, "bad", 7000)); });
        h.querySelector("#rbLater").onclick = () => { B.openRuns = null; renderBanner(); };
        Dock.schedule(); return;
      }
      // with no run open, a failed save of this browser's workspace is still said, once, until the next save works
      const unsaved = window.Session?.failure?.();
      if (unsaved) { h.classList.remove("hidden"); h.className = "runBanner stopped"; h.innerHTML = `<span class="why"><span class="bad">Not saved on this browser: ${esc(unsaved.message)}</span> — keep this tab open; it is tried again by itself</span>`; Dock.schedule(); return; }
      // nothing is running, so there is nothing to report: the banner is not a place to advertise from
      h.classList.add("hidden"); Dock.schedule(); return;
    }
    h.classList.remove("hidden"); h.className = "runBanner" + (r.status === "stopped" ? " stopped" : r.status === "complete" ? " done" : ["review","processed"].includes(r.status) ? " review" : "");
    const idx = O.stepIndex(r.step);
    const reviewN = Review.count(), engN = Engrave.pendingCount();
    const waitingFor = [reviewN ? `${reviewN} in Review` : "", engN ? `${engN} in Engraving` : ""].filter(Boolean).join(" · ") || "nothing";
    // with nothing for a person to do, say what the run is waiting for: lines on sheets that have not been released yet
    const onCards = r.status === "processed" ? Orders.rows().filter(x => x.state === "pooled").length : 0;
    const idle = onCards ? `${onCards} line${onCards === 1 ? "" : "s"} wait on sheets not released yet` : "waiting for sheets to release";
    const local = window.Session?.failure?.();   // a failed save of this browser's workspace stands here until the next one works
    const saveWarning = (r.saveError ? `<span class="bad">Not saved online: ${esc(r.saveError)}</span> · ` : "") + (local ? `<span class="bad">Not saved on this browser: ${esc(local.message)}</span> · ` : "");
    // Arrivals being nested read "Processing complete · … wait" beside Retry pending; and after Stop the sheets already
    // nesting carry on and save, which "Stopped" alone did not say. A queued one waits for Resume (its card says so).
    const intake = r.status === "processed" && !!r.arrivalBusy;
    const finishing = r.status === "stopped" ? allSheets().filter(p => p.runId === r.runId && (["nesting", "finishing"].includes(p.status) || p.status === "queued" && !window.CN?.heldForResume?.(p))).length : 0;
    // the station can be back (taken again for an arrival) before anyone reads "Press Take control": then Resume is all that is left
    const fix = autoNote(r) || (r.status === "stopped" && /^Press Take control/.test(r.fix || "") && window.DesignLink?.inControl() && DesignLink.up() ? "The station is back · press Resume." : r.fix);
    const why = saveWarning + (r.status === "stopped" ? `<b>Stopped:</b> ${esc(r.stoppedBy || "")}${fix ? ` — <span>${esc(fix)}</span>` : ""}${finishing ? ` · ${finishing} sheet${finishing === 1 ? "" : "s"} still finishing` : ""}` : intake ? `<b>Adding new orders</b> · nesting them onto the sheets` : r.status === "review" ? `<b>Waiting for a person:</b> ${waitingFor}` : r.status === "processed" ? `<b>Processing complete</b> · ${waitingFor === "nothing" ? idle : waitingFor}${r.awaitCommit ? " · commit when ready" : ""}` : r.status === "paused" ? `<b>Ready to commit</b> — every sheet written, every engraving decided` : r.status === "complete" ? `<b>Complete</b> · ${(r.committed || []).length} committed · ${Object.keys(r.holds || {}).length} held` : `<b>${esc(STEP_WORDS[r.step] || r.step)}</b>${esc(stepDetail(r))}`);
    Dock.schedule();
    h.title = `run ${r.runId}`;
    /* Which run is this? Three cards on the Nest tab and a banner that named only a step left no way to tell this
       morning's set from yesterday's. The set, the day and the size of the run now lead it. */
    const outside = (r.lineArchive && r.lineArchive.base) || {}, nSheets = Object.keys(r.sheets || {}).length + (+outside.sheets || 0);   // with what a resumed run's record left out
    const seqOf = x => x.seq || +((/-(\d+)$/.exec(String(x.setId || "")) || [])[1] || 0) || null;
    const who = [seqOf(r) ? `Set ${seqOf(r)}` : "", r.day ? new Date(r.day + "T12:00:00").toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "",
      `${(Object.keys(r.lines || {}).length || Orders.rows().filter(x => x.state !== "gone").length) + (+outside.lines || 0)} lines`, nSheets ? `${nSheets} sheet${nSheets === 1 ? "" : "s"}` : ""].filter(Boolean).join(" \u00b7 ");
    h.innerHTML = `<button type="button" class="rid" title="run ${esc(r.runId)}${r.setId ? " \u00b7 set " + esc(r.setId) : ""}">${esc([seqOf(r) ? `Set ${seqOf(r)}` : "", r.day ? new Date(r.day + "T12:00:00").toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "", nSheets ? `${nSheets} sheets` : ""].filter(Boolean).join(" · "))}</button><span class="why">${why}</span><span class="spacer"></span><span class="acts">
      ${["paused","processed"].includes(r.status) && r.awaitCommit ? `<button class="btn sage sm" id="rbCommit" title="mark every order in the set design-complete on the station">Commit set</button>` : ""}${r.status === "stopped" && /\bsign/i.test(r.stoppedBy || "") ? `<button class="btn gold sm" id="rbConnect" title="sign the Design Station back in to Etsy, then the run can carry on">Connect Etsy</button>` : ""}${["stopped","processed"].includes(r.status) && !intake ? `<button class="btn gold sm" id="rbResume" title="carry on from the step this run stopped at"${settling === r ? " disabled" : ""}>${r.status === "processed" ? "Retry pending" : settling === r ? "Resuming…" : "Resume"}</button>` : ""}${["running", "review", "paused"].includes(r.status) || intake ? `<button class="btn ghost sm" id="rbStop" title="${r.arrivalBusy ? "stop now — the charms already placed stay where they are, and the new orders wait for Resume" : "stop after the step in progress — the run can be resumed from where it stopped"}">Stop</button>` : ""}${r.status === "complete" ? `<button class="btn ghost sm" id="rbClear" title="take the finished run off the cards — its files and records are kept">Clear run</button>` : ""}<details class="runMenu" id="runMenu"${menuWasOpen ? " open" : ""}><summary id="runMenuToggle" aria-label="Run options" title="Run options">⋯</summary><div class="runMenuBody"><div class="runDetail"><b>${esc(who)}</b><small>${esc(r.runId)}</small>${why}</div>
      ${r.at ? `<button class="btn ghost sm" id="rbAt">Show the sheet</button>` : ""}<button class="btn ghost sm" id="rbHistory">Run history…</button>
      ${r.status !== "complete" ? `<button class="btn ghost sm" id="rbAbandon" title="Saved sheets and files are kept">Abandon run…</button>` : ""}</div></details></span>`;
    const q = id => h.querySelector("#" + id);
    if (focusId) q(focusId)?.focus({ preventScroll: true });
    q("rbHistory").onclick = () => { q("runMenu").open = false; RunHistory.show(); };
    const rid = h.querySelector(".rid"); if (rid) { rid.tabIndex = 0; rid.title += " \u2014 click for every run on record"; rid.onclick = () => RunHistory.show(); rid.onkeydown = e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); RunHistory.show(); } }; }
    if (q("rbAt")) q("rbAt").onclick = () => { CN.setMode("nest"); const i = CN.pagesOf(r.at.metal).findIndex(p => p.page === r.at.page); CN.showPage(r.at.metal, Math.max(0, i)); };
    if (q("rbConnect")) q("rbConnect").onclick = () => DesignLink.connectEtsy().catch(e => toast(e.message, "bad", 6000)); 
    if (q("rbCommit")) q("rbCommit").onclick = () => commitNow(); if (q("rbResume")) q("rbResume").onclick = () => resume(); if (q("rbStop")) q("rbStop").onclick = () => operatorStop(); if (q("rbClear")) q("rbClear").onclick = () => { if (confirm("Clear the finished run from the cards? Files and records are kept.")) clearRunState(); };
    if (q("rbAbandon")) q("rbAbandon").onclick = () => {
      // a person's decision with no back file written yet: an approval, confirmed words, a placement moved by hand
      const eng = [...Engrave.items().values()].filter(j => ["approved", "words", "review"].includes(j.state) && !(j.backs && j.backs.length) && (j.state === "approved" || j.decision || j.nudged || j.edited)).length;
      // Review.count() is what still waits for a decision, not decisions made: it used to be added to the work "lost"
      const rev = Review.count();
      const lost = eng ? `\n\n${eng} engraving decision${eng === 1 ? "" : "s"} made in this run ${eng === 1 ? "is" : "are"} not written yet and will be lost.` : "";
      const open = rev ? `\n\n${rev} review item${rev === 1 ? " still waits" : "s still wait"} for a decision.` : "";
      if (confirm(`Give up run ${r.runId.slice(-8)}?${lost}${open}\n\nThe sheets and files already saved are kept.`)) clearRunState(()=>releaseRun(r),{drop:'released'});
    };
    Orders.render();
  }
  return { optionsChanged, recoverReviewStop, membershipUpdated, start, next, resume, stop, operatorStop, stopIfRunning, poke, backgroundSettled, save, onSheetDone, pickResume, resumeRun, restoreRunSheets, commitNow, setMode: setRunMode, setRunMode, renderBanner: bannerNow, renderModeBtn, clearRunState, run, autoResume, stopKind: stopKindOf };
})();

/* ═══ 24 · Review — every decision a person must make ═════════════════════ */
const Review = window.Review = (() => {
  const items = () => B.review.items;
  const mine = it => !String(it.key || "").startsWith("eng:") && !(it.row && it.row.state === "gone");   // engraving is the Engraving tab's
  const isNotice = it => String(it.key || "").startsWith("held:");        // an order left open, not a decision to make
  const count = () => items().filter(it => mine(it) && !isNotice(it)).length;
  function put(it) { const i = items().findIndex(x => x.key === it.key); const fresh = i < 0; if (fresh) items().push(Object.assign({ t: Date.now() }, it)); else items()[i] = Object.assign(items()[i], it); if (fresh && B.run && ["review", "paused", "stopped"].includes(B.run.status)) notifyPerson("Charm Sorter needs a person", it.why || it.kind); }
  // the list at once (a person may be working in it); the strip and the banner once a frame (CNFrame)
  function redraw() { render(); LiveStrip.render(); if (window.CNFrame) CNFrame.later("banner", RunCtl.renderBanner); else RunCtl.renderBanner(); }
  function add(it) { put(it); redraw(); }
  const settled = [];                                                     // what this shift has answered, newest first
  function remove(key, how) {
    const n = items().length;
    const gone = items().find(x => x.key === key);
    B.review.items = items().filter(x => x.key !== key);
    if (n === items().length) return;
    if (gone && !/^(eng|held):/.test(String(key))) settled.unshift({ key, row:gone.row || gone.rows?.[0] || null, kind: gone.kind, why: gone.why || "", lines: (gone.rows || [gone.row]).filter(Boolean).length, orders: [...new Set((gone.rows || [gone.row]).filter(Boolean).map(r2 => r2.order.receiptId))], by: how || employeeName() || "", t: Date.now() });
    if (settled.length > 200) settled.length = 200;
    redraw();
  }
  function problemText(p) { return p.kind === "needsMaterial" ? `needs material (${p.metalLabel || "none"})` : p.kind === "needsMapping" ? `option "${p.optionName}: ${p.optionValue}" not mapped` : p.kind === "unmatchedSku" ? `SKU ${p.sku || "?"}: ${p.reason}` : p.kind === "blockedSku" ? `SKU ${p.sku} blocked: ${p.reason}` : p.kind === "missingSize" ? `no design for size ${p.size || "(none)"} (have ${(p.available || []).join(", ")})` : p.kind === "oversize" ? `oversize for the ${labelOf(p.material)} plate` : p.kind; }
  /** The key of the DECISION a problem asks for, not of the line that raised it. An unknown SKU is one decision however
   *  many orders bought it; an unmapped option is one decision however many lines carry it. A run that raised 180 of the
   *  first and 79 of the second showed 259 items where 148 decisions were waiting. */
  const SEP = "\u0000";
  function decisionKey(row, p) {
    if (p.kind === "unmatchedSku") return p.sku ? `ord:sku:${p.sku}` : `ord:listing:${p.listingId || row.key}`;
    if (p.kind === "blockedSku") return `ord:blocked:${p.sku}`;
    if (p.kind === "needsMapping") return `ord:opt:${p.optionName}${SEP}${p.optionValue}`;
    if (p.kind === "needsMaterial") return `ord:mat:${p.listingId || row.key}${SEP}${p.metalLabel || ""}`;
    return `ord:${row.key}:${p.kind}`;                                   // a size or a plate is this charm's own
  }
  /** Order-level items follow the rows' problems: added when a problem appears, removed when it is fixed. */
  function syncOrderItems() {
    const keep = new Map();
    // a row a person parked is parked: it used to re-raise its decision the moment the next card was answered, because
    // interpretAll recomputes problems from scratch and sync rebuilt the queue from them
    for (const row of Orders.rows()) { if (row.state === "gone" || row.hold) continue; for (const p of row.problems || []) {
      const key = decisionKey(row, p);
      if (!keep.has(key)) keep.set(key, { kind: p.kind, key, row, problem: p, rows: [], why: problemText(p) });
      const it = keep.get(key); if (!it.rows.includes(row)) it.rows.push(row);
    } }
    // new decisions join the queue here and the queue is drawn once, below: it used to be drawn again for each one
    for (const [key, it] of keep) { const had = items().find(x => x.key === key); if (had) Object.assign(had, { rows: it.rows, row: it.row, problem: it.problem, why: it.why }); else put(it); }
    B.review.items = items().filter(x => !x.key.startsWith("ord:") || keep.has(x.key));
    // a changed-order notice goes with its line: one whose line left Etsy, was cut and committed, or left the list was kept for good
    B.review.items = items().filter(x => { if (!String(x.key).startsWith("chg:")) return true; const cur = x.row && B.orders.byKey?.get(x.row.key); return !!cur && !["gone", "committed"].includes(cur.state); });
    // a held notice is a notice, not a decision: it goes when the order it names is committed, gone, or no longer held
    const byRid = new Map();
    for (const r of Orders.rows()) { const k = r.order.receiptId; if (!byRid.has(k)) byRid.set(k, []); byRid.get(k).push(r); }
    B.review.items = items().filter(x => {
      if (!isNotice(x)) return true;
      const lines = byRid.get(x.rid); if (!lines || !lines.length) return false;
      if (lines.every(l => ["committed", "gone"].includes(l.state))) return false;
      const held = lines.find(l => l.hold);
      if (!held && !lines.some(l => l.problems && l.problems.length)) return false;
      x.note = held ? held.hold : (lines.find(l => l.reason) || {}).reason || "unresolved line";
      x.line = held ? held.key : (lines.find(l => l.problems && l.problems.length) || lines[0]).key;
      return true;
    });
    redraw();
  }
  /** Every line the item speaks for — the group when it has one, the single row otherwise. */
  const rowsOf = it => (it.rows && it.rows.length ? it.rows : it.row ? [it.row] : []).filter(r => r.state !== "gone");
  /** Apply one decision to every line it covers, then re-pool them together. */
  async function repoolAll(it, before) {
    const rows = rowsOf(it);
    for (const r of rows) if (before) before(r);
    for (const r of rows) await repool(r);
  }
  function focus(rowKey) {
    const it=items().find(it=>rowsOf(it).some(r=>r.key===rowKey));
    RV.filter=null;RV.limit=items().length;reviewFilter=null;RV.open=it?.key || null;render();
    const c=it && reviewRows.get(it.key)?.node;
    if(c){const button=c.querySelector('[data-review-open]');if(button?.getAttribute('aria-expanded')==='false')button.click();c.scrollIntoView({behavior:'smooth',block:'center'});c.classList.add('pulse');setTimeout(()=>c.classList.remove('pulse'),1300);}
  }

  async function repool(row) {
    const old0=new Set(row.poolIds || []);
    // A piece on a sheet already released to the laser, or cut in a committed set, is not taken off its sheet (as for a
    // cancelled order, takeOffGone): that sheet's files and labels went out as they are, and arranging it again rewrote
    // them under the same name, moved other orders' pieces and made the unchanged copies a second time. A changed line
    // whose piece is there waits for a person, who makes the difference by hand or skips it; releasing the hold then
    // keeps the piece on that sheet (the path below for a line whose pieces are on sheets).
    const cut=sh=>!!sh.roseCutAt || Sets.ofRun(sh.runId).some(s=>s.committedAt && s.sheetIds.includes(sh.sheetId));
    const fixedSheets=allSheets().filter(sh=>(cut(sh) || sh.releaseFull || sh.recalled || Gate.modern(sh.runId) && sh.setId && !sh.draft) && sh.charms.some(c=>old0.has(c.poolId)));
    if(row.repoolChanged && fixedSheets.length){
      row.changePending=false;row.repoolChanged=false;row.state="held";
      const wasCut=fixedSheets.some(cut);
      row.hold=row.reason=`Etsy changed this line after its piece was ${wasCut ? "cut on" : "released to the laser on"} ${fixedSheets.map(sh=>sh.fileBase || sheetName(sh)).join(", ")} — make the change by hand, then release the line (the piece on that sheet is kept)${wasCut ? " or skip it" : "; its set waits for that"}`;
      add({kind:"heldOrder",key:"held:"+row.order.receiptId,rid:row.order.receiptId,why:`${row.order.receiptId} held — ${row.hold}`,line:row.key});
      agent({run:B.run?.runId},"warn",`${row.order.receiptId}: ${row.hold}`);
      syncOrderItems();Orders.render();RunCtl.poke();return;
    }
    // Lifting a hold, or accepting a change that leaves the pieces as they are, keeps the pieces already on sheets:
    // making the line up again put a second copy of each on the intake sheet under the same pool id.
    if(!row.repoolChanged && Pool.onSheets(row)){
      // a plain line's engraving decision was reset with the Etsy change; its piece is kept as it is, so it is plain again
      // (left reset, the sheet never read as ready and its set waited for good)
      if(row.engrave && row.engrave.state==="reclassify" && row.spec && !row.spec.engraveCandidate)row.engrave={needed:false,state:"none",approved:true};
      row.changePending=false;row.problems=[];row.hold=null;Pool.settle(row);Orders.interpretAll();
      syncOrderItems();Orders.render();renderRail();updateTopSub();refreshAllCards();if(OrderWin.isOpen())OrderWin.paint();RunCtl.poke();return;
    }
    if(row.repoolChanged){
      const old=new Set(row.poolIds || []);
      for(const sh of allSheets())if(sh.charms.some(c=>old.has(c.poolId))){sh.charms=sh.charms.filter(c=>!old.has(c.poolId));sh.placements=sh.placements.filter(pl=>sh.charms.some(c=>c.id===pl.id));Orders.keepRest(sh);}
      await Pool.update([...old],{state:"superseded",sheetId:null,setId:null});for(const id of old)B.pool.rows.delete(id);
      row.poolIds=[];row.repoolChanged=false;
    }
    row.changePending=false; row.problems = []; row.state = "pulled"; row.reason = null; row.hold = null; Orders.interpretAll(); if (row.problems.length) { Orders.render(); return; } if (B.run && O.stepIndex(B.run.step) >= O.stepIndex("pool")) { try { await Pool.poolAdd(row, B.run); } catch (e) { row.state = "held"; row.reason = e.message; } if (row.state === "pooled" && row.spec.engraveCandidate) Engrave.classify(row).catch(() => {}); } syncOrderItems(); Orders.render(); renderRail(); updateTopSub(); refreshAllCards(); if (OrderWin.isOpen()) OrderWin.paint(); RunCtl.poke(); }
  const by = () => employeeName() || askEmployee();
  /** A decision saved to the library holds the card's buttons while it saves, and a failed save says so and leaves the
   *  card as it was, to press again. It used to fail in silence: the card stayed and nothing said why. */
  const saving = (c, fn) => async () => {
    if (c.dataset.saving) return;
    const bs = [...c.querySelectorAll(".fixes button")]; c.dataset.saving = "1"; bs.forEach(b => { b.disabled = true; });
    try { await fn(); }
    catch (e) { toast("Could not save that: " + e.message, "bad", 7000); }
    finally { delete c.dataset.saving; bs.forEach(b => { b.disabled = false; }); c.querySelectorAll("[data-f]").forEach(f => f.dispatchEvent(new Event("change"))); }
  };
  /** What a person has typed or picked in a decision card. The card is rebuilt when its group grows or its order changes
   *  (another order with the same unknown SKU arrives); the rebuild used to empty the field under their fingers. */
  const typedIn = host => host ? [...host.querySelectorAll("[data-f]")].map(f => ({ f: f.dataset.f, v: f.type === "checkbox" ? f.checked : f.value, at: document.activeElement === f ? [f.selectionStart, f.selectionEnd] : null })) : [];
  function putTyped(host, was) {
    let refocus = null;
    for (const w of was || []) {
      const f = host.querySelector(`[data-f="${w.f}"]`); if (!f) continue;
      if (f.type === "checkbox") f.checked = w.v;
      else if (w.v && f.value !== w.v) { f.value = w.v; f.dispatchEvent(new Event("input")); f.closest(".fixes.hidden")?.classList.remove("hidden"); }
      if (w.at) refocus = () => { f.focus({ preventScroll: true }); try { f.setSelectionRange(w.at[0], w.at[1]); } catch (_) {} };
    }
    return refocus;
  }
  /** What Claude decided, in one line and one number: a reading is either text to engrave or a note to the shop. */
  function claudeVerdict(j) {
    const pct = Math.round((j.confidence || 0) * 100);
    const text = (j.text || "").trim();
    const from = SOURCE_LABEL[j.source] != null ? SOURCE_LABEL[j.source] : esc(String(j.source || ""));
    if (!text) return `<b>nothing to engrave</b> — what the customer wrote reads as a note to the shop, not words for the charm <i style="color:var(--ink45)">· ${pct}% sure it is not engraving${j.quote ? ` · "${esc(j.quote)}"` : ""}</i>`;
    return `<b>engrave this</b>${from ? ` — read from ${from}` : ""} <i style="color:var(--ink45)">· ${pct}% sure${j.quote ? ` · "${esc(j.quote)}"` : ""}</i>`;
  }
  /** A primary action whose field is empty cannot be pressed, and says so, instead of returning in silence. */
  function bindNeeds(c, action, field) {
    const b = c.querySelector(`[data-a=${action}]`), f = c.querySelector(`[data-f=${field}]`);
    if (!b || !f) return;
    const sync = () => { const empty = !String(f.value || "").trim(); b.disabled = empty; b.title = empty ? "fill the field beside it first" : ""; };
    f.addEventListener("input", sync); f.addEventListener("change", sync); sync();
  }
  function card(it) {
    const c = el("div", "rvItem hoverItem"); c.dataset.kind = it.kind; if (it.row) { c.dataset.row = it.row.key; c.dataset.rid = String(it.row.order.receiptId); }
    const r = it.row, sp = r && r.spec, p = it.problem || {};
    const group = rowsOf(it);
    const orders = [...new Set(group.map(x => x.order.receiptId))];
    const scope = group.length > 1
      ? `<span class="pill neutral" title="${esc(orders.slice(0, 20).join(" · ") + (orders.length > 20 ? " …" : ""))}">${group.length} lines · ${orders.length} order${orders.length === 1 ? "" : "s"} · one decision</span>` : "";
    const head = (kind, ttl, sub) => `<div class="rh"><span class="kind">${esc(kind)}</span><span class="ttl">${esc(ttl)}</span><span class="sub">${esc(sub || "")}</span>${scope}</div>`;
    const evRow = (lbl, val) => `<div><span class="lbl">${esc(lbl)}</span>${val}</div>`;
    const orderSub = r ? `${r.order.receiptId} · ${sp && sp.designSku || r.line.sku || "no SKU"} · ${r.line.title}` : "";
    if (it.kind === "needsMaterial") {
      c.innerHTML = head("Needs material", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Station read", esc(p.metalLabel || "nothing"))}${evRow("Options", (r.line.variations || []).map(v => `<q>${esc(v.name)}: ${esc(v.value)}</q>`).join(" "))}${evRow("Title", esc(r.line.title))}</div><div class="why">${esc(it.why)}</div>
        <div class="fixes"><select data-f="mat"><option value="">pick a material…</option>${METALS.map(m => `<option value="${m.key}">${esc(m.label)}</option>`).join("")}</select><button class="btn gold sm" data-a="mat">Use it (writes a staff note)</button><button class="btn ghost sm" data-a="skip">Skip line</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      bindNeeds(c, "mat", "mat");
      c.querySelector("[data-a=mat]").onclick = async () => { const m = c.querySelector("[data-f=mat]").value; if (!m) return; const who = by(); if (!who) return; row_material(it, m, who); };
    } else if (it.kind === "needsMapping") {
      /* This was a dropdown, a free-text box, a second dropdown and a button that did nothing at all until you had
         guessed the exact word it wanted. The question only ever has a handful of answers, so they are the buttons:
         one press decides it, and "Something else" opens the old typing for the rare value none of them covers. */
      const lids = [...new Set(group.map(x => String(x.line.listingId)))];
      const CHOICES = [["necklace", "form", "Necklace"], ["earrings", "form", "Earrings"], ["huggie", "form", "Huggie"], ["charm", "form", "Charm only"], ["bracelet", "form", "Bracelet"], ["keychain", "form", "Keychain"]];
      const guess = CharmNestOrders.FORM_VALUES[CharmNestOrders.norm(p.optionValue)] || null;
      c.innerHTML = head("Needs mapping", `${p.optionName}: ${p.optionValue}`, `${lids.length === 1 ? "listing " + p.listingId : lids.length + " listings"} · ${p.title || r.line.title}`) +
        `<div class="ask">What does this option decide?</div>
        <div class="fixes pick">${CHOICES.map(([v, f, lbl]) => `<button class="btn ${v === guess ? "gold" : "ghost"} sm" data-pick="${v}" data-field="${f}">${lbl}</button>`).join("")}<button class="btn ghost sm" data-a="ignore" title="it changes nothing about what gets made">Nothing — ignore it</button><button class="btn ghost sm" data-a="other">Something else…</button></div>
        <div class="fixes other hidden"><select data-f="field"><option value="form">form</option><option value="size">size</option><option value="chain">chain length</option></select><input data-f="val" placeholder="the value to remember"><button class="btn gold sm" data-a="map">Remember it</button></div>
        ${lids.length > 1 ? `<label class="scopeOne"><input type="checkbox" data-f="one"> only for listing ${esc(p.listingId)} — otherwise all ${lids.length} are mapped together</label>` : ""}`;
      const oneOnly = () => { const b = c.querySelector("[data-f=one]"); return !!(b && b.checked); };
      const put = async (field, value) => {
        const who = by(); if (!who) return;
        const wide = lids.length > 1 && !oneOnly();
        [...c.querySelectorAll("button")].forEach(b => { b.disabled = true; });
        try {
          await api("charmNestLibrary", { op: "optionMapPut", listingId: wide ? "*" : p.listingId, optionName: p.optionName, optionValue: p.optionValue, map: { field, value: field === "size" ? String(value).toUpperCase() : String(value).toLowerCase() }, by: who });
          await Orders.loadMaps(true);
          toast(`“${p.optionValue}” → ${value} · remembered for ${wide ? "every listing" : "this listing"}`, "ok");
          for (const rr of Orders.rows()) if (rr.problems.some(x => x.kind === "needsMapping")) await repool(rr);
        } catch (e) { toast("Could not save that: " + e.message, "bad", 7000); [...c.querySelectorAll("button")].forEach(b => { b.disabled = false; }); }
      };
      c.querySelectorAll("[data-pick]").forEach(b => { b.onclick = () => put(b.dataset.field, b.dataset.pick); });
      c.querySelector("[data-a=other]").onclick = () => { c.querySelector(".fixes.other").classList.toggle("hidden"); const f = c.querySelector("[data-f=val]"); if (f) f.focus(); };
      bindNeeds(c, "map", "val");
      c.querySelector("[data-a=map]").onclick = () => { const val = c.querySelector("[data-f=val]").value.trim(); if (!val) return; put(c.querySelector("[data-f=field]").value, val); };
      c.querySelector("[data-a=ignore]").onclick = saving(c, async () => { const who = by(); if (!who) return; for (const lid of (oneOnly() ? [String(p.listingId)] : lids)) await api("charmNestLibrary", { op: "optionMapPut", listingId: lid, optionName: p.optionName, optionValue: p.optionValue, map: { field: "ignore" }, by: who }); await Orders.loadMaps(true); await repoolAll(it); });
    } else if (it.kind === "unmatchedSku" || it.kind === "blockedSku") {
      const skus = [...B.master.entries.keys()].sort();
      c.innerHTML = head(it.kind === "blockedSku" ? "SKU blocked" : "Unmatched SKU", p.sku || "no SKU", orderSub) + `<div class="why">${esc(p.reason || it.why)}</div>
        <div class="fixes"><input list="rvSkus" data-f="sku" placeholder="pick the charm from the master index…"><datalist id="rvSkus">${skus.map(s => `<option value="${esc(s)}">`).join("")}</datalist><button class="btn gold sm" data-a="alias" title="every line of this listing uses that charm from now on">Use this charm</button><button class="btn ghost sm" data-a="nodesign" title="this line never needs a design — remembered, so it stops asking">Nothing to cut</button><button class="btn ghost sm" data-a="hold" title="hold the whole order until someone sorts it out">Hold order</button>${it.kind === "blockedSku" ? `<button class="btn ghost sm" data-a="master">Open Master</button>` : ""}</div>`;
      bindNeeds(c, "alias", "sku");
      c.querySelector("[data-a=alias]").onclick = saving(c, async () => { const sku = c.querySelector("[data-f=sku]").value.trim().toUpperCase(); if (!sku) return; const who = by(); if (!who) return; if (!B.master.entries.has(sku)) { toast(`${sku} is not in the master index`, "bad"); return; } const lids = [...new Set(rowsOf(it).map(x => String(x.line.listingId)))]; for (const lid of lids) await api("charmNestLibrary", { op: "aliasPut", listingId: lid, sku, by: who, title: r.line.title }); await Orders.loadMaps(true); toast(`${lids.length} listing${lids.length === 1 ? "" : "s"} → ${sku} remembered`, "ok"); for (const rr of Orders.rows()) if (lids.includes(String(rr.line.listingId))) await repool(rr); });
      c.querySelector("[data-a=nodesign]").onclick = saving(c, async () => { const who = by(); if (!who) return; const sku = p.sku || (sp && sp.designSku); if (sku) await api("charmNestLibrary", { op: "noDesignPut", sku, by: who, note: r.line.title }); else await api("charmNestLibrary", { op: "noDesignPut", pattern: "^" + String(r.line.title).replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 40), by: who, note: "by title" }); await Orders.loadMaps(true); await repoolAll(it); });
      const mb = c.querySelector("[data-a=master]");
      if (mb) mb.onclick = () => {
        const sku = p.sku || (sp && sp.designSku) || "";
        modeFromUser = true;                                            // pushState, so Back returns to this card
        setMode("master");
        const f = document.getElementById("mSearch"); if (!f) return;
        f.value = sku; Master.render(); f.focus(); f.select();
        let tries = 0;
        const show = () => {
          const t = document.querySelector("#mGrid .skuTile");
          if (!t) { if (++tries < 40) setTimeout(show, 150); return; }   // a cold tab is still loading the library
          t.scrollIntoView({ behavior: "smooth", block: "center" });
          t.classList.add("pulse"); setTimeout(() => t.classList.remove("pulse"), 1300);
        };
        show();
      };
    } else if (it.kind === "missingSize") {
      c.innerHTML = head("Missing size", `${p.sku} · size ${p.size || "(none)"}`, orderSub) + `<div class="ev">${evRow("Sizes available", (p.available || []).join(", "))}${evRow("Options", (r.line.variations || []).map(v => `<q>${esc(v.name)}: ${esc(v.value)}</q>`).join(" "))}</div><div class="fixes"><select data-f="size">${(p.available || []).map(s => `<option>${esc(s)}</option>`).join("")}</select><button class="btn gold sm" data-a="size">Use this size (staff decision)</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      c.querySelector("[data-a=size]").onclick = async () => { const s = c.querySelector("[data-f=size]").value; const who = by(); if (!who) return; r.sizeOverride = s; try { await DesignLink.call("notes.set", { receiptId: r.order.receiptId, text: `${sp.staffNote ? sp.staffNote + "\n" : ""}Size ${s} chosen by ${who} (sorter)` }); } catch (_) {} await repool(r); };
    } else if (it.kind === "oversize") {
      c.innerHTML = head("Oversize", `${p.sku} · ${p.widthMm.toFixed(1)} × ${p.heightMm.toFixed(1)} mm`, orderSub) + `<div class="ev">${evRow("Plate", `${labelOf(p.material)} ${fmt.mm(stockFor(p.material).wPt)} × ${fmt.mm(stockFor(p.material).hPt)} under the ${fmt.pct(S.settings.maxFill)} ceiling`)}</div><div class="fixes"><button class="btn ghost sm" data-a="stock">Different stock (Settings)</button><button class="btn ghost sm" data-a="retry">Try again</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      c.querySelector("[data-a=stock]").onclick = () => openSettings(); c.querySelector("[data-a=retry]").onclick = () => repool(r);
    } else if (it.kind === "engraveWords" || it.kind === "notRepresentable" || it.kind === "fontMissing") {
      const j = it.job; const miss = j.missing || [];
      const hl = t => esc(t).replace(/\n/g, "<br>"); const marked = miss.length ? [...(j.text || "")].map(ch => miss.includes(ch) ? `<span class="miss">${esc(ch)}</span>` : esc(ch) === "\n" ? "<br>" : esc(ch)).join("") : hl(j.text || "");
      c.innerHTML = head(it.kind === "notRepresentable" ? "Not representable" : it.kind === "fontMissing" ? "Font files missing" : "Engraving words", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Customer typed", `<q>${esc((sp.personalization || []).join(" / ") || "—")}</q>`)}${evRow("Buyer message", `<q>${esc(sp.buyerMessage || "—")}</q>`)}${evRow("Staff note", `<q>${esc(sp.staffNote || "—")}</q>`)}${sp.messages && sp.messages.length ? evRow("Staff messages", sp.messages.map(m => `<q>${esc(m.senderName)}: ${esc(m.text)}</q>`).join(" ")) : ""}${evRow("Claude read", claudeVerdict(j))}${j.questions && j.questions.length ? evRow("Claude asks", j.questions.map(q => `<q>${esc(q)}</q>`).join(" ")) : ""}${j.requests && (j.requests.font || j.requests.handwriting || j.requests.image || (j.requests.side && !["back", "unspecified"].includes(j.requests.side))) ? evRow("Customer asks", esc(JSON.stringify(j.requests))) : ""}</div><div class="why">${esc(it.why || j.reason || "")}</div>
        <div class="fixes"><textarea data-f="text">${esc(j.text || (sp.personalization || []).join("\n"))}</textarea></div>
        <div class="fixes"><button class="btn gold sm" data-a="confirm">Engrave this text</button>${miss.length ? `<button class="btn ghost sm" data-a="drop">Drop the character${miss.length > 1 ? "s" : ""} ${esc(miss.join(" "))}</button>` : ""}<button class="btn ghost sm" data-a="msg">Ask the customer</button><button class="btn ghost sm" data-a="none">Don't engrave</button>${it.kind === "fontMissing" ? `<button class="btn ghost sm" data-a="fonts">Retry font files</button>` : ""}</div>`;
      c.querySelector("[data-a=confirm]").onclick = () => Engrave.decideWords(j, { text: c.querySelector("[data-f=text]").value, note: c.querySelector("[data-f=text]").value.trim() !== (j.text || "").trim() ? "edited" : "confirmed" });
      const dr = c.querySelector("[data-a=drop]"); if (dr) dr.onclick = () => { let t = c.querySelector("[data-f=text]").value; for (const ch of miss) t = t.split(ch).join(""); c.querySelector("[data-f=text]").value = t.replace(/[ ]{2,}/g, " ").trim(); };
      c.querySelector("[data-a=msg]").onclick = async () => { const who = by(); if (!who) return; const draft = prompt("Message to post in the order's internal chat (the station staff will contact the customer):", `${who}: please confirm the engraving text for ${sp.designSku} — we read "${(j.text || "").replace(/\n/g, " / ")}"${miss.length ? `; the symbol ${miss.join(" ")} cannot be engraved exactly` : ""}.`); if (!draft) return; try { await DesignLink.call("chat.post", { receiptId: r.order.receiptId, text: draft, sender: "Charm Sorter" }); toast("Posted to the station chat", "ok"); } catch (e) { toast(e.message, "bad"); } };
      c.querySelector("[data-a=none]").onclick = () => Engrave.decideWords(j, { none: true });
      const fb = c.querySelector("[data-a=fonts]"); if (fb) fb.onclick = async () => { B.engrave.fonts.ok = false; B.engrave.fonts.error = null; await Engrave.loadFonts(); if (B.engrave.fonts.ok) { remove(it.key); await Engrave.setReady(j); RunCtl.poke(); } };
    } else if (it.kind === "flipFailed") {
      const j = it.job; const ch = it.checks || (j.flipError && j.flipError.checks) || {};
      c.innerHTML = head("Flip check failed", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Checks", Object.keys(ch).length ? Object.entries(ch).map(([k, ok]) => `${esc(k)} ${ok ? "✓" : "<b style='color:#8a3a26'>✗</b>"}`).join(" · ") : esc(it.why || ""))}</div><div class="imgs" style="display:flex;gap:8px"></div><div class="why">${esc(it.why || j.reason || "")}</div><div class="fixes"><button class="btn gold sm" data-a="rerun">Re-run</button><button class="btn ghost sm" data-a="noeng">No engraving for this order</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      const imgs = it.images || (j.flipError && j.flipError.images); if (imgs) { for (const [k, m] of [["front", imgs.F], ["back", imgs.B], ["front flipped", imgs.flipF]]) { if (!m) continue; const cv = document.createElement("canvas"); cv.width = m.w; cv.height = m.h; cv.style.cssText = "width:150px;border:1px solid var(--line);background:#fff"; cv.title = k; const ctx = cv.getContext("2d"); const img = ctx.createImageData(m.w, m.h); for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) { const i = ((m.h - 1 - y) * m.w + x) * 4, v = m.bits[y * m.w + x] ? 40 : 255; img.data[i] = v; img.data[i + 1] = v; img.data[i + 2] = v; img.data[i + 3] = 255; } ctx.putImageData(img, 0, 0); c.querySelector(".imgs").appendChild(cv); } }
      c.querySelector("[data-a=rerun]").onclick = () => { remove(it.key); j.state = "ready"; Engrave.fitJob(j).catch(e => toast(e.message, "bad")); };
      c.querySelector("[data-a=noeng]").onclick = async () => { const who = by(); if (!who) return; remove(it.key); Engrave.decideWords(j, { none: true, by: who }); };
    } else if (it.kind === "placement") {
      return Engrave.placementCard(it.job, 1);
    } else if (it.kind === "orderChanged") {
      c.innerHTML = head("Order changed", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Was", `<q>${esc(it.old && it.old.text || (it.old && it.old.spec && it.old.spec.personalization || []).join(" / ") || "—")}</q> · ${esc(it.old && it.old.spec ? `${it.old.spec.designSku} · ${it.old.spec.material || "?"} · ${it.old.spec.form || ""} ${it.old.spec.size || ""}` : "")}`)}${evRow("Now", `<q>${esc((sp.personalization || []).join(" / ") || "—")}</q> · ${esc(`${sp.designSku} · ${sp.material || "?"} · ${sp.form || ""} ${sp.size || ""}`)}`)}${evRow("Buyer / note", `<q>${esc(sp.buyerMessage || "—")}</q> / <q>${esc(sp.staffNote || "—")}</q>`)}</div><div class="why">${esc(it.why)}</div><div class="fixes"><button class="btn gold sm" data-a="accept">Accept the new order (re-read, re-fit)</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      c.querySelector("[data-a=accept]").onclick = async () => { remove(it.key); r.changePending=false; if(r.repoolChanged){await repool(r);RunCtl.poke();return;} if (r.state === "written" || r.state === "pooled") { if (sp.engraveCandidate) await Engrave.classify(r); else r.engrave = { needed: false, state: "none", approved: true }; } else await repool(r); RunCtl.poke(); };
    } else if (it.kind === "heldOrder") {
      c.innerHTML = head("Held order", it.rid, it.why) + `<div class="fixes"><button class="btn ghost sm" data-a="jump">Jump to the line's item</button></div>`;
      c.querySelector("[data-a=jump]").onclick = () => { remove(it.key); focus(it.line); };
    } else { c.innerHTML = head(it.kind, it.why || "", orderSub); }
    const skipB = c.querySelector("[data-a=skip]"); if (skipB) skipB.onclick = () => { const who = by(); if (!who) return; for (const rr of rowsOf(it)) { rr.state = "skipped"; rr.reason = `line skipped by ${who}`; rr.problems = []; rr.hold = `line skipped by ${who}`; } syncOrderItems(); Orders.render(); RunCtl.poke(); };
    const holdB = c.querySelector("[data-a=hold]"); if (holdB) holdB.onclick = () => { const who = by(); if (!who) return; const g = rowsOf(it); for (const rr of g) { rr.hold = `held by ${who}`; rr.reason = rr.hold; rr.state = "held"; } remove(it.key); Orders.render(); RunCtl.poke(); const ords = [...new Set(g.map(x => x.order.receiptId))]; toast(`${ords.length === 1 ? ords[0] : ords.length + " orders"} held by ${who} — release them from the Orders tab`, ""); };
    return c;
  }
  async function row_material(it, m, who) {
    for (const r of rowsOf(it)) {
      r.materialOverride = m;
      try { await DesignLink.call("notes.set", { receiptId: r.order.receiptId, text: `${r.spec.staffNote ? r.spec.staffNote + "\n" : ""}Material: ${labelOf(m)} (${who}, sorter)` }); } catch (e) { toast("Staff note not written: " + e.message, "bad"); }
    }
    await repoolAll(it);
  }
  const KIND_WORDS = { needsMaterial: "Material", needsMapping: "Options", unmatchedSku: "Unknown SKU", blockedSku: "Blocked SKU", missingSize: "Size", oversize: "Too big", fontMissing: "Font", engraveWords: "Words", notRepresentable: "Characters", flipFailed: "Flip", placement: "Placement", orderChanged: "Changed", heldOrder: "Held" };
  const RV = { filter: null, limit:40, open:null };
  let reviewFilter=null;
  const reviewRows=new Map();
  /** What a decision card is drawn from: while it reads the same, the card (and whatever is typed in it) is kept. */
  function stampOf(it) {
    const row=it.row || rowsOf(it)[0],group=rowsOf(it);
    return JSON.stringify([it.kind,it.why,it.problem,row?.spec,row?.line,row?.poolIds,group.map(r=>[r.key,r.order.receiptId])]);
  }
  /** The decision card inside another view (the order window): rebuilt only when its decision changed, and then with
   *  what was typed or picked carried over, as the Review list does. The window used to rebuild it on every repaint, so
   *  a repool elsewhere emptied the field being typed in. */
  function cardIn(host, it) {
    const stamp=stampOf(it);
    if(host._rvStamp===stamp&&host._rvKey===it.key&&host.firstChild)return;
    const was=host._rvKey===it.key?typedIn(host):[];
    host.innerHTML='';host.appendChild(card(it));host._rvStamp=stamp;host._rvKey=it.key;
    const refocus=putTyped(host,was);if(refocus)refocus();
  }
  function reviewRow(it) {
    const row=it.row || rowsOf(it)[0],group=rowsOf(it),open=RV.open===it.key;
    const stamp=stampOf(it);
    const cached=reviewRows.get(it.key);if(cached?.stamp===stamp)return cached.node;
    const was=typedIn(cached?.node?.querySelector('.reviewDetails'));   // carried into the rebuilt card
    const node=el('div','doneRow workRow reviewListRow'+(open?' open':''));node.dataset.row=row?.key || '';node.dataset.rid=String(row?.order?.receiptId || '');
    const orders=new Set(group.map(r=>r.order.receiptId));
    node.innerHTML=(row?ListMedia.pair(row):'<div class="compareUnavailable">Production review</div>')+`<div class="engravingIdentity"><span class="queueLabel">Review required</span><div class="engravingOrder"><b class="mono">${esc(row?.order?.receiptId || it.rid || 'Production')}</b><span class="sku mono">${esc(row?.spec?.designSku || row?.line?.sku || '')}</span></div><span class="purchaseLabel">${esc(KIND_WORDS[it.kind] || it.kind)}</span><span class="rowExcerpt reviewReason" title="${esc(it.why || '')}">${esc(it.why || 'Decision needed')}</span>${group.length>1 ? `<span class="groupScope">${orders.size} orders · ${group.length} lines · first item shown</span>` : ''}</div><div class="purchaseSummary">${row?purchaseMarkup(row):'<span class="purchaseMissing">Sheet-level decision</span>'}</div><div class="rowActions"><button class="btn ghost sm" data-review-open aria-expanded="${open}">${open?'Close details':'Review & resolve'}</button></div><div class="reviewDetails"${open?'':' hidden'}></div>`;
    const btn=node.querySelector('[data-review-open]'),detail=node.querySelector('.reviewDetails');
    const show=()=>{if(!detail.childNodes.length){detail.appendChild(card(it));const f=putTyped(detail,was.splice(0));if(f&&!node.isConnected)node._refocus=f;}detail.hidden=false;node.classList.add('open');btn.textContent='Close details';btn.setAttribute('aria-expanded','true');};
    btn.onclick=()=>{if(detail.hidden){RV.open=it.key;show();}else{RV.open=null;detail.hidden=true;node.classList.remove('open');btn.textContent='Review & resolve';btn.setAttribute('aria-expanded','false');}};
    if(cached?.node){const pair=cached.node.querySelector('.comparePair');if(pair)node.querySelector('.comparePair')?.replaceWith(pair);}
    if(open)show();reviewRows.set(it.key,{stamp,node});return node;
  }
  function render() {
    if(window.CharmNestInteraction?.defer('review-view',render))return;
    const v = document.getElementById("reviewView"); LiveStrip.render(); if (!v || v.classList.contains("hidden")) return;
    const active=v.contains(document.activeElement)?document.activeElement:null;
    const oldScroll=v.querySelector(".egPane.scroll")?.scrollTop || 0;
    if(reviewFilter!==RV.filter){RV.limit=40;reviewFilter=RV.filter;}
    const all = items().filter(it => mine(it) && !isNotice(it));
    const alive=new Set(all.map(it=>it.key));for(const key of reviewRows.keys())if(!alive.has(key))reviewRows.delete(key);
    const ORDER = ["needsMaterial", "needsMapping", "unmatchedSku", "blockedSku", "missingSize", "oversize", "fontMissing", "engraveWords", "notRepresentable", "flipFailed", "placement", "orderChanged", "heldOrder"];
    const arrivalOf = it => Math.max(0, ...(it.rows || [it.row]).filter(Boolean).map(r => r.arrivedAt || 0));
    all.sort((a, b) => arrivalOf(b) - arrivalOf(a) || ORDER.indexOf(a.kind) - ORDER.indexOf(b.kind) || a.t - b.t);
    // the kinds present are the filter: one chip each, so a long mixed list becomes the one kind being worked through
    const byKind = new Map(); for (const it of all) byKind.set(it.kind, (byKind.get(it.kind) || 0) + 1);
    if (RV.filter && RV.filter !== "done" && !byKind.has(RV.filter)) RV.filter = null;
    const list = RV.filter && RV.filter !== "done" ? all.filter(it => it.kind === RV.filter) : RV.filter === "done" ? [] : all;
    const chip = (id, label, n, cls) => `<button class="egTab${(RV.filter || "") === id ? " on" : ""}" data-k="${esc(id)}" title="${esc(label)}">${esc(label)}${n ? `<b class="${cls || "warn"}">${n}</b>` : ""}</button>`;
    if(!v.querySelector('#rvList'))v.innerHTML='<div class="ordBar egBar"></div><div class="egPane grow scroll"><div class="rvList" id="rvList"></div></div>';
    v.querySelector('.ordBar').innerHTML = `${chip("", "Everything", all.length, "info")}${ORDER.filter(k => byKind.has(k)).map(k => chip(k, KIND_WORDS[k] || k, byKind.get(k))).join("")}${settled.length ? chip("done", "Decided", settled.length, "ok") : ""}<span class="spacer"></span><button class="btn ghost xs" id="rvName" title="every decision is recorded under this name — click to change it">${esc(employeeName() || "set your name")}</button>`;
    v.querySelector("#rvName").onclick = () => { askEmployee(); render(); };
    v.querySelectorAll("[data-k]").forEach(b => b.onclick = () => { RV.filter = b.dataset.k || null; render(); });
    const host = v.querySelector("#rvList");
    if (RV.filter === "done") {
      // what this shift settled: the other half of "what has been approved", which the screen never used to say
      host.innerHTML=settled.length ? '' : '<div class="libEmpty">Nothing settled yet this session</div>';
      for(const d of settled.slice(0,RV.limit)){
        const node=el('div','doneRow workRow reviewListRow');node.dataset.rid=String(d.orders?.[0] || '');
        node.innerHTML=(d.row?ListMedia.pair(d.row):'<div class="compareUnavailable">Decision recorded</div>')+`<div class="engravingIdentity"><span class="queueLabel">Review · resolved</span><div class="engravingOrder"><b class="mono">${esc((d.orders || []).slice(0,2).join(' · '))}</b></div><span class="purchaseLabel">${esc(KIND_WORDS[d.kind] || d.kind)}</span><span class="rowExcerpt" title="${esc(d.why)}">${esc(d.why)}</span><span class="groupScope">${d.lines} lines</span></div><div class="purchaseSummary">${d.row?purchaseMarkup(d.row):''}</div><div class="rowActions"><span class="ost ok">Resolved</span><span class="by">${esc(d.by)}${d.t?' · '+fmtT(d.t):''}</span></div>`;
        host.appendChild(node);if(d.row)ListMedia.mount(node,d.row);
      }
      ListMedia.more(host,settled.length,Math.min(RV.limit,settled.length),()=>{RV.limit+=40;render();});
      v.querySelector('.egPane.scroll').scrollTop=oldScroll;
      return;
    }
    if (!list.length) host.innerHTML = `<div class="libEmpty">Nothing waits for a decision.</div>`;
    else {const desired=list.slice(0,RV.limit).map(it=>({it,node:reviewRow(it)})),keep=new Set(desired.map(x=>x.node));desired.forEach(({it,node},i)=>{if(host.children[i]!==node)host.insertBefore(node,host.children[i]||null);if(it.row)ListMedia.mount(node,it.row);});for(const node of [...host.children])if(!keep.has(node))node.remove();for(const {node} of desired)if(node._refocus){const f=node._refocus;node._refocus=null;f();}}
    ListMedia.more(host,list.length,Math.min(RV.limit,list.length),()=>{RV.limit+=40;render();});
    v.querySelector('.egPane.scroll').scrollTop=oldScroll;
    if(active?.isConnected)active.focus({preventScroll:true});
    const notices = items().filter(isNotice);
    if (notices.length) {
      host.insertAdjacentHTML("beforeend", `<div class="rvNotices"><div class="nHead">Left open on the station — no decision needed here</div>${notices.map(n => `<div class="nRow"><b class="mono">${esc(n.rid)}</b><span class="w">${esc(n.note || String(n.why || "").replace(n.rid + " held — ", ""))}</span><button class="btn ghost xs" data-open="${esc(n.line || "")}" title="open this order on the cards">Open order ↗</button></div>`).join("")}</div>`);
      host.querySelectorAll("[data-open]").forEach(b => b.onclick = () => { if (b.dataset.open) OrderWin.open(b.dataset.open); });
    }
  }
  return { view: () => RV, settled: () => settled, items, count, add, remove, render, card, cardIn, problemText, syncOrderItems, focus, repool };
})();

/* ═══ 24b · Sandbox — a stored copy of the open orders, an emulated Etsy, isolated records (nothing real is touched) ═══ */
const Sandbox = window.Sandbox = (() => {
  const on = () => WORKSPACE_SANDBOX;
  let status = null,refreshTask=null;
  /* ── the order stream: rather than the whole snapshot at once, the emulated Etsy lists 2 to 5 new orders per simulated
     ten minutes (etsySandbox builds them; charmNestLibrary sandboxStream keeps the seed and the clock). Each arrivals
     check moves the clock one step; SimClock plays the time between steps at the chosen speed. ── */
  const streaming = () => on() && S.settings.sandboxStream === "on";
  const speed = () => Math.max(1, Math.min(1000, Math.round(+S.settings.sandboxSpeed || 50)));
  let stream = null, readyTask = null;
  const streamApi = (action, extra) => api("charmNestLibrary", Object.assign({ op: "sandboxStream", action, seed: +S.settings.sandboxSeed || 0, speed: speed() }, extra || {}), { quiet: true });
  function adopt(s) { stream = s && s.on ? s : null; SimClock.set(stream && streaming() ? { base: stream.simNow, stepMs: stream.stepMs, speed: speed() } : null); render(); return stream; }
  /** The stream exists before the station's first sweep in the sandbox, or the emulator would list the whole snapshot.
      `strict` (a sweep about to go ahead): a stream that cannot start is an error, not a warning. */
  function ready(strict) {
    if (!streaming()) return Promise.resolve(null);
    const task = stream ? Promise.resolve(stream) : (readyTask ||= streamApi("ensure").then(r => adopt(r.stream)).finally(() => { readyTask = null; }));
    return task.then(s => { if (!s) throw new Error("it is off"); return s; }).catch(e => { if (strict) throw new Error(`The sandbox order stream could not start: ${e.message}`); agent({ bridge: true }, "warn", `Sandbox order stream: ${e.message}`); return null; });
  }
  /** One simulated step: the next ten minutes of orders become listable. The arrivals check calls it before it sweeps. */
  async function advance() {
    const s = await ready(true), r = await streamApi("tick", { expect: s.simNow });
    if (!r.stream) { adopt(null); throw new Error("The sandbox order stream was reset: the next check starts it again"); }
    return adopt(r.stream);
  }
  /** Settings were saved: the stream switched on or off, or plays at a new speed. */
  function restream() {
    if (!on()) return;
    stream = null;
    if (streaming()) { ready(); return; }
    SimClock.set(null); render(); streamApi("off").catch(e => toast(`Sandbox order stream: ${e.message}`, "bad", 6000));
  }
  const simText = t => new Date(t).toLocaleString("en-US", { weekday: "short", hour: "2-digit", minute: "2-digit", hourCycle: "h23" });
  /** What the pill and the arrivals counter say: the mode, and while the stream plays its speed and simulated time. */
  function label() { return !on() ? "" : streaming() && SimClock.on() ? `Sandbox ${speed()}x · sim ${simText(SimClock.now())}` : "Sandbox"; }
  function streamText() { return stream && streaming() ? `Order stream: seed ${stream.seed} · step ${stream.tick} · simulated ${new Date(SimClock.now()).toLocaleString()} · ${speed()}x` : on() && !streaming() ? "Orders: the whole snapshot at once" : ""; }
  async function refresh() {
    if (!S.cloud.ok) return null;if(refreshTask)return refreshTask;
    refreshTask=(async()=>{try {status=await api("charmNestLibrary",{op:"sandboxStatus"});}catch(e){status={error:e.message};}render();return status;})();
    try{return await refreshTask;}finally{refreshTask=null;}
  }
  /** One real read of the open orders through the station (production mode), stored as JSON under charmnest/sandbox/. */
  async function snapshot() {
    if (on()) throw new Error("switch the sandbox OFF first: the snapshot is taken from the real Etsy through the station");
    if (!S.cloud.ok) throw new Error("cloud offline");
    await DesignLink.ensure();
    if (!DesignLink.etsyBudgetOk("the sandbox snapshot")) throw new Error("Etsy call budget reached");
    const r = await DesignLink.call("orders.raw", { refresh: true }, { timeoutMs: 20 * 60 * 1000, onProgress: p => { if (p.text) agent({ bridge: true }, "DS", `Snapshot: ${p.text}`); } });
    DesignLink.meter(r, "the sandbox snapshot");
    const at = Date.now(); const path = `charmnest/sandbox/orders-${new Date(at).toISOString().replace(/[:.]/g, "-")}.json`;
    const bytes = new TextEncoder().encode(JSON.stringify({ at, count: r.count, receipts: r.receipts }));
    const up = await uploadBytes(path, bytes, "application/json", "Saving the sandbox snapshot");
    const put = await api("charmNestLibrary", { op: "sandboxPut", path: up.path, count: r.count, at, takenBy: employeeName() || "operator" });
    agent({ bridge: true }, "ok", `Sandbox snapshot: ${r.count} open order(s) copied to ${up.path} (${(bytes.length / 1024).toFixed(0)} KB)`);
    toast(`Snapshot taken: ${r.count} orders — switch the sandbox ON in Settings to run against it`, "ok", 8000);
    await refresh(); return put.snapshot;
  }
  const waitFor = (fn, ms, why) => new Promise((res, rej) => { const t0 = Date.now(); (function tick() { if (fn()) return res(true); if (Date.now() - t0 > ms) return rej(new Error(why)); setTimeout(tick, 500); })(); });
  /** The whole chain from one press: the station signed in (its own window if needed), the snapshot taken, the sandbox
      switched on, the sorter reloaded, and the orders pulled from the copy (a run starts by itself in Auto mode). */
  async function enable() {
    if (on()) return;
    if (!(status && status.snapshot)) {
      toast("No snapshot yet — taking one from Etsy first", "", 5000);
      await DesignLink.ensure();
      if (!(DesignLink.state() && DesignLink.state().etsy.signedIn)) {
        toast("The station is not signed in — Connect Etsy opens in its own window", "", 6000);
        await DesignLink.connectEtsy();
        await waitFor(() => DesignLink.state() && DesignLink.state().etsy.signedIn, 4 * 60 * 1000, "the Etsy sign-in did not complete within 4 minutes");
      }
      await snapshot();
    }
    S.settings.sandbox = "on"; saveSettings();
    try { sessionStorage.setItem("cn.sandboxAutoPull", "1"); } catch (_) {}
    toast(S.settings.sandboxStream === "on" ? "Sandbox ON — reloading; the orders then arrive a few at a time" : "Sandbox ON — reloading, then pulling the orders from the copy", "ok", 4000);
    setTimeout(() => location.reload(), 700);
  }
  /** After the reload that switched the sandbox on: straight to the Orders tab and a pull (Auto mode starts its run instead).
      With the stream the orders come by themselves, one simulated ten minutes per check. */
  function afterReload() {
    let want = false; try { want = sessionStorage.getItem("cn.sandboxAutoPull") === "1"; sessionStorage.removeItem("cn.sandboxAutoPull"); } catch (_) {}
    if (streaming()) ready(); else if (on()) streamApi("off").catch(() => {});   // this sorter asks for the whole snapshot: a stream left playing would hide it
    if (!want || !on()) return;
    setTimeout(async () => {
      setMode("orders");
      if (S.settings.runMode === "auto") { agent({ bridge: true }, "DS", "Sandbox on — Auto mode starts the run"); return; }
      if (streaming()) { toast(`Sandbox on — new orders arrive a few at a time, ${speed()}x faster than real time`, "ok", 6000); return; }
      try { await Orders.pull(null); } catch (e) { toast(e.message, "bad", 8000); }
    }, 900);
  }
  /** The station keeps the orders it finished in a browser ledger of its own, which the records' reset cannot reach: an
      order replayed under the same number would stay hidden there as finished. (A station without the command keeps it.) */
  const forgetCompletions = () => DesignLink.ensure().then(() => (DesignLink.state()?.commands || []).includes("sandbox.reset") && DesignLink.call("sandbox.reset", {}, { timeoutMs: 15000 }))
    .catch(e => agent({ bridge: true }, "warn", `Sandbox reset: the station kept its own list of finished orders (${e.message})`));
  async function reset() {
    const replay = streaming();
    if (!confirm(`Delete every sandbox record (sandbox pools, sets, runs, sheets, locks, ledger, archive) and the sandbox's files? The snapshot stays, and so do the engraving readings Claude was paid for. Production data is untouched.${replay ? " The order stream starts over, and the sorter clears its run and reloads." : ""}`)) return;
    // no arrivals check may sweep while the records go: with the stream deleted the emulator lists the whole snapshot
    await Arrivals.pause(); let reloading = false;
    try {
      if (replay && RunCtl.clearRunState(null, { drop: "all" }) === false) return;   // a Rose Gold sheet still saving: nothing is deleted
      // a sandbox that streamed for days holds more than one call can delete: each works a few seconds and says if more is left
      let r = null, records = 0, files = 0;
      for (let i = 0; i < 400; i++) { r = await api("charmNestLibrary", { op: "sandboxReset" }); records += r.deleted || 0; files += r.files || 0; if (!r.more) break; }
      if (r.more) throw new Error(`it stopped part way (${records} record(s) and ${files} file(s) removed): press Reset again to finish`);
      toast(`Sandbox reset — ${records} record(s) and ${files} file(s) removed${r.filesError ? ` · files not deleted: ${r.filesError}` : ""}`, r.filesError ? "bad" : "ok");
      await forgetCompletions();
      // the stream's clock, arrivals and orders went with the records: a replay starts from nothing, as the first one did
      adopt(null); Arrivals.reset();
      if (replay) { reloading = true; setTimeout(() => location.reload(), 1200); return; }
    } finally { if (!reloading) Arrivals.resume(); }
    await refresh();
  }
  /* No strip of its own any more: the SANDBOX pill in the top bar says the mode, the station's own banner says it again,
     and Reset and the switch live in Settings. */
  function mountPanel(v) { void v; const old = document.getElementById("sandboxBar"); if (old) old.remove(); const pill = document.getElementById("sandboxPill"); if (pill) { pill.style.cursor = "pointer"; pill.onclick = () => { if (window.CN && CN.openSettings) CN.openSettings(); else { const b = document.getElementById("btnSettings"); if (b) b.click(); } }; } if (!status) refresh(); }
  function statusText() { if (!status || status.error) return status && status.error ? `status: ${status.error}` : ""; const sn = status.snapshot; const rec = status.records || {}; return `${sn ? `snapshot of ${sn.count} order(s) taken ${new Date(sn.at).toLocaleString()}${sn.takenBy ? " by " + sn.takenBy : ""}` : "no snapshot yet"} · sandbox records: ${rec.Charm_Pool || 0} pool, ${rec.Charm_Nest_Sets || 0} sets, ${rec.Charm_Nest_Runs || 0} runs, ${rec.Charm_Nest_Sheets || 0} sheets${streamText() ? " · " + streamText() : ""}`; }
  function render() {
    const el = document.getElementById("sbStatus"); if (el) el.title = statusText() || el.title;
    const pill = document.getElementById("sandboxPill"); if (pill) { pill.classList.toggle("hidden", !on()); if (on()) { const text = label(); if (pill.textContent !== text) pill.textContent = text; pill.title = streamText() ? `Sandbox: emulated Etsy; all records and files use sandbox copies. ${streamText()}` : "Sandbox: emulated Etsy; all records and files use sandbox copies"; } }
    document.documentElement.classList.toggle("sandbox", on());
  }
  return { on, refresh, snapshot, enable, afterReload, reset, mountPanel, render, status: () => status, streaming, speed, ready, advance, restream, label, streamText, seed: () => stream && stream.seed, stream: () => stream };
})();


/* ═══ 24b · OrderWin — one line, everything about it, and the way to settle it ═══
   The Design Station's own order window, here: the picture, the SKU, what the customer typed, the staff note that saves
   itself, the internal thread every station shares, and — the reason it is worth having here — the review decision the
   line is waiting on, answered without leaving the order. Messages go over the bridge, so the station keeps the one Etsy
   session and the one Firestore listener and this page never grows a second of either. */
const OrderWin = window.OrderWin = (() => {
  const W = { key: null, rid: null, at: 0, dlg: null, thread: [], tray: [], poll: 0, noteTimer: 0, wired: false };
  const byId = id => document.getElementById(id);
  const rowOf = key => Orders.rows().find(r => r.key === key) || null;
  const me = () => employeeName() || "";

  function wire() {
    if (W.wired) return; W.wired = true;
    W.dlg = byId("orderWin"); if (!W.dlg) return;
    const close = () => W.dlg.close();
    byId("owClose").onclick = close;
    // a note typed just before the window closed (Escape, ×) is saved too: its timer and its blur both found no order
    W.dlg.addEventListener("close", () => { clearTimeout(W.noteTimer); saveNote(); clearInterval(W.poll); W.poll = 0; W.key = null; W.tray.forEach(t => { try { URL.revokeObjectURL(t.url); } catch (_) {} }); W.tray = []; try { window.CustomerMail?.orderClosed(); } catch (_) {} });
    byId("owPhoto").onclick = e => e.currentTarget.classList.toggle("zoom");
    byId("owCopy").onclick = async () => { const r = rowOf(W.key); const sku = r && (r.spec.designSku || r.line.sku); if (!sku) return; try { await navigator.clipboard.writeText(sku); toast("SKU copied", "ok", 1800); } catch (_) {} };
    byId("owWhoBtn").onclick = () => { askEmployee(); paintWho(); };
    const note = byId("owNote");
    note.oninput = () => { clearTimeout(W.noteTimer); W.noteTimer = setTimeout(saveNote, 700); };
    note.onblur = () => { clearTimeout(W.noteTimer); saveNote(); };
    const input = byId("owInput");
    const grow = () => { input.style.height = "auto"; input.style.height = Math.min(120, input.scrollHeight) + "px"; byId("owSend").disabled = !input.value.trim() && !W.tray.length; };
    input.oninput = grow;
    input.onkeydown = e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } };
    input.addEventListener("paste", e => { const f = [...(e.clipboardData || {}).items || []].filter(i => i.type.startsWith("image/")).map(i => i.getAsFile()).filter(Boolean); if (f.length) { e.preventDefault(); addFiles(f); } });
    byId("owSend").onclick = send;
    byId("owAttach").onclick = () => byId("owFile").click();
    byId("owFile").onchange = e => { addFiles([...e.target.files]); e.target.value = ""; };
    const pane = W.dlg.querySelector("#owPaneTeam") || W.dlg.querySelector(".owChat");
    pane.addEventListener("dragover", e => { e.preventDefault(); });
    pane.addEventListener("drop", e => { e.preventDefault(); addFiles([...(e.dataTransfer.files || [])].filter(f => f.type.startsWith("image/"))); });
    byId("owSkip").onclick = toggleSkip;
    byId("owFind").onclick = async () => {
      const r = rowOf(W.key); if (!r) return;
      try { await DesignLink.call("ui.scrollTo", { receiptId: r.order.receiptId }); toast("Shown on the Design Station", "ok", 2200); }
      catch (e) { toast(e.message, "bad", 5000); }
    };
    byId("owPrev").onclick = () => step(-1);
    byId("owNext").onclick = () => step(1);
    // the customer's side of the order: its own tab beside the team's chat (charm-nest-mail.js)
    try { window.CustomerMail?.orderWindow(W.dlg); } catch (e) { console.warn("customer mail:", e); }
  }
  /** The lines the Orders tab is showing, so Previous and Next walk what the person is actually looking at. */
  const siblings = () => Orders.visibleRows();
  function step(d) {
    const list = siblings(); const i = list.findIndex(r => r.key === W.key);
    // a line that has just left the list (skipped, or filtered out by the fix that was applied) resumes from where it was
    const from = i < 0 ? Math.min(W.at || 0, list.length - 1) : i;
    const at = from + d;
    if (at < 0 || at >= list.length) return;
    const next = list[at];
    if (next && next.key !== W.key) open(next.key);
  }
  function paintWho() {
    const w = byId("owWho"); if (w) w.textContent = me() || "— no name set —";
    // the thread and the staff note both travel through the Design Station: when that link is down, say so here rather
    // than letting a person type a message and meet an error
    const st = byId("owLink"); if (!st) return;
    const up = DesignLink.inControl() && DesignLink.up();
    const etsy = DesignLink.state() && DesignLink.state().etsy;
    const bad = !up ? "the Design Station link is down — messages and notes cannot be saved"
      : etsy && etsy.signedIn === false ? "the Design Station is not signed in to Etsy" : "";
    st.textContent = bad ? "● offline" : "● live";
    st.className = "owLink " + (bad ? "bad" : "ok");
    st.title = bad || "messages and notes are saving through the Design Station";
  }

  async function saveNote() {
    const r = rowOf(W.key); if (!r) return;
    const text = byId("owNote").value;
    // a note whose last save failed is sent again, although the box already shows it
    if (text === (r.spec.staffNote || "") && r.noteUnsaved == null) return;
    r.spec.staffNote = text; r.noteUnsaved = text;
    try {
      await DesignLink.call("notes.set", { receiptId: r.order.receiptId, text }, { quiet: true });
      // the order carries it too: each arrival re-reads the lines from their orders, which put the old note back
      r.order.staffNote = text; if (r.line.staffNote) r.line.staffNote = text;
      if (r.noteUnsaved === text) delete r.noteUnsaved;
      // the label promises "saved automatically": a save speaks up only to end an earlier failure (it used to toast at
      // every pause in the typing)
      if (r.noteFailed) { delete r.noteFailed; toast("Staff note saved", "ok", 1800); }
    } catch (e) { r.noteFailed = true; toast(`Staff note not saved: ${String(e.message).replace(/^staff note not saved:\s*/i, "")} — it is sent again when you leave the note or close this window`, "bad", 6000); }
    if (W.key === r.key) paintNote(r);
  }
  /** The note box shows what was typed; one that has not reached the station yet says so on the box itself. */
  function paintNote(r) {
    const note = byId("owNote"); if (!note) return;
    if (document.activeElement !== note) note.value = r.noteUnsaved != null ? r.noteUnsaved : (r.spec.staffNote || "");
    const unsaved = r.noteUnsaved != null;
    note.style.boxShadow = unsaved ? "inset 0 0 0 1px #8a3a26" : "";
    note.title = unsaved ? "not saved to the Design Station yet — it is sent again when you leave the note or close this window" : "";
  }
  function addFiles(files) {
    for (const f of files.slice(0, 6)) W.tray.push({ file: f, url: URL.createObjectURL(f) });
    paintTray(); byId("owSend").disabled = !byId("owInput").value.trim() && !W.tray.length;
  }
  function paintTray() {
    const t = byId("owTray"); if (!t) return;
    t.innerHTML = "";
    W.tray.forEach((x, i) => {
      const c = el("span", "chip", '<img crossorigin="anonymous" alt="" src="' + cors(x.url) + '"><span>' + esc(x.file.name.slice(0, 18)) + '</span><button type="button" title="remove">×</button>');
      c.querySelector("button").onclick = () => { try { URL.revokeObjectURL(x.url); } catch (_) {} W.tray.splice(i, 1); paintTray(); };
      t.appendChild(c);
    });
  }
  async function send() {
    const r = rowOf(W.key); if (!r) return;
    const who = me() || askEmployee(); if (!who) return;
    const text = byId("owInput").value.trim(), files = W.tray.slice();
    if (!text && !files.length) return;
    byId("owInput").value = ""; byId("owInput").style.height = "auto"; W.tray = []; paintTray(); byId("owSend").disabled = true;
    const rid = r.order.receiptId, unsent = files.slice(); let textSent = !text;
    try {
      for (const f of files) {
        const bytes = new Uint8Array(await f.file.arrayBuffer());
        const up = await uploadBytes("chatImages/" + rid + "/" + Date.now() + "_" + f.file.name.replace(/[^\w.\-]+/g, "_"), bytes, f.file.type || "image/png", "Sending image");
        await DesignLink.call("chat.post", { receiptId: rid, text: "Image attachment", sender: who, imageUrl: up.url });
        unsent.shift(); try { URL.revokeObjectURL(f.url); } catch (_) {}
      }
      if (text) { await DesignLink.call("chat.post", { receiptId: rid, text, sender: who }); textSent = true; }
      await loadThread(rid, true);
    } catch (e) {
      // what did not go goes back in the box to send again: the box was emptied before the send, and a failure lost it
      const back = W.key === r.key && W.dlg.open, input = byId("owInput");
      if (back) { if (!textSent && !input.value.trim()) input.value = text; W.tray = unsent.concat(W.tray); paintTray(); input.dispatchEvent(new Event("input")); }
      toast(`Message not sent: ${e.message}${back ? " — it is back in the box" : ""}`, "bad", 6000);
    }
  }
  async function loadThread(rid, force) {
    if (!rid || W.threadLoading===rid || (!force && W.rid === rid && W.thread.length)) return;
    W.threadLoading=rid;
    W.rid = rid;
    try { const res = await DesignLink.call("chat.list", { receiptId: rid, limit: 80 }, { quiet: true }); if(W.rid===rid)W.thread = res.messages || []; }
    catch (_) { const r = rowOf(W.key); if(W.rid===rid)W.thread = (r && r.spec.messages) || []; }
    if(W.threadLoading===rid)W.threadLoading=null;
    if (W.rid === rid) paintThread();
  }
  function paintThread(keep) {
    const t = byId("owThread"); if (!t) return;
    if (!W.thread.length) { t.innerHTML = '<div class="owEmpty"><b>No internal messages yet</b>Anything sent here reaches every station working this order. The customer never sees it.</div>'; return; }
    const mine = me().toLowerCase(), CM = window.CustomerMail, at = t.scrollTop;
    t.innerHTML = W.thread.map(m => {
      const own = String(m.senderName || "").toLowerCase() === mine && mine;
      const when = m.at ? new Date(m.at).toLocaleString("en-US", { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "";
      const words = m.text && m.text !== "Image attachment" ? m.text : "";
      // any message can be read in English or Ukrainian: two small buttons, and the translation under the original
      return '<div class="owMsg' + (own ? " me" : "") + '"' + (words && CM ? ' data-team-text="' + esc(words) + '"' : "") + '><span class="who">' + esc(m.senderName || "Staff") + (when ? " · " + esc(when) : "") + (words && CM ? CM.teamButtons(words) : "") + '</span>' +
        (words ? esc(words).replace(/\n/g, "<br>") : "") +
        (m.imageUrl ? '<img crossorigin="anonymous" loading="lazy" alt="" src="' + esc(cors(m.imageUrl)) + '">' : "") + (words && CM ? CM.teamTranslation(words) : "") + '</div>';
    }).join("");
    t.scrollTop = keep ? at : t.scrollHeight;
  }
  function toggleSkip() {
    const r = rowOf(W.key); if (!r) return;
    const who = me() || askEmployee(); if (!who) return;
    const on = r.state !== "skipped";
    if (on) { r.state = "skipped"; r.reason = "line skipped by " + who; r.problems = []; r.hold = r.reason; }
    else { r.state = "pulled"; r.reason = null; r.hold = null; Orders.interpretAll(); }
    Review.syncOrderItems(); Orders.render(); RunCtl.poke(); paint();
  }
  /** Paint the window from the row it is showing. */
  function paint() {
    const r = rowOf(W.key); if (!r) { if (W.dlg && W.dlg.open) W.dlg.close(); return; }
    const sp = r.spec || {};
    const sibs = (Orders.rows() || []).filter(x => x.order.receiptId === r.order.receiptId && x.state !== "gone");
    const li = sibs.findIndex(x => x.key === r.key);
    const list = siblings(); W.at = Math.max(0, list.findIndex(x => x.key === r.key));
    byId("owTitle").textContent = "Order " + r.order.receiptId + (sibs.length > 1 ? "  \u00b7  line " + (li + 1) + " of " + sibs.length : "");
    const pos = byId("owPos"); if (pos) pos.textContent = list.length ? (W.at + 1) + " of " + list.length : "";
    const pv = byId("owPrev"), nx = byId("owNext");
    if (pv) pv.disabled = W.at <= 0;
    if (nx) nx.disabled = W.at >= list.length - 1;
    const mp = byId("owMetal"); mp.textContent = r.material ? labelOf(r.material) : (sp.materialLabel || "no material");
    mp.className = "pill " + (r.material ? "neutral" : "bad");
    const ph = byId("owPhoto"); const url = Orders.imageFor(r);
    ph.classList.remove("zoom");
    ph.innerHTML = url ? '<img crossorigin="anonymous" alt="" src="' + esc(cors(url)) + '">' : '<span class="ph">no image</span>';
    ph.dataset.lid = String(r.line.listingId || ""); if (url) ph.dataset.painted = "1"; else { delete ph.dataset.painted; Orders.wantImage(r.line.listingId); }
    byId("owSku").textContent = "SKU: " + (sp.designSku || r.line.sku || "—");
    // the one field that must be read exactly: labelled, whole, and never boxed into a scroller under the staff note
    const said = [];
    if ((sp.personalization || []).length) said.push(["Personalisation", sp.personalization.join("\n")]);
    if (sp.buyerMessage) said.push(["Buyer message", sp.buyerMessage]);
    if (sp.messages && sp.messages.length) said.push(["Staff messages", sp.messages.map(m => `${m.senderName}: ${m.text}`).join("\n")]);
    const notes = byId("owNotes");
    notes.className = said.length ? "owSaid" : "owSaid none";
    notes.innerHTML = said.length ? said.map(([k, v2]) => `<span class="lbl">${esc(k)}</span>${esc(v2)}`).join("") : "— the customer wrote nothing —";
    paintNote(r);
    const st = Orders.statePill(r), where = Orders.placeOf(r);
    const mcell = (lbl, val) => '<div class="m"><i>' + esc(lbl) + '</i><span>' + esc(val) + '</span></div>';
    byId("owMeta").innerHTML =
      mcell("Quantity", String(sp.quantity || r.line.quantity || 1)) +
      mcell("Metal", r.material ? labelOf(r.material) : (sp.materialLabel || "none")) +
      mcell("State", st[1]) +
      (where ? mcell("Sheet", (where.set ? where.set + " · " : "") + (where.sheet || "")) : "") +
      mcell("Ship by", Orders.shipTxt(r)) +
      (sp.form ? mcell("Form", sp.form) : "") + (sp.size ? mcell("Size", sp.size) : "") + (sp.chain ? mcell("Chain", sp.chain) : "") +
      (r.engrave && r.engrave.needed ? mcell("Engraving", (r.engrave.approved ? "approved" : r.engrave.state || "waiting") + (r.engrave.text ? " · " + r.engrave.text : "")) : "") +
      (sp.options || []).filter(o => o.mapped).map(o => mcell(o.name, o.value)).join("") +
      mcell("Listing", String(r.line.listingId || "—")) +
      mcell("Title", r.line.title || "—");
    // the decision this line is waiting on, answered here; its card stays while the decision is the same, so a repaint
    // (a repool, another order arriving) never empties a field being typed in
    const fix = byId("owFix");
    const item = Review.items().find(x => (x.rows || [x.row]).some(y => y && y.key === r.key) && !String(x.key).startsWith("eng:"));
    if (item) {
      let slot = fix.querySelector(".owFix > .owFixCard");
      if (!slot) { fix.innerHTML = ""; const box = el("div", "owFix", '<div class="t">This line is waiting on a decision</div>'); slot = el("div", "owFixCard"); box.appendChild(slot); fix.appendChild(box); }
      Review.cardIn(slot, item);
    } else {
      fix.innerHTML = "";
      if (r.engrave && r.engrave.needed && !r.engrave.approved) {
        const box = el("div", "owFix", '<div class="t">Its engraving is still to be settled</div>');
        const b = el("button", "btn ghost sm", "Open it in Engraving");
        b.onclick = () => { W.dlg.close(); setMode("engrave"); Engrave.render(); };
        box.appendChild(b); fix.appendChild(box);
      }
    }
    const sw = byId("owSkip"); sw.setAttribute("aria-checked", r.state === "skipped" ? "true" : "false");
    paintWho();
  }
  function open(key, opts) {
    wire(); if (!W.dlg) return;
    const r = rowOf(key); if (!r) { toast("That line is no longer in the pull", "bad"); return; }
    W.key = key; W.thread = []; W.rid = null;
    paint();
    if (!W.dlg.open) W.dlg.showModal();
    try { window.CustomerMail?.orderShown(r, opts || {}); } catch (e) { console.warn("customer mail:", e); }
    paintThread();
    loadThread(r.order.receiptId, true);
    clearInterval(W.poll);
    W.poll = setInterval(() => { if (W.dlg.open && W.key && !document.hidden) loadThread(rowOf(W.key) ? rowOf(W.key).order.receiptId : null, true); }, 60000);
  }
  return { open, paint, close: () => W.dlg && W.dlg.close(), isOpen: () => !!(W.dlg && W.dlg.open), key: () => W.key, repaintThread: () => paintThread(true) };
})();

/* ═══ 24c · RunHistory — every run that ever ran, and the way back into one ═══════════════════════════════════════════
   Until now the only run a person could reach was the one in front of them. Yesterday's set, the order that went out on
   Tuesday, the sheet a charm was cut on — none of it had a door. This is the door: one list of runs, one search box over
   all of them, and two ways in — carry on with a run that never finished, or load a finished one back onto the cards to
   look at, download and print. It is reachable from the run banner, from Orders and from Engraving, because the question
   "which run was that?" is asked from wherever you happen to be standing. */
const RunHistory = window.RunHistory = (() => {
  const H = { q: "", when: "all", view: localStorage.getItem("cn.histView") || "cards", runs: [], sheets: [], sets: [], scanned: null, loading: false, err: null, dlg: null, open: new Set(), lines: new Map() };
  /* "Select previous run sets or days" is a filing question, so the dialog files them: four ways to narrow by time and
     state, and a heading for every day, because a flat list of eighty runs is a wall whatever order it is in. */
  const WHEN = [["all", "All"], ["today", "Today"], ["week", "Last 7 days"], ["open", "Unfinished"]];
  const DAY_MS = 86400000;
  function inWhen(r) {
    if (H.when === "open") return !["complete", "abandoned", "superseded"].includes(r.status);
    if (H.when === "all" || !r.day) return H.when === "all";
    const age = (Date.parse(today() + "T12:00:00") - Date.parse(r.day + "T12:00:00")) / DAY_MS;
    return H.when === "today" ? age === 0 : age >= 0 && age < 7;
  }
  function ensure() {
    if (H.dlg) return H.dlg;
    const d = el("dialog", "hist"); d.id = "histDlg";
    d.innerHTML = `<form method="dialog" class="x"><button class="btn ghost sm" value="cancel">Close</button></form>
      <h2>Sets</h2>
      <div class="hq"><input id="hQ" type="search" placeholder="order number, SKU, engraved words, a date, a set…" autocomplete="off">
        <button class="btn ghost sm" id="hRefresh" title="read the records again">Refresh</button></div>
      <div class="hWhen" id="hWhen"></div>
      <div class="hBody" id="hBody"></div>
      <div class="hFoot" id="hFoot"></div><button class="btn ghost sm" id="hMore" hidden>Load older sets</button>`;
    document.body.appendChild(d); H.dlg = d;
    const q = d.querySelector("#hQ");
    let t = 0;
    q.oninput = () => { H.q = q.value; clearTimeout(t); t = setTimeout(load, 260); };
    d.querySelector("#hWhen").onclick = e => { const b = e.target.closest("[data-when]"); if (b) { H.when = b.dataset.when; render(); return; } const v = e.target.closest("[data-view]"); if (v) { H.view = v.dataset.view; try { localStorage.setItem("cn.histView", H.view); } catch (_) {} render(); } };
    d.querySelector("#hMore").onclick = () => load(true);
    d.querySelector("#hRefresh").onclick = e => { e.preventDefault(); load(); };
    return d;
  }
  function show(q) {
    const d = ensure();
    if (q != null) { H.q = q; d.querySelector("#hQ").value = q; }
    if (!d.open) d.showModal();
    d.querySelector("#hQ").focus();
    load();
  }
  async function load(more = false) {
    if (!S.cloud.ok) { H.err = "the cloud is not connected, so there is nothing to read"; H.runs = []; H.sheets = []; render(); return; }
    H.loading = true; H.err = null; H.readAt = Date.now(); render();
    try {
      const query = H.q, request = (H.request || 0) + 1; H.request = request;
      // one page of the newest records (a search looks through 30 days of them, ending at this station's today);
      // `next` is where the older ones start, read only when asked for
      const r = await api("charmNestLibrary", { op: "history", q: query, limit: 60, today: today(), cursor: more ? H.next || null : null }, { quiet: true });
      if (H.request !== request || H.q !== query) return;
      const had = more ? H : { runs: [], sheets: [], sets: [] }, runIds = new Set(had.runs.map(x => x.runId)), sheetIds = new Set(had.sheets.map(x => x.id)), keys = new Set(had.sets.map(g => g.key || g.setId || ""));
      H.runs = had.runs.concat((r.runs || []).filter(x => !runIds.has(x.runId))); H.sheets = had.sheets.concat((r.sheets || []).filter(x => !sheetIds.has(x.id)));
      H.scanned = more && H.scanned && r.scanned ? Object.fromEntries(Object.keys(r.scanned).map(k => [k, (H.scanned[k] || 0) + (r.scanned[k] || 0)])) : r.scanned;
      H.next = r.next || null; H.from = r.window?.from || null;
      const groups = (r.sets || []).filter(g => !keys.has(g.key || g.setId || "")).map(g => {
        g.sheets.sort((a, b) => (a.sheetIndex || 0) - (b.sheetIndex || 0));
        const preview = g.sheets.find(x => x.metal === "gold" && x.preview) || g.sheets.find(x => x.preview);
        g.thumb = preview ? cors(preview.preview) : null; g.thumbOf = preview?.fileBase || null;
        return g;
      });
      // a run shown alone on one page and with its set on another is shown once, with its set
      const all = had.sets.concat(groups), named = new Set(all.filter(g => !String(g.key || "").startsWith("run:")).map(g => g.runId).filter(Boolean));
      H.sets = all.filter(g => !(String(g.key || "").startsWith("run:") && named.has(g.runId))).sort((a, b) => String(b.day || "").localeCompare(String(a.day || "")));
      const moreButton = H.dlg.querySelector("#hMore"); moreButton.hidden = !H.next; moreButton.textContent = query ? "Search older records" : "Load older sets";

    } catch (e) { H.err = e.message; H.runs = []; H.sheets = []; }
    H.loading = false; render();
  }
  const dayWord = d => d ? new Date(d + "T12:00:00").toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" }) : "";
  const STATUS = { processed: ["warn", "processing complete · follow-up"], complete: ["ok", "finished"], running: ["warn", "running"], review: ["warn", "waiting for a person"], paused: ["warn", "paused"], stopped: ["bad", "stopped"], abandoned: ["neutral", "given up"] };
  function render() {
    const b = H.dlg && H.dlg.querySelector("#hBody"); if (!b) return;
    const f = H.dlg.querySelector("#hFoot");
    if (H.loading && !H.runs.length) { b.innerHTML = `<div class="hEmpty"><span class="spin"></span> reading the records…</div>`; f.textContent = ""; return; }
    if (H.err) { b.innerHTML = `<div class="hEmpty bad">${esc(H.err)}</div>`; f.textContent = ""; return; }
    if (!H.sets.length && !H.runs.length && !H.sheets.length) {
      b.innerHTML = `<div class="hEmpty">${H.q ? `nothing matches “${esc(H.q)}”${H.from ? ` since ${esc(dayWord(H.from))}` : ""}` : "no runs on record yet"}</div>`;
      H.dlg.querySelector("#hWhen").innerHTML = "";
      f.textContent = [H.scanned ? `searched ${H.scanned.runs} runs and ${H.scanned.sheets} sheets` : "", H.next ? "older records below" : ""].filter(Boolean).join(" · ");
      return;
    }
    const cur = B.run && B.run.runId;
    const seenRuns = H.runs.filter(inWhen);
    const seenSets = H.sets.filter(inWhen);
    const wsel = H.dlg.querySelector("#hWhen");
    const countIn = (k, arr) => arr.filter(r => { const was = H.when; H.when = k; const yes = inWhen(r); H.when = was; return yes; }).length;
    wsel.innerHTML = WHEN.map(([k, lbl]) => `<button class="egTab${H.when === k ? " on" : ""}" data-when="${k}">${lbl}<b>${countIn(k, H.sets)}</b></button>`).join("")
      + `<span class="sp"></span><span class="viewSeg">${["cards", "list"].map(v => `<button data-view="${v}"${H.view === v ? ' class="on"' : ""} title="${v === "cards" ? "a picture of each set" : "one line per set"}">${v === "cards" ? "Cards" : "List"}</button>`).join("")}</span>`;
    if (!seenSets.length && !seenRuns.length) { b.innerHTML = `<div class="hEmpty">no sets ${H.when === "today" ? "today" : H.when === "week" ? "in the last seven days" : H.when === "open" ? "left unfinished" : "on record"}</div>`; f.textContent = ""; return; }
    const openRuns = seenRuns.filter(r => ["running", "review", "paused", "stopped"].includes(r.status) && r.runId !== cur);
    let lastDay = null, html = "";
    for (const g of seenSets) {
      if (g.day !== lastDay) { html += `<div class="hDay">${esc(dayWord(g.day) || "no date")}</div>`; lastDay = g.day; }
      const name = g.name || (g.seq ? `Set ${g.seq}` : "Working sheets");
      const mats = g.materials.map(m => labelOf(m)).join(" \u00b7 ");
      const isCur = g.runId && g.runId === cur;
      const openBtn = g.sheets.length ? `<button class="btn gold sm" data-a="open" title="Preview the saved sheets without replacing your current workspace">Preview</button>` : "";
      const exportButtons = g.sheets.length ? `<button class="btn ghost sm" data-export-set="${esc(JSON.stringify(g.sheets.map(sh => sh.id || sh.sheetId)))}" data-format="ai">Download .ai</button><button class="btn ghost sm" data-export-set="${esc(JSON.stringify(g.sheets.map(sh => sh.id || sh.sheetId)))}" data-format="dxf">.dxf</button>` : "";
      const resumeBtn = g.runId && openRuns.some(r => r.runId === g.runId) ? `<button class="btn ghost sm" data-a="resume" title="pick the unfinished run this set belongs to up where it stopped \u2014 it re-reads every order from Etsy first">Resume the run\u2026</button>` : "";
      html += H.view === "cards"
        ? `<div class="hSet card hoverItem${isCur ? " cur" : ""}" data-group="${esc(g.key || g.setId || "")}" data-set="${esc(g.setId || "")}" data-run="${esc(g.runId || "")}" tabindex="0">
            ${g.thumb ? `<img crossorigin="anonymous" class="hThumb" loading="lazy" alt="" src="${esc(cors(g.thumb))}" title="${esc(g.thumbOf || "")}">` : `<div class="hThumb ph">no preview</div>`}
            <div class="hRow"><span class="nm">${esc(name)}</span><span class="pill ${g.status === "complete" ? "ok" : "bad"}">${g.status}</span><span class="ct">${g.sheets.length} sheet${g.sheets.length === 1 ? "" : "s"} \u00b7 ${g.orders} order${g.orders === 1 ? "" : "s"}</span></div>
            <div class="hRow sub"><span class="ct">${esc(mats)}</span><span class="sp"></span>${isCur ? `<span class="pill neutral">on the cards</span>` : openBtn}${exportButtons}${resumeBtn}</div>
          </div>`
        : `<div class="hSet row hoverItem${isCur ? " cur" : ""}" data-group="${esc(g.key || g.setId || "")}" data-set="${esc(g.setId || "")}" data-run="${esc(g.runId || "")}"><div class="hRow">
            ${g.thumb ? `<img crossorigin="anonymous" class="hMini" loading="lazy" alt="" src="${esc(cors(g.thumb))}">` : `<span class="hMini ph"></span>`}
            <span class="nm">${esc(name)}</span><span class="pill ${g.status === "complete" ? "ok" : "bad"}">${g.status}</span>
            <span class="ct">${esc(mats)} \u00b7 ${g.sheets.length} sheet${g.sheets.length === 1 ? "" : "s"} \u00b7 ${g.orders} order${g.orders === 1 ? "" : "s"}</span><span class="sp"></span>${isCur ? `<span class="pill neutral">on the cards</span>` : openBtn}${exportButtons}${resumeBtn}</div></div>`;
    }
    // unfinished runs that wrote no sheet yet have nothing to picture, but can still be picked up
    for (const r of openRuns.filter(r => !seenSets.some(g => g.runId === r.runId))) html += `<div class="hSet row" data-run="${esc(r.runId)}"><div class="hRow"><span class="nm">${r.seq ? "Set " + r.seq : "run " + r.runId.slice(-8)}</span><span class="pill warn">${esc(r.status)}${r.stoppedBy ? " \u00b7 " + esc(r.stoppedBy) : ""}</span><span class="ct">${r.lines} line${r.lines === 1 ? "" : "s"} \u00b7 no sheet written yet</span><span class="sp"></span><button class="btn ghost sm" data-a="resume" title="pick it up where it stopped \u2014 it re-reads every order from Etsy first">Resume the run\u2026</button></div></div>`;
    b.innerHTML = `<div class="hSets ${H.view}">${html}</div>`;
    f.textContent = [H.scanned ? `searched ${H.scanned.runs} runs and ${H.scanned.sheets} sheets${H.from ? ` back to ${dayWord(H.from)}` : ""}` : "",
      H.next ? `${H.sets.length} groups shown — older records below` : ""].filter(Boolean).join(" · ");
    b.querySelectorAll(".hSet").forEach(node => {
      const setId = node.dataset.set || null, runId = node.dataset.run || null;
      const o = node.querySelector("[data-a=open]"); if (o) o.onclick = e => { e.stopPropagation(); if (H.dlg.open) H.dlg.close(); SetPicker.preview(H.sets.find(g => node.dataset.group ? (g.key || g.setId || "") === node.dataset.group : g.setId === setId && g.runId === runId)); };
      const rs = node.querySelector("[data-a=resume]"); if (rs) rs.onclick = e => {
        e.stopPropagation(); const r2 = H.runs.find(x => x.runId === runId) || {};
        if (!confirm(`Pick run ${r2.seq ? "Set " + r2.seq : String(runId).slice(-8)} up again?\n\nIt re-reads all ${r2.orders || ""} orders from Etsy through the Design Station before it can carry on, which takes a few minutes.\n\nTo look at what it already made, press Preview instead \u2014 that reads nothing from Etsy.`)) return;
        H.dlg.close(); RunCtl.resumeRun(runId).catch(err => toast(err.message, "bad", 7000));
      };
      if (o) node.onclick = e => { if (e.target.closest("button")) return; o.click(); };
    });
  }
  // every step of a run asks for a refresh; while the list is open it is read again at most once a minute
  let refreshTimer = 0;
  const refreshIfOpen = () => { if (H.dlg?.open && !refreshTimer) refreshTimer = setTimeout(() => { refreshTimer = 0; if (H.dlg?.open) load(); }, Math.max(700, (H.readAt || 0) + 60000 - Date.now())); };
  return { show, load, refreshIfOpen, runs: () => H.runs };
})();

/* ═══ 24d · Recall — a saved set back on the cards, from what was saved ═══════════════════════════════════════════════
   The material cards are the view. A recalled set is not a different screen: its sheets go back onto the GF card, the
   SS card, the RG card — as pages under the same tabs a live run uses — drawn from the picture each sheet saved when it
   was nested and the numbers in its record. One read of the sheet list; the pictures load as the cards scroll into view.
   Nothing is rebuilt from the master files unless a person presses "Rebuild to edit" on one card, and then only that
   sheet. The set's orders come back onto the Orders tab the same way, from the run record. */
const Recall = window.Recall = (() => {
  const RC = { runId: null, setId: null, live: null };
  const on = () => !!(RC.runId || RC.setId);
  /** Put a run's or a set's sheets onto their material cards. Instant: the slim sheet list is all it reads. */
  /* Recall stays on the tab it was asked from. Asked from Orders, the orders come up on Orders; from Nest, the sheets
     on Nest; from anywhere else, Nest — every tab is filled either way, because what is on the cards is what every
     tab is about, live or recalled. */
  let opening = null;
  async function open(sel) { if (opening) await opening.catch(() => {}); opening = openNow(sel); try { return await opening; } finally { opening = null; } }
  async function openNow(sel) {
    const from = S.mode;
    if(allSheets().some(p=>p.metal==='rose' && !p.roseCutAt && (p.rosePlan||p.roseProtected))){
      toast('The uncut Rose Gold contour is on the cards. Record its cut before replacing the cards with another set.','bad');
      return;
    }
    const q = sel.setId ? { setId: sel.setId } : { runId: sel.runId };
    const ls = await api("charmNestLibrary", Object.assign({ op: "listSheets", limit: 200 }, q), { label: "Reading the set" });
    /* One record per sheet name, the newest: before a re-nested sheet kept its identity, the library could hold two
       GF_Sep.17.26_Set-1_Sheet-1 records, one stale. */
    const byName = new Map();
    for (const x of ls.sheets || []) { const k = x.fileBase || x.id; const had = byName.get(k); if (!had || (x.updatedAt || 0) > (had.updatedAt || 0)) byName.set(k, x); }
    const sheets = [...byName.values()].sort((a, b) => (a.setSeq || 0) - (b.setSeq || 0) || (a.sheetIndex || 0) - (b.sheetIndex || 0));
    if (!sheets.length) { if (!sel.quiet) toast("Nothing is saved under that set", "bad", 5000); return; }
    if (B.run && !["complete", "abandoned"].includes(B.run.status)) { toast("Finish or abandon the current run before opening another set; your current decisions are kept", "bad", 6000); return; }
    if(RunCtl.clearRunState()===false)return;
    RC.runId = sel.runId || sheets[0].runId || null; RC.setId = sel.setId || null;
    for (const m of METALS) {
      const prim = S.sheets[m.key];
      // a recall replaces what a previous recall left: never pages stacked on pages
      for (const pg of prim.pages.slice(1)) { (pg.workers || []).forEach(w => w.terminate()); }
      prim.pages = [prim]; prim.active = 0; prim.el = prim.cardEl; prim.recalled = null; prim.charms = prim.charms.filter(c => !c.poolId); prim.status = prim.charms.length ? "ready" : "idle"; prim.placements = []; prim.fileBase = null; prim.setId = null; prim.seq = null; prim.sheetIndex = null;
      const mine = sheets.filter(x => x.metal === m.key); if (!mine.length) { CN.renderCard(prim); continue; }
      mine.forEach((rec, i) => {
        const pg = i === 0 ? prim.pages[0] : CN.addPage(m.key);
        pg.charms = []; pg.placements = []; pg.rejects = []; pg.outputs = null; pg.verification = rec.verification || null; pg.liveInfo = null; pg.dirty = false; pg.problem = null;
        delete pg.roseStock; delete pg.roseProtected; delete pg.roseHistory; delete pg.rosePlan; delete pg.rosePlanHash; delete pg.rosePlanKey; pg._roseLoaded=false; pg._roseError=null; pg.roseCutAt=rec.roseCutAt || null;
        pg.recalled = rec; pg.status = "complete"; pg.sheetId = rec.id; pg.runId = rec.runId || RC.runId; pg.setId = rec.setId; pg.seq = rec.setSeq; pg.setDay = rec.day; pg.cardStartedAt = rec.cardStartedAt || rec.createdAt || null; pg.sheetIndex = rec.sheetIndex; pg.fileBase = rec.fileBase; pg.group = null;
        pg.backPool = (rec.backs || []).map(bk => ({ sheetId:rec.id, approvedAt:bk.approvedAt, copy:bk.copy, previewWPt:bk.previewWPt, previewHPt:bk.previewHPt, pageWPt:bk.pageWPt, pageHPt:bk.pageHPt, poolId: bk.poolId, order: bk.order, sku: bk.sku, text: bk.text, lines: bk.lines || (bk.text ? String(bk.text).split("\n") : []), approvedBy: bk.approvedBy, capMm: bk.capMm, outputs: { png: bk.png ? { url: bk.png } : null, ai: bk.ai ? { url: bk.ai } : null } }));
        pg.cloud = Object.assign({ preview: rec.preview }, rec.outputs || {});
        pg.persistedDone = true; pg.persisted = Promise.resolve(); pg._img = null;
      });
      prim.active = 0; prim.el = prim.cardEl; CN.showPage(m.key, 0);
    }
    CN.refreshAllCards(); CN.renderRail(); CN.updateTopSub();
    // and the run's orders, as the Orders tab's own rows
    if (RC.runId) await ordersOf(RC.runId, sheets).catch(e => agent({ run: RC.runId }, "warn", `orders of the run: ${e.message}`));
    Engrave.fromRecall(); Review.syncOrderItems(); Review.render();
    agent({ run: RC.runId }, "cloud", `Recalled ${sheets.length} sheet(s)${sel.setId ? " of " + (sheets[0].setSeq ? "Set-" + sheets[0].setSeq : sel.setId) : ""} from ${sheets[0].day || "the record"} onto the cards — nothing is running`);
    setMode(["orders", "nest", "engrave", "review"].includes(from) ? from : "nest");
    Engrave.render(); Review.render();
  }
  async function ordersOf(runId, sheets) {
    // opened as a set, the Orders tab is that set's parcels: the orders the recalled sheets name
    const named = new Set((sheets || []).flatMap(x => (x.orders || []).map(String)));
    // the lines of orders the run was done with are in its line archive (RunCtl.save): asked for with the record's own,
    // for a set only those of its orders
    const r = await api("charmNestLibrary", Object.assign({ op: "runGet", runId, archived: true }, RC.setId && named.size ? { orders: [...named] } : {}), { quiet: true }); if (!r.run) return;
    await Orders.loadMaps().catch(() => {}); await Master.load().catch(() => {});
    let rows = Object.entries(r.run.lines || {}).map(([k, l]) => Orders.rowFromRecord(k, l));
    if (r.run.archiveTruncated) agent({ run: runId }, "info", `This run took more orders than one view shows: its ${rows.length} newest lines are listed. Open one of its sets for that set's orders, or search the run history.`);
    if (RC.setId && named.size) { const mine = rows.filter(x => named.has(String(x.order.receiptId))); if (mine.length) rows = mine; }
    B.orders.rows = rows;
    B.orders.byKey = new Map(B.orders.rows.map(x => [x.key, x]));
    B.orders.pulledAt = r.run.updatedAt || r.run.startedAt || null; B.orders.filtered = 0; B.orders.stale = false;
    const first = (sheets || []).find(x => x.setSeq) || {};
    B.orders.recalled = { runId, seq: first.setSeq || r.run.seq || +((/-(\d+)$/.exec(String(r.run.setId || "")) || [])[1] || 0) || null, day: first.day || r.run.day || null };
    Orders.interpretAll(); Orders.render();ListMedia.prepare(Orders.rows());
  }
  /** Bring one recalled sheet's charms back from the master files so it can be edited and nested again. On demand only. */
  // one rebuild per page at a time: a second press, or a button drawn again while it runs, joins the first instead of
  // adding every charm to the page twice
  function rebuild(pg) {
    if (pg._rebuilding) return pg._rebuilding;
    const task = rebuildOnce(pg).finally(() => { pg._rebuilding = null; });
    pg._rebuilding = task; return task;
  }
  async function rebuildOnce(pg) {
    const rec = pg.recalled; if (!rec) return;
    if(rec.roseCutAt || pg.roseCutAt)throw new Error("This layout was already cut. Start a new sheet for its remnant.");
    const bar = window.CNProgress ? CNProgress.start(`Rebuilding ${rec.fileBase || rec.id}`) : null;
    try {
      const d = (await api("charmNestLibrary", { op: "getSheet", id: rec.id })).sheet; if (!d) throw new Error("the sheet record is gone");
      for (const p of d.placements || []) {
        const rc = (d.charms || []).find(c => c.id === p.id); if (!rc || !rc.poolId) continue;
        const pool = B.pool.rows.get(rc.poolId) || ((await api("charmNestLibrary", { op: "poolGet", poolIds: [rc.poolId] })).pools || {})[rc.poolId]; if (!pool) continue; B.pool.rows.set(rc.poolId, pool);
        const entry = Master.entryFor(pool.sku) || await Master.fetchEntry(pool.sku); if (!entry) continue;
        const src = await Pool.masterCharm(entry, pool.size); const base = src.charms[0];
        const charm = Pool.cloneCharm(base, `${src.id}:${rc.poolId}`); charm.name = rc.name; charm.order = pool.orderId; charm.orderDate = pool.orderDate || 0; charm.arrivedAt = pool.arrivedAt || 0; charm.poolId = rc.poolId; charm.metal = d.metal; charm.lineKey = pool.lineKey; charm.orderInfo = { receiptId: pool.orderId, transactionId: pool.transactionId, sku: pool.sku, copy: pool.copy, quantity: pool.quantity, form: pool.form, size: pool.size }; charm.pinned = { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle };
        pg.charms.push(charm);
        pg.placements.push({ id: charm.id, angle: p.angle, cxPt: p.cxPt, cyPt: p.cyPt, wPt: p.wPt, hPt: p.hPt, layerName: p.layer, scale: 0.975 });
      }
      pg.recalled = null; pg.status = "complete"; pg.dirty = false;
      CN.computeSaturation(pg); CN.renderCard(pg);
      agent({ metal: pg.metal }, "cloud", `${rec.fileBase || rec.id}: ${pg.charms.length} charm(s) rebuilt from the master files — the sheet can be edited and nested again`);
    } finally { if (bar) bar.end(); }
  }
  return { open, rebuild, on, state: () => RC };
})();


/* ═══ 24e · Kin — the rest of the order, wherever it is ══════════════════════════════════════════════════
   A parcel with four charms in it is four lines, and they can be four cards on Orders, a row in Review, a chip in the
   engraving rail and a tile among the backs — on four different screens. Hovering any one of them lights the others,
   wherever they are, because the question a person is asking is always "what else is in this parcel?".

   One listener on the document does it for the whole application: anything that carries data-rid is kin to anything
   else with the same data-rid, and the ring is drawn only when there is more than one, because a single-piece order
   has no rest to show. Nothing has to register; new screens get it by carrying the attribute. */
const Kin = window.Kin = (() => {
  let cur = null;
  const all = rid => document.querySelectorAll(`[data-rid="${String(rid).replace(/["\\]/g, "")}"]`);
  function mark(rid) {
    if (rid === cur) return;
    if (cur) all(cur).forEach(n => n.classList.remove("kin"));
    cur = null;
    if (!rid) return;
    const kin = all(rid);
    if (kin.length < 2) return;                       // one piece is not a set: there is nothing to point at
    kin.forEach(n => n.classList.add("kin"));
    cur = rid;
  }
  function from(e) { const n = e.target && e.target.closest ? e.target.closest("[data-rid]") : null; mark(n ? n.dataset.rid : null); }
  function mount() {
    document.addEventListener("pointerover", from, true);
    document.addEventListener("focusin", from, true);
    document.addEventListener("pointerleave", () => mark(null), true);
    // a repaint under the cursor drops the classes with the old nodes: the next move puts them back
    document.addEventListener("scroll", () => mark(null), true);
  }
  return { mount, mark, of: () => cur };
})();

/* Durable workspace: large geometry stays in IndexedDB, never in the small localStorage quota.
   It is a checkpoint, not a second execution engine. Reload reconstructs PDF objects from the original bytes. */
const Session = window.Session = (() => {
  let dbP, db0 = null, ready = false, timer = 0, chain = Promise.resolve(), saving = false, forced = false, listening = false, quiet = false;
  const key = () => WORKSPACE_SANDBOX ? "sandbox" : "production";
  /* The sorter stays on for days (Paul, 24 Sep: "everything must be able to stay on indefinitely"). A checkpoint copies
     every sheet, order and decision, and one used to be written a second after every change, back to back while a sheet
     nested. Now one is written at most every ten seconds, never straight after the last, and not at all when nothing
     changed since it: rev counts the changes schedule() is told of, savedRev is the one the stored checkpoint holds. */
  const EVERY = 10000;
  let rev = 1, savedRev = 0, lastStart = 0, lastEnd = 0, lastMs = 0, failures = 0, failure = null, bestFailure = null;
  const bestPending = new Map(); let bestSaving = false, bestChain = Promise.resolve();
  const DROP = {};                                                      // a best-layout record to delete, in turn with the writes
  function queueBest(id, value) {
    bestPending.set(id, value);
    if (bestSaving) return bestChain;
    bestSaving = true;
    bestChain = bestChain.catch(() => {}).then(async () => {
      try {
        while (bestPending.size) {
          const [id, snapshot] = bestPending.entries().next().value; bestPending.delete(id);
          if (snapshot === DROP) { await remove(id).catch(() => {}); continue; }
          try { await io(snapshot, id); if (bestFailure) { bestFailure = null; banner(); } }
          catch (e) { if (!bestFailure) toast(`Best layout is kept in this tab, but its checkpoint could not be saved: ${e.message}`, "bad", 10000); bestFailure = { message: `best layout — ${e.message}`, at: Date.now() }; banner(); }
        }
      } finally { bestSaving = false; }
    });
    return bestChain;
  }
  function checkpointBest(sh) {
    if (!ready || !sh.sheetId || !sh.best || !sh.bestKey) return bestChain;
    return queueBest(key() + ":best:" + sh.sheetId, copy({ jobId: sh.jobId, bestKey: sh.bestKey, best: sh.best, bestRevision: sh.bestRevision, bestHistory: sh.bestHistory, bestWorker: sh.bestWorker, bestTrial: sh.bestTrial, bestInfo: sh.bestInfo }));
  }
  /** A best-layout record is read only to recover a sheet cut short while it nested, and one was kept for every sheet ever
      nested, for as long as the browser kept the workspace. Once the sheet is saved with its final layout, released or
      cleared, its record is deleted after the next checkpoint that no longer shows the sheet cut short (a crash before
      that still finds it), and never while the sheet is nesting again; a later write of the same sheet wins. */
  const afterSave = new Set(), cutShortIn = pages => new Set(pages.filter(p => ["nesting", "finishing", "queued"].includes(p.status) && p.sheetId).map(p => p.sheetId));
  function dropBest(sheetId) { if (sheetId) afterSave.add(sheetId); }
  function dropSaved(snapshot) {
    if (!afterSave.size) return;
    const held = new Set([...cutShortIn((snapshot.sheets || []).flatMap(g => g.pages || [])), ...cutShortIn(allSheets())]);
    for (const id of [...afterSave]) if (!held.has(id)) { afterSave.delete(id); queueBest(key() + ":best:" + id, DROP); }
  }
  /** At start, every best-layout record whose sheet is not one cut short in the restored workspace is left from a sheet
      saved, released or cleared long ago, or from a workspace that was not restored: it is deleted (keep: sheet ids). */
  async function sweepBest(keep) {
    try {
      const db = await open(), prefix = key() + ":best:";
      const keys = await new Promise((resolve, reject) => { const store = db.transaction("workspaces", "readonly").objectStore("workspaces"); if (typeof store.getAllKeys !== "function") return resolve([]); const req = store.getAllKeys(); req.onsuccess = () => resolve(req.result || []); req.onerror = () => reject(req.error); });
      const live = cutShortIn(allSheets());   // a sheet that started nesting since keeps the record it writes
      for (const k of keys) if (typeof k === "string" && k.startsWith(prefix) && !keep.has(k.slice(prefix.length)) && !live.has(k.slice(prefix.length))) queueBest(k, DROP);
    } catch (_) { /* tried again at the next start */ }
    return bestChain;
  }
  const OMIT = new Set(["parsed", "worker", "workers", "el", "cardEl", "pages", "persisted", "persisting", "bar", "evNest", "evSearch", "_img", "_geomVerify", "pool", "row", "job", "recalledFrom", "backSaving"]);
  // Bytes nothing changes once they are made (a file as read, a written sheet, a traced silhouette) go into the checkpoint
  // as they are: put() copies them itself, in the same task as the capture. Copying each here first held every one twice.
  const SHARED = new Set(["bits", "bytes", "ai", "labelled"]);
  function copy(v, seen = new Map(), k0) {
    if (v == null || typeof v !== "object") return typeof v === "function" ? undefined : v;
    if (seen.has(v)) return seen.get(v);
    if (ArrayBuffer.isView(v)) { const out = SHARED.has(k0) && v.slice ? v : v.slice ? v.slice() : new DataView(v.buffer.slice(v.byteOffset, v.byteOffset + v.byteLength)); seen.set(v, out); return out; }
    if (v instanceof ArrayBuffer) { const out = v.slice(0); seen.set(v, out); return out; }
    if (v instanceof Blob) return v;
    if (v instanceof Map) { const out = new Map(); seen.set(v, out); for (const [k, x] of v) out.set(k, copy(x, seen)); return out; }
    if (v instanceof Set) return new Set([...v].map(x => copy(x, seen)));
    if (!Array.isArray(v) && Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null) return undefined;
    const out = Array.isArray(v) ? [] : {}; seen.set(v, out);
    for (const [k, x] of Object.entries(v)) if (!(OMIT.has(k) && !(k === "pool" && typeof x === "boolean")) && !k.startsWith("_")) {
      // Defend checkpoints restored from older builds too: best.rec reaches
      // all prepared rotations, none of which is needed to recover a layout.
      let value = x;
      if (k === "best" && x && typeof x === "object") { const { grids, rec, ...layout } = x; value = layout; }
      const y = copy(value, seen, k); if (y !== undefined) out[k] = y;
    }
    return out;
  }
  function open() {
    if (!dbP) dbP = new Promise((resolve, reject) => {
      const req = indexedDB.open("charm-nest-workspace", 1);
      req.onupgradeneeded = () => req.result.createObjectStore("workspaces");
      req.onsuccess = () => resolve(db0 = req.result); req.onerror = () => reject(req.error);
    });
    return dbP;
  }
  function put(db, value, scope) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction("workspaces", "readwrite"); tx.objectStore("workspaces").put(value, scope);
      tx.oncomplete = () => resolve(); tx.onerror = () => reject(tx.error); tx.onabort = () => reject(tx.error || new Error("Workspace save interrupted"));
    });
  }
  async function io(value, scope = key()) {
    const db = await open(); if (value !== undefined) return put(db, value, scope);
    return new Promise((resolve, reject) => {
      const tx = db.transaction("workspaces", "readonly"), req = tx.objectStore("workspaces").get(scope);
      tx.oncomplete = () => resolve(req.result); tx.onerror = () => reject(tx.error); tx.onabort = () => reject(tx.error || new Error("Workspace save interrupted"));
    });
  }
  async function remove(scope) {
    const db = await open(); return new Promise((resolve, reject) => {
      const tx = db.transaction("workspaces", "readwrite"), store = tx.objectStore("workspaces");
      if (typeof store.delete !== "function") return resolve();
      store.delete(scope); tx.oncomplete = () => resolve(); tx.onerror = () => reject(tx.error); tx.onabort = () => reject(tx.error || new Error("Workspace clean-up interrupted"));
    });
  }
  /* Master designs that no sheet charm, pool row, open order line or back being edited uses are left out: a line that
     needs one again fetches it from the library (Pool.masterCharm; restore does not bring back that cache either), and
     the checkpoint kept every master ever loaded, bytes and all. */
  function poolSourcesInUse() {
    const all = S.poolSources || {}, ids = Object.keys(all); if (!ids.length) return all;
    const keep = new Set(), viaPath = p => { if (p) keep.add("pool:" + String(p).replace(/[^\w]+/g, "_")); };
    for (const p of allSheets()) for (const c of p.charms || []) keep.add(c.sourceId);
    for (const c of S.unassigned || []) keep.add(c.sourceId);
    for (const p of B.pool.rows.values()) if (p.state !== "committed") viaPath(p.aiPath);
    if (B.carry?.pools instanceof Map) for (const p of B.carry.pools.values()) viaPath(p.aiPath);
    for (const r of B.orders.rows || []) if (r.spec?.designSku && !["gone", "committed"].includes(r.state)) { try { const e = window.Master?.entryFor?.(r.spec.designSku); viaPath(e && window.Pool?.sizeEntry?.(e, r.spec.size)?.aiPath); } catch (_) {} }
    for (const j of B.engrave.items.values()) if (j.editCharm) keep.add(j.editCharm.sourceId);
    return ids.every(id => keep.has(id)) ? all : Object.fromEntries(ids.filter(id => keep.has(id)).map(id => [id, all[id]]));
  }
  /* The run's lines of orders it is done with (cut and committed, or gone) are left out where their rows are kept here:
     restore makes them again from those rows (Orders.lineRecord), as each run save does, and the run record keeps them
     in its line archive online. The lines of orders still in progress are kept as they are. */
  function runCopy(seen) {
    const r = B.run; if (!r || !r.lines) return copy(r, seen);
    const rows = new Set((B.orders?.rows || []).map(row => row.key)), from = new Set();
    try { for (const keys of O.closedOrders(r.lines).values()) for (const k of keys) if (rows.has(k)) from.add(k); } catch (_) { return copy(r, seen); }
    if (!from.size) return copy(r, seen);
    const out = copy(Object.assign({}, r, { lines: Object.fromEntries(Object.entries(r.lines).filter(([k]) => !from.has(k))) }), seen);
    out.linesFromRows = [...from]; return out;
  }
  function capture() {
    const seen = new Map();
    return { v: 1, at: Date.now(), packingCatalog:copy(S.packingCatalog, seen), carry: copy(B.carry, seen), run: runCopy(seen), orders: copy(B.orders, seen),
      sources: copy(S.sources, seen), poolSources: copy(poolSourcesInUse(), seen), unassigned: copy(S.unassigned, seen),
      sheets: METALS.map(m => ({ metal: m.key, active: S.sheets[m.key].active, pages: allSheets().filter(p => p.metal === m.key).map(p => copy(p, seen)) })),
      pools: copy(B.pool.rows, seen), sets: copy(B.sets, seen), jobs: [...B.engrave.items.values()].map(j => ({...copy(j, seen), ...(j.editingBack ? {editRow:copy(j.row, new WeakMap())} : {})})),
      review: B.review.items.map(it => Object.assign(copy(it, seen), { rowKey: it.row?.key, jobKey: it.job?.key })),
      mode: S.mode, orderViewVersion: 1, orderView: copy(Orders.view()), engravingView: copy(Engrave.view?.()), reviewView: copy(Review.view?.()), settled: copy(Review.settled?.()), gate: copy(Gate.state()), recall: copy(Recall.state()), logs: copy(LiveStrip.rows) };
  }
  /** Before each checkpoint, what is saved elsewhere leaves memory: written engravings keep only their placement
      (Engrave.shelveWritten) and saved sheets their cloud links (releaseSavedOutputs). */
  function tidy() {
    try { window.Engrave?.shelveWritten?.(); } catch (e) { console.warn("engraving upkeep", e); }
    try { if (window.releaseSavedOutputs) for (const p of allSheets()) window.releaseSavedOutputs(p); } catch (e) { console.warn("sheet upkeep", e); }
  }
  /** A save that failed is one standing line on the run banner (never a toast at each try), until the next one works. */
  function banner() { quiet = true; try { window.RunCtl?.renderBanner?.(); } catch (_) {} finally { quiet = false; } }
  function saved(e) {
    const was = failure;
    if (!e) { failures = 0; failure = null; if (was) banner(); return; }
    failures++; failure = { message: e.message || String(e), count: failures, at: Date.now() };
    if (failures === 1) toast(`Workspace could not be saved on this browser: ${failure.message}. Keep this tab open — it is tried again by itself.`, "bad", 10000);
    if (!was || was.message !== failure.message) banner();
  }
  /** When the next pass may start: ten seconds after the last one started, and never before the last has rested as long
      as it took (a second at least); after a failure, 15 s doubling to 5 min. */
  function due() {
    const t = Math.max(lastStart + EVERY, lastEnd + Math.max(1000, lastMs));
    return failures ? Math.max(t, lastEnd + Math.min(300000, 15000 * 2 ** (failures - 1))) : t;
  }
  function arm(delay) {
    if (!ready || timer || saving || rev === savedRev) return;
    timer = setTimeout(() => { timer = 0; flush(); }, Math.max(delay, due() - Date.now(), 0));
  }
  async function pass() {
    const scope = key(); lastStart = Date.now();
    try {
      await window.CharmNestInteraction?.idle();
      tidy();
      const db = await open(), at = rev, t0 = Date.now(), snapshot = capture();
      // captured and handed to IndexedDB in one task: nothing can change the shared bytes in between
      await put(db, snapshot, scope); lastMs = Date.now() - t0; savedRev = Math.max(savedRev, at); saved(null); dropSaved(snapshot);
    } catch (e) { saved(e); }
    finally { lastEnd = Date.now(); }
  }
  /** flush(true) writes now (after a pass in flight); flush() only when something changed and the pace allows. */
  function flush(force) {
    clearTimeout(timer); timer = 0; if (!ready) return chain;
    if (force) forced = true;
    else if (rev === savedRev) return chain;
    else if (due() > Date.now()) { arm(0); return chain; }
    if (saving) return chain;
    saving = true;
    chain = chain.catch(() => {}).then(async () => {
      try { do { forced = false; await pass(); } while (forced); }
      finally { saving = false; arm(1000); }
    });
    return chain;
  }
  function schedule() { if (quiet) return; rev++; arm(1000); }
  /** Leaving the page (reload, close, another tab): write the workspace now. flush() first waits for the pointer and the
      keyboard to rest and for a save already in flight, and a reloading page is gone before either happens, so whatever
      was done in the last second before a reload (a nudge, a word, a decision) was lost. The write is issued here, in the
      same task; IndexedDB orders it after any earlier write. Nothing changed since the last checkpoint: nothing to write. */
  function flushNow() {
    clearTimeout(timer); timer = 0; if (!ready || rev === savedRev) return;
    let snapshot; const scope = key(), at = rev;
    try { snapshot = capture(); } catch (_) { flush(true); return; }
    // synchronously on the open connection and committed at once: a transaction left to auto-commit, or created a
    // microtask later, is dropped with the unloading page
    const write = db => { const tx = db.transaction("workspaces", "readwrite"); tx.objectStore("workspaces").put(snapshot, scope); tx.oncomplete = () => { savedRev = Math.max(savedRev, at); saved(null); }; tx.onerror = () => saved(tx.error || new Error("Workspace save interrupted")); tx.commit?.(); };
    if (db0) { try { write(db0); } catch (_) { flush(true); } } else open().then(write).catch(() => {});
  }
  async function restore() {
    let d; try { d = await io(); } catch (e) { toast(`Workspace recovery unavailable: ${e.message}`, "bad"); return false; }
    if (!d || d.v !== 1) { sweepBest(new Set()); return false; }
    // the sheets cut short while nesting, whose best-layout records restore reads (and a reload before the next checkpoint reads again)
    const cutShort = cutShortIn((d.sheets || []).flatMap(g => g.pages || []));
    try {
      // A purged/abandoned cloud run must never be resurrected by an old browser checkpoint.
      if (d.run && S.cloud.ok) {
        let cloud;
        try { cloud = await api("charmNestLibrary", { op: "runGet", runId: d.run.runId }, { quiet: true }); }
        catch (e) { d.run.saveError = "Cloud check unavailable: " + e.message; }
        if (cloud?.run?.status === "abandoned" || cloud?.run?.status === "complete" && d.run.status !== "complete" || cloud && !cloud.run && d.run.cloudSavedAt && !d.run.saveError) { sweepBest(new Set()); return false; }
      }
      for (const src of (d.sources || []).concat(Object.values(d.poolSources || {}))) {
        if (src.bytes?.length) src.parsed = await P.parseSource(src.bytes, src.name);
        src.persisting = null; src.t0 = performance.now();
      }
      await Pool.repairRecoveredGeometry(d);
      S.packingCatalog = d.packingCatalog || {};
      S.sources = d.sources || []; S.poolSources = d.poolSources || {}; S.unassigned = d.unassigned || [];
      B.carry = d.carry;
      B.run = d.run; B.orders = d.orders; B.orders.byKey = new Map(B.orders.rows.map(r => [r.key, r]));
      // the lines of orders the run is done with come back from their rows (left out of the checkpoint, runCopy)
      if (B.run?.linesFromRows) { const lines = B.run.lines || (B.run.lines = {}); for (const k of B.run.linesFromRows) { const row = B.orders.byKey.get(k); if (row) lines[k] = Orders.lineRecord(row)[1]; } delete B.run.linesFromRows; }
      B.pool.rows = d.pools || new Map(); B.sets = d.sets || new Map();
      for (const group of d.sheets) {
        const prim = S.sheets[group.metal], card = prim.cardEl;
        const saved = group.pages[0]; Object.assign(prim, saved); prim.cardEl = card; prim.pages = [prim];
        for (const p of group.pages.slice(1)) prim.pages.push(p);
        for (const p of prim.pages) {
          const interrupted = ["nesting", "finishing", "queued"].includes(p.status);
          if (interrupted && p.sheetId && p.bestKey) {
            const checkpoint = await io(undefined, key() + ":best:" + p.sheetId).catch(() => null);
            if (checkpoint?.jobId === p.jobId && checkpoint.bestKey === p.bestKey && checkpoint.bestRevision >= (p.bestRevision || 0)) {
              Object.assign(p, checkpoint);
              p.placements = p.best.placements.map(pl => ({ ...pl })); p.rejects = (p.best.rejects || []).slice(); p.liveInfo = p.bestInfo; p.livePlacements = [];
              p.outputs = null; p.verification = null;
            }
          }
          p.worker = null; p.workers = []; p.pool = null; p.el = null; p.persisted = Promise.resolve();
          if (p.persistedDone === false || p.problem) { p.status = "ready"; p.dirty = true; }
          p.persistedDone = true;
          if (interrupted) { p.status = "ready"; p.dirty = true; p.stage = "Recovered best layout — ready to continue"; }
        }
        prim.active = Math.min(group.active || 0, prim.pages.length - 1); CN.showPage(group.metal, prim.active);
      }
      B.engrave.items = new Map((d.jobs || []).filter(j => j.editingBack && j.editRow || B.orders.byKey.has(j.key)).map(j => {
        j.lineGap ??= .18; j.row = j.editingBack ? j.editRow : B.orders.byKey.get(j.key); if (j.state === "fitting") j.state = "ready";
        return [j.key, j];
      }));
      B.review.items = (d.review || []).map(it => Object.assign(it, { row: B.orders.byKey.get(it.rowKey), job: B.engrave.items.get(it.jobKey) }));
      Engrave.restoreView?.(d.engravingView);
      if (Review.view) Object.assign(Review.view(), d.reviewView || {});
      if (Review.settled) Review.settled().splice(0, Review.settled().length, ...(d.settled || []));
      Object.assign(Gate.state(), d.gate || {}); Object.assign(Recall.state(), d.recall || {}); Object.assign(Orders.view(), d.orderView || {}, {view:d.orderViewVersion === 1 ? (d.orderView?.view || S.settings.orderView) : S.settings.orderView});
      LiveStrip.rows.splice(0, LiveStrip.rows.length, ...(d.logs || []));
      // An intake cut short by the refresh is taken up again. The flag was cleared only for a run that was running: a run
      // resting between updates ("processed") came back still marked busy, and every later update's orders were merged
      // but never pooled or nested (processPending and poke both wait for the flag), with no stop and no message.
      if (B.run && B.run.arrivalBusy) { B.run.arrivalBusy = false; if (B.run.status === "processed") window.Arrivals?.requeue?.(); }
      if (B.run && ["running", "review", "paused"].includes(B.run.status)) {
        B.run.arrivalBusy = false;
        B.run.workspaceRestored = true;
        if (allSheets().some(p => p.runId === B.run.runId && p.dirty) && O.stepIndex(B.run.step) > O.stepIndex("nest")) B.run.step = "nest";
        B.run.status = "stopped"; B.run.stoppedBy = "Workspace restored after refresh";
        if (B.run.intakeRecovery && O.stepIndex(B.run.step) > O.stepIndex("checkpoint")) B.run.step = "checkpoint";
        B.run.fix = "Your layouts and decisions are kept. Resume to continue processing.";
      }
      Orders.render(); Engrave.render(); Review.render(); RunCtl.renderBanner(); refreshAllCards(); renderRail(); updateTopSub();
      setMode(d.mode || "nest");
      sweepBest(cutShort);
      toast("Workspace restored — sheets, orders and decisions kept", "ok", 4000);
      return true;
    } catch (e) { toast(`Workspace recovery stopped: ${e.message}. The saved checkpoint is kept.`, "bad", 10000); throw e; }
  }
  function listen() {
    ready = true;
    if (listening) return; listening = true;
    for (const type of ["input", "change", "pointerup", "keyup"]) document.addEventListener(type, schedule, true);
    window.addEventListener("pagehide", flushNow);
    document.addEventListener("visibilitychange", () => { if (document.hidden) flushNow(); });
    // asked once, never waited for: a browser short of space may otherwise clear this workspace while the tab is closed
    try { navigator.storage?.persisted?.().then(p => p || navigator.storage.persist?.()).catch(() => {}); } catch (_) {}
  }
  return { copy, capture, restore, listen, flush, schedule, checkpointBest, dropBest, failure: () => failure || bestFailure };
})();

/* Import cadence is independent of run completion. The cloud ledger counts receipt IDs, not API calls. */
const Arrivals = window.Arrivals = (() => {
  const storageKey = () => "cn.arrivals." + (WORKSPACE_SANDBOX ? "sandbox" : "production");
  let state; try { state = JSON.parse(localStorage.getItem(storageKey()) || "null"); } catch (_) {}
  state = Object.assign({ seen: {}, lastCheck: 0, nextCheck: 0, lastAdded: 0, error: null }, state || {});
  let tick = 0, busy = false, paused = false, epoch = 0, fails = 0, lastTick = 0, kept = null;
  // a check that failed while the network was away is made again as soon as it is back, not a whole interval later
  window.addEventListener("online", () => { if (state.error) state.nextCheck = Math.min(state.nextCheck || Infinity, Date.now() + 3000); });
  window.addEventListener("storage", e => { if (e.key !== storageKey() || !e.newValue) return; try { const other = JSON.parse(e.newValue); Object.assign(state.seen, other.seen); if (other.lastCheck > state.lastCheck) { state.lastCheck = other.lastCheck; state.nextCheck = other.nextCheck; state.lastAdded = other.lastAdded; } paint(); } catch (_) {} });
  // the sandbox order stream checks every simulated ten minutes (12 s at 50x)
  const interval = () => (WORKSPACE_SANDBOX && S.settings.sandboxStream === "on" ? 600000 / Math.max(1, Math.min(1000, +S.settings.sandboxSpeed || 50)) : Math.max(10, Math.min(1440, +S.settings.pollMinutes || 10)) * 60000);
  const streaming = () => !!Sandbox.streaming?.();
  /* In Auto the stream's next step starts as soon as the sorter has taken in the last one: the charms come one after
     another with no time spent waiting between them (Paul, 24 Sep). Manual keeps a step every simulated ten minutes. */
  const eager = () => streaming() && S.settings.runMode === "auto" && !Recall.on();
  const stamp = () => (streaming() ? Math.round(SimClock.now()) : 0);   // first arrivals carry the stream's simulated time
  const at = id => state.seen[String(id)] || 0;
  const save = () => { try { localStorage.setItem(storageKey(), JSON.stringify(state)); } catch (_) {} };
  /* While the stream plays, its next step waits for the sorter: arrivals still being added (or, in Auto, waiting for the
     run to take them), a run at work, a sheet nesting. The simulated clock waits with it, so a replay never outruns the
     real processing and the sheets fill as a real day would fill them. Returns why, or "" when the sorter is free. */
  function held() {
    if (!streaming()) return "";
    const r = B.run, auto = S.settings.runMode === "auto" && !Recall.on();
    if (r?.arrivalBusy) return "adding the last arrivals";
    if (r?.status === "running") return "the run is at work";
    if (auto && r?.status === "stopped") return "the run is stopped";
    if (auto && state.pending) return "the last arrivals wait for the run";
    if (allSheets().some(p => ["nesting", "finishing"].includes(p.status) || p.status === "queued" && !window.CN?.heldForResume?.(p))) return "a sheet is nesting";
    // a sheet waiting for the stopped run's Resume holds the stream with it
    return allSheets().some(p => p.status === "queued") ? "the run is stopped" : "";
  }
  async function record(orders, simAt) {
    const now = Date.now(), when = simAt || now, ids = [...new Set(orders.map(o => String(o.receiptId)))];
    let stamps = Object.fromEntries(ids.map(id => [id, at(id) || when]));
    if (S.cloud.ok) {
      const r = await api("charmNestLibrary", Object.assign({ op: "arrivalRecord", orders: orders.filter(o=>!state.recorded?.[String(o.receiptId)]).map(o => ({ id: String(o.receiptId), createTs: +o.createTs || 0 })) }, simAt ? { now: simAt } : {}), { quiet: true });
      stamps = {...stamps,...r.firstSeen}; state.recorded ||= {}; for(const id of Object.keys(r.firstSeen || {}))state.recorded[id]=true; state.count24 = r.count24; state.count1 = r.count1;
    }
    const fresh = ids.filter(id => !at(id));
    Object.assign(state.seen, stamps); state.lastAdded = fresh.length;
    // first-arrival times are kept for 45 days, far past any open order; the list used to grow by every order ever seen
    const current = new Set(ids);
    for (const [id, t] of Object.entries(state.seen)) if (t < when - 45 * 86400000 && !current.has(id)) { delete state.seen[id]; if (state.recorded) delete state.recorded[id]; }
    state.lastCheck = now; state.nextCheck = now + interval(); save(); paint(); return fresh;
  }
  /* What the orders counter at the bottom of the page used to say. The counter is gone (Paul, 24 Sep: "completely remove
     that sandbox update, I don't wanna see any of that"); the text stays as the tooltip of the chip below, and for tests. */
  function text() {
    // while the sandbox stream plays, the counts, the countdown and the label read in its simulated time
    const sim = streaming(), real = Date.now(), now = sim ? SimClock.now() : real, times = Object.values(state.seen);
    // Use server aggregate counts; their timestamp is shown in the tooltip.
    const n24 = state.count24 ?? times.filter(t => t > now - 86400000).length, n1 = state.count1 ?? times.filter(t => t > now - 3600000).length;
    const left = Math.max(0, (state.nextCheck || real + interval()) - real) * (sim ? Sandbox.speed() : 1), wait = sim && !busy && !left ? held() : "";
    const inbox = Recall.on() && state.inbox?.length ? `${state.inbox.length} orders available · click to open · ` : "";
    const unread = !state.error && state.unread?.length ? `${state.unread.length} order${state.unread.length === 1 ? "" : "s"} unreadable, tried again next check · ` : "";
    const tail = state.error ? `Check failed: ${state.error}` : busy ? "Checking…" : S.settings.pollOrders === "off" ? "checks off" : wait ? `${unread}waiting for the sorter: ${wait}` : `${unread}next ${Math.floor(left / 60000)}:${String(Math.floor(left % 60000 / 1000)).padStart(2, "0")}`;
    return `${Sandbox.on() ? (Sandbox.label?.() || "Sandbox") + " · " : ""}Orders received · 24h ${n24} · 1h ${n1} · ${inbox}${tail}`;
  }
  /* Only what needs a person shows, as a small chip among the tools at the top right: new orders waiting behind an opened
     earlier set (a click puts the set away and brings them in), or order checks that fail (a click checks again now). The
     routine counts and countdown are not shown. */
  function paint() {
    if (streaming()) Sandbox.render();   // the sandbox pill keeps its simulated time
    for(const node of document.querySelectorAll('[data-new-order],.newArrival')){node.removeAttribute('data-new-order');node.classList.remove('newArrival');}
    const inbox = Recall.on() && state.inbox?.length ? state.inbox.length : 0, failed = !inbox && !!state.error && S.settings.pollOrders !== "off";
    let chip = document.getElementById("ordersChip");
    if (!inbox && !failed) { if (chip) chip.remove(); return; }
    const tools = document.querySelector(".topTools"); if (!tools) return;
    if (!chip) {
      chip = el("button", "ordersChip"); chip.id = "ordersChip"; chip.type = "button";
      chip.onclick = async () => {
        if (Recall.on() && state.inbox?.length) { const incoming = state.inbox; if (RunCtl.clearRunState() === false) return; delete state.inbox; await merge(incoming); return; }
        // the next tick checks, under the same lock as every check, so two open sorters still check once
        if (state.error && !busy) { state.nextCheck = Date.now(); save(); chip.disabled = true; }
      };
      tools.insertBefore(chip, document.getElementById("setPickerSlot"));   // among the tools, after the sandbox pill
    }
    chip.classList.toggle("bad", failed);
    chip.disabled = failed && busy;
    const label = inbox ? `${inbox} new order${inbox === 1 ? "" : "s"}` : busy ? "Checking orders…" : "Order check failed";
    if (chip.textContent !== label) chip.textContent = label;
    chip.title = inbox ? `${inbox} new order${inbox === 1 ? "" : "s"} came in while this earlier set is open. Click to put the set away and bring them in.`
      : `New orders could not be checked: ${state.error}. The sorter checks again by itself; click to check now.\n${text()}`;
  }
  async function merge(orders, openIds) {
    const current=Orders.rows();
    // the rows' update times by order and the open list as a set: each arrival was compared with every row, and each row
    // looked for in the whole open list
    const times=new Map();for(const row of current){const k=String(row.order.receiptId);if(!times.has(k))times.set(k,[]);times.get(k).push(+row.order.updateTs);}
    const changed=orders.some(o=>(times.get(String(o.receiptId))||[]).some(t=>t!==+o.updateTs));
    const open=Array.isArray(openIds)?new Set(openIds):null;
    const missing=!!open && current.some(row=>!["gone","committed"].includes(row.state) && !open.has(String(row.order.receiptId)));
    if(changed || missing){state.pending=true;state.revalidate=true;}
    const freshIds = await record(orders, stamp()), added = [];
    for (const order of orders) for (const line of order.lines || []) {
      const key = O.lineKey(order, line);
      if (B.orders.byKey.has(key)) continue; // Existing human decisions and pool membership belong to the existing row.
      const row = { key, order, line, arrivedAt: at(order.receiptId), spec: null, problems: [], state: "pulled", reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.unshift(row); B.orders.byKey.set(key, row); added.push(row);
    }
    Orders.interpretAll(); B.orders.pulledAt = Date.now();
    if (added.length) {
      state.pending = true; save();
      if (B.run && !["complete", "abandoned"].includes(B.run.status)) { B.run.orders = [...new Set((B.run.orders || []).concat(added.map(r => r.order.receiptId)))]; RunCtl.save().catch(() => {}); }
      toast(`${freshIds.length || new Set(added.map(r => r.order.receiptId)).size} new order(s) arrived`, "ok", 6000);
      notifyPerson("New Etsy orders", `${added.length} new order line(s) added to the sorter`);
    }
    Orders.render(); Review.render(); Engrave.render(); refreshAllCards(); Session.schedule(); paint();ListMedia.prepare(Orders.rows());
    return added;
  }
  async function processPending({checkpoint=false}={}) {
    if (!state.pending || S.settings.runMode !== "auto" || Recall.on()) return;
    const r = B.run;
    if (!r || ["complete", "abandoned"].includes(r.status)) { state.pending = false; save(); await RunCtl.start({ mode: "auto" }); return; }
    // Never alter the data while a run is producing labels or committing, nor revive an operator-stopped run.
    if (!((r.status === "review" && r.step === "engrave") || r.status === "processed" || checkpoint && r.status === "running" && r.step === "checkpoint") || r.arrivalBusy) return;
    state.pending = false; save(); r.arrivalBusy = true; RunCtl.renderBanner();   // the banner and ladder say the orders are going on now
    try {
      // The gold dot is a courtesy, never a lock (Orders.claim), and the station waves its cursor over the rows before it
      // writes it: the new charms go onto their sheets meanwhile, where they used to wait seconds for the wave.
      Orders.claim([...new Set(Orders.rows().filter(x => x.state === "pulled").map(x => x.order.receiptId))]).catch(() => {});
      if(state.revalidate){await Orders.revalidate(r,"during intake");state.revalidate=false;}
      await LiveNest.add(r);
      // the new lines' words are read and fitted beside the next orders (Engrave.background): the next charm never waits
      Engrave.background(r); RunCtl.save(r).catch(() => {});
    } catch (e) {
      state.pending = true; save();
      // Stop pressed while the orders went on keeps its own plain reason: the orders not placed yet wait, and this intake
      // runs again after Resume. It used to be stopped a second time, as "New orders could not be added: stopped by the
      // operator", with a red toast and an alert.
      if (r.status !== "stopped") RunCtl.stop(`New orders could not be added: ${e.message}`, "Resume after fixing the cause. Your earlier work is kept.");
    }
    finally { r.arrivalBusy = false; RunCtl.poke(); Session.schedule(); window.CN?.flushManualIntake?.(); }
  }
  async function check() {
    if (busy || paused || S.settings.pollOrders === "off") return;
    busy = true; paint(); const began = Date.now(), gen = epoch;
    try {
      await DesignLink.ensure();
      if (!DesignLink.etsyBudgetOk("new orders")) throw new Error("Etsy hourly cap reached");
      // the stream's next simulated ten minutes of orders become listable, and the station sweeps for them (no reuse window)
      // (the maps load alongside: the snapshot below needs both, neither needs the other)
      const [sim] = await Promise.all([streaming() && Sandbox.advance(), Orders.loadMaps(), Master.load()]);
      const known=Object.fromEntries(Orders.rows().map(row=>[String(row.order.receiptId),+row.order.updateTs || 0]));
      // (the snapshot of a check that failed after it, on the cloud's side, is taken again for 2 minutes: the Etsy calls it
      // cost are not made a second time)
      const res = !sim && kept && Date.now() - kept.at < 120000 ? kept.res : await DesignLink.call("orders.snapshot", Object.assign({ hydrate: true, refresh: true, intake:true, known }, sim ? { stream: true } : {}), { timeoutMs: 20 * 60000, quiet: true });
      if (res !== kept?.res) { DesignLink.meter(res, "new orders"); kept = sim ? null : { res, at: Date.now() }; }
      if (gen !== epoch) return;   // a sandbox reset while this check was out: its orders went with the records it deleted
      // An order Etsy will not return in full waits for the next check, where it is read again; the others come in now.
      // One unreadable receipt used to hold every other arrival back for as long as it stayed unreadable.
      const unread = (res.orders || []).filter(o => !o.hydrated);
      if (res.hydrated < res.total && (!res.hydrated || unread.length < res.total - res.hydrated)) throw new Error(`Only ${res.hydrated} of ${res.total} orders could be read`);
      state.unread = unread.map(o => String(o.receiptId));
      if (unread.length) agent({ bridge: true }, "warn", `${unread.length} of ${res.total} order(s) could not be read from Etsy (${state.unread.slice(0, 6).join(", ")}${unread.length > 6 ? "…" : ""}) — read again at the next check; the other ${res.hydrated} came in now`);
      const picked = Orders.applyPullRule((res.orders || []).filter(o => o.hydrated));
      if (Recall.on()) { await record(picked, stamp()); state.inbox = picked; }
      else await merge(picked, res.openIds);
      state.error = null; kept = null;
      processPending().catch(e=>{state.error=e.message;save();paint();});
    } catch (e) { if (gen === epoch) state.error = e.message; }
    // A check that failed for a passing reason (the network not back after a wake, a 5xx, the station slow to answer) is
    // made again 15 s, 30 s, 1 min, 2 min… later, never later than the usual interval; it used to wait the whole interval.
    finally { busy = false; if (gen === epoch) { const passing = /answer in time|timed out|network|Failed to fetch|HTTP 5\d\d|link is down|no reply|closed|offline/i.test(state.error || ""); fails = passing ? fails + 1 : 0; state.nextCheck = eager() ? Date.now() : (streaming() ? began : Date.now()) + (passing ? Math.min(interval(), 15000 * 2 ** Math.min(fails - 1, 8)) : interval()); save(); } paint(); }   // a stream step is timed from its check's start, so the sweep does not stretch it
  }
  function start() {
    clearInterval(tick); state.nextCheck = eager() ? Date.now() : Math.max(Date.now(), (state.lastCheck || Date.now()) + interval());
    paint();
    tick = setInterval(() => {
      // just woken (the machine slept: this tick came over a minute late), the network is given 10 s before the check
      const t = Date.now(); if (lastTick && t - lastTick > 60000 && state.nextCheck < t + 10000) state.nextCheck = t + 10000; lastTick = t;
      paint();
      if (busy || paused || S.settings.pollOrders === "off") return;
      if (Date.now() >= state.nextCheck && !held()) {
        if (navigator.locks) navigator.locks.request(storageKey(), { ifAvailable: true }, lock => { if (!lock) return; let shared; try { shared = JSON.parse(localStorage.getItem(storageKey()) || "null"); } catch (_) {} if (shared?.nextCheck > Date.now()) { Object.assign(state.seen, shared.seen); state.lastCheck = shared.lastCheck; state.nextCheck = shared.nextCheck; return; } return check(); }).catch(e => { state.error = e.message; });
        else check();
      } else processPending().catch(e => { state.error = e.message; });
    }, streaming() ? 250 : 1000);   // a stream step is 12 s at 50x: a whole second late would be most of a simulated minute
  }
  /** A sandbox reset deletes the records under the checks: none starts meanwhile, and one still out is waited for and its
      orders dropped (the stream it stepped is gone, and the records its orders would join). */
  async function pause() { paused = true; epoch++; for (const t = Date.now(); busy && Date.now() - t < 30000;) await new Promise(r => setTimeout(r, 200)); }
  const resume = () => { paused = false; };
  /** A sandbox reset starts the counts over with the records they counted. */
  function reset() { state = { seen: {}, lastCheck: 0, nextCheck: Date.now() + interval(), lastAdded: 0, error: null }; save(); paint(); }
  /** An intake interrupted by a page refresh runs again at the next tick (Session.restore). */
  function requeue() { state.pending = true; save(); paint(); }
  return { start, check, merge, record: orders => record(orders, stamp()), at, paint, text, processPending, held, pause, resume, reset, requeue, state: () => state };
})();

/* A new batch tries the newest open sheet. Existing sheets with approved backs are pinned; their approvals survive.
   Only uncommitted sheets belong here. The same solver and both existing verifiers still decide acceptance. */
const LiveNest = window.LiveNest = (() => {
  function prepareSheet(sh) {
    const items=activeCharms(sh);
    // every batch fills: charms go in one at a time and the sheet is never re-arranged, at a count, a fill level or a
    // miss (Paul, 24 Sep: redoing a sheet loses capacity); the budget is only a ceiling, the search ends when placed
    const plan={phase:'fill',budgetMs:(+S.settings.budgetS||180)*1000};
    sh.intakePhase=plan.phase;sh.intakeBudgetMs=plan.budgetMs;delete sh.intakeAppend;
    const placed=new Map((sh.placements||[]).map(p=>[p.id,p]));for(const c of items){const p=placed.get(c.id);if(p&&!c.pinned){c.pinned={cxPt:p.cxPt,cyPt:p.cyPt,angle:p.angle};c.arrivalPin=true;}}
    return plan;
  }
  // the page's own marks first; the sets of its run are looked at only when none says so
  const closed = p => !!p.roseCutAt || !!p.recalled || !!p.releaseFull || (!(p.metal==='rose'&&(p.rosePlan||p.roseProtected)) && (!!p.runHold || !!p.intakeFinalized)) ||
    Sets.ofRun(p.runId).some(set=>set.committedAt && set.sheetIds.includes(p.sheetId));
  /* Gold and Silver arrivals go to the run's earliest open sheet first (the pool puts them on the same one). An order
     that misses its gaps moves on to the next sheet, and the earlier sheet stays first in line: once a newer page existed
     it used to be passed over for good, short of full, so it was never released and its orders, the oldest, never cut.
     Rose Gold keeps the newest sheet, where its green line is. */
  function intakePage(m, run) {
    const pages = pagesOf(m), newest = pages.at(-1);
    if (!O.FAST_MATERIALS.has(m)) return newest;
    return pages.find(p => p !== newest && p.charms.length && run && p.runId === run.runId && !closed(p)) || newest;
  }
  async function add(run) {
    const prepare=async()=>{
    // stopped before these orders went on: they wait as they are, and the intake runs again after Resume
    if (run.status === "stopped") throw new Error(run.stoppedBy || "run stopped");
    run.intakeRecovery = run.intakeRecovery || { retire: [], backs: [] };
    const touched = new Set(Orders.rows().filter(r => ["pulled", "waiting"].includes(r.state)).map(r => r.spec?.material).filter(Boolean));
    const before = new Set(allSheets().flatMap(p => p.charms.map(c => c.poolId)).filter(Boolean));
    // An order can join previously independent material sets. Re-number that connected group together.
    const potential = O.kinGroups(Orders.rows().filter(r => r.spec?.material && !["gone", "noDesign"].includes(r.state)).map(r => ({ orderId: String(r.order.receiptId), material: r.spec.material })));
    for (const m of [...touched]) for (const linked of (potential[m] || m).split("+")) touched.add(linked);
    // The legacy grouping pass would retire old sheets after Pool.addAll has
    // already touched them. Refuse an unsafe regroup before claiming orders.
    if(!Gate.modern(run.runId))for(const group of new Set(Object.values(potential))){
      const old=Sets.ofRun(run.runId).filter(set=>set.group!==group && set.materials.some(m=>group.split('+').includes(m)));
      if(old.some(set=>allSheets().some(p=>p.setId===set.setId && p.metal==='rose' && (p.rosePlan||p.roseProtected) && !p.roseCutAt)))
        throw new Error('The protected Rose Gold contour belongs to an existing set. Finish that set before regrouping its sheets.');
    }
    async function reconcileGroups() {
    if (Gate.modern(run.runId)) return;
    for (const group of new Set(Object.values(run.groups || {}))) {
      const old = Sets.ofRun(run.runId).filter(s => s.materials.some(m => group.split("+").includes(m)) && s.group !== group);
      if (!old.length) continue;
      if (old.some(s => s.committedAt)) throw new Error("A related material set was already committed; finish this run before adding the new order");
      if(old.some(set=>allSheets().some(p=>p.setId===set.setId && p.metal==='rose' && (p.rosePlan||p.roseProtected) && !p.roseCutAt)))
        throw new Error('The protected Rose Gold contour belongs to an existing set. Finish that set before regrouping its sheets.');
      await Sets.ensure(run.runId, group);
      for (const set of old) {
        set.status = "superseded"; await Sets.save(set);
        for (const [key, value] of B.sets) if (value === set) B.sets.delete(key);
        for (const p of allSheets().filter(p => p.setId === set.setId)) {
          if (p.sheetId) { run.intakeRecovery.retire.push(p.sheetId); }
          p.sheetId = null; p.fileBase = null; p.setId = null; p.seq = null; p.sheetIndex = null; p.group = group;
        }
      }
      run.setIds = Sets.ofRun(run.runId).map(s => s.setId); run.setId = run.setIds[0] || null;
    }
    }
    // the layout each page to be filled had before, kept for that page only (every page's placements used to be copied)
    const previousSheets=new Map();
    const target = new Map(), force = Object.assign({}, Gate.state().forceFill);
    for (const m of touched) {
      const prim = S.sheets[m], pages = prim.pages.filter(p => p.runId === run.runId && !closed(p));
      if (pages.some(p => ["nesting", "finishing", "queued"].includes(p.status))) throw new Error("A sheet is still being written");
      const pick=intakePage(m, run);
      const p = pick.runId && pick.runId!==run.runId || closed(pick) ? addPage(m) : pick;
      if(p===pick)previousSheets.set(p,{placements:p.placements.slice(),intakeOptimized:p.intakeOptimized,intakeOptimizedCount:p.intakeOptimizedCount,density:p.density,liveInfo:p.liveInfo});
      // addPage inherits the primary page's run id; replace it before
      // Pool.addAll examines the newest page or it creates yet another page.
      if(p!==pick)p.runId=run.runId;
      // the card follows the page being filled. Setting the index alone left the card drawing the old page while the new
      // one nested unseen, and the card's buttons (they act on the active page) acting on a page nobody could see
      target.set(m, p); prim.active = prim.pages.indexOf(p); if (!p.el) window.CN?.showPage(m, prim.active);
      if (pages.some(p => p.charms.length)) Gate.state().forceFill[m] = true;
    }
    try { await Pool.addAll(run); } finally { Gate.state().forceFill = force; }
    await reconcileGroups();
    for (const [m, pg] of target) {
      const all=pg.charms,saved=previousSheets.get(pg);
      // An order that links materials brings every linked material here, and lines can wait for a later sheet, so a
      // material can come through with nothing new. A page opened for it above stays empty: it is taken away again,
      // not nested. After a set was released, the empty page stayed queued and stopped every later arrival.
      if(!activeCharms(pg).length){if(!saved&&!all.length)removePage(pg);continue;}
      if (Gate.modern(run.runId) && !Gate.nestable(pg, run)) continue;
      if(!all.some(c=>c.poolId&&!before.has(c.poolId))&&pg.fileBase)continue;
      if(saved){pg.placements=saved.placements;pg.intakeOptimized=saved.intakeOptimized;pg.intakeOptimizedCount=saved.intakeOptimizedCount;pg.density=saved.density;pg.liveInfo=saved.liveInfo;}
      pg.intakeAppend=pg.placements.length>0;pg.appendOnly=pg.intakeAppend;pg.status='ready';pg.dirty=true;
      agent({metal:m},'nest','Adding incoming pieces to the newest partial sheet; earlier sheets and saved positions are retained');
      startNest(pg);
    }
    };
    if(window.CharmNestOperations)await window.CharmNestOperations.run({key:'intake:'+run.runId,label:'Adding incoming orders',resources:['production:'+run.runId]},prepare);else await prepare();
    // Never advance to labels/commit until all overflow sheets and all cloud writes have finished.
    const rearranged = new Set();
    while (true) {
      const pages = allSheets().filter(p => p.runId === run.runId && !p.runHold && Gate.nestable(p, run) && p.charms.length);
      for (const p of pages) if (p.persisted && !p.persistedDone) await p.persisted.then(() => { p.persistedDone = true; });
      if (run.status === "stopped") throw new Error(run.stoppedBy || "run stopped");
      // A sheet can wait to be arranged again with nothing started on it: a cancelled order's pieces were taken off it and
      // no order for its metal came in. It is arranged here, once. Waiting on it unstarted stalled every later arrival.
      for (const p of pages) if (p.dirty && ["ready", "idle"].includes(p.status) && !p._operationStarting && !p._learnedStarting && !rearranged.has(p)) { rearranged.add(p); startNest(p); }
      if (!pages.some(p => ["nesting", "finishing", "queued"].includes(p.status) || p.dirty || (p.persisted && !p.persistedDone))) break;
      await sleep(250);
    }
    await finish(run);
  }
  // Also called at the normal checkpoint after refresh, so a crash cannot leave obsolete sheets or backs in the set.
  async function finish(run) {
    const recovery = run.intakeRecovery; if (!recovery) return;
    await Gate.assemble(run);
    for (const id of new Set(recovery.retire)) {
      const page = allSheets().find(p => p.runId === run.runId && p.sheetId === id);
      if (page?.charms.length) continue;
      if (!S.cloud.ok) throw new Error("Reconnect to finish saving the re-optimized set");
      await api("charmNestLibrary", { op: "archiveEmptySheet", id, runId: run.runId });
      delete run.sheets[id];
      if (page) { page.cloud = null; page.fileBase = null; page.sheetId = null; }
    }
    const rewrites = new Set(recovery.backs);
    for (const j of Engrave.items().values()) if (j.copies.some(id => rewrites.has(id)) && ["approved", "written"].includes(j.state) && (!Gate.modern(run.runId) || j.copies.every(id => Pool.sheetOf(id)?.fileBase))) await Engrave.writeBacks(j);
    // A committed set whose record saved is history: nothing an intake does changes it. Saving it again made the server
    // re-check its completion against the run's current lines, so one Etsy change to a held order whose piece was already
    // cut (its engraving decision reset to "reclassify") refused the save and stopped every later intake for good.
    for (const set of Sets.ofRun(run.runId)) if (!set.committedAt || set.savedCommit !== set.committedAt) await Sets.save(set);
    delete run.intakeRecovery; Session.schedule();
  }
  return { add, finish, prepareSheet, closed, intakePage };
})();

/* Compact, read-only set previews never replace the active bench. */
const SetPicker = window.SetPicker = (() => {
  let rows = [], dialog, box;
  function mount() {
    if (document.getElementById("setPicker")) return;
    box = el("details", "setPicker"); box.id = "setPicker";
    box.innerHTML = `<summary>Sets ▾</summary><div class="setMenu"><div data-setlist>Loading…</div><button class="btn ghost sm" data-history>Search all sets…</button></div>`;
    document.getElementById("setPickerSlot").appendChild(box);
    box.ontoggle = () => { if (box.open) load(); };
    box.querySelector('[data-history]').onclick = () => { box.open = false; RunHistory.show(); };
  }
  async function load() {
    const list = box.querySelector('[data-setlist]');
    const current = `<button type="button" data-current>Current workspace · ${allSheets().filter(p => p.charms.length).length} sheets</button>`;
    list.innerHTML = current + `<small>Reading saved sets…</small>`;
    list.querySelector('[data-current]').onclick = previewCurrent;
    try {
      const r = await api("charmNestLibrary", { op: "history", limit: 20 }, { quiet: true });
      rows = r.sets || [];
      list.innerHTML = `<button type="button" data-current>Current workspace · ${allSheets().filter(p => p.charms.length).length} sheets</button><small>${r.setCount || 0} saved sets · ${r.workingCount || 0} working groups · newest first${r.next ? " · older ones under Search all sets" : ""}</small>` + rows.map((g,i) => `<button type="button" data-preview="${i}">${esc(g.name || (g.seq ? "Set " + g.seq : "Working sheets"))} · ${esc(g.day || "")}<small>${g.sheets.length} sheets · ${esc(counts(g.sheets))}</small></button>`).join("");
      list.querySelector('[data-current]').onclick = () => previewCurrent();
      list.querySelectorAll('[data-preview]').forEach(b => b.onclick = () => preview(rows[+b.dataset.preview]));
    } catch (e) { list.innerHTML = current + `<small>Could not read saved sets: ${esc(e.message)}</small>`; list.querySelector('[data-current]').onclick = previewCurrent; }
  }
  const counts = sheets => METALS.map(m => { const n = sheets.filter(s => s.metal === m.key).length; return n ? `${labelOf(m.key)} ${n}` : ""; }).filter(Boolean).join(" · ");
  function previewCurrent() {
    const sheets = allSheets().filter(p => p.charms.length).map(p => ({ id:p.sheetId, stock:p.outputs?.report?.stock || stockFor(p.metal), poolIds:[...window.CharmNestBacks.placedIds(p)], backs:Engrave.sheetBacks(p), metal:p.metal, fileBase:p.fileBase || labelOf(p.metal), preview:p.cloud?.preview, placedCount:p.placements.length, draft:p.draft || !p.setId, setSeq:p.seq }));
    preview({ name:"Current workspace", day:B.run?.day || today(), sheets, status:B.run?.status || "manual" });
  }
  function preview(g) {
    if (!g) return;
    mount(); box.open = false;
    if (!dialog) { dialog = el("dialog", "hist"); dialog.id = "setPreview"; document.body.appendChild(dialog); }
    dialog.innerHTML = `<form method="dialog" class="x"><button class="btn ghost sm">Close preview</button></form><h2>${esc(g.name || (g.seq ? "Set " + g.seq : "Working sheets"))}</h2><p>${esc(g.day || "")} · ${esc(g.status || "")} · ${g.sheets.length} sheets</p><p>${esc(counts(g.sheets))}</p><div class="setPreviewGrid">${g.sheets.map(s => `<figure><div data-back-sheet="${esc(s.id || s.sheetId || "")}">${Engrave.backsMarkup(s)}</div>${s.preview ? `<img crossorigin="anonymous" src="${esc(cors(s.preview))}" alt="${esc(labelOf(s.metal))} sheet preview">` : `<div class="hEmpty">Preview not saved yet</div>`}<figcaption><b>${esc(labelOf(s.metal))}</b> · ${s.placedCount || 0} pieces · ${O.libraryGroup(s).standalone ? "standalone · not in a set" : s.draft ? "held for a later set" : "Set " + (s.setSeq || g.seq || "—")}<small>${esc(s.fileBase || "")}</small></figcaption></figure>`).join("")}</div><p class="help">Preview only. Your current workspace stays open.</p>`;
    dialog.showModal();
  }
  return { mount, previewCurrent, preview };
})();

/* ═══ 25 · boot ═══════════════════════════════════════════════════════════ */
async function bootBridge() {
  // The Design Station guards unload in three places; the sorter guarded it nowhere. A reload mid-run loses every
  // engraving approval not yet written to a back file and every review decision made that shift.
  window.addEventListener("beforeunload", e => {
    const r = B.run; if (!r) return;
    const live = ["running", "review", "paused"].includes(r.status);
    // a stopped run is the likeliest moment for a reload and holds the most unwritten work — but only nag when there is some
    const unwritten = r.status === "stopped" && ([...Engrave.items().values()].filter(j => ["approved", "words", "review"].includes(j.state) && !(j.backs && j.backs.length) && (j.state === "approved" || j.decision || j.nudged || j.edited)).length || Review.count());
    if (!live && !unwritten) return;
    e.preventDefault(); e.returnValue = "";
  });
  SetPicker.mount();
  RunCtl.renderModeBtn(); RunCtl.renderBanner(); LiveStrip.render(); Sandbox.render(); Sandbox.afterReload(); if (Sandbox.on()) agent({ bridge: true }, "warn", "SANDBOX mode: emulated Etsy from the stored snapshot, every record and file goes to sandbox copies");
  document.getElementById("btnRunMode").onclick = () => { const auto = S.settings.runMode !== "auto"; if (auto && !confirm("Auto mode: the sorter connects to the Design Station, pulls the latest orders by the date rule, nests, fits engraving, saves labels and marks the orders complete — continuing past items that need review or approval. Only ready work is released. Turn Auto on?")) return; RunCtl.setMode(auto ? "auto" : "manual"); };
  Orders.loadMaps().catch(() => {}); Master.load().catch(() => {});
  Engrave.loadFonts().catch(() => {});
  Kin.mount();
  // Direct #engrave navigation can happen before the bridge exists. Paint it
  // now instead of leaving an empty pane throughout checkpoint reconstruction.
  Views.onShow(S.mode);
  let recovered = false, recoveryFailed = false;
  try { recovered = await Session.restore(); }
  catch (error) { recoveryFailed = true; console.error("Workspace recovery failed; checkpoint retained", error); }
  // Never overwrite a checkpoint with a partially restored workspace or start
  // an automatic run over it. Navigation and the saved Library remain usable.
  if (!recoveryFailed) {
    Session.listen(); Arrivals.start(); ListMedia.start();
    // an approval whose back files a reload cut short is written now, not when the run next passes Engraving
    if (recovered) Engrave.resumeBacks();
    if(recovered)RunCtl.recoverReviewStop().catch(e=>RunCtl.stop(e.message,"Reconnect and Resume."));
    // a reload (a discarded tab, a crash, a browser update) left an Auto run stopped with no station until someone pressed
    // Resume: Auto takes the station again and carries on, from the step the restore rewound it to
    if (recovered && S.settings.runMode === "auto") setTimeout(() => RunCtl.autoResume(true), 1500);
    window.addEventListener("online", () => setTimeout(() => RunCtl.autoResume(true), 5000));
  }
  if (recovered) Engrave.refreshBackIndexes();
  Views.onShow(S.mode);
  /* The app used to open on an empty Orders tab whatever had happened yesterday, and the only way to anything was to
     pull again. It opens on the last run instead — its orders, its sheets, its engraving, read from the record, with
     one line at the top saying so and a Done that puts it down. An open run is offered for resume as before. */
  // (offered when the cloud answers: a page opened while it was offline offered nothing for the rest of the session)
  let offered = false;
  const offerRuns = () => { if (offered || recovered || recoveryFailed || !S.cloud.ok || B.run) return; offered = true; api("charmNestLibrary", { op: "runList", limit: 10 }).then(r => {
    const runs = r.runs || [];
    const open = runs.filter(x => !["complete", "abandoned"].includes(x.status));
    if (open.length) { B.openRuns = open; agent({ bridge: true }, "DS", `${open.length} open run(s) on record — offered on the run banner`); RunCtl.renderBanner(); }
    const last = runs.find(x => x.lines > 0);
    if (last && !B.run && !B.orders.rows.length && !Recall.on()) Recall.open({ runId: last.runId, quiet: true }).catch(() => {});
  }).catch(() => { offered = false; }); };
  offerRuns();
  /* The cloud back after a page load, a sleep or an outage it was offline for (the page's cloudRecovered): what the boot
     skipped without it is done now, the run's record is saved again (its "Not saved online" goes), back files waiting
     for it are written, and the bridge log kept meanwhile is sent. */
  window.addEventListener("cn-cloud-back", () => {
    Orders.loadMaps().catch(() => {}); Master.load().catch(() => {}); offerRuns();
    if (B.run && B.run.saveError) RunCtl.save(B.run).catch(() => {});
    if (!recoveryFailed) Engrave.resumeBacks();
    DesignLink.flushLog();
  });
  // the Design Station frame mounts on first visit to its tab; Auto mode mounts it now
  if (!recovered && !recoveryFailed && S.settings.runMode === "auto") { setTimeout(() => { if (!B.openRuns?.length && !Recall.on() && !B.run) RunCtl.setMode("auto"); }, 1500); }
  document.addEventListener("keydown", e => { if (e.altKey && e.key === "r") { e.preventDefault(); setMode("review"); } });
  agent({ bridge: true }, "DS", `Bridge ready · station ${DesignLink.origin()} · ${S.settings.runMode} mode`);
}
(function whenReady() { if (window.CN && S.cloud.ok !== null) bootBridge(); else setTimeout(whenReady, 150); })();
})();
