/* Shared display ordering and shop-local activity filters. Does not change production queue priority. */
(function(root,factory){const api=factory(root);if(typeof module==='object'&&module.exports)module.exports=api;else root.CNListActivity=api;})(typeof window!=='undefined'?window:null,function(root){
  'use strict';
  const ZONE='America/Toronto',KEY='cn.activityLists.v1',presets=[['all','Any time'],['today','Today'],['yesterday','Yesterday'],['7','Last 7 days'],['14','Last 14 days']];
  const fmt=new Intl.DateTimeFormat('en-CA',{timeZone:ZONE,year:'numeric',month:'2-digit',day:'2-digit'});
  const states={};let loaded=false;
  function ms(v){
    if(v==null)return 0;
    if(typeof v.toMillis==='function')return v.toMillis();
    if(typeof v==='object' && (v.seconds!=null || v._seconds!=null))return +(v.seconds??v._seconds)*1000+Math.floor(+(v.nanoseconds??v._nanoseconds??0)/1e6);
    const n=typeof v==='string' && !/^\d+(\.\d+)?$/.test(v)?Date.parse(v):+v;
    return Number.isFinite(n) && n>0?n:0;
  }
  const seconds=v=>ms(v)*1000;
  const eventFields=['activityAt','updatedAt','approvedAt','decidedAt','resolvedAt','removedAt','cancelledAt','restoredAt','reopenedAt','heldAt','completedAt','committedAt','lastPrintedAt','printedAt','laserDoneAt','roseCutAt','indexedAt','lastUsed','finishedAt','t','at'];
  function times(value,seen=new Set()){
    if(!value || typeof value!=='object' || seen.has(value))return [0,0];seen.add(value);
    let event=Math.max(0,...eventFields.map(k=>ms(value[k])),seconds(value.updateTs)),fallback=Math.max(ms(value.createdAt),ms(value.arrivedAt),seconds(value.createTs));
    for(const key of ['row','rows','order','record','settled','engrave','job','sheets','processSeals','seals','backs','backPool']){
      const child=value[key];for(const x of Array.isArray(child)?child:[child]){const [e,f]=times(x,seen);event=Math.max(event,e);fallback=Math.max(fallback,f);}
    }
    if(!fallback && /^\d{4}-\d{2}-\d{2}$/.test(value.day || ''))fallback=Date.parse(value.day+'T12:00:00Z');
    return [event,fallback];
  }
  const at=value=>{const [event,fallback]=times(value);return event || fallback;};
  function day(t){if(!ms(t))return '';const parts=fmt.formatToParts(new Date(ms(t))),get=k=>parts.find(p=>p.type===k).value;return `${get('year')}-${get('month')}-${get('day')}`;}
  const shift=(d,n)=>new Date(Date.parse(d+'T12:00:00Z')+n*86400000).toISOString().slice(0,10);
  function dayStart(d){let lo=Date.parse(d+'T00:00:00Z')-86400000,hi=lo+3*86400000;while(hi-lo>1){const mid=Math.floor((lo+hi)/2);if(day(mid)<d)lo=mid;else hi=mid;}return hi;}
  function bounds(range,now=Date.now()){
    const today=day(now);if(range==='today')return [today,today];if(range==='yesterday'){const d=shift(today,-1);return [d,d];}
    return range==='7'||range==='14'?[shift(today,1-Number(range)),today]:null;
  }
  function matches(value,range='all',now=Date.now()){const b=bounds(range,now);if(!b)return true;const d=day(typeof value==='number'?value:at(value));return !!d && d>=b[0] && d<=b[1];}
  const identity=x=>String(x.key || x.id || x.setId || x.runId || x.orderId || x.order?.receiptId || '');
  function compare(a,b,direction='desc'){
    const x=at(a),y=at(b);if(!x||!y)return (x?0:1)-(y?0:1) || identity(a).localeCompare(identity(b));
    return (direction==='asc'?x-y:y-x) || identity(a).localeCompare(identity(b));
  }
  /* The Library's blocks (Paul, 10 Oct 2026: "Actual Sets of Sheets should always be prioritized above all in-progress sheets when showing the
     order of sheets and Sets in the Library tab"). An actual set (a set record the Library already treats as one: it has its setId and is
     not the group of loose sheets that wait for a set, nor the 14K / 10K solids; a Completed row of kind "set") comes before every group of
     loose sheets, in Laser cutting, In progress and the Completed list, search and filter views included. Inside each of the two groups the
     order is the last-activity order as before, and nothing is left between them when one group is empty. Display order only. */
  const isSet=x=>!!x && x.kind!=='sheet' && (x.kind==='set' || (!!x.setId && !x.standalone && !x.working));
  const compareBlocks=(a,b,direction)=>(isSet(a)?0:1)-(isSet(b)?0:1) || compare(a,b,direction);
  function state(scope){
    if(!loaded){loaded=true;try{Object.assign(states,JSON.parse(root?.localStorage?.getItem(KEY)||'{}'));}catch(_){} }
    const old=states[scope] || {};return states[scope]={range:scope==='library'?'all':presets.some(p=>p[0]===old.range)?old.range:'all',direction:old.direction==='asc'?'asc':'desc'};
  }
  function set(scope,patch){const current=state(scope);states[scope]={...current,...patch};state(scope);try{root?.localStorage?.setItem(KEY,JSON.stringify(states));}catch(_){}return states[scope];}
  const key=scope=>{const s=state(scope);return s.range+':'+s.direction;};
  function select(scope,items,now=Date.now()){const s=state(scope);return (items || []).filter(x=>matches(x,s.range,now)).slice().sort((a,b)=>compare(a,b,s.direction));}
  function selectBlocks(scope,items,now=Date.now()){const s=state(scope);return (items || []).filter(x=>matches(x,s.range,now)).slice().sort((a,b)=>compareBlocks(a,b,s.direction));}
  function touch(value,t=Date.now()){if(value && typeof value==='object')value.activityAt=Math.max(ms(value.activityAt),ms(t));return value;}
  /* ═══ FILTERBAR (Paul, 17 Oct 2026: "This much is much too crowded. Consolidate this entire menu into the smallest and most efficient footprint ... real-time reactions with spinner/loader when waiting.") ═══
     The five date chips and the Newest/Oldest select were two rows of loose controls on every list that sorts by last activity. They are now two small controls: ONE date menu ("Any time ▾": the five ranges are its items,
     the one in force is named on the button and drawn in gold) and ONE sort control (a button that flips newest / oldest; where a list has more than one thing to sort by, as the Orders tab does, the button names the key,
     opens a menu of the keys, and a small arrow beside it flips the direction). The state (range, direction), its storage, select / selectBlocks / compare and every list that reads them are unchanged.
     This file also owns what the toolbars share: popover (a menu or a small panel under a button: Esc closes it and gives the focus back, a click outside closes it, a phone gets a bottom sheet), working (the busy
     state: aria-busy, the small .spin on the control that was touched, the list under it dimmed by the stylesheet) and react (a change is painted busy first, the work runs on the next frame, the busy state ends when the work has). */
  const SVG=d=>`<svg class="cnIc" viewBox="0 0 16 16" aria-hidden="true" focusable="false">${d}</svg>`;
  const ICON={
    search:SVG('<circle cx="7" cy="7" r="4.5"/><path d="m10.5 10.5 3 3"/>'),
    filter:SVG('<path d="M2.5 4.5h5M11.5 4.5h2M2.5 8h2M8 8h5.5M2.5 11.5h6.5M12.5 11.5h1"/><circle cx="9.5" cy="4.5" r="1.7"/><circle cx="6" cy="8" r="1.7"/><circle cx="11" cy="11.5" r="1.7"/>'),
    clock:SVG('<circle cx="8" cy="8" r="5.8"/><path d="M8 4.8V8l2.1 1.4"/>'),
    chev:'<svg class="cnChev" viewBox="0 0 12 12" aria-hidden="true" focusable="false"><path d="m3 4.6 3 3 3-3"/></svg>',
    down:'<svg class="cnArr cnArrDown" viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path d="M8 3v9.4M4.2 8.8 8 12.6l3.8-3.8"/></svg>',
    up:'<svg class="cnArr cnArrUp" viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path d="M8 13V3.6M4.2 7.2 8 3.4l3.8 3.8"/></svg>',
    check:'<svg class="cnCheck" viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path d="m3.6 8.4 3 3 5.8-6.6"/></svg>',
    x:'<svg class="cnIc" viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path d="m4.2 4.2 7.6 7.6M11.8 4.2l-7.6 7.6"/></svg>',
    cards:SVG('<rect x="2.5" y="2.5" width="4.6" height="4.6" rx="1.2"/><rect x="8.9" y="2.5" width="4.6" height="4.6" rx="1.2"/><rect x="2.5" y="8.9" width="4.6" height="4.6" rx="1.2"/><rect x="8.9" y="8.9" width="4.6" height="4.6" rx="1.2"/>'),
    list:SVG('<path d="M5.6 4h7.9M5.6 8h7.9M5.6 12h7.9"/><path d="M2.6 4h.1M2.6 8h.1M2.6 12h.1" stroke-width="2.2"/>')
  };
  const later=f=>{const w=root;if(w&&w.requestAnimationFrame)w.requestAnimationFrame(()=>w.setTimeout(f,0));else (w||globalThis).setTimeout(f,0);};
  const MIN_BUSY=240;
  /** The busy state, made at once and ended by the returned function (never sooner than 240 ms after it began, so a quick list still shows that it heard the person). o: { bar, list, touch, min }:
   *  bar gets data-busy, list (an element or a selector) aria-busy="true" (the stylesheet dims it), touch (the control that was touched) aria-busy="true" and a small .spin before its icon. */
  function working(o){
    if(!root||!root.document)return ()=>{};
    o=o||{};const bar=o.bar||null,t0=Date.now(),min=o.min==null?MIN_BUSY:o.min;
    const list=typeof o.list==='string'?root.document.querySelector(o.list):o.list||null,touch=o.touch||null;
    const st=bar?(bar._cnBusy||(bar._cnBusy={n:0,spins:new Map(),lists:new Set()})):{n:0,spins:new Map(),lists:new Set()};
    st.n++;if(bar)bar.setAttribute('data-busy','1');
    if(list){list.setAttribute('aria-busy','true');st.lists.add(list);}
    if(touch&&!st.spins.has(touch)){touch.setAttribute('aria-busy','true');const sp=root.document.createElement('span');sp.className='spin cnSpin';sp.setAttribute('aria-hidden','true');st.spins.set(touch,sp);touch.insertBefore(sp,touch.firstChild);}
    let ended=false;
    const end=()=>{
      if(ended)return;ended=true;root.clearTimeout(guard);
      root.setTimeout(()=>{
        if(--st.n>0)return;
        if(bar)bar.removeAttribute('data-busy');
        for(const l of st.lists)l.removeAttribute('aria-busy');st.lists.clear();
        for(const [t,sp] of st.spins){t.removeAttribute('aria-busy');sp.remove();}st.spins.clear();
      },Math.max(0,min-(Date.now()-t0)));
    };
    const guard=root.setTimeout(end,30000);          // (a read that never answers must not leave the list dimmed and unpressable)
    return end;
  }
  /** A change, reacted to at once: the busy state is painted now, `fn` (the list's own redraw or read) runs on the next frame, and the busy state ends when it has (a promise it returns is waited for).
   *  o: { touch, list, min, delay, key }: `delay` waits that long first, and `key` makes calls with the same key within it one (the search box's typing: one redraw, one busy state). */
  function react(bar,fn,o){
    o=o||{};if(!root||!root.document){try{fn&&fn();}catch(_){} return;}
    const st=bar?(bar._cnJobs||(bar._cnJobs={})):{};let job=o.key?st[o.key]:null;
    if(job)root.clearTimeout(job.t);else{job={end:working({bar,list:o.list,touch:o.touch,min:o.min})};if(o.key)st[o.key]=job;}
    const run=()=>{
      if(o.key&&st[o.key]===job)delete st[o.key];
      let r;try{r=fn&&fn();}catch(e){job.end();root.setTimeout(()=>{throw e;},0);return;}
      if(r&&typeof r.then==='function')r.then(job.end,job.end);else job.end();
    };
    if(o.sync&&!o.delay)run();                       // (a pile or a view: drawn in the same breath, as it always was; the busy state still shows for its minimum)
    else job.t=root.setTimeout(()=>later(run),o.delay||0);
  }
  /** A menu or a small panel under its trigger button. cfg: { trigger, panel, sheet (a bottom sheet on a phone), sheetMax (px), focus (what to focus on opening), onOpen, onClose }.
   *  Esc closes it and gives the focus back to the trigger; a click or Tab outside closes it; it is kept inside the window (placed from the trigger's own box, whatever clips or contains the bar). */
  const pops=new Set();
  function popover(cfg){
    const trigger=cfg.trigger,panel=cfg.panel,d=root&&root.document;let scrim=null;
    const sheetOn=()=>!!cfg.sheet&&root.innerWidth<=(cfg.sheetMax||560);
    function place(){
      const vw=root.innerWidth,vh=root.innerHeight,M=8,sheet=sheetOn(),st=panel.style;
      const probe=d.createElement('i');probe.style.cssText='position:fixed;left:0;top:0;width:0;height:0;visibility:hidden;pointer-events:none';panel.parentNode.appendChild(probe);
      const o=probe.getBoundingClientRect(),x0=o.left,y0=o.top;probe.remove();   // where a fixed box's left:0 / top:0 really is (a container or a transformed ancestor can move it)
      panel.classList.toggle('cnSheet',sheet);st.position='fixed';st.right='auto';st.bottom='auto';st.maxHeight='';st.width='';st.left='0px';st.top='0px';
      if(sheet){st.width=(vw-2*M)+'px';st.maxHeight=Math.round(vh*.8)+'px';const h=panel.offsetHeight;st.left=(M-x0)+'px';st.top=(vh-M-h-y0)+'px';return;}
      const r=trigger.getBoundingClientRect();st.maxHeight=Math.max(160,vh-2*M)+'px';
      const w=panel.offsetWidth,h=panel.offsetHeight;let X=r.left;if(X+w>vw-M)X=r.right-w;X=Math.max(M,Math.min(X,vw-w-M));
      let Y=r.bottom+6;if(Y+h>vh-M){const up=r.top-6-h;if(up>=M)Y=up;else{Y=Math.max(M,vh-M-h);st.maxHeight=Math.max(120,vh-Y-M)+'px';}}
      st.left=(X-x0)+'px';st.top=(Y-y0)+'px';
    }
    const gone=()=>{if(panel.isConnected)return false;api.close(false);return true;};
    const onDown=e=>{if(gone())return;if(panel.contains(e.target)||trigger.contains(e.target))return;api.close(false);};
    const onFocus=e=>{if(gone())return;if(panel.contains(e.target)||trigger.contains(e.target))return;api.close(false);};
    const onKey=e=>{if(gone())return;if(e.key==='Escape'&&[...pops].pop()===api){e.preventDefault();e.stopPropagation();api.close(true);}};
    const onMove=e=>{if(gone())return;if(e&&e.target&&e.target!==d&&e.target!==root&&e.target.nodeType===1&&panel.contains(e.target))return;place();};
    const api={trigger,panel,get isOpen(){return !panel.hidden;},place,
      open(focus){
        if(!d||!panel.isConnected||!panel.hidden)return;
        for(const p of [...pops])if(p!==api)p.close(false);
        panel.style.visibility='hidden';panel.hidden=false;place();panel.style.visibility='';
        trigger.setAttribute('aria-expanded','true');trigger.setAttribute('data-open','1');
        if(sheetOn()){scrim=d.createElement('div');scrim.className='cnScrim';scrim.onclick=()=>api.close(true);panel.parentNode.insertBefore(scrim,panel);}
        pops.add(api);d.addEventListener('pointerdown',onDown,true);d.addEventListener('focusin',onFocus,true);d.addEventListener('keydown',onKey,true);root.addEventListener('resize',onMove);d.addEventListener('scroll',onMove,true);
        panel.classList.remove('cnIn');void panel.offsetWidth;panel.classList.add('cnIn');
        if(focus!==false){const t=cfg.focus?cfg.focus(panel):panel.querySelector('[aria-checked=true],[aria-pressed=true]')||panel.querySelector('button:not([disabled]),input,[tabindex]');if(t&&t.focus)t.focus({preventScroll:true});}
        if(cfg.onOpen)cfg.onOpen(api);
      },
      close(ret){
        if(panel.hidden&&!pops.has(api))return;
        panel.hidden=true;trigger.setAttribute('aria-expanded','false');trigger.removeAttribute('data-open');
        if(scrim){scrim.remove();scrim=null;}
        pops.delete(api);d.removeEventListener('pointerdown',onDown,true);d.removeEventListener('focusin',onFocus,true);d.removeEventListener('keydown',onKey,true);root.removeEventListener('resize',onMove);d.removeEventListener('scroll',onMove,true);
        if(ret&&trigger.isConnected&&trigger.focus)trigger.focus({preventScroll:true});
        if(cfg.onClose)cfg.onClose(api);
      },
      toggle(focus){if(api.isOpen)api.close(true);else api.open(focus);}};
    return api;
  }
  /** Arrow keys inside a menu (role="menu" with menuitemradio items): up, down, Home, End; Tab closes; ArrowDown on the trigger opens it. */
  function menuKeys(pop){
    pop.panel.addEventListener('keydown',e=>{
      const items=[...pop.panel.querySelectorAll('[role^=menuitem]:not([hidden])')],i=items.indexOf(root.document.activeElement);
      if(e.key==='ArrowDown'||e.key==='ArrowUp'||e.key==='Home'||e.key==='End'){e.preventDefault();const n=e.key==='Home'?0:e.key==='End'?items.length-1:(i+(e.key==='ArrowDown'?1:-1)+items.length)%items.length;if(items[n])items[n].focus();}
      else if(e.key==='Tab')pop.close(false);
    });
    pop.trigger.addEventListener('keydown',e=>{if(e.key==='ArrowDown'||e.key==='ArrowUp'){e.preventDefault();pop.open();}});
  }
  const rangeLabel=id=>(presets.find(p=>p[0]===id)||presets[0])[1];
  const DIR_WORDS={desc:'Newest first',asc:'Oldest first'};
  function build(bar){
    const c=bar._cn,d=bar.ownerDocument,sort=c.opts&&c.opts.sort||null;
    if(c.scope!=='library'&&!bar.querySelector('.cnDate')){
      const w=d.createElement('span');w.className='cnDate';
      w.innerHTML=`<button type="button" class="cnBtn cnDateBtn" aria-haspopup="menu" aria-expanded="false">${ICON.clock}<span class="cnLbl"></span>${ICON.chev}</button><div class="cnPop cnMenu" role="menu" aria-label="Filter by last activity" hidden><div class="cnCap">Last activity</div>${presets.map(([id,label])=>`<button type="button" role="menuitemradio" aria-checked="false" data-range="${id}"><span>${label}</span>${ICON.check}</button>`).join('')}</div>`;
      bar.insertBefore(w,bar.firstChild);
      const pop=popover({trigger:w.querySelector('.cnDateBtn'),panel:w.querySelector('.cnMenu'),focus:p=>p.querySelector('[aria-checked=true]')});
      pop.trigger.onclick=()=>pop.toggle();menuKeys(pop);bar._datePop=pop;
    }
    const sig=sort?JSON.stringify([sort.id||'',(sort.keys||[]).map(k=>k[0])]):'dir';
    if(bar._sortSig!==sig){
      bar._sortSig=sig;const old=bar.querySelector('.cnSort');if(bar._keyPop)bar._keyPop.close(false);bar._keyPop=null;if(old)old.remove();
      const keys=sort&&sort.keys,w=d.createElement('span');w.className='cnSort'+(keys?' cnSplit':'');
      const dir=`<button type="button" class="cnBtn cnDir" data-dir="desc"><span class="cnLbl"></span>${ICON.down}${ICON.up}</button>`;
      w.innerHTML=keys?`<button type="button" class="cnBtn cnKey" ${sort.id?`id="${sort.id}" `:''}aria-haspopup="menu" aria-expanded="false"><span class="cnLbl"><span class="cnFull"></span><span class="cnShort"></span></span>${ICON.chev}</button><div class="cnPop cnMenu" role="menu" aria-label="Sort by" hidden>${keys.map(([id,label])=>`<button type="button" role="menuitemradio" aria-checked="false" data-sortkey="${id}"><span>${label}</span>${ICON.check}</button>`).join('')}</div>${dir}`:dir;
      bar.appendChild(w);
      if(keys){const pop=popover({trigger:w.querySelector('.cnKey'),panel:w.querySelector('.cnMenu'),focus:p=>p.querySelector('[aria-checked=true]')});pop.trigger.onclick=()=>pop.toggle();menuKeys(pop);bar._keyPop=pop;}
    }
  }
  function paint(bar){
    const put=(n,t)=>{if(n&&n.textContent!==t)n.textContent=t;};   // (only a changed word is written: the bar is refreshed on every redraw of its tab)
    const c=bar._cn,s=state(c.scope),sort=c.opts&&c.opts.sort||null;
    const db=bar.querySelector('.cnDateBtn');
    if(db){const l=rangeLabel(s.range);put(db.querySelector('.cnLbl'),l);if(s.range!=='all')db.setAttribute('data-on','1');else db.removeAttribute('data-on');db.setAttribute('aria-label','Last activity: '+l+'. Choose another range');db.title='Show only what had activity in this range';}
    for(const b of bar.querySelectorAll('[data-range]'))b.setAttribute('aria-checked',String(b.dataset.range===s.range));
    const key=sort&&sort.key?sort.key():'activity',dirNow=sort&&sort.dir?sort.dir():s.direction,flipped=dirNow==='asc'?'desc':'asc';
    const words=sort&&sort.words?sort.words(key,dirNow):DIR_WORDS[dirNow],back=sort&&sort.words?sort.words(key,flipped):DIR_WORDS[flipped];
    const kb=bar.querySelector('.cnKey');
    if(kb){const k=sort.keys.find(x=>x[0]===key)||sort.keys[0];put(kb.querySelector('.cnFull'),k[1]);put(kb.querySelector('.cnShort'),k[2]||k[1]);kb.setAttribute('aria-label','Sort by: '+k[1]+'. Choose another');kb.title='What the list is ordered by';if(sort.hideKeys)kb.setAttribute('hidden','');else kb.removeAttribute('hidden');for(const b of bar.querySelectorAll('[data-sortkey]'))b.setAttribute('aria-checked',String(b.dataset.sortkey===key));}
    const dir=bar.querySelector('.cnDir');
    if(dir){dir.dataset.dir=dirNow;put(dir.querySelector('.cnLbl'),words);dir.setAttribute('aria-label',words+'. Reverse the order');dir.title=words+' · press for '+back.toLowerCase();}
  }
  function fire(bar,touch){
    const c=bar._cn;paint(bar);
    react(c.host,()=>c.change&&c.change(),{touch,list:c.opts&&c.opts.list});
  }
  function wire(bar){
    bar.addEventListener('click',e=>{
      const t=e.target.closest&&e.target.closest('[data-range],[data-sortkey],.cnDir');if(!t||!bar.contains(t))return;
      const c=bar._cn,sort=c.opts&&c.opts.sort||null;
      if(t.dataset.range!=null){set(c.scope,{range:t.dataset.range});if(bar._datePop)bar._datePop.close(true);fire(bar,bar.querySelector('.cnDateBtn'));}
      else if(t.dataset.sortkey!=null){if(sort&&sort.setKey)sort.setKey(t.dataset.sortkey);if(bar._keyPop)bar._keyPop.close(true);fire(bar,bar.querySelector('.cnKey'));}
      else{if(sort&&sort.flip)sort.flip();else set(c.scope,{direction:state(c.scope).direction==='asc'?'desc':'asc'});fire(bar,t);}
    });
  }
  /** Draws (or refreshes) the scope's tools in `host`, after the search box (or after opts.after), and calls change() when a person changes one.
   *  opts: { after: the element to sit after, list: the list's element or selector (dimmed while it redraws),
   *          sort: { id, keys: [[id, label, short label for a phone]], hideKeys, key(), dir(), words(key, dir), setKey(id), flip() } }.
   *  Without opts.sort the sort control is the one Newest first / Oldest first button, on the scope's own direction. Called again on every redraw of the host: it only refreshes what it shows. */
  function mount(host,scope,change,opts){
    if(!root?.document || !host)return;
    let bar=host.querySelector(`[data-activity-tools="${scope}"]`);
    if(!bar){bar=root.document.createElement('span');bar.className='cnListTools';bar.dataset.activityTools=scope;wire(bar);
      const anchor=opts&&opts.after&&opts.after.parentNode?opts.after:null;
      if(anchor)anchor.after(bar);
      else{
        let search=host.querySelector('input[type=search],input.ordSearch');
        while(search && search.parentElement!==host)search=search.parentElement;
        if(search)search.after(bar);else host.appendChild(bar);
      }
    }
    bar._cn={scope,change,opts:opts||{},host};
    build(bar);paint(bar);
    return bar;
  }
  return {ZONE,presets,ms,at,day,shift,dayStart,bounds,matches,compare,isSet,compareBlocks,state,set,key,select,selectBlocks,touch,mount,popover,menuKeys,working,react,icons:ICON,rangeLabel};
});
