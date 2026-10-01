/* Shared display ordering and shop-local activity filters. Does not change production queue priority. */
(function(root,factory){const api=factory(root);if(typeof module==='object'&&module.exports)module.exports=api;else root.CNListActivity=api;})(typeof window!=='undefined'?window:null,function(root){
  'use strict';
  const ZONE='America/Toronto',KEY='cn.activityLists.v1',presets=[['all','All'],['today','Today'],['yesterday','Yesterday'],['7','Last 7 days'],['14','Last 14 days']];
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
  function state(scope){
    if(!loaded){loaded=true;try{Object.assign(states,JSON.parse(root?.localStorage?.getItem(KEY)||'{}'));}catch(_){} }
    const old=states[scope] || {};return states[scope]={range:scope==='library'?'all':presets.some(p=>p[0]===old.range)?old.range:'all',direction:old.direction==='asc'?'asc':'desc'};
  }
  function set(scope,patch){const current=state(scope);states[scope]={...current,...patch};state(scope);try{root?.localStorage?.setItem(KEY,JSON.stringify(states));}catch(_){}return states[scope];}
  const key=scope=>{const s=state(scope);return s.range+':'+s.direction;};
  function select(scope,items,now=Date.now()){const s=state(scope);return (items || []).filter(x=>matches(x,s.range,now)).slice().sort((a,b)=>compare(a,b,s.direction));}
  function touch(value,t=Date.now()){if(value && typeof value==='object')value.activityAt=Math.max(ms(value.activityAt),ms(t));return value;}
  function mount(host,scope,change){
    if(!root?.document || !host)return;
    let bar=host.querySelector(`[data-activity-tools="${scope}"]`);
    if(!bar){bar=root.document.createElement('span');bar.className='cnListTools';bar.dataset.activityTools=scope;
      bar.innerHTML=(scope==='library'?'':'<span class="cnDatePresets" role="group" aria-label="Filter by last activity">'+presets.map(([id,label])=>`<button type="button" data-range="${id}" aria-pressed="false">${label}</button>`).join('')+'</span>')+'<select class="cnActivitySort" aria-label="Sort by last activity" title="Last activity: newest or oldest first"><option value="desc">Newest first</option><option value="asc">Oldest first</option></select>';
      let search=host.querySelector('input[type=search],input.ordSearch');
      while(search && search.parentElement!==host)search=search.parentElement;
      if(search)search.after(bar);else host.appendChild(bar);
    }
    const s=state(scope);for(const b of bar.querySelectorAll('[data-range]')){b.setAttribute('aria-pressed',String(b.dataset.range===s.range));b.onclick=()=>{set(scope,{range:b.dataset.range});mount(host,scope,change);change?.();};}
    const sort=bar.querySelector('select');sort.value=s.direction;sort.onchange=()=>{set(scope,{direction:sort.value});change?.();};
    return bar;
  }
  return {ZONE,presets,ms,at,day,shift,dayStart,bounds,matches,compare,state,set,key,select,touch,mount};
});
