(function(root){
 'use strict';
 const active=new Map(),histories=new Map();let seq=0,notice;
 const duration=ms=>{const s=Math.max(0,Math.floor(ms/1000));return s<60?s+'s':Math.floor(s/60)+'m '+s%60+'s';};
 function begin(label){const id=++seq;active.set(id,{label:label||'Loading',started:Date.now()});return()=>active.delete(id);}
 function render(){
  const now=Date.now(),jobs=[...active.values()].filter(j=>now-j.started>=1800);
  if(!jobs.length){notice?.remove();notice=null;return;}
  const host=[...document.querySelectorAll('dialog[open]')].pop()||document.body;
  if(!notice){notice=document.createElement('div');notice.className='bp-activity';notice.setAttribute('role','status');notice.setAttribute('data-bp-managed','');notice.innerHTML='<span class="bp-spinner" aria-hidden="true"></span><div><b></b><progress class="bp-track" aria-label="Waiting for a response" hidden></progress><small></small></div>';}
  if(notice.parentNode!==host)host.appendChild(notice);
  const oldest=jobs.reduce((a,b)=>a.started<b.started?a:b),age=now-oldest.started;
  const label=jobs.length===1?oldest.label:jobs.length+' requests in progress';
  if(notice.querySelector('b').textContent!==label)notice.querySelector('b').textContent=label;
  notice.querySelector('progress').hidden=age<8000;notice.querySelector('.bp-spinner').hidden=age>=8000;
  notice.querySelector('small').setAttribute('aria-live','off');
  notice.querySelector('small').textContent=duration(age)+' elapsed · '+(age>60000?'Taking longer than usual. Waiting for a response; completion is not confirmed.':'Waiting for the server response');
 }
 function enhance(){
  document.querySelectorAll('progress').forEach(p=>{
   if(p.closest('[data-bp-managed]'))return;
   if(p.hidden||p.closest('[hidden]'))return;
   p.classList.add('bp-track');
   const key=(p.closest('[id]')?.id||'page')+'|'+p.getAttribute('aria-label');
   if(!p._bp){const old=histories.get(key);p._bp=old&&Date.now()-old.seen<2500?old:{started:Date.now(),changed:Date.now(),value:p.getAttribute('value')};histories.set(key,p._bp);}p._bp.seen=Date.now();
   const value=p.getAttribute('value');if(value!==p._bp.value){p._bp.value=value;p._bp.changed=Date.now();}
   let note=p.nextElementSibling;if(!note?.classList.contains('bp-note')){note=document.createElement('small');note.className='bp-note';note.setAttribute('aria-live','off');p.after(note);}
   const elapsed=Date.now()-p._bp.started,unchanged=Date.now()-p._bp.changed;
   note.hidden=elapsed<2000||p.hasAttribute('value')&&p.value>=p.max;
   note.textContent=duration(elapsed)+' elapsed'+(unchanged>30000?' · No new progress reported for '+duration(unchanged):p.hasAttribute('value')?' · Completed steps':' · Waiting for a response');
  });
  for(const [key,h] of histories)if(Date.now()-h.seen>10000)histories.delete(key);
  document.querySelectorAll('.bp-note').forEach(n=>{if(n.previousElementSibling?.tagName!=='PROGRESS')n.remove();else if(n.previousElementSibling.hidden)n.hidden=true;});
 }
 root.BritesProgress={begin,duration};
 setInterval(()=>{render();enhance();},500);
})(window);
