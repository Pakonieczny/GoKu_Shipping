/* Shared coordinator: serialize conflicting mutations, let independent work run,
 * coalesce queued latest-intent updates, and always release failed operations. */
(function(root,factory){const api=factory(root);if(typeof module==='object'&&module.exports)module.exports=api;else root.CharmNestOperations=api;})(typeof window!=='undefined'?window:globalThis,function(root){
  'use strict';
  function create(){
    const active=new Set(),queue=[];let sequence=0,scheduled=false;const listeners=new Set();
    const snapshot=()=>({active:[...active].map(t=>({key:t.key,label:t.label,resources:t.resources})),queued:queue.map(t=>({key:t.key,label:t.label,resources:t.resources}))});
    const emit=()=>{for(const fn of listeners)try{fn(snapshot());}catch(e){console.warn('Operation observer',e);}};
    const overlaps=(a,b)=>a.some(r=>b.includes(r));
    function pump(){
      scheduled=false;
      queue.sort((a,b)=>b.priority-a.priority||a.sequence-b.sequence);
      for(let i=0;i<queue.length;){
        const t=queue[i];
        if([...active].some(a=>overlaps(a.resources,t.resources))||queue.slice(0,i).some(a=>overlaps(a.resources,t.resources))){i++;continue;}
        queue.splice(i,1);active.add(t);const context={resources:t.resources,active:true};
        Promise.resolve().then(()=>t.work(context)).then(t.resolve,t.reject).finally(()=>{context.active=false;active.delete(t);emit();schedule();});
      }
      emit();
    }
    function schedule(){if(!scheduled){scheduled=true;queueMicrotask(pump);}}
    function run(options,work,context){
      const resources=[...new Set(options.resources||[])].sort();
      if(context?.active&&resources.every(r=>context.resources.includes(r)))return Promise.resolve().then(()=>work(context));
      if(context?.active&&overlaps(context.resources,resources))return Promise.reject(new Error('Nested operation must reuse its held resources'));
      if(options.latest){const pending=queue.find(t=>t.key===options.key);if(pending){if(JSON.stringify(pending.resources)!==JSON.stringify(resources))return Promise.reject(new Error('Operation resource mismatch'));pending.work=work;return pending.promise;}}
      const t={...options,resources,work,priority:options.priority||0,sequence:++sequence};t.promise=new Promise((resolve,reject)=>{t.resolve=resolve;t.reject=reject;});queue.push(t);emit();schedule();return t.promise;
    }
    return {run,snapshot,subscribe(fn){listeners.add(fn);return()=>listeners.delete(fn);},busy(resource){return [...active,...queue].some(t=>t.resources.includes(resource));},running(key){return [...active].some(t=>t.key===key);},async idle(resource){while([...active,...queue].some(t=>t.resources.includes(resource)))await Promise.allSettled([...active,...queue].filter(t=>t.resources.includes(resource)).map(t=>t.promise));}};
  }
  return Object.assign(create(),{create});
});
