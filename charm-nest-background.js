/* Bounded computation queues and interaction-aware presentation updates. */
(function(root){
  'use strict';
  const pending=new Map();let pointer=false,until=0,timer=0;
  const active=()=>pointer||performance.now()<until;
  function flush(){clearTimeout(timer);timer=0;if(active()){timer=setTimeout(flush,80);return;}const work=[...pending.values()];pending.clear();for(const fn of work)fn();}
  function touch(e){if(e.target?.closest?.('.placementThumb,canvas,input,textarea,select,.sheetOptions')){until=performance.now()+220;clearTimeout(timer);timer=setTimeout(flush,240);}}
  document.addEventListener('pointerdown',e=>{touch(e);if(e.target?.closest?.('.placementThumb,canvas'))pointer=true;},true);
  for(const event of ['pointerup','pointercancel'])document.addEventListener(event,e=>{pointer=false;touch(e);clearTimeout(timer);timer=setTimeout(flush,240);},true);
  root.addEventListener('blur',()=>{pointer=false;until=0;flush();});
  document.addEventListener('wheel',touch,{capture:true,passive:true});document.addEventListener('input',touch,true);
  root.CharmNestInteraction={active,defer(key,fn){if(!active())return false;pending.set(key,fn);if(!timer)timer=setTimeout(flush,240);return true;},idle(){return new Promise(resolve=>{if(!this.defer(Symbol(),resolve))resolve();});}};
  const P=root.CharmNestPDF;
  // No synchronous heavy fallback after a worker crash: reject that task so the
  // existing retry UI can recover without freezing the page.
  if(!P||!root.Worker||!root.OffscreenCanvas)return;
  function client(){
    const queue=[];let worker=null,current=null,id=0;
    function end(error,result){if(!current)return;const task=current;current=null;clearTimeout(task.timer);error?task.reject(error):task.resolve(result);setTimeout(pump,0);}
    function fail(error){worker?.terminate();worker=null;end(error instanceof Error?error:new Error(error?.message||'Background calculation stopped. Retry this item.'));}
    function pump(){if(current||!queue.length)return;current=queue.shift();try{
      if(!worker){worker=new Worker('charm-nest-compute-worker.js?v=20260922-background');const owner=worker;worker.onerror=e=>{if(owner===worker)fail(e);};worker.onmessageerror=()=>{if(owner===worker)fail(new Error('Unreadable background result'));};worker.onmessage=({data})=>{if(!current||data.id!==current.id)return;if(data.progress){current.progress?.(...data.progress);return;}end(data.error?new Error(data.error):null,data.result);};}
      current.timer=setTimeout(()=>fail(new Error('Background calculation timed out. Retry this item.')),120000);worker.postMessage({id:current.id,type:current.type,input:current.input});
    }catch(e){fail(e);}}
    return {run(type,input,progress){return new Promise((resolve,reject)=>{queue.push({id:++id,type,input,progress,resolve,reject});pump();});}};
  }
  const geometry=client(),preview=client();let key=0;
  const plain=p=>{const {doc,page,...rest}=p;return rest;};
  const charm=c=>Object.fromEntries(['id','sourceId','name','slug','bbox','outline','members','strokePt','topIndices','dropIndices','centerPt'].filter(k=>c[k]!==undefined).map(k=>[k,c[k]]));
  P.parseSource=(bytes,name)=>geometry.run('parse',{bytes,name,key:'source-'+(++key)});
  P.groupCharmsAsync=(parsed,opts)=>geometry.run('group',{parsed:plain(parsed),opts});
  P.buildSilhouettes=async(parsed,charms,scale,onProgress)=>{const result=await geometry.run('silhouettes',{charms:charms.map(charm),scale},onProgress);result.forEach((fields,i)=>Object.assign(charms[i],fields));return charms;};
  P.thumbnail=(c,size)=>preview.run('thumbnail',{charm:charm(c),size});
  P.frontPreview=(c,size)=>preview.run('front',{charm:charm(c),size});
  P.buildSheet=async spec=>{const data={...spec,placements:spec.placements.map(p=>({...p,charm:charm(p.charm)})),sources:new Map([...spec.sources].map(([key,p])=>[key,plain(p)]))};const result=await geometry.run('sheet',{spec:data});result.layers.forEach((layer,i)=>{spec.placements[i].layerName=layer;});return result.bytes;};
  P.buildSingleCharm=(c,parsed)=>geometry.run('single',{charm:charm(c),parsed:plain(parsed)});
  P.buildBackFile=spec=>geometry.run('back',{spec:{...spec,charm:charm(spec.charm),parsed:plain(spec.parsed)}});
  const E=root.CharmNestExport;
  // Parent-scale verification needs only each charm's identity, never its DOM,
  // rendered images, workers, masks or cached source document.
  E.compose=(front,sheet,backs)=>{
    const minimal={...sheet,charms:(sheet.charms||[]).map(c=>({id:c.id,poolId:c.poolId}))};
    const data=Object.fromEntries(['id','sheetId','fileBase','metal','roseStockId','rosePlanJson','rosePlan','placements','charms'].filter(k=>minimal[k]!==undefined).map(k=>[k,minimal[k]]));
    return geometry.run('compose',{front,sheet:data,backs:backs.map(b=>({poolId:b.poolId,bytes:b.bytes}))});
  };
  E.productionDxf=bytes=>geometry.run('dxf',{bytes});
  root.CharmNestBackground={run:(type,input)=>geometry.run(type,input),front:P.frontPreview};
})(window);
