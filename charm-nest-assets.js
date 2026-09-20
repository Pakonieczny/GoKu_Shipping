/* Same-origin production assets: bounded retries, complete byte ranges and image recovery. */
(function(root,factory){
  if(typeof module==='object'&&module.exports)module.exports=factory();
  else root.CharmNestAssets=factory();
})(typeof self!=='undefined'?self:this,function(){
  'use strict';
  const endpoint='/.netlify/functions/charmNestAsset', chunk=2*1024*1024, requests=new Map();
  const pause=ms=>new Promise(r=>setTimeout(r,ms));
  function url(value){
    const raw=String(value||'');
    try{const u=new URL(raw);if(u.protocol==='https:'&&u.hostname==='firebasestorage.googleapis.com'&&/^\/v0\/b\/[^/]+\/o\/charmnest%2f/i.test(u.pathname))return endpoint+'?url='+encodeURIComponent(raw);}catch{}
    return raw;
  }
  async function request(target,headers){
    for(let attempt=0;;attempt++){
      const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),25000);
      try{
        const r=await fetch(target,{headers,cache:'no-store',signal:controller.signal});
        if(!r.ok){const e=Error('Could not read artwork ('+r.status+').');e.status=r.status;throw e;}
        const data=new Uint8Array(await r.arrayBuffer());
        if(!data.length)throw Error('Artwork download was empty.');
        const length=Number(r.headers?.get('content-length'));
        if(length && length!==data.length)throw Error('Artwork download was interrupted.');
        return {data,range:r.headers?.get('content-range'),etag:r.headers?.get('etag'),status:r.status};
      }catch(e){if(attempt>=2 || (e.status && ![408,429,500,502,503,504].includes(e.status)))throw e;await pause(250*2**attempt);}
      finally{clearTimeout(timer);}
    }
  }
  async function read(value){
    const target=url(typeof value==='string'?value:value?.url);
    if(!target)throw Error('Artwork is still saving.');
    const ranged=target.startsWith(endpoint+'?'),parts=[];let offset=0,total=null,etag=null;
    do{
      const headers=ranged?{Range:`bytes=${offset}-${offset+chunk-1}`}:{};
      if(etag)headers['If-Match']=etag;
      const r=await request(target,headers);
      if(r.status===206){
        const m=/^bytes (\d+)-(\d+)\/(\d+)$/.exec(r.range||'');
        if(!m || +m[1]!==offset || +m[2]-offset+1!==r.data.length || +m[3]<=+m[2] || (total!==null && +m[3]!==total) || (etag && r.etag!==etag))throw Error('Artwork changed or was incomplete. Please download again.');
        total=+m[3];if(!r.etag)throw Error('Artwork version is missing.');etag=r.etag;
      }else{if(offset)throw Error('Artwork range was not returned.');total=r.data.length;}
      parts.push(r.data);offset+=r.data.length;
    }while(offset<total);
    const out=new Uint8Array(total);let at=0;for(const part of parts){out.set(part,at);at+=part.length;}return out;
  }
  function bytes(value){
    if(value instanceof Uint8Array)return Promise.resolve(value);
    const key=typeof value==='string'?value:value?.url;
    if(!requests.has(key))requests.set(key,read(value).finally(()=>requests.delete(key)));
    return requests.get(key);
  }
  const imageTasks=new WeakMap();
  function loadImage(img,source){
    const prior=imageTasks.get(img);if(prior?.source===source)return prior.promise;
    const state={source};imageTasks.set(img,state);
    state.promise=(async()=>{
      const data=await bytes(source);
      if(imageTasks.get(img)!==state)return;
      let binary='';for(let i=0;i<data.length;i+=8192)binary+=String.fromCharCode(...data.subarray(i,i+8192));
      const type=data[0]===255&&data[1]===216?'image/jpeg':String.fromCharCode(...data.subarray(0,4))==='RIFF'?'image/webp':'image/png';
      img.src='data:'+type+';base64,'+btoa(binary);await img.decode();
    })().catch(e=>{if(imageTasks.get(img)===state)imageTasks.delete(img);throw e;});
    return state.promise;
  }
  async function sheetImage(img,id,source){
    try{if(!source)throw Error('Missing preview');await loadImage(img,source);return;}catch(error){
      if(!id)throw error;
      const {sheet}=await CN.api('charmNestLibrary',{op:'getSheet',id},{label:'Restoring sheet preview'});
      if(!sheet)throw error;
      try{if(!sheet.outputs?.preview?.url)throw error;await loadImage(img,sheet.outputs.preview.url);return;}catch{}
      await CN.ensurePdfJs();
      const data=await bytes(sheet.outputs?.ai),task=window.pdfjsLib.getDocument({data});
      const pdf=await task.promise;
      try{const page=await pdf.getPage(1),base=page.getViewport({scale:1}),viewport=page.getViewport({scale:Math.min(3,1200/base.width)}),cv=document.createElement('canvas');cv.width=Math.ceil(viewport.width);cv.height=Math.ceil(viewport.height);await page.render({canvasContext:cv.getContext('2d'),viewport}).promise;img.src=cv.toDataURL('image/png');await img.decode();}finally{await pdf.destroy();}
    }
  }
  function mount(){
    const recovering=new WeakSet();
    async function recover(img){
      if(recovering.has(img))return;recovering.add(img);
      img.dataset.assetLoading='1';
      try{
        try{await loadImage(img,img.dataset.assetSource||img.getAttribute('src'));}
        catch(error){
          const id=img.dataset.sheetPreview||img.dataset.labelSheet;
          if(!id || !window.CN)throw error;
          const {sheet}=await CN.api('charmNestLibrary',{op:'getSheet',id},{label:'Restoring preview'});
          if(!sheet)throw error;
          if(img.dataset.labelSheet){
            const files=sheet.label?.files||[];
            const f=files.find(f=>f.path && f.path===img.dataset.labelPath) || files.find(f=>+f.part===+img.dataset.labelPart);
            if(!f)throw error;
            if(f.payload && window.Sets){img.src=(await Sets.renderLabelPng(f.payload,f.label)).dataUrl;await img.decode();}
            else await loadImage(img,f.url);
          }else{
            await sheetImage(img,id,sheet.outputs?.preview?.url);
          }
        }
        delete img.dataset.assetFailed;img.classList.remove('assetFailed');
      }catch{img.dataset.assetFailed='1';img.classList.add('assetFailed');img.alt='Preview unavailable — click to retry';}
      finally{delete img.dataset.assetLoading;recovering.delete(img);}
    }
    document.addEventListener('error',e=>{
      const img=e.target;if(img.tagName!=='IMG'||img.closest('.backThumb')||(!img.src.includes(endpoint)&&!img.dataset.sheetPreview&&!img.dataset.labelSheet))return;
      e.stopImmediatePropagation();if(!img.dataset.assetSource)img.dataset.assetSource=img.getAttribute('src');void recover(img);
    },true);
    document.addEventListener('click',e=>{const img=e.target.closest?.('img[data-asset-failed]');if(img){e.preventDefault();e.stopImmediatePropagation();void recover(img);}},true);
    const retry=()=>document.querySelectorAll('img[data-asset-failed]').forEach(img=>recover(img));
    window.addEventListener('online',retry);document.addEventListener('visibilitychange',()=>{if(!document.hidden)retry();});
    // Handle failures that occurred before this listener and records awaiting their preview URL.
    let queued=false;
    const scan=()=>{queued=false;document.querySelectorAll('img[data-sheet-preview],img[data-label-sheet]').forEach(img=>{if(!img.dataset.assetFailed && !img.dataset.assetLoading && (!img.getAttribute('src') || (img.complete&&!img.naturalWidth)))void recover(img);});};
    new MutationObserver(()=>{if(!queued){queued=true;requestAnimationFrame(scan);}}).observe(document.body,{childList:true,subtree:true});scan();
  }
  if(typeof document!=='undefined'){if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount,{once:true});else mount();}
  return {url,bytes,loadImage,sheetImage};
});
