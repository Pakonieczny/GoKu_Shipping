const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('charm-nest-assets.js','utf8');
const listeners={},images=[],reads=[];let rendered=0,qrPayload=null;
const url='https://firebasestorage.googleapis.com/v0/b/test/o/charmnest%2Fsheet.png?alt=media&token=test';
const img=dataset=>({tagName:'IMG',dataset:{...dataset},src:url,complete:true,naturalWidth:0,classList:{add(){},remove(){}},closest:()=>null,getAttribute(k){return this[k]},async decode(){if(!this.src.startsWith('data:image/png;base64,'))throw Error('invalid image');this.naturalWidth=100;}});
const png='data:image/png;base64,iVBORw0KGgo=';
const ctx={URL,AbortController,Uint8Array,setTimeout,clearTimeout,btoa,console,Promise,
  fetch:async(target)=>{reads.push(target);return new Response('missing',{status:404});},
  requestAnimationFrame:f=>f(),MutationObserver:class{observe(){}},
  document:{readyState:'complete',body:{},addEventListener:(name,fn)=>listeners[name]=fn,querySelectorAll:()=>images,createElement:()=>({getContext:()=>({}),toDataURL:()=>png})},
  CN:{api:async()=>({sheet:{outputs:{ai:{url:'data:application/pdf;base64,JVBERg=='}},label:{files:[{part:1,path:'label.png',payload:'gold;40000001,40000002',label:'GF Set 1 Sheet 1'}]}}}),ensurePdfJs:async()=>{}},
  Sets:{renderLabelPng:async(payload)=>{qrPayload=payload;return {dataUrl:png}}},
  pdfjsLib:{getDocument:()=>({promise:Promise.resolve({getPage:async()=>({getViewport:()=>({width:200,height:100}),render:()=>({promise:Promise.resolve(rendered++)})}),destroy:async()=>{}})})},
  addEventListener(){},Response};ctx.window=ctx;ctx.self=ctx;
vm.createContext(ctx);vm.runInContext(source,ctx);
const waitFor=async(check)=>{for(let i=0;i<100;i++){if(check())return;await new Promise(r=>setTimeout(r,5));}throw Error('Recovery did not settle');};
(async()=>{
 const qr=img({labelSheet:'sheet',labelPath:'label.png',labelPart:'1'});images.push(qr);
 listeners.error({target:qr,stopImmediatePropagation(){}});
 await waitFor(()=>qr.naturalWidth>0);
 assert.equal(qrPayload,'gold;40000001,40000002','QR recovers the exact saved order payload');
 assert.equal(qr.src,png);assert(!qr.dataset.assetFailed);
 ctx.fetch=async(target)=>target.startsWith('data:application/pdf')?new Response('%PDF-test'):new Response('missing',{status:404});
 const sheet=img({sheetPreview:'sheet'});images.push(sheet);
 listeners.error({target:sheet,stopImmediatePropagation(){}});
 await waitFor(()=>sheet.naturalWidth>0);
 assert.equal(rendered,1,'missing sheet PNG is rebuilt from the saved AI page');
 assert.equal(sheet.src,png,'recovered pixels survive enlargement without revoked blob URLs');
 const offline=img({sheetPreview:'unavailable'});ctx.CN.api=async()=>{throw Error('offline')};images.push(offline);
 listeners.error({target:offline,stopImmediatePropagation(){}});
 await waitFor(()=>offline.dataset.assetFailed==='1');
 assert.match(offline.alt,/click to retry/,'unrecoverable failure stays actionable');
 assert(reads.every(u=>u.startsWith('/.netlify/functions/charmNestAsset?')));
 console.log('Preview recovery OK: saved QR payload, AI-rendered sheet fallback, stable enlarged image and explicit retry');
})().catch(e=>{console.error(e);process.exitCode=1});
