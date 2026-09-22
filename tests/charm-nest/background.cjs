const assert=require('node:assert/strict'),fs=require('node:fs'),{Worker}=require('node:worker_threads'),{JSDOM}=require('jsdom');
const workers=[];let tick;
class Adapter{
 constructor(){this.w=new Worker(`
 const {parentPort}=require('node:worker_threads'),fs=require('node:fs'),vm=require('node:vm'),{createCanvas}=require('@napi-rs/canvas');
 class Canvas{constructor(w,h){const cv=createCanvas(w,h);cv.convertToBlob=async()=>({_bytes:cv.toBuffer('image/png')});return cv;}}
 class Reader{readAsDataURL(b){this.result='data:image/png;base64,'+b._bytes.toString('base64');queueMicrotask(()=>this.onload());}}
 class SyncReader{readAsDataURL(b){return 'data:image/png;base64,'+b._bytes.toString('base64');}}
 const c=vm.createContext({console,Intl,navigator:{userAgent:"node-worker"},TextEncoder,TextDecoder,setTimeout,clearTimeout,queueMicrotask,Uint8Array,Uint16Array,Uint32Array,Int32Array,Float32Array,Float64Array,ArrayBuffer,Map,Set,OffscreenCanvas:Canvas,FileReader:Reader,FileReaderSync:SyncReader});
 c.self=c;c.postMessage=data=>parentPort.postMessage(data);c.importScripts=(...files)=>files.forEach(file=>vm.runInContext(fs.readFileSync(file.split('?')[0],'utf8'),c,{filename:file}));c.importScripts('charm-nest-compute-worker.js');parentPort.on('message',data=>c.onmessage({data}));
 `,{eval:true});workers.push(this);this.w.on('message',data=>this.onmessage?.({data}));this.w.on('error',e=>this.onerror?.(e));}
 postMessage(d){this.w.postMessage(d);}terminate(){this.w.terminate();}
}
global.self=global;global.PDFLib=require('../../vendor/pdf-lib-1.17.1.min.js');require('../../charm-nest-pdf.js');const E=require('../../charm-nest-export.js');
const original={...CharmNestPDF},nativeCompose=E.compose;
const dom=new JSDOM('<span class="placementThumb"></span>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
w.Worker=Adapter;w.OffscreenCanvas=function(){};w.CharmNestPDF=CharmNestPDF;w.CharmNestExport=E;
w.eval(fs.readFileSync('charm-nest-background.js','utf8'));
const P=w.CharmNestPDF,wait=ms=>new Promise(r=>setTimeout(r,ms));
async function fixture(count=1){const L=PDFLib,d=await L.PDFDocument.create(),p=d.addPage([200,200]);p.node.normalize();p.node.addContentStream(d.context.register(d.context.flateStream(Array.from({length:count},(_,i)=>`0 0 0 RG .5 w ${10+i%10*15} ${10+Math.floor(i/10)*15} 10 10 re S`).join('\n'))));return d.save({useObjectStreams:false});}
(async()=>{
 const bytes=await fixture(100);let beats=0,last=performance.now(),maxPause=0;tick=setInterval(()=>{beats++;maxPause=Math.max(maxPause,performance.now()-last);last=performance.now();},5);
 const parsed=await P.parseSource(bytes,'fixture');assert(!parsed.doc);assert(parsed.computeKey);const group=await P.groupCharmsAsync(parsed,{minPt:6});assert(group.charms.length>50);
 const expected=original.groupCharms(await original.parseSource(bytes,'fixture'),{minPt:6});assert.deepEqual(group.charms.map(c=>c.bbox),expected.charms.map(c=>c.bbox));
 const c=group.charms.find(c=>c.bbox[2]-c.bbox[0]<12);c.id='a';c.poolId='order_1';c.sourceId='s';c.name='Parent';
 let progress=0;await P.buildSilhouettes(parsed,[c],4,()=>progress++);assert.equal(progress,1);assert(c.bits instanceof Uint8Array);assert(c.bits.some(Boolean));assert(c.thumb.startsWith('data:image/png'));
 const thumb=await P.frontPreview(c,220);assert(thumb.startsWith('data:image/png'));
 const spec={sheet:{wPt:100,hPt:70},placements:[{charm:c,angle:0,cxPt:30,cyPt:30,scale:.975}],sources:new Map([['s',parsed]])};
 const ai=await P.buildSheet(spec);assert(ai.length>100);assert.equal(spec.placements[0].layerName,'Parent','worker preserves export layer-name side effect');
 const isolated=await P.buildSingleCharm(c,parsed);const nativeSingle=await original.buildSingleCharm(c,await original.parseSource(bytes,'fixture'));assert.equal((await original.parseSource(isolated,'single')).pageW,(await original.parseSource(nativeSingle,'single')).pageW,'isolated charm keeps its exact artboard');
 const sheet={id:'test',charms:[c],placements:[{id:'a',n:1,layer:'Parent',scale:.975}]};
 const back=await P.buildBackFile({charm:c,parsed,cutMembers:[c.outline],cx:c.centerPt[0],cy:c.centerPt[1],angleDeg:0,padPt:5,glyphs:[],view:'asSeenFromBack'});assert(back.bytes.length>100);
 const combined=await E.compose(ai,sheet,[{poolId:c.poolId,bytes:back.bytes}]);assert.equal(combined.layout[0].scale,.975,'worker composition retains exact parent scale with backs');
 const shapes=await w.CharmNestBackground.run('roseShapes',{charms:[c],placements:[{id:c.id,cxPt:30,cyPt:30,angle:0,scale:1}]});assert.equal(shapes.length,1);
 const made=await E.compose(ai,sheet,[]);assert.equal(made.heightPt,70);const dxf=await E.productionDxf(made.ai);assert(dxf.text.includes('$INSUNITS'));assert(dxf.entityCount>0);
 // Do not silently accept a bad worker job, or poison the queue after one fails.
 await assert.rejects(()=>P.parseSource(new Uint8Array([1,2]),'bad'),/not a PDF/);assert((await P.parseSource(bytes,'next')).pageW===200);
 clearInterval(tick);assert(beats>5,'caller continues handling events during geometry and exports');assert(maxPause<300,`caller stalled ${maxPause}ms`);assert.equal(workers.length,2,'preview and geometry use bounded independent workers');
 const target=w.document.querySelector('.placementThumb');let applied=0;target.dispatchEvent(new w.Event('pointerdown',{bubbles:true}));assert(w.CharmNestInteraction.defer('list',()=>applied++));assert(w.CharmNestInteraction.defer('list',()=>applied+=2));await wait(260);assert.equal(applied,0);target.dispatchEvent(new w.Event('pointerup',{bubbles:true}));await wait(300);assert.equal(applied,2,'gesture updates coalesce to newest result after pointer release');
 console.log('Background OK: real worker PDF parsing/grouping, silhouettes, thumbnails, exact export scale/layers, bounded queues, error recovery and uninterrupted caller/gestures');
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>{clearInterval(tick);workers.forEach(w=>w.terminate());dom.window.close();});
