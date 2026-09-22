/* WebGPU coarse placement search. Owned by ONE solver worker; no DOM/network.
 * All rotations/positions are dispatched together. Hierarchical top-32 reduction
 * stays on the GPU; only the finalists return to the CPU's exact fine-grid pass.
 * Geometry is uploaded once and bounded by an LRU; buffers/pipelines are reused.
 */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CharmNestGPU=api;})(typeof self!=='undefined'?self:globalThis,function(){
'use strict';
const TOP=32,WG=128,INVALID=-1e30,MAX_BYTES=64*1024*1024;
const shader=`
struct Pair { score:f32, pos:u32 };
@group(0) @binding(0) var<storage,read> cfg:array<f32>;
@group(0) @binding(1) var<storage,read> masks:array<u32>;
@group(0) @binding(2) var<storage,read> shapeInfo:array<u32>;
@group(0) @binding(3) var<storage,read> grids:array<u32>;
@group(0) @binding(4) var<storage,read_write> output:array<Pair>;
var<workgroup> scores:array<f32,128>;
var<workgroup> positions:array<u32,128>;
fn sumBox(x0:i32,y0:i32,x1:i32,y1:i32)->f32 {
 let w=i32(cfg[0]);let h=i32(cfg[1]);let a=clamp(x0,0,w);let b=clamp(y0,0,h);let c=clamp(x1,0,w);let d=clamp(y1,0,h);
 var inside=0i;let base=u32(cfg[3]);
 if(c>a && d>b){inside=i32(grids[base+u32(d*(w+1)+c)])-i32(grids[base+u32(b*(w+1)+c)])-i32(grids[base+u32(d*(w+1)+a)])+i32(grids[base+u32(b*(w+1)+a)]);}
 return f32(inside+(x1-x0)*(y1-y0)-max(0,c-a)*max(0,d-b));
}
fn overlap(off:u32,mw:u32,mh:u32,x:i32,y:i32,base:u32,limit:u32,wall:bool)->u32 {
 let gw=i32(cfg[2]);let gh=i32(cfg[1]);let wx=x>>5;let shift=u32(x&31);var count=0u;
 for(var row=0u;row<mh;row++){
  let gy=y+i32(row);
  for(var k=0u;k<mw;k++){
   var bits=masks[off+row*mw+k]<<shift;
   if(shift>0u && k>0u){bits=bits | (masks[off+row*mw+k-1u]>>(32u-shift));}
   let gx=wx+i32(k);var g=0u;
   if(gy>=0 && gy<gh && gx>=0 && gx<gw){g=grids[base+u32(gy*gw+gx)];if(wall && gx==gw-1 && (u32(cfg[0])&31u)>0u){g=g | (0xffffffffu<<(u32(cfg[0])&31u));}}else if(wall){g=0xffffffffu;}
   count+=countOneBits(g&bits);if(count>limit){return count;}
  }
 }
 return count;
}
fn hash(n:u32)->f32 {var x=n;x=(x^(x>>16u))*0x7feb352du;x=(x^(x>>15u))*0x846ca68bu;return f32(x^(x>>16u))/4294967296.0;}
fn grade(pos:u32,angle:u32)->f32 {
 let cw=u32(cfg[0]);let ch=u32(cfg[1]);let m=angle*16u;let w=shapeInfo[m];let h=shapeInfo[m+1u];let x=pos%cw;let y=pos/cw;
 if(y+h>ch || x+w>cw){return -1e30;}
 let inner=sumBox(i32(x),i32(y),i32(x+w),i32(y+h));let cells=shapeInfo[m+4u];
 if(f32(w*h)-inner<f32(cells)-2.0){return -1e30;}
 var ov=0u;if(inner>0.0){ov=overlap(shapeInfo[m+3u],shapeInfo[m+2u],h,i32(x),i32(y),0u,2u,true);if(ov>2u){return -1e30;}}
 let ro=shapeInfo[m+7u];let rw=shapeInfo[m+5u];let rh=shapeInfo[m+6u];let rc=max(1.0,f32(shapeInfo[m+8u]));
 var contact=0.0;var edge=0.0;var straight=0.0;
 if(cfg[11]>0.0){contact=(sumBox(i32(x)-1,i32(y)-1,i32(x+w)+1,i32(y+h)+1)-inner)/f32(2u*(w+h)+4u);}else{
  contact=f32(overlap(ro,rw,rh,i32(x)-1,i32(y)-1,u32(cfg[4]),0xffffffffu,false))/rc;
  edge=f32(overlap(ro,rw,rh,i32(x)-1,i32(y)-1,u32(cfg[5]),0xffffffffu,true))/rc;
  let gaps=vec4f(f32(x)-cfg[16],f32(y)-cfg[17],cfg[18]-f32(x+w),cfg[19]-f32(y+h));
  for(var side=0u;side<4u;side++){if(gaps[side]>=0.0 && gaps[side]<=1.0){straight=max(straight,bitcast<f32>(shapeInfo[m+9u+side])*(1.0-gaps[side]/2.0));}}
 }
 let gx=select(f32(x),f32(cw-x-w),cfg[9]>0.0);let gy=select(f32(y),f32(ch-y-h),cfg[10]>0.0);
 var growth=0.0;if(cfg[12]>0.0){growth=max(cfg[13],select(f32(y+h),f32(x+w),cfg[12]==1.0));}
 var guided=0.0;
 if(cfg[20]>0.0){
  let ratio=cfg[15];var near=0.0;
  for(var i=0u;i<4u;i++){let a=24u+i*5u;let dx=max(0.0,max(cfg[a]-f32(x+w)*ratio,f32(x)*ratio-cfg[a]-cfg[a+2u]));let dy=max(0.0,max(cfg[a+1u]-f32(y+h)*ratio,f32(y)*ratio-cfg[a+1u]-cfg[a+3u]));near=max(near,cfg[a+4u]/(1.0+length(vec2f(dx,dy))/ratio));}
  var fit=0.0;let role=u32(cfg[21]);
  if(role>0u){let dx=min(f32(x),f32(cw-x-w));let dy=min(f32(y),f32(ch-y-h));let horizontal=cw>=ch;let alongLong=select(dx,dy,horizontal);let alongShort=select(dy,dx,horizontal);var gap=min(dx,dy);var parallel=true;
   if(role==1u){gap=alongLong;parallel=select(h>=w,w>=h,horizontal);}else if(role==2u){gap=alongShort;parallel=select(w>=h,h>=w,horizontal);}else if(role==3u){gap=max(dx,dy);}
   fit=cfg[22]*select(f32(min(w,h))/f32(max(1u,max(w,h))),1.0,parallel)/(1.0+max(0.0,gap));
  }
  guided=1.2*near+0.15*cfg[22]*edge+0.25*fit;
 }
 return contact+0.35*edge+0.15*straight+guided-cfg[14]*growth-cfg[6]*(gx+gy)/f32(cw+ch)-0.05*f32(ov)+cfg[7]*hash(pos+angle*cw*ch+bitcast<u32>(cfg[8]));
}
fn sortTile(lid:u32){
 for(var k=2u;k<=128u;k*=2u){for(var j=k/2u;j>0u;j/=2u){
  let peer=lid^j;let a=scores[lid];let ap=positions[lid];let b=scores[peer];let bp=positions[peer];
  let aBetter=a>b || (a==b && ap<bp);let keepBetter=((lid&k)==0u)==((lid&j)==0u);
  workgroupBarrier();if(aBetter!=keepBetter){scores[lid]=b;positions[lid]=bp;}workgroupBarrier();
 }}
}
@compute @workgroup_size(128) fn scan(@builtin(local_invocation_index) lid:u32,@builtin(workgroup_id) group:vec3u){
 let pos=group.x*128u+lid;scores[lid]=grade(pos,group.y);positions[lid]=pos;workgroupBarrier();sortTile(lid);
 if(lid<32u){output[(group.y*u32(cfg[23])+group.x)*32u+lid]=Pair(scores[lid],positions[lid]);}
}
`;
const reduceShader=`
struct Pair {score:f32,pos:u32};
@group(0) @binding(0) var<storage,read> input:array<Pair>;
@group(0) @binding(1) var<storage,read_write> output:array<Pair>;
@group(0) @binding(2) var<uniform> cfg:vec4u;
var<workgroup> scores:array<f32,128>;var<workgroup> positions:array<u32,128>;
@compute @workgroup_size(128) fn reduce(@builtin(local_invocation_index) lid:u32,@builtin(workgroup_id) group:vec3u){
 let i=group.x*128u+lid;var a=Pair(-1e30,0xffffffffu);if(i<cfg.x*32u){a=input[group.y*cfg.x*32u+i];}scores[lid]=a.score;positions[lid]=a.pos;workgroupBarrier();
 for(var k=2u;k<=128u;k*=2u){for(var j=k/2u;j>0u;j/=2u){let peer=lid^j;let s=scores[lid];let p=positions[lid];let t=scores[peer];let q=positions[peer];let better=s>t || (s==t && p<q);let keep=((lid&k)==0u)==((lid&j)==0u);workgroupBarrier();if(better!=keep){scores[lid]=t;positions[lid]=q;}workgroupBarrier();}}
 if(lid<32u){output[(group.y*cfg.y+group.x)*32u+lid]=Pair(scores[lid],positions[lid]);}
}
`;
const clock=()=>performance.now();
function deadline(promise,ms,label){let timer;return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(label+' timed out')),ms);})]).finally(()=>clearTimeout(timer));}
async function create(options={}){
 const gpu=options.gpu || (typeof navigator!=='undefined'&&navigator.gpu);
 if(!gpu)throw new Error('WebGPU unavailable');
 const adapter=await deadline(gpu.requestAdapter({powerPreference:'high-performance'}),5000,'GPU adapter');
 if(!adapter)throw new Error('No WebGPU adapter');
 const info=adapter.info||{};
 if(!options.allowSoftware&&(info.isFallbackAdapter||adapter.isFallbackAdapter||/swiftshader|llvmpipe|software/i.test(info.description||'')))throw new Error('Software GPU; using CPU');
 const device=await deadline(adapter.requestDevice(),5000,'GPU device');
 let dead=false,cacheBytes=0;const buffers=new Map(),geometry=new Map();let serial=0;
 const stats={adapter:[info.vendor,info.architecture,info.description].filter(Boolean).join(' ')||'WebGPU',calls:0,candidates:0,gpuMs:0,readbackBytes:0,uploads:0,peakBytes:0};
 device.lost.then(()=>{dead=true;});device.addEventListener?.('uncapturederror',()=>{dead=true;});
 const destroy=()=>{dead=true;for(const b of buffers.values())b.destroy();for(const g of geometry.values()){g.mask.destroy();g.meta.destroy();}buffers.clear();geometry.clear();device.destroy();};
 let scan,reduce;
 try {scan=await deadline(device.createComputePipelineAsync({layout:'auto',compute:{module:device.createShaderModule({code:shader}),entryPoint:'scan'}}),8000,'GPU scan pipeline');reduce=await deadline(device.createComputePipelineAsync({layout:'auto',compute:{module:device.createShaderModule({code:reduceShader}),entryPoint:'reduce'}}),8000,'GPU reduction pipeline');}catch(e){destroy();throw e;}
 const U=typeof GPUBufferUsage!=='undefined'?GPUBufferUsage:{MAP_READ:1,COPY_SRC:4,COPY_DST:8,UNIFORM:64,STORAGE:128};
 function checkMemory(extra){const total=cacheBytes+[...buffers.values()].reduce((n,b)=>n+b.size,0)+extra;if(total>MAX_BYTES*2)throw new Error('GPU working memory limit');stats.peakBytes=Math.max(stats.peakBytes,total);}
 function buffer(key,size,usage){size=Math.max(16,Math.ceil(size/4)*4);if(size>Math.min(MAX_BYTES,device.limits.maxBufferSize,usage&U.STORAGE?device.limits.maxStorageBufferBindingSize:Infinity))throw new Error('GPU batch exceeds memory limit');let b=buffers.get(key);if(!b||b.size<size){checkMemory(size-(b?.size||0));b?.destroy();b=device.createBuffer({size,usage});buffers.set(key,b);}return b;}
 const write=(key,array,usage=U.STORAGE)=>{const b=buffer(key,array.byteLength,usage|U.COPY_DST);device.queue.writeBuffer(b,0,array);return b;};
 function shape(p,S){
  // Variants are stable within a search; repair variants use their own cache entry.
  const key=p.variants;let g=geometry.get(key);if(g){g.used=++serial;return g;}
  const data=[],meta=new Uint32Array(key.length*16),floats=new Float32Array(meta.buffer);let offset=0;
  for(let i=0;i<key.length;i++){
   const v=key[i],c=v.coarse,pm=c.pm;
   if(!c.contactRing){const bits=c.bits||(()=>{const b=new Uint8Array(c.w*c.h);for(let y=0;y<c.h;y++)for(let x=0;x<c.w;x++)b[y*c.w+x]=(pm.variants[0][y*pm.words+(x>>5)]>>>(x&31))&1;return b;})();const r=S.ring(bits,c.w,c.h,1);c.contactRing=S.packShifted(r.bits,r.w,r.h);}
   const ring=c.contactRing,a=i*16;meta.set([c.w,c.h,pm.words,offset,pm.cells,ring.words,ring.h,offset+pm.variants[0].length,ring.cells],a);
   data.push(pm.variants[0],ring.variants[0]);offset+=pm.variants[0].length+ring.variants[0].length;
   // Prime the same straight-edge profile used by the CPU scorer.
   S.straightEdgeAt(c,{W:c.w,H:c.h},0,0,1);floats.set(c.edgeProfile.sides,a+9);
  }
  const bytes=offset*4+meta.byteLength;
  if(bytes>MAX_BYTES/2)throw new Error('Rotation geometry exceeds GPU cache limit');
  while(cacheBytes+bytes>MAX_BYTES/2 && geometry.size){let oldest;for(const pair of geometry)if(!oldest||pair[1].used<oldest[1].used)oldest=pair;oldest[1].mask.destroy();oldest[1].meta.destroy();cacheBytes-=oldest[1].bytes;geometry.delete(oldest[0]);}
  checkMemory(bytes);
  const packed=new Uint32Array(offset);let at=0;for(const part of data){packed.set(part,at);at+=part.length;}
  const mask=device.createBuffer({size:Math.max(4,packed.byteLength),usage:U.STORAGE|U.COPY_DST}),mb=device.createBuffer({size:Math.max(4,meta.byteLength),usage:U.STORAGE|U.COPY_DST});device.queue.writeBuffer(mask,0,packed);device.queue.writeBuffer(mb,0,meta);
  g={mask,meta:mb,bytes,used:++serial};geometry.set(key,g);cacheBytes+=bytes;stats.uploads++;return g;
 }
 async function candidates(args,S,seed=1){
  if(dead)throw new Error('GPU device lost');
  const t=clock(),[p,fine,coarse,ratio,gravW,noise,random,cornerX,cornerY,strip,boundarySeed]=args;
  if(!p.variants.length)return [];
  const n=p.variants.length,cells=coarse.W*coarse.H,tiles=Math.ceil(cells/WG);
  if(tiles>device.limits.maxComputeWorkgroupsPerDimension||n>device.limits.maxComputeWorkgroupsPerDimension||cells*n>8000000)throw new Error('Sheet exceeds GPU batch limit');
  const g=shape(p,S),occ=coarse.occ,sat=coarse.sat,material=coarse.material?.occ||new Uint32Array(occ.length),walls=coarse.walls?.occ||new Uint32Array(occ.length);
  const grid=new Uint32Array(occ.length+sat.length+material.length+walls.length);grid.set(occ);grid.set(sat,occ.length);grid.set(material,occ.length+sat.length);grid.set(walls,occ.length+sat.length+material.length);
  const b=S.sheetBounds(coarse),cfg=new Float32Array(44),profile=p.guidance?.profiles?.[p.id];
  cfg.set([coarse.W,coarse.H,coarse.words,occ.length,occ.length+sat.length,occ.length+sat.length+material.length,gravW,noise,0,cornerX,cornerY,Number(!!boundarySeed),strip?(strip.axis==='x'?1:2):0,strip?strip.end/ratio:0,strip?.weight??4,ratio,b.left,b.top,b.right,b.bottom,Number(!!profile),profile?({interior:0,'long-edge':1,'short-edge':2,corner:3}[profile.edgeRole]??4):0,(profile?.edgeAffinity||0)/100,tiles]);new Uint32Array(cfg.buffer)[8]=seed>>>0;
  if(profile){const partners=(fine.parts||[]).map(part=>({part,weight:S.packingCompatibility(p.id,part.id,p.guidance)})).filter(x=>x.weight>0).sort((a,b)=>b.weight-a.weight).slice(0,4);partners.forEach(({part,weight},i)=>cfg.set([part.x,part.y,part.w,part.h,weight],24+i*5));}
  const config=write('config',cfg),gridBuffer=write('grid',grid),out=buffer('level0',tiles*n*TOP*8,U.STORAGE|U.COPY_SRC);
  const bind=device.createBindGroup({layout:scan.getBindGroupLayout(0),entries:[config,g.mask,g.meta,gridBuffer,out].map((buffer,binding)=>({binding,resource:{buffer}}))});
  const encoder=device.createCommandEncoder();let pass=encoder.beginComputePass();pass.setPipeline(scan);pass.setBindGroup(0,bind);pass.dispatchWorkgroups(tiles,n);pass.end();
  let count=tiles,level=0,input=out;
  while(count>1){const next=Math.ceil(count/4),output=buffer('level'+(++level),next*n*TOP*8,U.STORAGE|U.COPY_SRC),rc=write('reduceCfg'+level,new Uint32Array([count,next,n,0]),U.UNIFORM);const group=device.createBindGroup({layout:reduce.getBindGroupLayout(0),entries:[input,output,rc].map((buffer,binding)=>({binding,resource:{buffer}}))});pass=encoder.beginComputePass();pass.setPipeline(reduce);pass.setBindGroup(0,group);pass.dispatchWorkgroups(next,n);pass.end();count=next;input=output;}
  const bytes=n*TOP*8,read=buffer('read',bytes,U.MAP_READ|U.COPY_DST);encoder.copyBufferToBuffer(input,0,read,0,bytes);device.queue.submit([encoder.finish()]);
  try{await deadline(read.mapAsync(1,0,bytes),8000,'GPU shortlist');}catch(e){destroy();throw e;}
  const view=read.getMappedRange(0,bytes),scores=new Float32Array(view),ids=new Uint32Array(view),result=[];
  for(let v=0;v<n;v++){const row=[];for(let k=0;k<TOP;k++){const i=(v*TOP+k)*2;if(scores[i]>INVALID/2){const pos=ids[i+1];row.push({x:pos%coarse.W,y:Math.floor(pos/coarse.W),s:scores[i]});}}result.push(row);}
  read.unmap();if(dead)throw new Error('GPU validation failed');stats.calls++;stats.candidates+=p.variants.reduce((sum,v)=>sum+Math.max(0,coarse.W-v.coarse.w+1)*Math.max(0,coarse.H-v.coarse.h+1),0);stats.gpuMs+=clock()-t;stats.readbackBytes+=bytes;return result;
 }
 return {candidates,destroy,stats};
}
// An isolated, bounded comparison. It never publishes a layout or calls an API.
async function benchmark(source,S,cb={}) {
 const started=clock();cb.onStage?.('Checking GPU availability…');
 let gpu;
 try{gpu=await (cb.createGPU||create)();}catch(e){return {available:false,reason:String(e.message||e)};}
 const startupMs=clock()-started;
 try{
  let sample=source?.pieces?.length?'loaded charms':'reference shapes';
  const reference=()=>({sheet:{wPt:100,hPt:50,insetPt:1},fineRes:2,coarseRes:.5,clearancePt:0,maxFill:.8,angles:Array.from({length:36},(_,i)=>i*10),pieces:Array.from({length:12},(_,i)=>{
   const w=28+i%4*3,h=25+i%5*3,bits=new Uint8Array(w*h);
   for(let y=0;y<h;y++)for(let x=0;x<w;x++){const dx=(x-w/2)/(w/2),dy=(y-h/2)/(h/2);bits[y*w+x]=i%3===0?+(dx*dx+dy*dy<1):i%3===1?+(x<w*.45||y>h*.6):+(Math.abs(dx)+Math.abs(dy)<1);}
   return {id:'reference-'+i,w,h,bits,scale:2};
  })});
  const job={...(source?.pieces?.length?source:reference()),initialLayout:null,seed:317,timeBudgetMs:6000,maxTrials:1,stallMs:0,gpuBenchmark:true,packingPending:false};
  job.pieces=job.pieces.slice(0,12); // A sample, not a production nesting run.
  const results={};
  for(const mode of ['cpu','gpu']){
   if(cb.shouldStop?.())throw new Error('Comparison cancelled');
   cb.onStage?.(`Comparing ${mode.toUpperCase()} · ${job.pieces.length} ${sample}…`);
   const result=await S.solve(job,{gpu:mode==='gpu'?gpu:null,shouldStop:cb.shouldStop});
   if(cb.shouldStop?.())throw new Error('Comparison cancelled');
   const verified=S.verify(job,result.placements,6).ok;
   const ms=result.elapsedMs+(mode==='gpu'?startupMs:0);
   results[mode]={ms,layouts:result.trials,checks:result.metrics.positions,gpuChecks:result.metrics.gpuPositions,checksPerSecond:result.metrics.positions/Math.max(.001,ms/1000),placed:result.placements.length,fill:result.density,verified};
  }
  return {available:true,sample,pieces:job.pieces.length,adapter:gpu.stats.adapter,startupMs,...results};
 }finally{gpu.destroy();}
}
return {create,benchmark,shader,reduceShader};
});
