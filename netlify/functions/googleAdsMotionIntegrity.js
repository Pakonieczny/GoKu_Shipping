// Product geometry is preserved by construction: an original transparent
// photograph is translated as one rigid layer over a real moving scene.
// Only scenery is generated. No segmentation, deformation or still-card fallback.
const crypto=require('crypto'),fs=require('fs/promises'),path=require('path'),os=require('os'),sharp=require('sharp');
const POLICY='original-layer-moving-scene-v2',MODE='protected-product-motion',FPS=24,SECONDS=10,FRAMES=FPS*SECONDS,MIN_SSIM=.99;
const KEYS=['mobile_portrait','mobile_square','desktop_landscape'];
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
function sourceAllowed(source,productId){
 const identity=require('./googleAdsAdIdentity');
 if(!source?.asset?.path||source.artwork===true||source.role==='inspiration'||identity.foreign(source,productId))return false;
 return source.source?.kind==='product'&&identity.productKey(source.source.productId)===identity.productKey(productId)
  ||source.source?.kind==='upload'&&identity.owns(source.productIds,productId)
  ||source.identityVerified===identity.POLICY&&identity.productKey(source.identityProductId)===identity.productKey(productId);
}
function complete(job){
 return job?.motionMode===MODE&&job.integrityPolicy===POLICY&&/^[a-f0-9]{64}$/.test(job.integritySourceHash||'')
  &&KEYS.every(key=>{const v=job.variants?.find(v=>v.key===key),i=v?.integrity;return i?.policy===POLICY&&i.sourceHash===job.integritySourceHash&&i.assetHash===v.asset?.hash&&!!v.asset?.path&&/^[a-f0-9]{64}$/.test(i.videoHash||'')&&i.framesChecked===FRAMES&&Number.isFinite(i.minSsim)&&i.minSsim>=MIN_SSIM&&i.minSsim<=1&&i.fullSource===true&&i.overlaysOutsideSource===true&&i.transform==='rigid-translation'&&i.productTravelPx===32&&i.motionMeasuredOn==='final-video'&&i.sceneMotion?.samples===20&&Number.isInteger(i.sceneMotion.activeSamples)&&i.sceneMotion.activeSamples>=6&&i.sceneMotion.activeSamples<=19&&/^[a-f0-9]{64}$/.test(i.sceneHash||'');});
}
function assertVideo(variant,bytes){
 if(!Buffer.isBuffer(bytes)||hash(bytes)!==variant?.integrity?.videoHash)throw Error('The video bytes differ from the file that passed product preservation. Rebuild before uploading.');
}
function enforceReview(quality,keys=KEYS){
 const checks=require('./googleAdsAdIdentity').CHECKS;
 const missing=keys.filter(key=>!checks.every(c=>quality?.formatIdentity?.[key]?.[c.field]===true));
 if(quality.exactProductIdentity!==true||missing.length){
  quality.pass=false;quality.productFaithful=false;quality.exactProductIdentity=false;
  quality.issues=[...(quality.issues||[]),'Exact charm identity was not confirmed for every format'+(missing.length?': '+missing.join(', '):'')+'.'];
 }
 return quality;
}
function geometry(format){
 const {width:W,height:H,key}=format;
 return key==='landscape'?{photo:{x:448,y:36,w:W-484,h:H-72},copy:{x:48,y:170,w:352,h:H-218}}:
  {photo:{x:36,y:key==='portrait'?340:268,w:W-72,h:H-(key==='portrait'?376:304)},copy:{x:48,y:128,w:W-96,h:key==='portrait'?184:116}};
}
const xml=v=>String(v||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&apos;'}[c]));
async function fontFiles(){
 const out=[];
 for(const name of ['CormorantGaramond.ttf','OpenSans-Regular.ttf']){
  let found;for(const dir of [path.join(__dirname,'fonts'),path.join(process.cwd(),'netlify/production-functions/fonts'),path.join(process.cwd(),'netlify/functions/fonts')]){const file=path.join(dir,name);try{await fs.access(file);found=file;break;}catch{}}
  if(!found)throw Error('Video caption font is missing: '+name);out.push(found);
 }return out;
}
function wrap(text,size,width){
 const limit=Math.max(8,Math.floor(width/(size*.57))),lines=[];
 for(const word of String(text||'').trim().split(/\s+/).filter(Boolean)){
  if(word.length>limit){for(let i=0;i<word.length;i+=limit)lines.push(word.slice(i,i+limit));}
  else if(lines.length&&lines.at(-1).length+word.length+1<=limit)lines[lines.length-1]+=' '+word;
  else lines.push(word);
 }return lines;
}
async function caption(beat,format,geo){
 let size=format.key==='square'?32:42,small=22,title,support;
 for(;size>=24;size-=2){title=wrap(beat.title,size,geo.copy.w);support=wrap(beat.support,small,geo.copy.w);if(title.length*size*1.16+(support.length?12+support.length*small*1.25:0)<=geo.copy.h)break;}
 if(size<24)throw Error('Shorten the saved video message so it fits beside the protected product photo.');
 const {x,y}=geo.copy,text=title.map((t,i)=>`<text x="${x}" y="${y+size+i*size*1.16}" font-family="Cormorant Garamond" font-size="${size}">${xml(t)}</text>`).join('')+support.map((t,i)=>`<text x="${x}" y="${y+title.length*size*1.16+12+small+i*small*1.25}" font-family="Open Sans" font-size="${small}">${xml(t)}</text>`).join('');
 // Clip to the copy rectangle as a second guard against long glyphs and fonts.
 const svg=`<svg xmlns="http://www.w3.org/2000/svg" width="${format.width}" height="${format.height}"><defs><clipPath id="copy"><rect x="${x}" y="${y-4}" width="${geo.copy.w}" height="${geo.copy.h+8}"/></clipPath></defs><g fill="#30291f" clip-path="url(#copy)">${text}</g></svg>`;
 const Resvg=require('@resvg/resvg-js').Resvg;
 return Buffer.from(new Resvg(svg,{font:{fontFiles:await fontFiles(),loadSystemFonts:false}}).render().asPng());
}
function auditStats(text){
 const rows=text.trim().split(/\r?\n/).filter(Boolean),values=rows.map(row=>Number(row.match(/\bAll:([\d.]+)/)?.[1]));
 if(rows.length!==FRAMES||values.some(v=>!Number.isFinite(v)||v<MIN_SSIM||v>1))throw Error('The exported video failed its every-frame product preservation check ('+rows.length+' frames; minimum similarity '+Math.min(...values).toFixed(6)+'). It cannot be accepted or published.');
 return {framesChecked:rows.length,minSsim:Math.min(...values)};
}
// Alpha is supplied by the photographer/operator, never guessed by a generative
// background remover. JPEGs and photos with an opaque backdrop stop before spend.
async function inspectSource(bytes){
 const input=sharp(bytes,{limitInputPixels:40000000}),meta=await input.metadata();
 if(!['png','webp'].includes(meta.format)||!meta.hasAlpha||Number(meta.pages||1)!==1)throw sourceError();
 const {data,info}=await input.rotate().toColourspace('srgb').ensureAlpha().raw().toBuffer({resolveWithObject:true});
 let clear=0,solid=0,left=info.width,top=info.height,right=-1,bottom=-1;
 for(let y=0;y<info.height;y++)for(let x=0;x<info.width;x++){
  const a=data[(y*info.width+x)*4+3];if(a===0)clear++;if(a>=250)solid++;
  if(a>0){left=Math.min(left,x);top=Math.min(top,y);right=Math.max(right,x);bottom=Math.max(bottom,y);}
 }
 if(clear<info.width*info.height*.1||solid<info.width*info.height*.005||right<left)throw sourceError();
 return {width:info.width,height:info.height,bounds:{left,top,width:right-left+1,height:bottom-top+1}};
}
function sourceError(){return Object.assign(Error('Moving videos need an original product cutout with a transparent background. Add a PNG or WebP product photo that preserves the complete charm, its holes and necklace, then save the design and generate again. An opaque photo will not be substituted with a still ad.'),{code:'PRODUCT_CUTOUT_REQUIRED'});}
// Constant scale and integer translations only. Every source pixel, including
// holes and fine chain links, receives the same transform. No separate charm rig.
function motionPath(frame){return {x:Math.round(16*Math.sin(2*Math.PI*frame/(FPS*8))),y:Math.round(3*Math.sin(2*Math.PI*frame/(FPS*8)))};}
function sceneMotion(raw,width,height,geo){
 const size=width*height,samples=raw.length/size;
 if(samples!==20)throw Object.assign(Error('The scenery must contain a complete ten-second video, not a still image.'),{code:'SCENE_MOTION_REQUIRED'});
 let activeSamples=0;
 for(let n=1;n<samples;n++){
  let changed=0,count=0;
  for(let y=0;y<height;y++)for(let x=0;x<width;x++){
   // Exclude the final jewelry area and the copy zone; words never count as motion.
   const inside=r=>x/width>=r.x/geo.width&&x/width<=(r.x+r.w)/geo.width&&y/height>=r.y/geo.height&&y/height<=(r.y+r.h)/geo.height;
   if(inside(geo.photo)||inside(geo.copy))continue;
   count++;if(Math.abs(raw[n*size+y*width+x]-raw[(n-1)*size+y*width+x])>=8)changed++;
  }
  if(count&&changed/count>=.015)activeSamples++;
 }
 if(activeSamples<6)throw Object.assign(Error('The scene has too little visible movement around the jewelry. Use Redo this video to remake its scenery; moving captions alone are not a video.'),{code:'SCENE_MOTION_REQUIRED'});
 return {samples,activeSamples};
}
async function render(bytes,orientation,plan,{ffmpeg,beats,formats}){
 if(plan.motionMode!==MODE)throw Error('Exact product rendering requires the protected moving-layer mode.');
 const sourceInfo=await inspectSource(bytes);
 if(!Buffer.isBuffer(plan.backgroundVideo)||!plan.backgroundVideo.length)throw Error('A moving scenery video is required. No still-photo fallback is permitted.');
 const format=formats.find(f=>f.key===orientation);if(!format)throw Error('Unknown protected video format.');
 const device=orientation==='landscape'?'desktop':'mobile',key=device+'_'+orientation;if((plan.skipKeys||[]).includes(key))return [];
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'brites-moving-'));
 try{
  const geo={...geometry(format),width:format.width,height:format.height};
  // Trimming removes transparent margins only. One uniform resize retains all
  // nonzero alpha pixels; movement has its own safety margin inside the hero area.
  const product=await sharp(bytes).rotate().extract(sourceInfo.bounds).resize({width:geo.photo.w-48,height:geo.photo.h-24,fit:'inside'}).png().toBuffer({resolveWithObject:true});
  const rect={x:Math.floor((geo.photo.x+(geo.photo.w-product.info.width)/2)/2)*2,y:Math.floor((geo.photo.y+(geo.photo.h-product.info.height)/2)/2)*2,w:product.info.width,h:product.info.height};
  const source=path.join(dir,'product.png'),scene=path.join(dir,'scene.mp4'),clean=path.join(dir,'protected.nut'),file=path.join(dir,'video.mp4'),stats=path.join(dir,'audit.txt');
  await fs.writeFile(source,product.data);await fs.writeFile(scene,plan.backgroundVideo);
  const scenery=`scale=${format.width}:${format.height}:force_original_aspect_ratio=increase,crop=${format.width}:${format.height},setsar=1,fps=${FPS}`;
  const probe=path.join(dir,'scene.raw');
  await ffmpeg(['-y','-i',scene,'-vf',scenery+',fps=2,scale=160:160,format=gray','-frames:v','20','-f','rawvideo',probe]);
  const visible={...geo,photo:{x:rect.x-18,y:rect.y-6,w:rect.w+36,h:rect.h+12},copy:orientation==='landscape'?{x:0,y:0,w:geo.photo.x,h:format.height}:{x:0,y:0,w:format.width,h:geo.photo.y}};
  sceneMotion(await fs.readFile(probe),160,160,visible);
  // The moving reference is lossless, and is compared to EVERY final encoded
  // frame. Background and captions can never modify the protected product layer.
  const x=`${rect.x}+round(16*sin(2*PI*t/8))`,y=`${rect.y}+round(3*sin(2*PI*t/8))`;
  await ffmpeg(['-y','-i',scene,'-loop','1','-framerate',String(FPS),'-i',source,'-filter_complex_threads','1','-filter_complex',`[0:v]${scenery}[scene];[scene][1:v]overlay=x='${x}':y='${y}':format=auto,format=yuv420p[out]`,'-map','[out]','-frames:v',String(FRAMES),'-an','-c:v','ffv1','-threads','2',clean]);
  const assets=require('../../brites-brand-assets'),logo=assets.get('brites_brand_wide');
  const brand=await sharp(Buffer.from(assets.dataUrl(logo.id).split(',')[1],'base64')).extract({left:logo.crop.x,top:logo.crop.y,width:logo.crop.width,height:logo.crop.height}).resize({width:148}).png().toBuffer();
  // A translucent wash keeps messaging legible while the full-bleed scene lives
  // behind it. Its end is strictly before the product's swept bounding rectangle.
  const wash=Buffer.from(`<svg xmlns="http://www.w3.org/2000/svg" width="${format.width}" height="${format.height}"><defs><linearGradient id="fade" x2="${orientation==='landscape'?'1':'0'}" y2="${orientation==='landscape'?'0':'1'}"><stop stop-color="#fffaf3" stop-opacity=".96"/><stop offset=".72" stop-color="#fffaf3" stop-opacity=".92"/><stop offset="1" stop-color="#fffaf3" stop-opacity="0"/></linearGradient></defs><rect width="${orientation==='landscape'?geo.photo.x:format.width}" height="${orientation==='landscape'?format.height:geo.photo.y}" fill="url(#fade)"/></svg>`);
  const branding=await sharp(wash).composite([{input:brand,left:48,top:28}]).png().toBuffer(),brandFile=path.join(dir,'brand.png');await fs.writeFile(brandFile,branding);
  const args=['-y','-i',clean,'-loop','1','-framerate',String(FPS),'-i',brandFile],filters=['[0:v][1:v]overlay=0:0[branded]'];
  for(let i=0;i<beats.length;i++){
   const layer=path.join(dir,'caption'+i+'.png');await fs.writeFile(layer,await caption(beats[i],format,geo));args.push('-loop','1','-framerate',String(FPS),'-i',layer);
   const b=beats[i];filters.push(`[${i+2}:v]format=rgba,fade=t=in:st=${b.start}:d=0.35:alpha=1${b.end<SECONDS?',fade=t=out:st='+(b.end-.25)+':d=0.25:alpha=1':''}[c${i}];${i?'[v'+(i-1)+']':'[branded]'}[c${i}]overlay=0:0:enable='gte(t,${b.start})*lt(t,${b.end})'[v${i}]`);
  }
  args.push('-filter_complex_threads','1','-filter_complex',filters.join(';'),'-map','[v'+(beats.length-1)+']','-frames:v',String(FRAMES),'-an','-c:v','libx264','-threads','2','-preset','veryfast','-crf','10','-pix_fmt','yuv420p','-movflags','+faststart',file);await ffmpeg(args);
  const crop=`crop=${geo.photo.w}:${geo.photo.h}:${geo.photo.x}:${geo.photo.y}`;
  await ffmpeg(['-y','-i',file,'-i',clean,'-filter_complex_threads','1','-filter_complex',`[0:v]${crop},settb=1/24,setpts=N[a];[1:v]${crop},settb=1/24,setpts=N[b];[a][b]ssim=stats_file=${stats}:shortest=1`,'-frames:v',String(FRAMES),'-an','-f','null','-']);
  const audit=auditStats(await fs.readFile(stats,'utf8')),video=await fs.readFile(file),frames=[];
  // Measure the exported film, outside both the moving jewelry and the complete
  // messaging wash. Hidden scenery, moving type and the logo cannot qualify.
  await ffmpeg(['-y','-i',file,'-vf','fps=2,scale=160:160,format=gray','-frames:v','20','-f','rawvideo',probe]);
  const movement=sceneMotion(await fs.readFile(probe),160,160,visible);
  if(video.length>100000000)throw Error('The protected video exceeds the upload size limit.');
  for(const second of [.3,1.5,3.5,5,7.5,9.5]){const frame=path.join(dir,'frame.jpg');await ffmpeg(['-y','-ss',String(second),'-i',file,'-frames:v','1','-vf','scale=640:640:force_original_aspect_ratio=decrease',frame]);frames.push(await fs.readFile(frame));}
  const points=Array.from({length:FRAMES},(_,i)=>motionPath(i));
  return [{key,device,format:orientation,width:format.width,height:format.height,seconds:SECONDS,bytes:video,frames,integrity:{policy:POLICY,sourceHash:hash(bytes),sceneHash:hash(plan.backgroundVideo),videoHash:hash(video),...audit,fullSource:true,overlaysOutsideSource:true,sourceRect:rect,transform:'rigid-translation',productTravelPx:Math.max(...points.map(p=>p.x))-Math.min(...points.map(p=>p.x)),motionMeasuredOn:'final-video',sceneMotion:movement},composition:{mode:'protected-moving-product',notes:[]}}];
 }finally{await fs.rm(dir,{recursive:true,force:true});}
}
module.exports={POLICY,MODE,FRAMES,MIN_SSIM,KEYS,hash,sourceAllowed,complete,assertVideo,enforceReview,geometry,auditStats,inspectSource,sourceError,motionPath,sceneMotion,render};
