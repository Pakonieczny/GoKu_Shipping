const assert=require('node:assert/strict'),sharp=require('sharp');
const integrity=require('../../netlify/functions/googleAdsMotionIntegrity');
const {createMotionService,renderVariants,qualityPass,motionPrompt,motionRequest}=require('../../netlify/functions/googleAdsAdMotion');
const {selection,reviewHash}=require('../../netlify/functions/googleAdsMotionPublication');
const clone=x=>JSON.parse(JSON.stringify(x));let checks=0;
const check=(v,m)=>{assert(v,m);checks++;};
function memory(){
 const docs=new Map(),doc=p=>({id:p.split('/').pop(),path:p,get:async()=>({exists:docs.has(p),data:()=>clone(docs.get(p)||null)}),set:async v=>docs.set(p,clone(v)),update:async v=>docs.set(p,{...docs.get(p),...clone(v)}),collection:n=>collection(p+'/'+n)}),collection=p=>({doc:n=>doc(p+'/'+n),get:async()=>({docs:[...docs].filter(([k])=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).map(([k,v])=>({id:k.split('/').pop(),data:()=>clone(v)}))})});
 return {docs,db:{collection,runTransaction:fn=>fn({get:r=>r.get(),set:(r,v)=>r.set(v),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})}};
}
const copy={headline:'A little everyday wonder',shortHeadline:'Gecko Necklace',description:'Find your favourite.',cta:'Shop now'};
const good=()=>({pass:true,productFaithful:true,exactProductIdentity:true,mobileReadable:true,footageLettering:false,multipleProducts:false,score:98,issues:[],formatIdentity:Object.fromEntries(integrity.KEYS.map(k=>[k,{sameOutline:true,noAddedDetail:true,noMissingDetail:true,sameFeatures:true,evidence:'Matches the original'}]))});
(async()=>{
 // An asymmetric silhouette with a tail opening, fine fingers and a small hole.
 // Four coloured corner witnesses prove the full source survives every format.
 const photo=await sharp(Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="800" height="600"><path fill="#b89745" fill-rule="evenodd" d="M390 65Q450 50 450 105L430 125 460 170 490 145 500 157 477 183 500 193 495 209 457 205Q420 250 435 292L480 315 472 337 446 325 447 359 428 359 423 326Q340 310 315 360Q290 450 380 462Q460 470 465 408Q465 365 410 367L400 390Q450 380 438 422Q423 448 373 435Q325 424 345 380Q380 345 407 349L385 285Q333 250 350 185L315 168 295 178 288 164 307 151 288 136 299 124 326 143 346 144 375 121Q350 80 390 65ZM399 81A9 9 0 1 0 399 99A9 9 0 1 0 399 81Z"/><rect width="24" height="24" fill="red"/><rect x="776" width="24" height="24" fill="lime"/><rect y="576" width="24" height="24" fill="blue"/><rect x="776" y="576" width="24" height="24" fill="yellow"/></svg>')).png().toBuffer();
 const fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path'),exec=require('node:util').promisify(require('node:child_process').execFile),bin=process.env.BRITES_FFMPEG_PATH||require('@ffmpeg-installer/ffmpeg').path;
 const temp=await fs.mkdtemp(path.join(os.tmpdir(),'moving-fixture-'));
 await exec(bin,['-y','-loglevel','error','-f','lavfi','-i','testsrc2=size=1280x720:rate=24:duration=10','-c:v','libx264','-threads','2','-crf','16',path.join(temp,'scene.mp4')]);
 const scene=await fs.readFile(path.join(temp,'scene.mp4'));
 await fs.rm(temp,{recursive:true,force:true});
 const rendered={};
 for(const orientation of ['portrait','square','landscape']){
  const variants=await renderVariants(photo,orientation,{motionMode:integrity.MODE,pipelineVersion:5,copy,backgroundVideo:scene});
  check(variants.length===1,'one independent format');const v=variants[0];rendered[orientation]=v;if(process.env.BRITES_MOTION_QA_DIR){await fs.mkdir(process.env.BRITES_MOTION_QA_DIR,{recursive:true});await fs.writeFile(path.join(process.env.BRITES_MOTION_QA_DIR,orientation+'.mp4'),v.bytes);await fs.writeFile(path.join(process.env.BRITES_MOTION_QA_DIR,orientation+'.jpg'),v.frames[2]);}console.log(orientation+' every-frame audit '+v.integrity.minSsim);
  check(v.integrity.sceneMotion.activeSamples>=6&&v.integrity.productTravelPx>=24,'visible scenery and jewelry motion, excluding words');
  check(v.integrity.framesChecked===240&&v.integrity.minSsim>=.99,'every exported frame compared with source');
  check(v.integrity.sourceHash===integrity.hash(photo)&&v.integrity.videoHash===integrity.hash(v.bytes),'evidence binds source and output bytes');
  for(const frame of v.frames){
   const {data,info}=await sharp(frame).removeAlpha().raw().toBuffer({resolveWithObject:true}),corners=[0,0,0,0];
   for(let i=0;i<data.length;i+=info.channels){const r=data[i],g=data[i+1],b=data[i+2];if(r>160&&g<90&&b<90)corners[0]++;if(g>160&&r<90&&b<90)corners[1]++;if(b>160&&r<90&&g<90)corners[2]++;if(r>160&&g>160&&b<90)corners[3]++;}
   check(corners.every(n=>n>15),v.key+' retains the whole photograph, including all corners');
  }
 }
 const stats=Array.from({length:240},(_,i)=>'n:'+(i+1)+' All:0.999').join('\n');
 check(integrity.auditStats(stats).framesChecked===240,'full audit accepted');
 assert.throws(()=>integrity.auditStats(stats.replace('n:127 All:0.999','n:127 All:0.2')),/preservation/);checks++;
 assert.throws(()=>integrity.auditStats(stats.split('\n').slice(1).join('\n')),/preservation/);checks++;
 // Corrupt only frame 127, between the visual review's sampled timestamps.
 // The export audit must catch it before the renderer can return a video.

 let corrupted=false;
 const ffmpeg=async args=>{
  if(args.join(' ').includes('ssim=stats_file=')&&!corrupted){
   corrupted=true;const file=args[args.indexOf('-i')+1],bad=file+'.bad.mp4';
   await exec(bin,['-y','-loglevel','error','-i',file,'-vf',"drawbox=x=260:y=320:w=180:h=180:color=red:t=fill:enable='eq(n,127)'",'-c:v','libx264','-threads','2','-crf','10',bad]);await fs.rename(bad,file);
  }
  return exec(bin,['-hide_banner','-loglevel','error','-nostdin',...args],{timeout:180000,maxBuffer:1000000});
 };
 await assert.rejects(()=>integrity.render(photo,'square',{motionMode:integrity.MODE,backgroundVideo:scene},{ffmpeg,beats:require('../../netlify/functions/googleAdsAdMotion').captionCopy({copy,renderVersion:10}),formats:require('../../brites-ad-format-policy').video.formats}),/preservation/);checks++;
 check(corrupted,'negative test altered an actual encoded frame');
 assert.throws(()=>integrity.assertVideo(rendered.square,Buffer.from('substituted video')),/differ/);checks++;
 const original={id:'original',source:{kind:'product',productId:'p'},productId:'p',asset:{path:'original',hash:integrity.hash(photo)},width:800,height:600};
 check(integrity.sourceAllowed(original,'p'),'verified listing photograph allowed');
 for(const bad of [{...original,source:{kind:'library',imageId:'generated'}},{...original,artwork:true},{...original,source:{kind:'product',productId:'other'}},{asset:{path:'unknown'}},{...original,role:'inspiration'}])check(!integrity.sourceAllowed(bad,'p'),'unverified, generated, foreign or inspiration source blocked');
 const still=Buffer.alloc(160*160*20,128),geo={...integrity.geometry({key:'square',width:720,height:720}),width:720,height:720};
 assert.throws(()=>integrity.sceneMotion(still,160,160,geo),/too little visible movement/);checks++;
 // Motion exclusively in the caption region can never pass the scene gate.
 for(let n=0;n<20;n++)for(let y=30;y<50;y++)for(let x=20;x<100;x++)still[n*160*160+y*160+x]=n%2?255:0;
 assert.throws(()=>integrity.sceneMotion(still,160,160,geo),/too little visible movement/);checks++;
 const movingPrompt=motionPrompt({motionMode:integrity.MODE,creativeDirection:{setting:'Gold jewelry on a stone. Leaves sway beside an empty stage.'}},'square');
 check(!/gold|jewelry/.test(movingPrompt)&&/leaves|leaf/.test(movingPrompt),'only scenery reaches provider text');
 check(motionRequest({motionMode:integrity.MODE,pipelineVersion:5,title:'Gecko Necklace'}).input[0].content.includes('BACKGROUND PLATE'),'planner designs empty moving background');
 console.log('PASS '+checks+' legacy moving-charm checks; 720 exported frames audited; real scene motion required');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e.stack);process.exitCode=1});
