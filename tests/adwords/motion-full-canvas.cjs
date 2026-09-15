const assert=require('node:assert/strict'),fs=require('fs/promises'),os=require('os'),path=require('path'),sharp=require('sharp'),{promisify}=require('util'),exec=promisify(require('child_process').execFile);
const {renderVariants,captionLayers}=require('../../netlify/functions/googleAdsAdMotion'),{geometry,validateBounds}=require('../../netlify/functions/googleAdsMotionComposition');
(async()=>{
 const subjects=validateBounds({portrait:{bounds:[.34,.49,.32,.22],complete:true,confidence:.99,note:'fixture'},landscape:{bounds:[.61,.28,.25,.43],complete:true,confidence:.99,note:'fixture'}});
 assert.throws(()=>validateBounds({portrait:{bounds:[.2,.2,.3,.4],complete:false,confidence:1}}),/complete jewelry/);
 assert.throws(()=>geometry({key:'square',width:720,height:720},{x:.1,y:.1,w:.8,h:.8}),/whole jewelry/);
 const plan={pipelineVersion:2,renderVersion:10,motionMode:'generated',copy:{headline:'For your favorite foodie',shortHeadline:'Peach Charm',description:'Gift-ready packaging',cta:'Shop now'},style:{headlineFont:'Georgia',background:'#fff7ee',ink:'#30291f',accent:'#a67c35'}};
 const kinetic=await captionLayers({...plan,renderVersion:5,composition:{x:.34,y:.43,w:.32,h:.4}},{key:'square',width:720,height:720});assert(kinetic.length>3,'tight square layout uses sequential large messages');assert.equal(kinetic[kinetic.length-1].title,'Shop now');assert.equal(kinetic[kinetic.length-1].end,10);assert(kinetic.every(l=>l.fontSize>=56&&l.end-l.start>=1.5));
 const tmp=await fs.mkdtemp(path.join(os.tmpdir(),'full-canvas-'));
 try{for(const orientation of ['portrait','landscape']){
  const W=orientation==='portrait'?720:1280,H=orientation==='portrait'?1280:720,subject=subjects[orientation],cx=orientation==='portrait'?360:940,cy=orientation==='portrait'?790:390;
  const svg=`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}"><defs><linearGradient id="scene"><stop stop-color="#d5bc9a"/><stop offset="1" stop-color="#f1e5d1"/></linearGradient></defs><rect width="${W}" height="${H}" fill="url(#scene)"/><circle cx="${cx}" cy="${cy}" r="100" fill="#c99b42"/><circle cx="${cx}" cy="${cy-117}" r="22" fill="none" stroke="#b18b42" stroke-width="8"/></svg>`;
  const input=path.join(tmp,orientation+'.jpg'),source=path.join(tmp,orientation+'.mp4');await fs.writeFile(input,await sharp(Buffer.from(svg)).jpeg().toBuffer());await exec(require('@ffmpeg-installer/ffmpeg').path,['-y','-loop','1','-i',input,'-t','10','-r','24','-pix_fmt','yuv420p',source]);
  for(const f of (orientation==='portrait'?[{key:'portrait',width:720,height:1280}]:[{key:'square',width:720,height:720},{key:'landscape',width:1280,height:720}])){
   const g=geometry(f,subject,orientation),layers=await captionLayers({...plan,composition:subject,sourceOrientation:orientation},f);assert.equal(layers.filter(l=>l.persistent).length,1);assert.deepEqual(layers.filter(l=>!l.persistent).map(l=>l.title),['For your favorite foodie','Peach Charm','Shop now']);for(const layer of layers){
    if(layer.persistent){assert.equal(layer.logoBox.w,148);assert.equal(layer.logoBox.x,f.width*.055);assert.equal(layer.logoBox.y,f.height*.05);assert.equal(layer.start,0);assert.equal(layer.end,10);}else assert(layer.fontSize>=56,'readable minimum typography');const {data,info}=await sharp(layer.bytes).raw().toBuffer({resolveWithObject:true});
    for(const offset of (layer.persistent?[0]:[0,12]))for(let y=Math.ceil(g.product.y*f.height);y<Math.floor((g.product.y+g.product.h)*f.height);y++)for(let x=Math.ceil(g.product.x*f.width);x<Math.floor((g.product.x+g.product.w)*f.width);x++)assert.equal(data[((y-offset)*info.width+x)*4+3],0,'text, wash and transition never overlap protected jewelry');
   }
  }
  const rows=await renderVariants(await fs.readFile(source),orientation,{...plan,composition:subject,squareMaster:'landscape'});assert.equal(rows.length,orientation==='portrait'?1:2);for(const row of rows){assert(row.bytes.length>5000);
   const rendered=path.join(tmp,row.key+'-final.mp4');await fs.writeFile(rendered,row.bytes);const samples=[];
   for(const t of [2.75,3,3.25,6.75,7,7.25]){const still=path.join(tmp,row.key+'-'+t+'.png');await exec(require('@ffmpeg-installer/ffmpeg').path,['-y','-ss',String(t),'-i',rendered,'-frames:v','1',still]);samples.push(await sharp(still).extract({left:Math.round(row.width*.065),top:Math.round(row.height*.055),width:176,height:98}).raw().toBuffer());}
   for(const sample of samples.slice(1)){let delta=0;for(let i=0;i<sample.length;i++)delta+=Math.abs(sample[i]-samples[0][i]);assert(delta/sample.length<2,'stationary logo and wash must remain visually stable across every text transition');}
   if(process.env.BRITES_MOTION_PROOFS){await fs.mkdir(process.env.BRITES_MOTION_PROOFS,{recursive:true});await fs.writeFile(path.join(process.env.BRITES_MOTION_PROOFS,row.key+'.mp4'),row.bytes);for(let i=0;i<row.frames.length;i++)await fs.writeFile(path.join(process.env.BRITES_MOTION_PROOFS,row.key+'_'+i+'.jpg'),row.frames[i]);}}
 }}finally{await fs.rm(tmp,{recursive:true,force:true});}
 // Messaging sits beside the wordmark, clear of it, so the film keeps its height.
 {
  const f={key:'square',width:720,height:720},layers=await captionLayers({...plan,composition:{x:.28,y:.52,w:.44,h:.36},sourceOrientation:'landscape'},f);
  const logo=layers.find(l=>l.persistent).logoBox,beats=layers.filter(l=>!l.persistent);
  const beside=beats.filter(l=>l.zone.name==='header');
  assert(beside.length>=2,'messaging that fits sits beside the wordmark rather than under it');
  assert(beside.every(l=>l.zone.x>=logo.x+logo.w+36),'messaging beside the wordmark keeps clear separation from it');
  assert(beside.every(l=>l.zone.y<logo.y+logo.h),'messaging beside the wordmark aligns with it, not below it');
  assert(beats.every(l=>l.zone.name==='header'||l.zone.y>=logo.y+logo.h),'copy too long to sit beside the wordmark clears it instead of colliding');
 }
 // A band layout must read as light falling away, never as a printed line across the film.
 {
  const big={x:.05,y:.05,w:.9,h:.9},f={key:'square',width:720,height:720};
  const layers=await captionLayers({...plan,composition:big,sourceOrientation:'landscape'},f);
  assert.equal(layers.mode,'band','an oversized subject falls back to the band layout');
  const seam=layers.geometry.seam;assert(seam&&seam.edge==='top','the band records which edge meets the film');
  const base=layers.find(l=>l.persistent);assert(base,'the band layout keeps its persistent brand layer');
  const {data,info}=await sharp(base.bytes).raw().toBuffer({resolveWithObject:true});
  const column=Math.round(info.width/2),alpha=y=>data[(y*info.width+column)*4+3];
  let fields=0,inside=false;for(let y=0;y<f.height;y++){const a=alpha(y);if(a>6&&!inside){fields++;inside=true;}if(a<=6)inside=false;}
  assert.equal(fields,1,'the film shows exactly one fading field, never a duplicate');
  assert(Math.abs(alpha(seam.at)-alpha(seam.at+1))<=8,'the seam never jumps opacity across a single pixel');
  let falling=true;for(let y=1;y<f.height;y++)if(alpha(y)>alpha(y-1)+1)falling=false;
  assert(falling,'the field only ever fades outward, so it never reads as a second edge');
  assert(alpha(0)>=230&&alpha(seam.at)>20&&alpha(seam.at)<210,'the join sits part-way down one continuous ramp rather than at a cut');
 }
 console.log('PASS full-canvas films, measured crops, large type, transitions, a blended band seam and protected jewelry in all three ratios');
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
