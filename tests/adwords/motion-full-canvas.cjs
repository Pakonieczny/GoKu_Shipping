const assert=require('node:assert/strict'),fs=require('fs/promises'),os=require('os'),path=require('path'),sharp=require('sharp'),{promisify}=require('util'),exec=promisify(require('child_process').execFile);
const {renderVariants,captionLayers}=require('../../netlify/functions/googleAdsAdMotion'),{geometry,validateBounds}=require('../../netlify/functions/googleAdsMotionComposition');
(async()=>{
 const subjects=validateBounds({portrait:{bounds:[.34,.49,.32,.22],complete:true,confidence:.99,note:'fixture'},landscape:{bounds:[.61,.28,.25,.43],complete:true,confidence:.99,note:'fixture'}});
 assert.throws(()=>validateBounds({portrait:{bounds:[.2,.2,.3,.4],complete:false,confidence:1}}),/complete jewelry/);
 assert.throws(()=>geometry({key:'square',width:720,height:720},{x:.1,y:.1,w:.8,h:.8}),/whole jewelry/);
 const plan={pipelineVersion:2,renderVersion:5,motionMode:'generated',copy:{headline:'For your favorite foodie',shortHeadline:'Peach Charm',description:'Gift-ready packaging',cta:'Shop now'},style:{headlineFont:'Georgia',background:'#fff7ee',ink:'#30291f',accent:'#a67c35'}};
 const kinetic=await captionLayers({...plan,composition:{x:.34,y:.43,w:.32,h:.4}},{key:'square',width:720,height:720});assert(kinetic.length>3,'tight square layout uses sequential large messages');assert.equal(kinetic[kinetic.length-1].title,'Shop now');assert.equal(kinetic[kinetic.length-1].end,10);assert(kinetic.every(l=>l.fontSize>=56&&l.end-l.start>=1.5));
 const tmp=await fs.mkdtemp(path.join(os.tmpdir(),'full-canvas-'));
 try{for(const orientation of ['portrait','landscape']){
  const W=orientation==='portrait'?720:1280,H=orientation==='portrait'?1280:720,subject=subjects[orientation],cx=orientation==='portrait'?360:940,cy=orientation==='portrait'?790:390;
  const svg=`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}"><defs><linearGradient id="scene"><stop stop-color="#d5bc9a"/><stop offset="1" stop-color="#f1e5d1"/></linearGradient></defs><rect width="${W}" height="${H}" fill="url(#scene)"/><circle cx="${cx}" cy="${cy}" r="100" fill="#c99b42"/><circle cx="${cx}" cy="${cy-117}" r="22" fill="none" stroke="#b18b42" stroke-width="8"/></svg>`;
  const input=path.join(tmp,orientation+'.jpg'),source=path.join(tmp,orientation+'.mp4');await fs.writeFile(input,await sharp(Buffer.from(svg)).jpeg().toBuffer());await exec(require('@ffmpeg-installer/ffmpeg').path,['-y','-loop','1','-i',input,'-t','10','-r','24','-pix_fmt','yuv420p',source]);
  for(const f of (orientation==='portrait'?[{key:'portrait',width:720,height:1280}]:[{key:'square',width:720,height:720},{key:'landscape',width:1280,height:720}])){
   const g=geometry(f,subject,orientation);for(const layer of await captionLayers({...plan,composition:subject,sourceOrientation:orientation},f)){
    assert(layer.fontSize>=56,'readable minimum typography');const {data,info}=await sharp(layer.bytes).raw().toBuffer({resolveWithObject:true});
    for(const offset of [0,12])for(let y=Math.ceil(g.product.y*f.height);y<Math.floor((g.product.y+g.product.h)*f.height);y++)for(let x=Math.ceil(g.product.x*f.width);x<Math.floor((g.product.x+g.product.w)*f.width);x++)assert.equal(data[((y-offset)*info.width+x)*4+3],0,'text, wash and transition never overlap protected jewelry');
   }
  }
  const rows=await renderVariants(await fs.readFile(source),orientation,{...plan,composition:subject,squareMaster:'landscape'});assert.equal(rows.length,orientation==='portrait'?1:2);for(const row of rows){assert(row.bytes.length>5000);if(process.env.BRITES_MOTION_PROOFS){await fs.mkdir(process.env.BRITES_MOTION_PROOFS,{recursive:true});await fs.writeFile(path.join(process.env.BRITES_MOTION_PROOFS,row.key+'.mp4'),row.bytes);for(let i=0;i<row.frames.length;i++)await fs.writeFile(path.join(process.env.BRITES_MOTION_PROOFS,row.key+'_'+i+'.jpg'),row.frames[i]);}}
 }}finally{await fs.rm(tmp,{recursive:true,force:true});}
 console.log('PASS full-canvas films, measured crops, large type, transitions and protected jewelry in all three ratios');
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
