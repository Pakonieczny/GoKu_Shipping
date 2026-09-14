const assert=require('node:assert/strict'),fs=require('node:fs/promises'),path=require('node:path'),os=require('node:os'),sharp=require('sharp');
const {renderVariants,captionLayers,captionCopy,motionPrompt,qualityPass}=require('../../netlify/functions/googleAdsAdMotion');
(async()=>{
 const plan={pipelineVersion:2,renderVersion:3,motionMode:'generated',copy:{headline:'A little sweetness',shortHeadline:'Peach Charm',description:'A thoughtful gift',cta:'Shop Peach Charm'},style:{headlineFont:'Georgia',background:'#fff7ee',ink:'#30291f'},nativeCopy:{headlines:['Peach Charm']}};
 assert.equal(captionCopy(plan)[2].support,plan.copy.cta);
 const still=await sharp(Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="720" height="1280"><rect width="720" height="1280" fill="#dfb585"/><circle cx="360" cy="400" r="95" fill="#bd8627"/><circle cx="360" cy="292" r="26" stroke="#aa7222" fill="none" stroke-width="9"/></svg>')).jpeg().toBuffer();
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'motion-proof-')),input=path.join(dir,'source.jpg'),output=path.join(dir,'source.mp4');await fs.writeFile(input,still);await require('node:util').promisify(require('node:child_process').execFile)(require('@ffmpeg-installer/ffmpeg').path,['-y','-loop','1','-i',input,'-t','10','-r','24','-pix_fmt','yuv420p',output]);const source=await fs.readFile(output);await fs.rm(dir,{recursive:true,force:true});
 for(const format of [{key:'portrait',width:720,height:1280},{key:'square',width:720,height:720},{key:'landscape',width:1280,height:720}]){for(const layer of await captionLayers(plan,format)){const {data,info}=await sharp(layer.bytes).raw().toBuffer({resolveWithObject:true});let alpha=0;for(let y=0;y<Math.floor(format.height*.70/2)*2;y++)for(let x=0;x<info.width;x++)alpha+=data[(y*info.width+x)*4+3];assert.equal(alpha,0,'captions and fade never touch the product area');}}
 let count=0;for(const orientation of ['portrait','landscape']){
  const rows=await renderVariants(source,orientation,plan);assert.equal(rows.length,orientation==='portrait'?2:1);
  for(const row of rows){assert(row.bytes.length>1000);assert.equal(row.frames.length,6);const same=Buffer.compare(row.frames[0],row.frames[4])===0;assert(!same,'opening and CTA frames have different captions');count++;
   if(process.env.BRITES_MOTION_PROOFS){await fs.mkdir(process.env.BRITES_MOTION_PROOFS,{recursive:true});await fs.writeFile(path.join(process.env.BRITES_MOTION_PROOFS,row.key+'.mp4'),row.bytes);for(let i=0;i<row.frames.length;i++)await fs.writeFile(path.join(process.env.BRITES_MOTION_PROOFS,row.key+'_'+i+'.jpg'),row.frames[i]);}
  }
 }
 assert.equal(count,3);assert(qualityPass({rubric:'complete-ad-v2',pass:true,productFaithful:true,mobileReadable:true,score:92}));assert(!qualityPass({pass:true,productFaithful:true,mobileReadable:true,score:92}),'old scores are not silently upgraded');
 assert(motionPrompt({title:'Peach Charm',plan,creativeDirection:{setting:'Orchard light',lighting:'Moving softbox',opening:'Metal glint',middle:'Detail',ending:'Hero',portrait:'Safe crop',landscape:'Product right',identity:'Exact'}},'portrait','').includes('Orchard light'));
 console.log('PASS actual rendering of all three captioned video ratios, changing messages, saved proofs and old/new score gates');
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
