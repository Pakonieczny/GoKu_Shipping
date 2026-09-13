const assert=require('node:assert/strict'),sharp=require('sharp');
const {renderVariants}=require('../../netlify/functions/googleAdsAdMotion');
(async()=>{
 const svg=Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="800" height="600"><rect width="800" height="600" fill="white"/><rect width="24" height="24" fill="red"/><rect x="776" width="24" height="24" fill="lime"/><rect y="576" width="24" height="24" fill="blue"/><rect x="776" y="576" width="24" height="24" fill="yellow"/></svg>');
 const source=await sharp(svg).jpeg({quality:100}).toBuffer();let checks=0;
 for(const orientation of ['portrait','landscape']){
  const variants=await renderVariants(source,orientation,{motionMode:'photograph'});assert.equal(variants.length,3);checks++;
  for(const v of variants){
   assert(v.bytes.length>1000&&v.seconds===10&&v.frames.length===3);checks++;
   for(const frame of v.frames){
    const {data,info}=await sharp(frame).removeAlpha().raw().toBuffer({resolveWithObject:true}),counts=[0,0,0,0];
    for(let i=0;i<data.length;i+=info.channels){const r=data[i],g=data[i+1],b=data[i+2];if(r>160&&g<90&&b<90)counts[0]++;if(g>160&&r<90&&b<90)counts[1]++;if(b>160&&r<90&&g<90)counts[2]++;if(r>160&&g>160&&b<90)counts[3]++;}
    assert(counts.every(n=>n>=12),v.key+' preserves all four photograph corners throughout motion: '+counts);checks++;
   }
  }
 }
 console.log('PASS '+checks+' actual photograph video renders and edge preservation checks');
})().catch(e=>{console.error(e.stack);process.exitCode=1});
