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
 // A band sits on exactly one edge; the film fills the rest of the canvas.
 for(const [f,src] of [[{key:'landscape',width:1280,height:720},'landscape'],[{key:'portrait',width:720,height:1280},'portrait'],[{key:'square',width:720,height:720},'landscape'],[{key:'square',width:720,height:720},'portrait']]){
  const g=geometry(f,{x:.05,y:.05,w:.9,h:.9},src,{mode:'band'}),h=g.hero;
  const strips=[h.y>0,h.x>0,h.x+h.w<f.width,h.y+h.h<f.height].filter(Boolean).length;
  assert(strips<=1,f.key+' from '+src+' must leave at most one band, never a border on several sides');
  if(strips===1)assert(g.seam&&(g.seam.edge==='top'?h.y>0:h.x>0),'the single band is the recorded seam edge');
  const p=g.product;
  assert(p.x>=-0.002&&p.y>=-0.002&&p.x+p.w<=1.002&&p.y+p.h<=1.002,'the band layout never crops the jewelry out of frame');
 }
 // A small subject is framed in close, so no format ever renders the piece as a
 // distant speck just because its source photo left a lot of room around it.
 for(const src of ['portrait','landscape'])for(const f of [{key:'square',width:720,height:720},{key:'portrait',width:720,height:1280},{key:'landscape',width:1280,height:720}])for(const mode of ['full','band']){
  const subject={x:.36,y:.40,w:.28,h:.20},g=geometry(f,subject,src,{mode});
  // Measured against the film itself, so a reserved brand band is not mistaken for distance.
  const film=g.hero?{w:g.hero.w/f.width,h:g.hero.h/f.height}:{w:1,h:1};
  const fill=Math.max(g.product.w/film.w,g.product.h/film.h);
  assert(fill>=.5,f.key+' from '+src+' in '+mode+' mode must frame a small piece close, not leave it distant: filled '+fill.toFixed(2));
  assert(g.product.x>=-.002&&g.product.y>=-.002&&g.product.x+g.product.w<=1.002&&g.product.y+g.product.h<=1.002,'framing close never cuts the jewelry');
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
  // The band itself is solid brand colour, so the field must still be solid where
  // the band ends and only fade past it; a ramp that has already thinned at the
  // seam leaves a visible step between the two.
  assert(alpha(0)>=250&&alpha(seam.at)>=250,'the field stays solid down to the band edge, so the two meet without a step');
  const past=Math.min(f.height-1,seam.at+Math.round(f.height*.14));
  assert(alpha(past)<=30,'the field has faded away by the far edge of its reach');
 }
 // Standard fade (renderVersion 11): one mostly transparent ramp and one size per format, in the film's own colour.
 {
  const lum_=h=>{const c=[1,3,5].map(i=>parseInt(h.slice(i,i+2),16)/255).map(v=>v<=.03928?v/12.92:Math.pow((v+.055)/1.055,2.4));return .2126*c[0]+.7152*c[1]+.0722*c[2];},contrast_=(a,b)=>(Math.max(lum_(a),lum_(b))+.05)/(Math.min(lum_(a),lum_(b))+.05);
  const {FADE_STOPS,FADE_SIZE,FADE_MIN_CONTRAST,fadeColour,ensureVisibleFade,primaryColour,inkFor,captions}=require('../../netlify/functions/googleAdsMotionComposition'),green='#3f8f55';
  assert(FADE_STOPS[0][0]===0&&FADE_STOPS[0][1]<=.85&&FADE_STOPS[0][1]>=.75,'the fade starts clearly perceptible (about .75-.85) at the outer edge');
  assert.equal(FADE_STOPS[FADE_STOPS.length-1][0],1);assert.equal(FADE_STOPS[FADE_STOPS.length-1][1],0);
  assert(FADE_STOPS.every(([o,a],i)=>i===0||(o>FADE_STOPS[i-1][0]&&a<FADE_STOPS[i-1][1])),'the ramp only ever eases outward');
  assert.equal(FADE_SIZE.side,.335);assert.deepEqual(FADE_SIZE.top,{square:.33,landscape:.33,portrait:.33},'every film shape gets a fade 33% of its height');
  {const mean=FADE_STOPS.slice(1).reduce((a,[o,v],i)=>a+(o-FADE_STOPS[i][0])*(v+FADE_STOPS[i][1])/2,0);assert(mean<.5,'the fade stays mostly transparent overall: '+mean);}
  const v11={...plan,renderVersion:11,fadeColor:green},seen=new Set();
  const formats=[{key:'portrait',width:720,height:1280},{key:'square',width:720,height:720},{key:'landscape',width:1280,height:720}];
  const fixtures=[{x:.3,y:.46,w:.4,h:.3},{x:.6,y:.25,w:.3,h:.5},{x:.05,y:.2,w:.3,h:.6},{x:.02,y:.3,w:.32,h:.5}];
  for(const f of formats)for(const composition of fixtures)for(const src of ['portrait','landscape']){
   const layers=await captionLayers({...v11,composition,sourceOrientation:src},f),base=layers.find(l=>l.persistent);
   assert.equal(layers.fadeColor,green,'the fade takes the film colour');
   const {data,info}=await sharp(base.bytes).raw().toBuffer({resolveWithObject:true}),px=(x,y)=>{const i=(Math.min(info.height-1,Math.max(0,Math.round(y)))*info.width+Math.min(info.width-1,Math.max(0,Math.round(x))))*4;return [data[i],data[i+1],data[i+2],data[i+3]];};
   for(const fade of base.fades){
    seen.add(fade.name);const outer=fade.name==='right'?f.width-1:0,dir=fade.name==='right'?-1:1,vertical=fade.name==='top',span=vertical?fade.h:fade.w;
    if(!layers.geometry.seam){
     if(vertical)assert(Math.abs(fade.h-FADE_SIZE.top[f.key]*f.height)<=.5,f.key+' top fade is the standard fraction of the height');
     else assert(Math.abs(fade.w-FADE_SIZE.side*f.width)<=.5,f.key+' '+fade.name+' fade is 33.5% of the width (full height)');
    }
    const at=t=>vertical?px(f.width/2,t):px(outer+dir*t,f.height/2);
    if(!layers.geometry.seam){
     assert(at(0)[3]<=.85*255+2&&at(0)[3]>=.75*255-2,f.key+' outer edge is clearly visible: '+at(0)[3]);
     assert.equal(at(Math.round(span)-1)[3]<=2,true,'the fade reaches zero at its inner edge');
     for(let t=1;t<span;t++)assert(at(t)[3]<=at(t-1)[3]+1,'the fade only falls away from the edge');
     for(const t of [0,span*.3,span*.6]){const [r,g,b]=at(t);assert(Math.abs(r-0x3f)<=6&&Math.abs(g-0x8f)<=6&&Math.abs(b-0x55)<=6,'the fade is drawn in the film colour');}
    }
    // The wordmark sits wholly inside the fade.
    const L=base.logoBox;assert(L.x>=fade.x-.5&&L.y>=fade.y-.5&&L.x+L.w<=fade.x+fade.w+.5&&L.y+L.h<=fade.y+fade.h+.5,f.key+' wordmark inside the '+fade.name+' fade');
   }
   for(const layer of layers.filter(l=>!l.persistent)){
    const r=layer.fade,b=layer.textBox;assert(b.x>=r.x&&b.y>=r.y&&b.x+b.w<=r.x+r.w&&b.y+b.h<=r.y+r.h,f.key+' text box inside its fade');
    const {data:t,info:ti}=await sharp(layer.bytes).raw().toBuffer({resolveWithObject:true});let x0=1e9,y0=1e9,x1=-1,y1=-1;
    for(let y=0;y<ti.height;y++)for(let x=0;x<ti.width;x++)if(t[(y*ti.width+x)*4+3]>0){x0=Math.min(x0,x);y0=Math.min(y0,y);x1=Math.max(x1,x);y1=Math.max(y1,y);}
    assert(x0>=r.x&&y0>=r.y&&x1<=r.x+r.w&&y1+12<=r.y+r.h,f.key+' rendered text (including its entrance offset) never crosses the fade edge');
   }
  }
  assert(['top','left','right'].every(n=>seen.has(n)),'every fade edge was exercised: '+[...seen]);
  // Older films re-render exactly as saved: renderVersion 10 ignores the standard fade and its colour.
  for(const f of formats){
   const composition=fixtures[0],a=await captionLayers({...plan,composition,sourceOrientation:'portrait'},f),b=await captionLayers({...plan,composition,sourceOrientation:'portrait',fadeColor:'#00ff00'},f);
   assert.equal(a.length,b.length);a.forEach((l,i)=>assert(Buffer.compare(l.bytes,b[i].bytes)===0,'renderVersion 10 output does not change with a fade colour'));
   const {data,info}=await sharp(a.find(l=>l.persistent).bytes).raw().toBuffer({resolveWithObject:true});let peak=0;for(let i=3;i<data.length;i+=4)peak=Math.max(peak,data[i]);assert(peak>=.9*255,'renderVersion 10 keeps its original heavy wash');
  }
  // The primary colour is sampled from the film, and ink stays readable on it.
  const swatch=async(rgb,w=320,h=180)=>sharp({create:{width:w,height:h,channels:3,background:rgb}}).jpeg().toBuffer();
  const sampled=await primaryColour(await swatch({r:0x3f,g:0x8f,b:0x55}));assert(/^#[0-9a-f]{6}$/.test(sampled));
  const [sr,sg,sb]=[1,3,5].map(i=>parseInt(sampled.slice(i,i+2),16));assert(sg>sr+40&&sg>sb+30,'a green frame gives a green fade: '+sampled);assert(Math.abs(sg-0x8f)<=6,'the sample matches the frame colour');
  const mixed=await sharp({create:{width:320,height:180,channels:3,background:{r:0x3f,g:0x8f,b:0x55}}}).composite([{input:await swatch({r:220,g:190,b:120},60,40),left:10,top:10}]).jpeg().toBuffer();
  const dom=await primaryColour(mixed);assert(parseInt(dom.slice(3,5),16)>parseInt(dom.slice(1,3),16)+40,'a small accent does not change the dominant colour');
  const left=await sharp({create:{width:320,height:180,channels:3,background:{r:255,g:0,b:0}}}).composite([{input:await swatch({r:0,g:0,b:255},160,180),left:160,top:0}]).jpeg().toBuffer(),region=await primaryColour(left,{region:{x:.5,y:0,w:.5,h:1}});
  assert(parseInt(region.slice(5,7),16)>parseInt(region.slice(1,3),16)+100,'a region samples only that part of the frame');
  assert.equal(inkFor('#ffffff','#30291f'),'#30291f');assert.equal(inkFor('#1f4d2c','#30291f'),'#fffaf0','dark fade colour switches to a light ink');
  // The fade colour is guaranteed visibly different from the frame under it, on the same hue.
  {const flat=await swatch({r:0x3f,g:0x8f,b:0x55}),fc=await fadeColour(flat,{mode:'dominant'});
   assert(contrast_(fc,'#3f8f55')>=FADE_MIN_CONTRAST-.05,'a fade colour equal to the frame is moved until it is visible: '+fc);
   const [r,g,b]=[1,3,5].map(i=>parseInt(fc.slice(i,i+2),16));assert(g>r+30&&g>b+20,'the adjusted fade stays the film green: '+fc);
   assert.equal(ensureVisibleFade('#ffffff',['#101010']),'#ffffff','an already visible colour is untouched');
   const lifted=ensureVisibleFade('#101010',['#101010']);assert(contrast_(lifted,'#101010')>=FADE_MIN_CONTRAST-.05,'a near-black frame lightens the fade instead: '+lifted);}
  // renderVariants samples the master once and gives every format cut from it the same fade colour.
  const shots=[],realCaptions=require('../../netlify/functions/googleAdsMotionComposition').captions,module_=require('../../netlify/functions/googleAdsMotionComposition');
  const tmp2=await fs.mkdtemp(path.join(os.tmpdir(),'fade-colour-'));
  try{
   const sourceJpeg=path.join(tmp2,'green.jpg'),sourceMp4=path.join(tmp2,'green.mp4');await fs.writeFile(sourceJpeg,await sharp(Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="720"><rect width="1280" height="720" fill="#3f8f55"/><circle cx="940" cy="390" r="100" fill="#c99b42"/></svg>')).jpeg().toBuffer());
   await exec(require('@ffmpeg-installer/ffmpeg').path,['-y','-loop','1','-i',sourceJpeg,'-t','10','-r','24','-pix_fmt','yuv420p',sourceMp4]);
   module_.captions=async(p,f,b)=>{shots.push({format:f.key,fadeColor:p.fadeColor});return realCaptions(p,f,b);};
   try{await renderVariants(await fs.readFile(sourceMp4),'landscape',{...v11,fadeColor:undefined,composition:subjects.landscape,squareMaster:'landscape'});}finally{module_.captions=realCaptions;}
   assert(shots.length>=2&&shots.every(s=>s.fadeColor===shots[0].fadeColor),'every format from one master uses one fade colour');
   const [r,g,b]=[1,3,5].map(i=>parseInt(shots[0].fadeColor.slice(i,i+2),16));assert(g>r+40&&g>b+30,'the fade colour is the film green, not the brand cream: '+shots[0].fadeColor);
  }finally{await fs.rm(tmp2,{recursive:true,force:true});}
 }
 // Close framing: a charm measured (from the master's own pixels) below its format's size band is enlarged by one fixed
 // crop about the charm, never past 2x, keeping the copy area and the wordmark inside the fade; nothing is bought.
 {
  const F=require('../../netlify/functions/googleAdsMotionFraming'),LAND={key:'landscape',width:1280,height:720},PORT={key:'portrait',width:720,height:1280},SQ={key:'square',width:720,height:720};
  const body=(x,y,w,h)=>({x,y,w,h,mw:w,mh:h,n:8,of:8}),inFrame=c=>c.x>=-1e-9&&c.y>=-1e-9&&c.x+c.w<=1+1e-9&&c.y+c.h<=1+1e-9;
  const holds=(c,b)=>b.x>=c.x&&b.y>=c.y&&b.x+b.w<=c.x+c.w&&b.y+b.h<=c.y+c.h;
  // The landscape Duck case: the charm hangs on a chain from the top edge and fills about a quarter of the frame height.
  const duck=(mh,b=body(.70,.41,.13,mh))=>({x:.66,y:0,w:.22,h:.72,body:b});
  {
   const plain=geometry(LAND,{x:.66,y:0,w:.22,h:.72},'landscape',{mode:'full'});assert.equal(plain.framing,null,'no pixel measurement, no enlargement: the existing framing stands');
   const g=geometry(LAND,duck(.27),'landscape',{mode:'full'}),b=duck(.27).body;
   assert.equal(g.framing.status,'zoomed');assert(inFrame(g.crop),'the window stays inside the frame');assert(Math.abs(g.crop.w-.5)<1e-9&&Math.abs(g.crop.h-.5)<1e-9,'a charm at 27% needs more than 2x, so it is enlarged by the 2x limit exactly');
   assert(holds(g.crop,b),'the whole charm stays in the window');assert(Math.abs(g.framing.zoom-2)<1e-9);assert(/enlarged 2\.0x/.test(g.framing.note));
   assert(b.mh/g.crop.h>=.53&&b.mh/g.crop.h<=.56,'charm share after the enlargement: '+b.mh/g.crop.h);
   const cx=(b.x+b.w/2-g.crop.x)/g.crop.w,cy=(b.y+b.h/2-g.crop.y)/g.crop.h;assert(cx>.6&&cx<.8&&Math.abs(cy-.5)<.06,'the charm sits right of centre and the left stays open for the copy: '+cx+','+cy);
   assert(g.product.x>=0&&g.product.y>=0&&g.product.x+g.product.w<=1+1e-9&&g.product.y+g.product.h<=1+1e-9,'the protected product box is the part still in view (the chain leaves at the top)');
   assert(g.zones[0].name==='left'&&!g.zones.crowded&&g.zones[0].w>=LAND.width*.3,'the copy keeps a left area beside the enlarged charm');
   const mid=geometry(LAND,duck(.40),'landscape',{mode:'full'});assert.equal(mid.framing.status,'zoomed');assert(Math.abs(.40/mid.crop.h-.66)<.005&&Math.abs(mid.framing.zoom-1.65)<.01,'a charm at 40% is enlarged 1.65x to the 66% target (a visible 55-60% of the height)');
   const small=geometry(LAND,duck(.22),'landscape',{mode:'full'});assert.equal(small.framing.status,'too-small');assert(Math.abs(small.crop.w-.5)<1e-9&&/still small and distant/.test(small.framing.note),'beyond the 2x limit the film is enlarged as far as allowed and flagged for the review');
   const big=geometry(LAND,duck(.62),'landscape',{mode:'full'});assert.equal(big.framing.status,'ok');assert.equal(big.framing.note,undefined);assert.deepEqual(big.crop,geometry(LAND,{x:.66,y:0,w:.22,h:.72},'landscape',{mode:'full'}).crop,'a charm already in the size band is left exactly as framed');
   const band=geometry(LAND,duck(.27),'landscape',{mode:'band'});assert.equal(band.framing.status,'zoomed');assert(band.product.x>=-.002&&band.product.y>=-.002&&band.product.x+band.product.w<=1.002&&band.product.y+band.product.h<=1.002,'the band layout is enlarged too and keeps its product box on the canvas');
  }
  // A whole jewelry that is not hanging out of the frame is never cut: the enlargement stops at its box.
  {const g=geometry(LAND,{x:.6,y:.15,w:.25,h:.7,body:body(.62,.4,.2,.25)},'landscape',{mode:'full'});assert(g.framing.zoom<1.3,'a whole visible jewelry limits the enlargement: '+g.framing.zoom);assert(holds(g.crop,{x:.6,y:.15,w:.25,h:.7}),'the complete product box stays in the window');}
  // A pendant on a wide necklace box is not the charm to enlarge.
  assert.equal(geometry(LAND,{x:.2,y:.25,w:.6,h:.4,body:body(.45,.4,.1,.15)},'landscape',{mode:'full'}).framing,null,'a measurement that is small against the product box is not trusted');
  {// Portrait sizes the charm by frame width, square by height.
   const p=geometry(PORT,{x:.3,y:.5,w:.4,h:.34,body:body(.32,.53,.36,.28)},'portrait',{mode:'full'});assert.equal(p.framing.status,'zoomed');assert(p.crop.w<.6&&inFrame(p.crop)&&.36/p.crop.w>=.66&&.36/p.crop.w<=.72,'portrait share of the width: '+.36/p.crop.w);assert(p.zones[0].name==='header'||p.zones[0].name==='top','portrait keeps its copy above the charm');
   const q=geometry(SQ,{x:.4,y:.3,w:.3,h:.42,body:body(.42,.36,.17,.3)},'square',{mode:'full'});assert.equal(q.framing.status,'zoomed');assert(inFrame(q.crop)&&q.crop.w<.5625&&Math.abs(q.crop.w*1280-q.crop.h*720)<1e-6,'the square window stays square and inside the wide frame');assert(.3/q.crop.h>=.55&&.3/q.crop.h<=.62);
  }
  // Pixels: rock, a gold duck on a chain from the top edge at about a quarter of the frame height.
  const rng=seed=>{let s=seed>>>0;return()=>{s=(s*1664525+1013904223)>>>0;return s/4294967296;};};
  const scene=async(H_,hgt)=>{
   const r=rng(7),tiny=Buffer.alloc(40*23*3);for(let i=0;i<tiny.length;i+=3){const v=90+r()*70,warm=r()*22;tiny[i]=v+warm;tiny[i+1]=v*.92+warm*.5;tiny[i+2]=v*.82;}
   const base=await sharp(tiny,{raw:{width:40,height:23,channels:3}}).resize(1280,H_,{kernel:'cubic'}).raw().toBuffer(),fine=Buffer.alloc(base.length);for(let i=0;i<base.length;i++)fine[i]=Math.max(0,Math.min(255,base[i]+(r()-.5)*36));
   const u=hgt/100,cx=980,cy=400,svg=`<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="${H_}"><g transform="translate(${cx},${cy}) scale(${u})"><path d="M 0 ${-cy/u} L 0 -62" stroke="#a9a08a" stroke-width="3" fill="none"/><circle cx="0" cy="-56" r="7" fill="none" stroke="#c99b30" stroke-width="3"/><ellipse cx="0" cy="12" rx="44" ry="38" fill="#d9a92f" stroke="#8d6414" stroke-width="2"/><circle cx="-14" cy="-30" r="24" fill="#e2b53c" stroke="#8d6414" stroke-width="2"/><path d="M -34 -28 L -58 -20 L -34 -14 Z" fill="#cf8f22"/></g></svg>`;
   return sharp(fine,{raw:{width:1280,height:H_,channels:3}}).composite([{input:Buffer.from(svg)}]).png().toBuffer();
  };
  // Height share of the gold pixels (the charm, not the grey chain or the rock) in an image.
  const goldShare=async input=>{const {data,info}=await sharp(input).removeAlpha().raw().toBuffer({resolveWithObject:true});let y0=1e9,y1=-1;for(let y=0;y<info.height;y++){let n=0;for(let x=0;x<info.width;x++){const i=(y*info.width+x)*3;if(data[i]-data[i+2]>100&&data[i]>150)n++;}if(n>=4){y0=Math.min(y0,y);y1=Math.max(y1,y);}}return {share:(y1-y0+1)/info.height,top:y0,bottom:y1,height:info.height};};
  const tmp3=await fs.mkdtemp(path.join(os.tmpdir(),'close-framing-'));
  try{
   const png=await scene(720,190),before=await goldShare(png);assert(before.share>.25&&before.share<.32,'the generated charm is small: '+before.share);
   const {data,info}=await sharp(png).resize({width:320,height:320,fit:'inside'}).removeAlpha().raw().toBuffer({resolveWithObject:true}),frames=[0,1,2,3,4,5].map(()=>({data,width:info.width,height:info.height,channels:info.channels}));
   const found=F.charmBody(frames,{x:.68,y:0,w:.17,h:.75},{maxSide:320});assert(found&&found.mh>=before.share&&found.mh<=before.share+.09&&found.n===6,'the charm body is measured from pixels, without its chain: '+JSON.stringify(found));
   assert.equal(F.charmBody(frames.slice(0,2),{x:.68,y:0,w:.17,h:.75}),null,'too few frames is not a measurement');assert.equal(F.charmBody(frames,{x:.05,y:.05,w:.2,h:.2}),null,'nothing is found where the product box points at bare rock');
   const jpg=path.join(tmp3,'duck.jpg'),mp4=path.join(tmp3,'duck.mp4');await fs.writeFile(jpg,await sharp(png).jpeg({quality:95}).toBuffer());await exec(require('@ffmpeg-installer/ffmpeg').path,['-y','-loop','1','-i',jpg,'-t','10','-r','24','-pix_fmt','yuv420p',mp4]);
   const layout=validateBounds({landscape:{bounds:[.69,0,.14,.66],complete:true,confidence:.99,note:'fixture'}},['landscape']).landscape,v11={...plan,pipelineVersion:3,renderVersion:11,fadeColor:'#e9dcc4'};
   const rows=await renderVariants(await fs.readFile(mp4),'landscape',{...v11,composition:layout});assert.equal(rows.length,1);const row=rows[0];
   assert(row.composition.notes.some(n=>/landscape film: the charm fills about \d+% of the height.*enlarged 2\.0x/.test(n)),'the enlargement is reported: '+row.composition.notes);
   const out=path.join(tmp3,'out.mp4'),still=path.join(tmp3,'out.png');await fs.writeFile(out,row.bytes);await exec(require('@ffmpeg-installer/ffmpeg').path,['-y','-ss','5','-i',out,'-frames:v','1',still]);
   const after=await goldShare(still);assert(after.share>=before.share*1.8&&after.share>=.5&&after.share<=.7,'the finished film shows the charm large: '+before.share+' to '+after.share);assert(after.top>=8&&after.bottom<=after.height-8,'the enlarged charm is whole inside the film');
   // Copy and wordmark still sit wholly inside the fade, clear of the enlarged charm.
   const layers=await captionLayers({...v11,composition:{...layout,body:found},sourceOrientation:'landscape'},LAND),base=layers.find(l=>l.persistent),gp=layers.geometry.product;assert.equal(layers.geometry.framing.status,'zoomed');
   const inside=(b,r)=>b.x>=r.x-.5&&b.y>=r.y-.5&&b.x+b.w<=r.x+r.w+.5&&b.y+b.h<=r.y+r.h+.5,product={x:gp.x*LAND.width,y:gp.y*LAND.height,w:gp.w*LAND.width,h:gp.h*LAND.height};
   assert(base.fades.some(f=>inside(base.logoBox,f)),'the wordmark stays inside a fade');
   for(const layer of layers.filter(l=>!l.persistent)){assert(inside(layer.textBox,layer.fade),'headline inside its fade');assert(!(layer.textBox.x<product.x+product.w&&layer.textBox.x+layer.textBox.w>product.x&&layer.textBox.y<product.y+product.h&&layer.textBox.y+layer.textBox.h>product.y),'headline clear of the enlarged charm');}
  }finally{await fs.rm(tmp3,{recursive:true,force:true});}
 }
 console.log('PASS full-canvas films, measured crops, large type, transitions, a blended band seam, a standard primary-colour fade and protected jewelry in all three ratios');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
