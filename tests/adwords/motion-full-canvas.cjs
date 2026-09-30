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
  const {FADE_STOPS,FADE_PEAK,FADE_SIZE,fadeAlpha,fadeColour,fadeSample,primaryColour,inkFor,inkOver,captions}=require('../../netlify/functions/googleAdsMotionComposition'),green='#3f8f55';
  assert(FADE_STOPS[0][0]===0&&FADE_STOPS[0][1]<=.7&&FADE_STOPS[0][1]>=.6&&FADE_STOPS[0][1]===FADE_PEAK,'the fade starts at about .6-.7 opacity at the outer edge (softer than the former .8)');
  assert.equal(FADE_STOPS[FADE_STOPS.length-1][0],1);assert.equal(FADE_STOPS[FADE_STOPS.length-1][1],0);
  assert(FADE_STOPS.every(([o,a],i)=>i===0||(o>FADE_STOPS[i-1][0]&&a<FADE_STOPS[i-1][1])),'the ramp only ever eases outward');
  assert.equal(FADE_SIZE.side,.335);assert.deepEqual(FADE_SIZE.top,{square:.33,landscape:.33,portrait:.33},'every film shape gets a fade 33% of its height');
  {const mean=FADE_STOPS.slice(1).reduce((a,[o,v],i)=>a+(o-FADE_STOPS[i][0])*(v+FADE_STOPS[i][1])/2,0);assert(mean<.4,'the fade stays mostly transparent overall: '+mean);
   // Soft on both ends and never steep: no hard start at the screen edge, no visible edge where it runs out.
   const n=FADE_STOPS.length,slopes=FADE_STOPS.slice(1).map(([o,a],i)=>(FADE_STOPS[i][1]-a)/(o-FADE_STOPS[i][0]));
   assert(slopes[0]<.15,'no hard start: the ramp is flat where it begins: '+slopes[0]);assert(slopes[slopes.length-1]<.25,'no visible edge: the ramp arrives at zero almost flat (last stop step '+slopes[slopes.length-1]+'): a few 8-bit levels over its last 6% at most');
   assert(Math.max(...slopes)<=1.15,'the falloff is never steep (the former ramp fell at 1.25): '+Math.max(...slopes));
   assert(FADE_STOPS[n-2][1]<.02&&fadeAlpha(1)===0&&Math.abs(fadeAlpha(.5)-FADE_STOPS[8][1])<1e-3,'the stops are samples of the one fade curve');}
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
     assert(at(0)[3]<=.7*255+2&&at(0)[3]>=.6*255-2,f.key+' outer edge is perceptible but soft (about .65): '+at(0)[3]);
     assert.equal(at(Math.round(span)-1)[3]<=2,true,'the fade reaches zero at its inner edge');
     for(let t=1;t<span;t++)assert(at(t)[3]<=at(t-1)[3]+1,'the fade only falls away from the edge');
     // The same curve on every shape and edge: opacity depends only on how far across the fade a pixel is.
     for(const share of [0,.2,.4,.6,.8])assert(Math.abs(at(Math.round(span*share))[3]-fadeAlpha(share)*255)<=3,f.key+' '+fade.name+' fade follows the one standard curve at '+share+': '+at(Math.round(span*share))[3]+' vs '+Math.round(fadeAlpha(share)*255));
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
  // The fade colour comes from the pixels under the fade itself (the frame's own tone, blended with the film's primary colour) and stays soft.
  {const rgb=h=>[1,3,5].map(i=>parseInt(h.slice(i,i+2),16)),chroma=h=>Math.max(...rgb(h))-Math.min(...rgb(h)),mixHex=(a,b,t)=>'#'+rgb(a).map((v,i)=>Math.round(v+(rgb(b)[i]-v)*t).toString(16).padStart(2,'0')).join(''),dist=(a,b)=>Math.hypot(...rgb(a).map((v,i)=>v-rgb(b)[i]));
   const paint=(w,h,body)=>sharp(Buffer.from(`<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}">${body}</svg>`)).jpeg({quality:95}).toBuffer();
   const jewel='<circle cx="940" cy="390" r="100" fill="#c99b42"/>',right={x:.6,y:.25,w:.3,h:.5},ink='#30291f',light='#fffaf0';
   const wide={
    // blue water with beige rock at the lower left and a large saturated yellow-green algae mat elsewhere: the film's saturated primary is green, the scene under the fade is blue
    water:await paint(1280,720,'<rect width="1280" height="720" fill="#4a90b5"/><path d="M0 480 C 200 420 380 470 470 540 L 520 720 L 0 720 Z" fill="#cdb894"/><ellipse cx="640" cy="700" rx="420" ry="90" fill="#a8bf2c"/>'+jewel),
    sand:await paint(1280,720,'<rect width="1280" height="720" fill="#d8c39a"/>'+jewel),
    grass:await paint(1280,720,'<rect width="1280" height="720" fill="#5b9a3c"/>'+jewel),
    dark:await paint(1280,720,'<rect width="1280" height="720" fill="#12161c"/>'+jewel)};
   const under={water:'#4a90b5',sand:'#d8c39a',grass:'#5b9a3c',dark:'#12161c'},got={};
   for(const name of Object.keys(wide)){
    const s=await fadeSample(wide[name],{mode:'dominant',orientation:'landscape',subject:right,ink}),fade=s.colour;got[name]=s;
    assert(/^#[0-9a-f]{6}$/.test(fade)&&Object.keys(s.backdrops).sort().join()==='left,right,top',name+' fade sample has a colour and the frame colour under each edge');
    assert.equal(await fadeColour(wide[name],{mode:'dominant',orientation:'landscape',subject:right,ink}),fade,'fadeColour is the sampled colour');
    assert(dist(mixHex(under[name],fade,FADE_PEAK),under[name])>=20,name+' fade is perceptible over the frame under it: '+fade);
    if(name!=='dark'){
     assert(chroma(fade)<chroma(under[name]),name+' fade is softer (less saturated) than the frame: '+fade);
     assert(lum_(fade)<=lum_(under[name])+.005,name+' fade is never brighter than the frame under it (no glowing patch): '+fade);
     const pick=inkOver(fade,ink,s.backdrops.left);
     for(const t of [.15,.4])assert(contrast_(mixHex(s.backdrops.left,fade,fadeAlpha(t)),pick)>=3.5,name+' copy stays legible at '+t+' of the fade: '+pick);
    }
   }
   {const [r,g,b]=rgb(got.water.colour);assert(b>r+20&&b>=g,'blue water gives a blue-ish fade, not the saturated yellow-green primary: '+got.water.colour);}
   {const [r,g,b]=rgb(got.sand.colour);assert(r>b+15&&r>=g,'sand gives a sand-toned fade: '+got.sand.colour);}
   {const [r,g,b]=rgb(got.grass.colour);assert(g>r+8&&g>b+15,'grass gives a green fade: '+got.grass.colour);}
   assert(lum_(got.dark.colour)<.06&&inkOver(got.dark.colour,ink,got.dark.backdrops.left)===light,'a dark frame keeps a dark, gently lifted fade with light ink: '+got.dark.colour);
   assert.equal(inkOver('#ffffff',ink,'#e8e8e8'),ink);assert.equal(inkOver('#1f4d2c',ink,'#1f4d2c'),light);assert.equal(inkOver('#3f8f55',ink),inkFor('#3f8f55',ink),'without a frame colour the plain ink choice applies');
   // The jewelry does not tint the fade: with the piece on the left the fade edge is the clear right side.
   {const leftPiece=await paint(1280,720,'<rect width="1280" height="720" fill="#2f8f95"/><rect x="40" y="130" width="360" height="460" fill="#e0b13c"/>'),s=await fadeSample(leftPiece,{mode:'dominant',orientation:'landscape',subject:{x:.03,y:.15,w:.3,h:.7},ink}),[r,g,b]=rgb(s.colour);
    assert(b>r+15&&g>r+15,'the fade takes the water under the clear side, not the gold jewelry: '+s.colour);}
   // Portrait masters are read along the top edge, square masters inside their central square.
   {const tall=await paint(720,1280,'<rect width="720" height="1280" fill="#d8c39a"/><rect y="700" width="720" height="580" fill="#2f6f95"/>'),s=await fadeSample(tall,{mode:'dominant',orientation:'portrait',subject:{x:.2,y:.5,w:.6,h:.4},ink}),[r,g,b]=rgb(s.colour);
    assert(r>b&&r>=g-4,'a portrait fade follows the top of the frame: '+s.colour);
    const strip=await paint(1280,720,'<rect width="1280" height="720" fill="#2f6f95"/><rect x="280" width="720" height="720" fill="#d8c39a"/>'),q=await fadeSample(strip,{mode:'dominant',orientation:'square',region:{x:.21875,y:0,w:.5625,h:1},ink}),[r2,g2,b2]=rgb(q.colour);
    assert(r2>b2,'a square fade only looks inside the central square: '+q.colour);}
   // Legible copy in every ratio with a sampled fade, and the wordmark follows the ink.
   const logoTone=async layers=>{const base=layers.find(l=>l.persistent),{data,info}=await sharp(base.bytes).raw().toBuffer({resolveWithObject:true}),L=base.logoBox;let n=0,sum=0;for(let y=Math.round(L.y);y<Math.round(L.y+L.h);y++)for(let x=Math.round(L.x);x<Math.round(L.x+L.w);x++){const i=(y*info.width+x)*4;if(data[i+3]>=250){n++;sum+=(data[i]+data[i+1]+data[i+2])/3;}}return n?sum/n/255:NaN;};
   for(const f of formats)for(const name of ['water','sand','dark']){
    const composition=f.key==='portrait'?{x:.2,y:.5,w:.6,h:.35}:{x:.6,y:.25,w:.3,h:.5},layers=await captionLayers({...v11,composition,sourceOrientation:f.key==='portrait'?'portrait':'landscape',fadeColor:got[name].colour,fadeBackdrops:got[name].backdrops},f);
    const edge=layers.find(l=>l.persistent).fades[0].name,tone=await logoTone(layers);
    if(name!=='water')assert.equal(layers.ink,name==='dark'?light:ink,f.key+' '+name+' copy ink follows the frame under the fade');
    assert(layers.ink===light?tone>.6:tone<.35,f.key+' '+name+' wordmark follows the ink (light script with light ink, near-black with dark ink): '+tone.toFixed(2));
    assert(contrast_(mixHex(got[name].backdrops[edge],got[name].colour,fadeAlpha(.15)),layers.ink)>=(name==='dark'?4.5:3.5),f.key+' '+name+' headline contrast holds on the '+edge+' fade');
   }
  }
  // Beside a solid brand band the standard curve starts at full strength and eases out from there, so the join never steps.
  {const f={key:'square',width:720,height:720},layers=await captionLayers({...v11,composition:{x:.05,y:.05,w:.9,h:.9},sourceOrientation:'landscape'},f),seam=layers.geometry.seam;
   assert.equal(layers.mode,'band');assert(seam&&seam.edge==='top');
   const {data,info}=await sharp(layers.find(l=>l.persistent).bytes).raw().toBuffer({resolveWithObject:true}),column=Math.round(info.width/2),alpha=y=>data[(y*info.width+column)*4+3];
   assert(alpha(0)>=250&&alpha(seam.at)>=250,'the band and the start of its fade are one solid colour');
   assert(Math.abs(alpha(seam.at)-alpha(seam.at+1))<=8&&Math.abs(alpha(seam.at-1)-alpha(seam.at))<=8,'the fade meets the band without a step in opacity');
   for(let y=1;y<f.height;y++)assert(alpha(y)<=alpha(y-1)+1,'the field only ever fades outward');
   assert(alpha(Math.min(f.height-1,seam.at+Math.round(f.height*.12)))<=8,'the field has faded away below the band');}
  // renderVariants samples the master once and gives every format cut from it the same fade colour.
  const shots=[],realCaptions=require('../../netlify/functions/googleAdsMotionComposition').captions,module_=require('../../netlify/functions/googleAdsMotionComposition');
  const tmp2=await fs.mkdtemp(path.join(os.tmpdir(),'fade-colour-'));
  try{
   const sourceJpeg=path.join(tmp2,'green.jpg'),sourceMp4=path.join(tmp2,'green.mp4');await fs.writeFile(sourceJpeg,await sharp(Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="720"><rect width="1280" height="720" fill="#3f8f55"/><circle cx="940" cy="390" r="100" fill="#c99b42"/></svg>')).jpeg().toBuffer());
   await exec(require('@ffmpeg-installer/ffmpeg').path,['-y','-loop','1','-i',sourceJpeg,'-t','10','-r','24','-pix_fmt','yuv420p',sourceMp4]);
   module_.captions=async(p,f,b)=>{shots.push({format:f.key,fadeColor:p.fadeColor,fadeBackdrops:p.fadeBackdrops});return realCaptions(p,f,b);};
   try{await renderVariants(await fs.readFile(sourceMp4),'landscape',{...v11,fadeColor:undefined,composition:subjects.landscape,squareMaster:'landscape'});}finally{module_.captions=realCaptions;}
   assert(shots.length>=2&&shots.every(s=>s.fadeColor===shots[0].fadeColor),'every format from one master uses one fade colour');
   assert(shots.every(s=>JSON.stringify(s.fadeBackdrops)===JSON.stringify(shots[0].fadeBackdrops)&&/^#[0-9a-f]{6}$/.test(s.fadeBackdrops?.left||'')),'every format also gets the frame colour under each fade edge, for its ink');
   const [r,g,b]=[1,3,5].map(i=>parseInt(shots[0].fadeColor.slice(i,i+2),16));assert(g>r+8&&g>b+4&&Math.max(r,g,b)-Math.min(r,g,b)<0x8f-0x3f,'the fade colour is a soft version of the film green, not the brand cream: '+shots[0].fadeColor);
  }finally{await fs.rm(tmp2,{recursive:true,force:true});}
 }
 console.log('PASS full-canvas films, measured crops, large type, transitions, a blended band seam, a standard soft fade matched to the pixels under it (one curve, legible copy) and protected jewelry in all three ratios');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
