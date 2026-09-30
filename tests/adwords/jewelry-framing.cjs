const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const r=require('../../brites-ad-responsive'),research=require('../../netlify/functions/googleAdsAdDesignResearch'),scenes=require('./fixtures/jewelry-framing.cjs'),{plan:base}=require('./ad-responsive.cjs');
let checks=0;const ok=(value,message)=>{assert(value,message);checks++;};
const plan={...base,copy:{...base.copy,shortHeadline:'Saturn Necklace',cta:'Shop now'},style:{...base.style,treatment:'soft-fade'}};
const images=r.sceneCatalog.map(s=>({id:s.key,width:s.format.width,height:s.format.height,forFamilies:s.families,forBoards:s.boards,jewelryType:'necklace',...scenes[s.key]}));
const before=JSON.stringify({plan,images});
const intersects=(a,b)=>Math.min(a.x+a.width,b.x+b.width)-Math.max(a.x,b.x)>1&&Math.min(a.y+a.height,b.y+b.height)-Math.max(a.y,b.y)>1;
for(const b of r.variants){
 const im=r.selectImage(plan,images,b),d=r.document(plan,im,b,b.device),p=d.objects.find(o=>o.editorRole==='photo'),f=im.focus,c=im.contextFocus;
 const map=q=>({x:p.left+(q.x*im.width-p.cropX)*p.scaleX,y:p.top+(q.y*im.height-p.cropY)*p.scaleY,width:q.width*im.width*p.scaleX,height:q.height*im.height*p.scaleY}),charm=map(f),context=map(c);
 ok(p.scaleX===p.scaleY,b.key+' preserves the photographed assembly proportions');
 ok(p.cropX<=c.x*im.width+.001&&p.cropY<=c.y*im.height+.001&&p.cropX+p.width>=(c.x+c.width)*im.width-.001&&p.cropY+p.height>=(c.y+c.height)*im.height-.001,b.key+' retains both nearby chain runs');
 ok(context.x>=-.001&&context.y>=-.001&&context.x+context.width<=b.width+.001&&context.y+context.height<=b.height+.001,b.key+' chain and jewelry stay within the ad');
 ok(charm.height/b.height<=.46&&charm.width/b.width<=.40,b.key+' never fills the frame with an oversized pendant');
 ok(charm.height/b.height>=.17,b.key+' pendant still reads clearly');
 ok(context.y<charm.y,b.key+' keeps the extra context actually present in this fixture');
 ok(d.sceneFit.mode!=='legacy',b.key+' keeps continuous photography');
 for(const o of d.objects.filter(o=>['headline','description','brand','button'].includes(o.editorRole)))ok(!intersects(context,{x:o.left,y:o.top,width:o.width*(o.scaleX||1),height:(o.aiBoxHeight||o.height)*(o.scaleY||1)}),b.key+' copy clears chain as well as charm');
}
ok(JSON.stringify({plan,images})===before,'reframing does not rewrite source identity or saved plans');
for(const shape of [{x:.40,y:.30,width:.20,height:.20},{x:.25,y:.38,width:.5,height:.12},{x:.44,y:.2,width:.12,height:.45}]){
 const source={width:1000,height:1000,focus:shape,jewelryType:'earrings'},plain=r.framingFocus(source,plan),necklace=r.framingFocus({...source,jewelryType:'necklace'},plan);
 assert.deepEqual(plain,necklace);checks++;ok(!r.framingFocus.toString().includes('necklace'),'crop geometry does not assume a necklace or impose a chain length');
 for(const type of ['earrings','ring','bracelet','charm','other']){
  const f=r.framingFocus({...source,jewelryType:type},plan);ok(f.x<=shape.x&&f.y<=shape.y&&f.x+f.width>=shape.x+shape.width&&f.y+f.height>=shape.y+shape.height,type+' retains its complete arbitrary geometry');
 }
}
const input={imageDataUrl:'data:image/jpeg;base64,test',product:{title:'Cloud Necklace'},width:1000,height:1000},request=research.buildSubjectFocusRequest(input),schema=request.text.format.schema;
ok(schema.required.includes('context')&&schema.required.includes('jewelryType'),'existing localization pass also locates connected chain context');
const located=research.validateSubjectFocus({left:400,top:300,right:600,bottom:500,context:{left:300,top:100,right:700,bottom:500},jewelryType:'necklace',complete:true,cutEdges:[],confident:true},input);
assert.deepEqual(located.contextFocus,{x:.3,y:.1,width:.4,height:.4});checks++;
ok(located.x===.4&&located.y===.3&&located.width===.2&&located.height===.2,'chain box does not replace shape verification bounds');
const invalid=research.validateSubjectFocus({left:400,top:300,right:600,bottom:500,context:{left:450,top:100,right:700,bottom:500},jewelryType:'necklace',complete:true,cutEdges:[],confident:true},input);
ok(!invalid.contextFocus,'a context box omitting part of the product cannot authorize a crop');
const old=research.validateSubjectFocus({x:.4,y:.3,width:.2,height:.2,confident:true},input);ok(old.x===.4&&!old.contextFocus,'paid legacy localization receipts remain usable');
for(const s of r.sceneCatalog)ok(s.direction.includes(r.photographyGuidance),'every new scene family includes product-aware wider framing: '+s.key);
const prompt=request.input[0].content[0].text;ok(!/ears|tail|they may be cropped away/.test(prompt),'localization instructions are generic and no longer discard necklace chain');
const adapters=fs.readFileSync(path.join(__dirname,'../../netlify/functions/googleAdsAdDesignAdapters.js'),'utf8');ok(adapters.includes("${require('../../brites-ad-responsive').photographyGuidance}"),'actual image provider prompt receives the shared camera guidance');
ok(!/pendant-height|one pendant|necklace chain context|Show the connected necklace chain/.test(r.photographyGuidance+r.sceneCatalog.map(s=>s.direction).join(' ')),'no fixed chain-length targets enter generation');
console.log('PASS '+checks+' necklace scale, visible chain, generic geometry and context checks');
