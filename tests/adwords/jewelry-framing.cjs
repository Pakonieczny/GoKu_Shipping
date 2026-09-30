const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const r=require('../../brites-ad-responsive'),research=require('../../netlify/functions/googleAdsAdDesignResearch'),scenes=require('./fixtures/jewelry-framing.cjs'),baseline=require('./fixtures/jewelry-framing-original.json'),{plan:base}=require('./ad-responsive.cjs');
let checks=0;const ok=(value,message)=>{assert(value,message);checks++;};
const plan={...base,copy:{...base.copy,shortHeadline:'Saturn Necklace',cta:'Shop now'},style:{...base.style,treatment:'soft-fade'}};
const images=r.sceneCatalog.map(s=>({id:s.key,width:s.format.width,height:s.format.height,forFamilies:s.families,forBoards:s.boards,jewelryType:'necklace',...scenes[s.key]}));
const before=JSON.stringify({plan,images});
const intersects=(a,b)=>Math.min(a.x+a.width,b.x+b.width)-Math.max(a.x,b.x)>1&&Math.min(a.y+a.height,b.y+b.height)-Math.max(a.y,b.y)>1;
for(const b of r.variants){
 const im=r.selectImage(plan,images,b),d=r.document(plan,im,b,b.device),p=d.objects.find(o=>o.editorRole==='photo'),f=im.focus;
 const charm={x:p.left+(f.x*im.width-p.cropX)*p.scaleX,y:p.top+(f.y*im.height-p.cropY)*p.scaleY,width:f.width*im.width*p.scaleX,height:f.height*im.height*p.scaleY};
 ok(p.scaleX===p.scaleY,b.key+' preserves the photographed assembly proportions');
 ok(p.cropX<=f.x*im.width+.001&&p.cropY<=f.y*im.height+.001&&p.cropX+p.width>=(f.x+f.width)*im.width-.001&&p.cropY+p.height>=(f.y+f.height)*im.height-.001,b.key+' retains the complete decorative jewelry and its immediate hardware');
 ok(charm.x>=-.001&&charm.y>=-.001&&charm.x+charm.width<=b.width+.001&&charm.y+charm.height<=b.height+.001,b.key+' jewelry stays within the ad');
 const ratio=p.scaleX/baseline.scales[b.key];ok(ratio>=.90&&ratio<=.94,b.key+' is only 6–10 percent smaller than the original close framing, not a wide shot');
 for(const o of d.objects.filter(o=>['headline','description','brand','button'].includes(o.editorRole)))ok(!intersects(charm,{x:o.left,y:o.top,width:o.width*(o.scaleX||1),height:(o.aiBoxHeight||o.height)*(o.scaleY||1)}),b.key+' copy clears the decorative jewelry');
 const withoutContext=r.document(plan,{...im,contextFocus:undefined},b,b.device),longContext=r.document(plan,{...im,contextFocus:{x:0,y:0,width:1,height:1}},b,b.device);
 assert.deepEqual(d,withoutContext);assert.deepEqual(d,longContext);checks+=2;
}
ok(JSON.stringify({plan,images})===before,'reframing does not rewrite source identity or saved plans');
for(const type of ['necklace','earrings','ring','bracelet','charm','other'])for(const shape of [{x:.40,y:.30,width:.20,height:.20},{x:.25,y:.38,width:.5,height:.12},{x:.44,y:.2,width:.12,height:.45}]){
 const source={id:'generic',width:1000,height:1000,focus:shape},a=r.document(plan,source,r.boards[0]),b=r.document(plan,{...source,jewelryType:type,contextFocus:{x:0,y:0,width:1,height:1}},r.boards[0]);
 assert.deepEqual(a,b);checks++;ok(b.objects.find(o=>o.editorRole==='photo').scaleX===a.objects.find(o=>o.editorRole==='photo').scaleX,type+' uses the same generic scale adjustment without invented attachment length');
}
const input={imageDataUrl:'data:image/jpeg;base64,test',product:{title:'Cloud Necklace'},width:1000,height:1000},request=research.buildSubjectFocusRequest(input),schema=request.text.format.schema;
ok(schema.required.includes('context')&&schema.required.includes('jewelryType'),'existing localization metadata remains compatible');
const located=research.validateSubjectFocus({left:400,top:300,right:600,bottom:500,context:{left:300,top:100,right:700,bottom:500},jewelryType:'necklace',complete:true,cutEdges:[],confident:true},input);
assert.deepEqual(located.contextFocus,{x:.3,y:.1,width:.4,height:.4});checks++;
ok(located.x===.4&&located.y===.3&&located.width===.2&&located.height===.2,'attachment context does not replace shape verification bounds');
const invalid=research.validateSubjectFocus({left:400,top:300,right:600,bottom:500,context:{left:450,top:100,right:700,bottom:500},jewelryType:'necklace',complete:true,cutEdges:[],confident:true},input);
ok(!invalid.contextFocus,'malformed context cannot replace identity bounds');
const old=research.validateSubjectFocus({x:.4,y:.3,width:.2,height:.2,confident:true},input);ok(old.x===.4&&!old.contextFocus,'paid legacy localization receipts remain usable');
for(const s of r.sceneCatalog)ok(s.direction.includes(r.photographyGuidance),'every scene requests close framing with only a slight reduction: '+s.key);
const prompt=request.input[0].content[0].text;ok(prompt.includes('do not use the entire context box to set zoom'),'localization no longer demands a full chain crop');
const adapters=fs.readFileSync(path.join(__dirname,'../../netlify/functions/googleAdsAdDesignAdapters.js'),'utf8');ok(adapters.includes("${require('../../brites-ad-responsive').photographyGuidance}"),'actual image provider receives the revised close camera guidance');
console.log('PASS '+checks+' original-scale comparisons, generic framing and identity checks');
