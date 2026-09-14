const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),sharp=require('sharp');
const root=path.resolve(__dirname,'../..'),r=require(root+'/brites-ad-responsive'),{plan:base}=require('./ad-responsive.cjs');
const plan={...base,style:{...base.style,treatment:'soft-fade'},copy:{headline:'A Peach for Your Foodie',shortHeadline:'Peach Charm',description:'Gift-ready packaging included.',cta:'Shop now'}};
let n=0;const ok=(v,m)=>{assert(v,m);n++};
(async()=>{
 const focusByScene={landscape:{x:.58,y:.08,width:.29,height:.8},square:{x:.3,y:.04,width:.4,height:.38},portrait:{x:.28,y:.04,width:.44,height:.5},tall:{x:.29,y:.18,width:.42,height:.42},banner:{x:.04,y:.08,width:.23,height:.84},slim:{x:.29,y:.18,width:.42,height:.42}};
 const planned=r.sceneCatalog.map(s=>({id:s.key,width:s.format.width,height:s.format.height,forFamilies:s.families,forBoards:s.boards,focus:focusByScene[s.key]}));
 for(const b of r.variants){const image=r.selectImage(plan,planned,b),d=r.document(plan,image,b,b.device);ok(d.sceneFit.mode!=='legacy',b.key+' planned scene fits without an exposed join');if(['square','landscape','portrait'].includes(b.key)){const c=r.cleanCrop(image,b);ok(c.x>=0&&c.y>=0,'planned crop valid');}}
 const {JSDOM}=require('jsdom'),dom=new JSDOM('<body></body>',{pretendToBeVisual:true,runScripts:'outside-only',resources:'usable',url:'https://example.test'}),w=dom.window;
 w.ResizeObserver=class{observe(){}disconnect(){}};w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.HTMLDialogElement.prototype.close=function(){this.open=false};
 for(const file of ['vendor/fabric-7.4.0.min.js','brites-ad-responsive.js','brites-ad-editor.js'])w.eval(fs.readFileSync(root+'/'+file,'utf8'));
 const brand=require(root+'/brites-brand-assets');w.BritesBrandAssets={get:id=>{const a=brand.get(id);return a?{...a,url:brand.dataUrl(id)}:undefined;}};
 const bytes=process.env.SCENE_TEST_IMAGE?fs.readFileSync(process.env.SCENE_TEST_IMAGE):await sharp({create:{width:2048,height:1072,channels:3,background:'#ac9878'}}).png().toBuffer(),meta=await sharp(bytes).metadata();
 const photo={id:'scene',width:meta.width,height:meta.height,url:'data:image/png;base64,'+bytes.toString('base64'),focus:{x:.455,y:.045,width:.285,height:.79}};
 const e=await w.BritesAdEditor.open({title:'Peach Charm',workspaceId:'test',productId:base.productId,groupRef:base.groupRef,format:'square',photos:[],request:async action=>action==='adDesignEditorState'?{ok:true,sources:[photo],designs:[]}:action==='adDesignSavedDesigns'?{ok:true,savedDesigns:[]}:{ok:true}});
 const tiles=[];
 for(const b of r.variants){
  if(process.env.SCENE_TEST_OUTPUT)console.log("Rendering "+b.device+" "+b.key);
  const d=r.document(plan,photo,b,b.device),prior=r.document({...plan,style:{...plan.style,atmospheric:false}},photo,b,b.device),roles=['headline','description','brand','button'];
  if(d.sceneFit.mode==='legacy'){assert.deepEqual(d.objects,prior.objects);ok(/dedicated|separate/.test(d.sceneFit.reason),b.key+' preserves prior layout rather than creating an exposed seam');continue;}
  ok(d.sceneFit.mode!=='legacy',b.key+' receives full photographic treatment');
  assert.deepEqual(d.objects.filter(o=>roles.includes(o.editorRole)),prior.objects.filter(o=>roles.includes(o.editorRole)));n++;
  const p=d.objects.find(o=>o.editorRole==='photo'),pp=prior.objects.find(o=>o.editorRole==='photo');
  ok(Math.abs((p.left-p.cropX*p.scaleX)-(pp.left-pp.cropX*pp.scaleX))<.001&&Math.abs((p.top-p.cropY*p.scaleY)-(pp.top-pp.cropY*pp.scaleY))<.001&&p.scaleX===pp.scaleX,b.key+' preserves exact product position and scale');
  const bg=d.objects.find(o=>o.id==='ai_atmosphere_surface')||p;
  ok(bg.left<.01&&bg.top<.01&&bg.width*bg.scaleX>=b.width-.01&&bg.height*bg.scaleY>=b.height-.01,b.key+' photo covers all edges');
  const s=d.sceneFit.subject;ok(s.x>=0&&s.y>=0&&s.x+s.w<=1&&s.y+s.h<=1,b.key+' full charm retained');
  const fade=d.objects.find(o=>o.id==='ai_atmosphere_fade'),stops=fade.fill.colorStops.map(c=>({x:c.offset,a:Number(c.color.match(/,([^,]+)\)$/)[1])}));
  const alpha=x=>{for(let i=1;i<stops.length;i++)if(x<=stops[i].x){const a=stops[i-1],b=stops[i];return a.a+(b.a-a.a)*(x-a.x)/(b.x-a.x||1);}return stops.at(-1).a;};
  for(const x of d.sceneFit.axis==='bottom'?[s.y,s.y+s.h]:[s.x,s.x+s.w])ok(alpha(x)<.001,b.key+' fade never crosses jewelry');
  if(bg!==p){const f=photo.focus;ok(bg.cropX+bg.width<=f.x*photo.width||bg.cropX>=(f.x+f.width)*photo.width||bg.cropY+bg.height<=f.y*photo.height||bg.cropY>=(f.y+f.height)*photo.height,b.key+' continuation cannot duplicate jewelry');}
  e.board=b;await e.restore(d);for(const o of e.canvas.getObjects())e.fitAIText(o);e.canvas.renderAll();
  const sb={left:s.x*b.width,top:s.y*b.height,width:s.w*b.width,height:s.h*b.height};
  for(const o of e.canvas.getObjects().filter(o=>roles.includes(o.editorRole))){const x=o.getBoundingRect();ok(x.left>=-1&&x.top>=-1&&x.left+x.width<=b.width+1&&x.top+x.height<=b.height+1,b.key+' fitted '+o.editorRole+' inside canvas');ok(Math.min(x.left+x.width,sb.left+sb.width)-Math.max(x.left,sb.left)<=1||Math.min(x.top+x.height,sb.top+sb.height)-Math.max(x.top,sb.top)<=1,b.key+' fitted copy does not cover product');}
  if(process.env.SCENE_TEST_OUTPUT){e.canvas.setDimensions({width:b.width,height:b.height});e.canvas.setViewportTransform([1,0,0,1,0,0]);e.canvas.renderAll();const png=Buffer.from(e.canvas.toDataURL({format:'png',multiplier:1}).split(',')[1],'base64');const width=Math.min(400,b.width),height=Math.round(b.height*width/b.width);tiles.push({input:await sharp(png).resize(width,height).png().toBuffer(),width,height,label:b.device+' '+b.key});}
 }
 for(const f of [{x:0,y:0,width:.2,height:.2},{x:.8,y:.8,width:.2,height:.2},photo.focus])for(const b of r.boards.slice(0,3)){const c=r.cleanCrop({...photo,focus:f},b);ok(c.x>=0&&c.y>=0&&c.x+c.width<=1.000001&&c.y+c.height<=1.000001,'clean crop remains in source bounds');ok(c.x<=f.x+.00001&&c.y<=f.y+.00001&&c.x+c.width>=f.x+f.width-.00001&&c.y+c.height>=f.y+f.height-.00001,'clean export retains off-center product');}
 console.log('PASS scene geometry and saved-photo layout checks; checking final pixels');
 // Pixel-test the complete new scene-family path using known product silhouettes.
 for(const im of planned){const f=im.focus,svg='<svg width="'+im.width+'" height="'+im.height+'"><rect width="100%" height="100%" fill="#a89678"/><ellipse cx="'+((f.x+f.width/2)*im.width)+'" cy="'+((f.y+f.height/2)*im.height)+'" rx="'+(f.width*im.width/2)+'" ry="'+(f.height*im.height/2)+'" fill="#fbd05b" stroke="#5b3513" stroke-width="5"/></svg>';im.url='data:image/png;base64,'+(await sharp(Buffer.from(svg)).png().toBuffer()).toString('base64');}
 const proofs=await e.renderAIProofs({artboard:r.boards[0],device:'mobile',document:r.document(plan,r.selectImage(plan,planned,r.boards[0]),r.boards[0]),sources:planned,responsive:{layoutVersion:r.layoutVersion,plan,images:planned}});ok(proofs.length===27&&proofs.every(p=>p.renderCheck.visiblePhotoFraction>.005),'all final scene-family exports retain visible product pixels');

 const noFocus=r.document(plan,{...photo,focus:null},r.boards[0]);ok(noFocus.sceneFit.mode==='legacy','unknown product location uses safe legacy framing');
 ok(r.selectImage(plan,[{id:'generic'},{id:'tall',forFamilies:['skyscraper']},{id:'slim',forBoards:['display_120x600']}],r.boards.find(b=>b.key==='display_120x600')).id==='slim','dedicated narrow scene wins over generic family');
 if(tiles.length){let y=0,inputs=[];for(let i=0;i<tiles.length;i+=4){let h=Math.max(...tiles.slice(i,i+4).map(t=>t.height))+35;for(let j=i;j<Math.min(i+4,tiles.length);j++){const t=tiles[j],x=(j-i)*420;inputs.push({input:t.input,left:x,top:y});inputs.push({input:Buffer.from('<svg width="410" height="30"><text x="0" y="20" font-size="13">'+t.label+'</text></svg>'),left:x,top:y+t.height});}y+=h;}await sharp({create:{width:1680,height:y,channels:3,background:'#ffffff'}}).composite(inputs).png().toFile(process.env.SCENE_TEST_OUTPUT);}
 console.log('PASS '+n+' atmospheric coverage, preserved layout, product isolation and fitted typography checks');e.dirty=false;await e.close();dom.window.close();
})().catch(e=>{console.error(e.stack);process.exitCode=1});
