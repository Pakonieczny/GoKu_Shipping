const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),sharp=require('sharp');
const root=path.resolve(__dirname,'../..'),responsive=require(root+'/brites-ad-responsive'),research=require(root+'/netlify/functions/googleAdsAdDesignResearch');
const productId='gid://shopify/Product/11',groupRef='customers/123/assetGroups/7';
const plan={productId,groupRef,rationale:'Product-led duck jewelry, framed for mobile clarity.',masterFormat:'landscape',alternateNeeded:false,alternateFormat:'portrait',alternateReason:'',imageDirections:[{concept:'New ivory scene',composition:'Exact duck centered, soft daylight and clean space.',lighting:'Soft daylight',background:'Ivory',preserveProduct:['Duck silhouette'],avoid:['Text'],sourceIds:['product:'+productId]}],copy:{headline:'A Little Duck, Close to You',shortHeadline:'Your Little Duck',description:'Discover the duck pendant necklace.',cta:'Shop the necklace'},nativeCopy:{headlines:['Duck Necklace','Your Little Duck','A Duck Pendant','Explore Duck Jewelry','Brites Duck Necklace'],longHeadlines:['Discover the duck pendant necklace from Brites Jewelry.'],descriptions:['Explore the duck pendant necklace.','Choose your duck necklace at Brites.']},style:{headlineFont:'Georgia',bodyFont:'Arial',background:'#fff9f0',ink:'#302318',accent:'#573a26',buttonInk:'#ffffff'},layouts:['mobile','desktop'].flatMap(device=>['square','portrait','landscape','banner','skyscraper'].map(family=>({device,family,photoSide:'left',photoFraction:device==='mobile'?.6:.5,textAlign:'left',zoom:1,focalX:.5,focalY:.5,showHeadline:true,showDescription:device!=='mobile',showBrand:true,showButton:true}))),factClaims:[{claim:'Duck pendant necklace',quote:'Duck pendant necklace',sourceId:'product:'+productId}],sourceIds:['product:'+productId],limitations:[]};
module.exports={plan};
if(require.main===module)(async()=>{
 let checks=0;const ok=(v,m)=>{assert(v,m);checks++};
 const evidence={hash:'facts',sourceBindings:{landingUrl:'https://britesjewelry.com/products/duck'},sources:[{id:'product:'+productId,status:'available',data:{title:'Duck necklace',description:'Duck pendant necklace'}}],warnings:[]};
 research.validateResponsivePlan({output:plan,request:{productId,groupRef},evidence});ok(true,'fact-grounded recipe validates');
 assert.throws(()=>research.validateResponsivePlan({output:{...plan,productId:'wrong'},request:{productId,groupRef},evidence}),/changed its product/);checks++;
 const longOnly=JSON.parse(JSON.stringify(plan));longOnly.nativeCopy.headlines=['Explore Duck Jewelry','Brites Duck Necklace','Choose Your Duck Necklace','A Little Duck, Close to You','Discover Duck Necklaces'];
 const compactPlan=research.validateResponsivePlan({output:longOnly,request:{productId,groupRef},evidence});ok(compactPlan.nativeCopy.headlines.some(t=>t.length<=15),'generated native copy always includes a compact headline');ok(longOnly.nativeCopy.headlines.length===5,'validation does not mutate the provider response');
 ok(responsive.boards.length===23&&responsive.variants.filter(b=>b.device==='mobile').length>=6,'mobile and desktop coverage');
 // Narrow formats must retain a central charm with breathing room even when
 // a saved recipe requests its maximum zoom. Geometry bounds alone miss this.
 for(const board of responsive.boards.filter(b=>responsive.family(b)==='skyscraper')){
   const zoomed=JSON.parse(JSON.stringify(plan));zoomed.layouts.forEach(l=>{l.zoom=1.35;});
   const p=responsive.document(zoomed,{id:'hero',width:1956,height:1024},board,'desktop').objects[0];
   ok(p.cropX<=1956*.36&&p.cropX+p.width>=1956*.64,board.key+' keeps the central jewelry silhouette and margin');
 }
 const {JSDOM}=require(process.env.BRITES_EDITOR_DOM_RUNTIME?path.join(process.env.BRITES_EDITOR_DOM_RUNTIME,'jsdom'):'jsdom');
 const dom=new JSDOM('<body></body>',{pretendToBeVisual:true,runScripts:'outside-only',resources:'usable',url:'https://example.test'}),w=dom.window;
 w.ResizeObserver=class{observe(){}disconnect(){}};w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.HTMLDialogElement.prototype.close=function(){this.open=false};
 for(const file of ['vendor/fabric-7.4.0.min.js','brites-ad-responsive.js','brites-ad-editor.js'])w.eval(fs.readFileSync(root+'/'+file,'utf8'));
 const url='data:image/jpeg;base64,'+(await sharp({create:{width:1600,height:1000,channels:3,background:'#b49b74'}}).jpeg().toBuffer()).toString('base64'),photo={id:'photo',url,width:1600,height:1000,focus:{x:.47,y:.69,width:.06,height:.16}};
 const e=await w.BritesAdEditor.open({title:'Duck necklace',workspaceId:'test',productId,groupRef,format:'square',photos:[],request:async action=>action==='adDesignEditorState'?{ok:true,sources:[photo],designs:[]}:action==='adDesignSavedDesigns'?{ok:true,savedDesigns:[]}:{ok:true}});
 const boxesOverlap=(a,b)=>Math.min(a.left+a.width,b.left+b.width)-Math.max(a.left,b.left)>1&&Math.min(a.top+a.height,b.top+b.height)-Math.max(a.top,b.top)>1;
 const incompletePlan=JSON.parse(JSON.stringify(plan));incompletePlan.layouts.forEach(l=>{l.showBrand=false;l.showButton=false;l.showHeadline=false;});
 for(const copy of [plan.copy,{headline:"Corgi Necklace for Dog Lovers",shortHeadline:"Corgi Necklace",description:"A handcrafted corgi pendant on a beady chain.",cta:"Shop Now"},{headline:'Gold Peach Fruit Charm',shortHeadline:'Peach Fruit Charm',description:'A sweet gift for food lovers.',cta:'Shop Peach Charm'},{headline:'A Peach for Your Foodie',shortHeadline:'Peach Charm',description:'Gift-ready packaging included.',cta:'Shop charm'}])for(const board of responsive.variants){
   incompletePlan.copy=copy;if(copy.cta==='Shop charm')incompletePlan.style={...plan.style,background:'#2D231E',ink:'#FFF9F0'};else incompletePlan.style=plan.style; e.board=board;await e.restore(responsive.document(incompletePlan,photo,board,board.device));ok(['headline','button'].every(role=>e.canvas.getObjects().some(o=>o.editorRole===role)),board.key+' keeps a concise product caption and action');for(const o of e.canvas.getObjects())e.fitAIText(o);e.canvas.renderAll();
   ok(e.canvas.getObjects().find(o=>o.editorRole==='headline').text===copy.shortHeadline,board.key+' retains the product type in compact messaging');
   const text=e.canvas.getObjects().filter(o=>'text'in o||o.editorRole==='button');
   if(board.key.startsWith('display_')){
     ok(!text.find(o=>o.editorRole==='brand')||text.find(o=>o.editorRole==='brand').fontSize>=7.9,board.key+' brand remains legible at native size');
     ok(text.find(o=>o.editorRole==='button').getObjects().find(o=>'text'in o).fontSize>=11.9,board.key+' action remains legible at native size');
   }
   if(['square','landscape','portrait'].includes(board.key)){
     const brand=text.find(o=>o.editorRole==='brand'),previewWidth=board.device==='desktop'?600:360;
     ok(!brand||brand.fontSize*previewWidth/board.width>=8,board.key+' brand remains readable at typical display width');
     ok(e.canvas.getObjects().find(o=>o.type==='image').getBoundingRect().height>=board.height*.68,board.key+' devotes most of the height to the photograph');
     ok(text.find(o=>o.editorRole==='button').getObjects().find(o=>'text'in o).fontSize*previewWidth/board.width>=11.9,board.key+' CTA remains readable at actual display width');
     ok(!text.find(o=>o.editorRole==='description')||text.find(o=>o.editorRole==='description').fontSize*previewWidth/board.width>=11.9,board.key+' supporting copy remains readable at typical display width');
   }
   for(const o of text){const b=o.getBoundingRect();ok(b.left>=-1&&b.top>=-1&&b.left+b.width<=board.width+1&&b.top+b.height<=board.height+1,board.device+' '+board.key+' '+o.editorRole+' fits '+JSON.stringify(b));}
   for(let i=0;i<text.length;i++)for(let j=i+1;j<text.length;j++)ok(!boxesOverlap(text[i].getBoundingRect(),text[j].getBoundingRect()),board.key+' '+text[i].editorRole+' avoids '+text[j].editorRole);
   const image=e.canvas.getObjects().find(o=>o.type==='image');ok(image.scaleX===image.scaleY&&image.angle===0,'photo preserves proportions');
   const imageBox=image.getBoundingRect();
   ok(imageBox.left<=1&&(responsive.family(board)==='banner'?imageBox.height>=board.height*.98:imageBox.width>=board.width*.98),board.key+' photo starts at the edge instead of inside a padded thumbnail');
   // The full charm remains inside every crop, including its off-center position.
   const fx=photo.focus.x*photo.width,fy=photo.focus.y*photo.height,fw=photo.focus.width*photo.width,fh=photo.focus.height*photo.height;
   ok(image.cropX<=fx&&image.cropY<=fy&&image.cropX+image.width>=fx+fw&&image.cropY+image.height>=fy+fh,board.key+' keeps the complete located charm visible');
   ok(Math.max(fw*image.scaleX/imageBox.width,fh*image.scaleY/imageBox.height)>=.59,board.key+' gives the charm a dominant share of the image frame');
   ok(imageBox.top<=1,board.key+' removes the empty band above the photograph');
   const action=text.find(o=>o.editorRole==='button').getObjects().find(o=>'text'in o);
   ok(action.textLines.length===1,board.key+' action stays on one line');

 }
 const fadePlan={...plan,style:{...plan.style,treatment:'soft-fade'},copy:{headline:'Sweet on Peach Charm',shortHeadline:'Peach Charm',description:'A gift for food lovers.',cta:'Shop Peach Charm'}};let fades=0;
 for(const device of ['mobile','desktop'])for(const board of [{key:'square',width:2048,height:2048},{key:'portrait',width:1638,height:2048}]){
   const doc=responsive.document(fadePlan,{id:'peach',width:2048,height:1072,focus:{x:.539,y:.199,width:.258,height:.682}},board,device),fade=doc.objects.find(o=>o.id==='ai_image_fade'),photo=doc.objects.find(o=>o.editorRole==='photo');
   ok(!!fade,device+' '+board.key+' keeps a vertical fade for a wide source photograph instead of falling back to a footer');
   ok(fade.fill.coords.x2===0&&fade.fill.coords.y2===1,'portrait fade runs downward');
   ok(fade.fill.colorStops.some(s=>s.color.endsWith(',1)')&&s.offset*board.height<=photo.height*photo.scaleY+.01),'fade becomes opaque before the photograph ends, preventing a hard boundary');
 }

 for(const board of responsive.variants){e.board=board;await e.restore(responsive.document(fadePlan,photo,board,board.device));for(const o of e.canvas.getObjects())e.fitAIText(o);const fade=e.canvas.getObjects().find(o=>o.id==='ai_image_fade');if(!fade)continue;fades++;
   const im=e.canvas.getObjects().find(o=>o.type==='image'),bounds=im.getBoundingRect(),subject={left:(photo.focus.x*photo.width-im.cropX)*im.scaleX,top:(photo.focus.y*photo.height-im.cropY)*im.scaleY,width:photo.focus.width*photo.width*im.scaleX,height:photo.focus.height*photo.height*im.scaleY};
   ok(Math.abs(bounds.width-board.width)<1&&(Math.abs(bounds.height-board.height)<1||fade.fill.coords.y2===1&&fade.fill.colorStops.some(s=>new w.fabric.Color(s.color).getAlpha()===1&&s.offset*board.height<=bounds.height+1)),'soft fade fills the width and conceals the photo edge '+board.key);
   const text=e.canvas.getObjects().filter(o=>'text'in o||o.editorRole==='button');for(const o of text){const b=o.getBoundingRect();ok(!boxesOverlap(b,subject),'overlay does not cover product '+board.key);ok(b.left>=-1&&b.top>=-1&&b.left+b.width<=board.width+1&&b.top+b.height<=board.height+1,'overlay stays on artboard '+board.key);}
   for(let i=0;i<text.length;i++)for(let j=i+1;j<text.length;j++)ok(!boxesOverlap(text[i].getBoundingRect(),text[j].getBoundingRect()),'soft fade text and CTA do not overlap '+board.key);
   ok(fade.fill.colorStops.some(s=>new w.fabric.Color(s.color).getAlpha()===0)&&fade.fill.colorStops.some(s=>new w.fabric.Color(s.color).getAlpha()===1),'fade remains editable and smoothly transparent');
 }
 const fadeProofs=await e.renderAIProofs({artboard:{key:'square',width:2048,height:2048},device:'mobile',document:responsive.document(fadePlan,photo,{key:'square',width:2048,height:2048},'mobile'),sources:[photo],responsive:{layoutVersion:responsive.layoutVersion,plan:fadePlan,images:[photo]}});ok(fadeProofs.length===27,'fade proofs retain visible photograph pixels in every size');
 ok(fades>=6,'soft fade is the practical default across master formats');
 const noFocus=responsive.document(fadePlan,{...photo,focus:null},{key:'square',width:2048,height:2048},'mobile');ok(!noFocus.objects.some(o=>o.id==='ai_image_fade'),'unlocated products retain safe framing instead of speculative overlays');
 const focused={id:'focus',width:1956,height:1024,focus:{x:.45,y:.70,width:.06,height:.16}};
 const small=responsive.document(plan,focused,{key:'display_300x50',width:300,height:50},'mobile').objects[0],large=responsive.document(plan,focused,{key:'square',width:2048,height:2048},'mobile').objects[0];
 ok(small.height<large.height,'small placements use a tighter source crop around the charm');
 assert.throws(()=>research.validateSubjectFocus({x:.95,y:.8,width:.1,height:.1,confident:true}),/reliably/);checks++;
 assert.throws(()=>research.validateSubjectFocus({x:.45,y:.7,width:.06,height:.16,confident:false}),/reliably/);checks++;
 ok(research.buildSubjectFocusRequest({imageDataUrl:'data:image/jpeg;base64,test',product:{title:'Corgi necklace'}}).text.format.name==='brites_subject_focus','focus analysis has its own structured response rather than a guessed center');
 const beforeProofBoard=e.board,beforeProofDoc=JSON.stringify(e.document());const proofs=await e.renderAIProofs({artboard:{key:'square',width:2048,height:2048},device:'mobile',document:responsive.document(plan,photo,{key:'square',width:2048,height:2048},'mobile'),sources:[photo],responsive:{layoutVersion:responsive.layoutVersion,plan,images:[photo]}});
 ok(proofs.length===27&&proofs[0].key==='active'&&new Set(proofs.map(p=>p.key)).size===27,'browser renders the active canvas plus all 26 responsive variants');
 ok(e.board===beforeProofBoard&&JSON.stringify(e.document())===beforeProofDoc,'rendering quality proofs does not modify editable artwork');
 ok(proofs.every(p=>p.renderCheck.version===1&&p.renderCheck.visiblePhotoFraction>=.005),'every exported proof verifies visible photo pixels before review');
 const invisiblePhoto={...photo,url:'data:image/png;base64,'+(await sharp({create:{width:1600,height:1000,channels:4,background:{r:0,g:0,b:0,alpha:0}}}).png().toBuffer()).toString('base64')};
 await assert.rejects(()=>e.renderAIProofs({artboard:{key:'square',width:2048,height:2048},device:'mobile',document:responsive.document(plan,invisiblePhoto,{key:'square',width:2048,height:2048},'mobile'),sources:[invisiblePhoto],responsive:{layoutVersion:responsive.layoutVersion,plan,images:[invisiblePhoto]}}),/photograph is not visible/);checks++;
 ok(e.board===beforeProofBoard&&JSON.stringify(e.document())===beforeProofDoc,'missing-photo rejection preserves the editable design and active artboard');
 for(const p of proofs){const {data,info}=await sharp(Buffer.from(p.dataBase64,'base64')).raw().toBuffer({resolveWithObject:true});let photoPixels=0;for(let i=0;i<data.length;i+=info.channels)if(Math.abs(data[i]-180)<12&&Math.abs(data[i+1]-155)<12&&Math.abs(data[i+2]-116)<12)photoPixels++;ok(photoPixels>info.width*info.height*.01,p.key+' includes photograph pixels');}
 const proofMeta=await sharp(Buffer.from(proofs[0].dataBase64,'base64')).metadata();ok(proofMeta.width===960&&proofMeta.height===960&&proofMeta.format==='jpeg','review uses actual bounded browser pixels');
 await e.dispose();dom.window.close();console.log('PASS '+checks+' responsive photo, mobile layout, typography and product scope checks');
})().catch(e=>{console.error(e.stack);process.exitCode=1});
