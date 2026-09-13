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
 const url='data:image/jpeg;base64,'+(await sharp({create:{width:1600,height:1000,channels:3,background:'#b49b74'}}).jpeg().toBuffer()).toString('base64'),photo={id:'photo',url,width:1600,height:1000};
 const e=await w.BritesAdEditor.open({title:'Duck necklace',workspaceId:'test',productId,groupRef,format:'square',photos:[],request:async action=>action==='adDesignEditorState'?{ok:true,sources:[photo],designs:[]}:action==='adDesignSavedDesigns'?{ok:true,savedDesigns:[]}:{ok:true}});
 const boxesOverlap=(a,b)=>Math.min(a.left+a.width,b.left+b.width)-Math.max(a.left,b.left)>1&&Math.min(a.top+a.height,b.top+b.height)-Math.max(a.top,b.top)>1;
 const incompletePlan=JSON.parse(JSON.stringify(plan));incompletePlan.layouts.forEach(l=>{l.showBrand=false;l.showButton=false;l.showHeadline=false;});
 for(const copy of [plan.copy,{headline:'Gold Peach Fruit Charm',shortHeadline:'Peach Fruit Charm',description:'A sweet gift for food lovers.',cta:'Shop Peach Charm'},{headline:'A Peach for Your Foodie',shortHeadline:'Peach Charm',description:'Gift-ready packaging included.',cta:'Shop charm'}])for(const board of responsive.variants){
   incompletePlan.copy=copy;if(copy.cta==='Shop charm')incompletePlan.style={...plan.style,background:'#2D231E',ink:'#FFF9F0'};else incompletePlan.style=plan.style; e.board=board;await e.restore(responsive.document(incompletePlan,photo,board,board.device));ok(['brand','headline','button'].every(role=>e.canvas.getObjects().some(o=>o.editorRole===role)),board.key+' restores essential ad content omitted by the AI recipe');for(const o of e.canvas.getObjects())e.fitAIText(o);e.canvas.renderAll();
   const text=e.canvas.getObjects().filter(o=>'text'in o||o.editorRole==='button');
   if(board.key.startsWith('display_')){
     ok(text.find(o=>o.editorRole==='brand').fontSize>=7.9,board.key+' brand remains legible at native size');
     ok(text.find(o=>o.editorRole==='button').getObjects().find(o=>'text'in o).fontSize>=11.9,board.key+' action remains legible at native size');
   }
   if(['square','landscape','portrait'].includes(board.key)){
     const brand=text.find(o=>o.editorRole==='brand'),previewWidth=board.device==='desktop'?600:360;
     ok(brand.fontSize*previewWidth/board.width>=8,board.key+' brand remains readable at typical display width');
     ok(text.some(o=>o.editorRole==='description'),board.key+' includes supporting product copy');
     ok(text.find(o=>o.editorRole==='button').getObjects().find(o=>'text'in o).fontSize*previewWidth/board.width>=11.9,board.key+' CTA remains readable at actual display width');
     ok(text.find(o=>o.editorRole==='description').fontSize*previewWidth/board.width>=11.9,board.key+' supporting copy remains readable at typical display width');
   }
   for(const o of text){const b=o.getBoundingRect();ok(b.left>=-1&&b.top>=-1&&b.left+b.width<=board.width+1&&b.top+b.height<=board.height+1,board.device+' '+board.key+' '+o.editorRole+' fits '+JSON.stringify(b));}
   for(let i=0;i<text.length;i++)for(let j=i+1;j<text.length;j++)ok(!boxesOverlap(text[i].getBoundingRect(),text[j].getBoundingRect()),board.key+' '+text[i].editorRole+' avoids '+text[j].editorRole);
   const image=e.canvas.getObjects().find(o=>o.type==='image');ok(image.scaleX===image.scaleY&&image.angle===0,'photo preserves proportions');
   if(['skyscraper','portrait','square'].includes(responsive.family(board))){const imageBox=image.getBoundingRect(),brand=text.find(o=>o.editorRole==='brand').getBoundingRect(),previewScale=['square','landscape','portrait'].includes(board.key)?board.width/(board.device==='desktop'?600:360):1;ok((brand.top-imageBox.top-imageBox.height)/previewScale<=30,board.key+' keeps photograph and copy together without an empty photo region');}
 }
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
