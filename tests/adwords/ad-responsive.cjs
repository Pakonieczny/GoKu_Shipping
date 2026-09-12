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
 const {JSDOM}=require(process.env.BRITES_EDITOR_DOM_RUNTIME?path.join(process.env.BRITES_EDITOR_DOM_RUNTIME,'jsdom'):'jsdom');
 const dom=new JSDOM('<body></body>',{pretendToBeVisual:true,runScripts:'outside-only',resources:'usable',url:'https://example.test'}),w=dom.window;
 w.ResizeObserver=class{observe(){}disconnect(){}};w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.HTMLDialogElement.prototype.close=function(){this.open=false};
 for(const file of ['vendor/fabric-7.4.0.min.js','brites-ad-responsive.js','brites-ad-editor.js'])w.eval(fs.readFileSync(root+'/'+file,'utf8'));
 const url='data:image/jpeg;base64,'+(await sharp({create:{width:1600,height:1000,channels:3,background:'#b49b74'}}).jpeg().toBuffer()).toString('base64'),photo={id:'photo',url,width:1600,height:1000};
 const e=await w.BritesAdEditor.open({title:'Duck necklace',workspaceId:'test',productId,groupRef,format:'square',photos:[],request:async action=>action==='adDesignEditorState'?{ok:true,sources:[photo],designs:[]}:action==='adDesignSavedDesigns'?{ok:true,savedDesigns:[]}:{ok:true}});
 const boxesOverlap=(a,b)=>Math.min(a.left+a.width,b.left+b.width)-Math.max(a.left,b.left)>1&&Math.min(a.top+a.height,b.top+b.height)-Math.max(a.top,b.top)>1;
 for(const board of responsive.variants){
   e.board=board;await e.restore(responsive.document(plan,photo,board,board.device));for(const o of e.canvas.getObjects())e.fitAIText(o);e.canvas.renderAll();
   const text=e.canvas.getObjects().filter(o=>'text'in o||o.editorRole==='button');
   for(const o of text){const b=o.getBoundingRect();ok(b.left>=-1&&b.top>=-1&&b.left+b.width<=board.width+1&&b.top+b.height<=board.height+1,board.device+' '+board.key+' '+o.editorRole+' fits '+JSON.stringify(b));}
   for(let i=0;i<text.length;i++)for(let j=i+1;j<text.length;j++)ok(!boxesOverlap(text[i].getBoundingRect(),text[j].getBoundingRect()),board.key+' '+text[i].editorRole+' avoids '+text[j].editorRole);
   const image=e.canvas.getObjects().find(o=>o.type==='image');ok(image.scaleX===image.scaleY&&image.angle===0,'photo preserves proportions');
 }
 await e.dispose();dom.window.close();console.log('PASS '+checks+' responsive photo, mobile layout, typography and product scope checks');
})().catch(e=>{console.error(e.stack);process.exitCode=1});
