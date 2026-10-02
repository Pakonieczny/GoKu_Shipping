// Fixed-size Display ads publish their reviewed proof pixels, so those proofs are rendered and
// saved at the exact upload size (300×1050, 970×250, 980×120, 970×90 …), never saved at 960 px and
// enlarged. Older proofs capped at 960 px keep loading and publishing. Offline: fake providers only.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),sharp=require('sharp');
const root=path.resolve(__dirname,'../..'),responsive=require(root+'/brites-ad-responsive'),{plan}=require('./ad-responsive.cjs'),R=require(root+'/netlify/functions/googleAdsCampaignStyles');
let checks=0;const ok=(v,m)=>{assert(v,m);checks++;};
const LARGE=['display_300x1050','display_970x250','display_980x120','display_970x90'],DISPLAY=new Set(responsive.boards.filter(b=>b.key.startsWith('display_')).map(b=>b.width+'x'+b.height));
const capped=b=>{const s=Math.min(1,960/Math.max(b.width,b.height));return [Math.round(b.width*s),Math.round(b.height*s)];},exact=b=>DISPLAY.has(b.width+'x'+b.height)?[b.width,b.height]:capped(b);
// A detailed photo-like texture with hard text-like edges: the hardest case for the 150 KB limit.
async function texture(width,height){const raw=Buffer.alloc(width*height*3);let s=7;const rnd=()=>{s=(s*1103515245+12345)&0x7fffffff;return s/0x7fffffff;};for(let y=0;y<height;y++)for(let x=0;x<width;x++){const i=(y*width+x)*3,n=(rnd()-.5)*60,t=((x>>3)+(y>>4))%5===0?-90:0;raw[i]=Math.max(0,Math.min(255,180+n+t));raw[i+1]=Math.max(0,Math.min(255,155+n+t));raw[i+2]=Math.max(0,Math.min(255,116+n+t));}return sharp(raw,{raw:{width,height,channels:3}}).jpeg({quality:80}).toBuffer();}
(async()=>{
 // 1. Browser proofs: fixed Display boards (including an active Display artboard) keep their exact size.
 const {JSDOM}=require(process.env.BRITES_EDITOR_DOM_RUNTIME?path.join(process.env.BRITES_EDITOR_DOM_RUNTIME,'jsdom'):'jsdom');
 const dom=new JSDOM('<body></body>',{pretendToBeVisual:true,runScripts:'outside-only',resources:'usable',url:'https://example.test'}),w=dom.window;
 w.ResizeObserver=class{observe(){}disconnect(){}};w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.HTMLDialogElement.prototype.close=function(){this.open=false};
 for(const file of ['vendor/fabric-7.4.0.min.js','brites-ad-responsive.js','brites-ad-editor.js'])w.eval(fs.readFileSync(root+'/'+file,'utf8'));
 const brand=require(root+'/brites-brand-assets');w.BritesBrandAssets={get:id=>{const a=brand.get(id);return a?{...a,url:brand.dataUrl(id)}:undefined;}};
 const url='data:image/jpeg;base64,'+(await sharp({create:{width:1600,height:1000,channels:3,background:'#b49b74'}}).jpeg().toBuffer()).toString('base64'),photo={id:'photo',url,width:1600,height:1000,focus:{x:.47,y:.69,width:.06,height:.16}};
 const e=await w.BritesAdEditor.open({title:'Duck necklace',workspaceId:'test',productId:plan.productId,groupRef:plan.groupRef,format:'square',photos:[],request:async action=>action==='adDesignEditorState'?{ok:true,sources:[photo],designs:[]}:action==='adDesignSavedDesigns'?{ok:true,savedDesigns:[]}:{ok:true}});
 const tall={key:'display_300x1050',width:300,height:1050},variants=responsive.variants.filter(b=>LARGE.includes(b.key)||b.key==='landscape'&&b.device==='desktop');
 const candidate={artboard:tall,device:'desktop',document:responsive.document(plan,photo,tall,'desktop'),sources:[photo],responsive:{layoutVersion:responsive.layoutVersion,plan,images:[photo],variants}};
 const proofs=await e.renderAIProofs(candidate);ok(proofs.length===6,'active board and five variants rendered');
 for(const p of proofs){const m=await sharp(Buffer.from(p.dataBase64,'base64')).metadata(),[width,height]=exact(p);ok(m.format==='jpeg'&&Math.abs(m.width-width)<=1&&Math.abs(m.height-height)<=1&&(!DISPLAY.has(p.width+'x'+p.height)||m.width===p.width&&m.height===p.height),p.key+' proof is '+width+'×'+height);}
 const master=await sharp(Buffer.from(proofs.find(p=>p.key==='desktop_landscape').dataBase64,'base64')).metadata();ok(master.width===960&&Math.abs(master.height-503)<=1,'master artwork review proofs stay capped at 960 px');
 const dense=await e.renderAIProofs(candidate,{preview:true}),denseMeta=await sharp(Buffer.from(dense[0].dataBase64,'base64')).metadata();ok(denseMeta.format==='png'&&dense[0].width===300&&denseMeta.width>300,'high-density previews are unchanged');
 await e.dispose();dom.window.close();

 // 2. Server review: exact-size Display proofs are saved as sent; older capped proofs still load.
 const fixture=path.join(__dirname,'ad-design-workflow.cjs'),source=fs.readFileSync(fixture,'utf8').split('(async()=>{const e=await setup();')[0];
 const ctx=vm.createContext({require:require('node:module').createRequire(fixture),__dirname,process,Buffer,console,Date,setTimeout,clearTimeout});vm.runInContext(source+'\nglobalThis.setupFixture=setup;',ctx);
 // The borrowed setup answers the paid subject-focus request with the workflow's own focusReply fake, so both are taken together.
 const wf=fs.readFileSync(path.join(__dirname,'ad-responsive-workflow.cjs'),'utf8'),setup=vm.runInNewContext(wf.slice(wf.indexOf('function focusReply('),wf.indexOf('\n(async()=>{'))+';setup',{ctx,sharp,plan,require,framingScenes:require('./fixtures/jewelry-framing.cjs'),Date,Math,JSON,Buffer});
 async function awaiting(){const s=await setup({autoProofs:false});let status;for(let i=0;i<8;i++){await s.svc.editorAIRun({workspaceId:s.id,jobId:s.jobId});status=await s.svc.editorAIStatus({...s.input,jobId:s.jobId});if(status.phase==='awaiting_review')break;}assert.equal(status.phase,'awaiting_review');return {...s,candidate:status.candidate,bytes:await sharp({create:{width:1024,height:1024,channels:3,background:'#d1b284'}}).jpeg().toBuffer()};}
 const boardsOf=c=>[{...c.artboard,key:'active'},...responsive.variants.map(b=>({...b,key:b.device+'_'+b.key}))];
 const send=async(s,size,edit=p=>p)=>{const list=[];for(const b of boardsOf(s.candidate)){const [width,height]=size(b);list.push(edit({key:b.key,width:b.width,height:b.height,renderCheck:{version:1,visiblePhotoFraction:.3},dataBase64:(await sharp(s.bytes).resize(width,height,{fit:'fill'}).jpeg().toBuffer()).toString('base64')}));}return s.svc.editorAIResume({...s.input,jobId:s.jobId,candidateHash:s.candidate.candidateHash,reviewProofs:list});};
 const saved=s=>[...s.f.docs.entries()].find(([k])=>k.endsWith('/'+s.jobId+'/data/ad_proofs_v11'))?.[1];
 let s=await awaiting();await send(s,exact);let proof=saved(s);
 ok(proof&&proof.images.length===27&&proof.images.every(i=>{const [width,height]=exact(i);return i.asset.width===width&&i.asset.height===height;}),'exact-size Display proofs are stored as sent');
 for(const key of LARGE){const i=proof.images.find(i=>i.key==='desktop_'+key);ok(i.asset.width===i.width&&i.asset.height===i.height,key+' is saved at its full upload size');}
 s=await awaiting();await send(s,capped);proof=saved(s);ok(proof.images.find(i=>i.key==='desktop_display_300x1050').asset.height===960,'older 960 px proofs still load');
 s=await awaiting();await assert.rejects(()=>send(s,b=>b.key==='desktop_display_300x1050'?[290,1015]:exact(b)),/unexpected pixel dimensions/);ok(!saved(s),'a Display proof at any other size is refused');
 s=await awaiting();await assert.rejects(()=>send(s,b=>b.key==='desktop_landscape'?[1200,628]:exact(b)),/unexpected pixel dimensions/);ok(!saved(s),'master artwork is never accepted above its 960 px review size');

 // 3. Campaign Styles publish the reviewed pixels at exact size within Google's 150 KB limit.
 const pub=path.join(__dirname,'design-publication.cjs'),pubSource=fs.readFileSync(pub,'utf8').split('(async()=>{')[0],pctx=vm.createContext({require:require('node:module').createRequire(pub),__dirname,process,console,Buffer,Date,URL,setTimeout,clearTimeout});
 vm.runInContext(pubSource+'\nthis.factory=engine;this.memoryFactory=memory;',pctx);
 for(const legacy of [false,true]){
  const E=pctx.factory(),f=pctx.memoryFactory(),ws=f.db.collection('workspaces').doc('test'),images=[];
  for(const key of LARGE){const b=responsive.boards.find(x=>x.key===key),[width,height]=legacy?capped(b):[b.width,b.height],bytes=await texture(width,height),p='Brites_GAds_Creative/test/'+key+'.jpg';f.files.set(p,bytes);images.push({key:'desktop_'+key,width:b.width,height:b.height,asset:{path:p,width,height,bytes:bytes.length,hash:E.E.creativeHash(bytes.toString('base64'))}});}
  await ws.collection('editorAIJobs').doc('eai_test').collection('data').doc('ad_proofs_v11').set({images});
  const w={context:{itemIds:['shopify_US_1_2'],handle:'charms'},settings:{productId:'1',groupRef:'g'}};await ws.set(w);
  E.bind({fb:()=>f,_adDesignWorkspaceRef:()=>ws,_reportContext:async()=>({budgetCurrency:'CAD'}),_saveCreativeAsset:async(id,bytes,name,meta)=>{const p='Brites_GAds_Creative/test/'+name+'.jpg';f.files.set(p,bytes);return {...meta,path:p,bytes:bytes.length,hash:E.E.creativeHash(bytes.toString('base64'))};}});
  const styles=await E.get('_prepareCampaignStyles')({item:{sourceHash:'source',designReview:{workspaceId:'test',copy:{headlines:['Peach Charm'],longHeadlines:['Give a playful peach charm'],descriptions:['Shop the peach charm at Brites Jewelry.']},layoutReview:{jobId:'eai_test',reviewVersion:11}}},context:{w,product:{id:'1',title:'Peach Charm',url:'https://britesjewelry.com/products/peach'}},choice:R.selection(['fixed_display'],{fixed_display:5},['2840']),identity:'e'.repeat(64)});
  const fixed=styles.payload.generatedAssets.filter(a=>a.fixed).map(a=>a.asset);
  for(const key of LARGE){const b=responsive.boards.find(x=>x.key===key),a=fixed.find(a=>a.width===b.width&&a.height===b.height),m=a&&await sharp(f.files.get(a.path)).metadata();ok(a&&a.bytes<=150*1024&&m.width===b.width&&m.height===b.height,(legacy?'older ':'')+key+' publishes at '+b.width+'×'+b.height+' within 150 KB');}
 }
 console.log('PASS '+checks+' exact-size Display proof rendering, review storage and fixed publication checks');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
