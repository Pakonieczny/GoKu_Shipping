// Charm placement check: where the charm really is in a photo (measured from its
// pixels, no AI request), and whether every ad size shows it whole and clear of
// text. Offline: drawn photos with a known charm, the saved-box mistake that
// cropped the Duck Charm Necklace ads, and a few real ad boards.
const assert=require('node:assert/strict'),path=require('node:path'),sharp=require('sharp');
const root=path.resolve(__dirname,'../..'),P=require(root+'/brites-ad-placement'),responsive=require(root+'/brites-ad-responsive');
let n=0;const ok=(v,m)=>{assert(v,m);n++;};

// A cream fabric photo with a gold charm (an oval body, a head and a jump ring) whose true box is known.
async function photo(w,h,box,{cutTop=0}={}){
 const cx=(box.x+box.width/2)*w,cy=(box.y+box.height/2)*h,rx=box.width*w/2,ry=box.height*h/2,top=cy-ry-cutTop*ry*2;
 const svg=`<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}"><defs><linearGradient id="bg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#efe4d0"/><stop offset="1" stop-color="#e6d8bf"/></linearGradient><radialGradient id="au" cx=".35" cy=".3" r=".9"><stop offset="0" stop-color="#fff0b8"/><stop offset=".5" stop-color="#d9a93a"/><stop offset="1" stop-color="#8a6214"/></radialGradient></defs><rect width="100%" height="100%" fill="url(#bg)"/><ellipse cx="${cx}" cy="${cy-cutTop*ry*2}" rx="${rx}" ry="${ry}" fill="url(#au)" stroke="#6f4c10" stroke-width="${Math.max(2,w*.004)}"/></svg>`;
 const {data,info}=await sharp(Buffer.from(svg)).resize({width:256,height:256,fit:'inside'}).removeAlpha().raw().toBuffer({resolveWithObject:true});
 return {data,width:info.width,height:info.height,channels:info.channels};
}
const covers=(a,t,tol=.03)=>a.x<=t.x+tol&&a.y<=t.y+tol&&a.x+a.width>=t.x+t.width-tol&&a.y+a.height>=t.y+t.height-tol;
const grow=(b,g)=>({x:b.x-g,y:b.y-g,width:b.width+2*g,height:b.height+2*g});
// The base plan of the responsive tests: soft-fade treatment, the copy the layouts place.
const plan={productId:'p1',groupRef:'g1',rationale:'Duck.',masterFormat:'portrait',imageDirections:[{concept:'x',composition:'x',lighting:'x',background:'x',preserveProduct:[],avoid:[],sourceIds:[]}],copy:{headline:'A Little Duck, Close to You',shortHeadline:'Your Little Duck',description:'Discover the duck pendant necklace.',cta:'Shop the necklace'},nativeCopy:{headlines:['Duck Necklace'],longHeadlines:['Discover the duck pendant necklace from Brites Jewelry.'],descriptions:['Explore the duck pendant necklace.']},style:{headlineFont:'Georgia',bodyFont:'Arial',background:'#fff9f0',ink:'#302318',accent:'#573a26',buttonInk:'#ffffff',treatment:'soft-fade'},layouts:['mobile','desktop'].flatMap(device=>['square','portrait','landscape','banner','skyscraper'].map(family=>({device,family,photoSide:'left',photoFraction:device==='mobile'?.6:.5,textAlign:'left',zoom:1,focalX:.5,focalY:.5,showHeadline:true,showDescription:device!=='mobile',showBrand:true,showButton:true}))),factClaims:[],sourceIds:[],limitations:[]};

(async()=>{
 const truth={x:.36,y:.1,width:.28,height:.3},W=1638,H=2048;
 // 1. The charm is found where it is, even when the AI box sits far off (Paul's Duck Charm Necklace case).
 const px=await photo(W,H,truth),exact=P.measureProduct(px,grow(truth,.03));
 ok(exact.confident&&exact.cutEdges.length===0&&covers(exact.box,truth),'an exact AI box is confirmed and the charm is complete');
 for(const [name,shift] of [['low',{y:.14}],['high',{y:-.06}],['left',{x:-.1}],['right',{x:.1}]]){
  const wrong=grow(truth,.03),moved={...wrong,x:wrong.x+(shift.x||0),y:wrong.y+(shift.y||0)},m=P.measureProduct(px,moved);
  ok(m.confident&&covers(m.box,truth),'the charm is still found when the AI box sits '+name);
 }
 // 2. A photo that itself cuts the charm is named, edge by edge.
 const cutBox={x:.36,y:.0,width:.28,height:.3},cut=P.measureProduct(await photo(W,H,cutBox,{cutTop:.25}),{x:.34,y:0,width:.32,height:.27});
 ok(cut.cutEdges.includes('top'),'a photo that cuts the charm at the top is flagged');
 ok(!exact.cutEdges.includes('top'),'a complete charm is not flagged');
 // 3. Every size is checked against the verified charm: the mistaken box cuts the top in the portrait sizes; the verified one does not.
 const image=(focus,check)=>({id:'portrait',width:W,height:H,focalX:.5,focalY:.5,sceneKey:'portrait',forFamilies:['portrait'],forBoards:[],focus,...(check?{focusCheck:check}:{})});
 const lowBox={...grow(truth,.03),y:grow(truth,.03).y+.14};
 const wrongCheck=P.measureProduct(px,lowBox),boards=[{key:'portrait',width:1080,height:1350},{key:'display_240x400',width:240,height:400},{key:'display_250x360',width:250,height:360},{key:'square',width:1080,height:1080}];
 let flagged=0,clean=0;
 for(const board of boards){
  const layoutBoard={...board,device:'mobile'};
  // What the studio made before: the layout trusted the unchecked box.
  const trusting=image(lowBox),chosen=responsive.selectImage(plan,[trusting],layoutBoard),doc=responsive.document(plan,chosen,layoutBoard,'mobile');
  const source={id:chosen.id,width:W,height:H,focus:lowBox,focusCheck:wrongCheck},result=P.checkDocument(doc,layoutBoard,[source],{label:board.key});
  if(!result.ok&&result.issues.some(i=>i.kind==='cut'))flagged++;
  // What it makes now: the layout uses the verified box, so the same size is whole.
  const verified=image(wrongCheck.box,wrongCheck),chosen2=responsive.selectImage(plan,[verified],layoutBoard),doc2=responsive.document(plan,chosen2,layoutBoard,'mobile');
  const again=P.checkDocument(doc2,layoutBoard,[{id:chosen2.id,width:W,height:H,focus:wrongCheck.box,focusCheck:wrongCheck}],{label:board.key});
  if(again.ok)clean++;
 }
 ok(flagged>=3,'a layout fitted to a wrong box is caught as cutting the charm ('+flagged+' of '+boards.length+' sizes)');
 ok(clean===boards.length,'the same sizes laid out around the verified charm show it whole ('+clean+' of '+boards.length+')');
 // 4. Nothing to check against never blocks: an unknown charm position is a warning, not a failure.
 const unknown=P.checkDocument(responsive.document(plan,image(null),{key:'portrait',width:1080,height:1350,device:'mobile'},'mobile'),{key:'portrait',width:1080,height:1350},[{id:'portrait',width:W,height:H,focus:null}],{label:'portrait'});
 ok(unknown.ok&&unknown.verified===false&&unknown.warnings.some(w=>w.kind==='unverified'),'an unknown charm position is reported as unverified, not as a failure');
 // 5. Text over the charm is found, and the summary names the size.
 const boardP={key:'portrait',width:1080,height:1350,device:'mobile'},base=responsive.document(plan,image(wrongCheck.box,wrongCheck),boardP,'mobile'),spot=P.checkDocument(base,boardP,[{id:'portrait',width:W,height:H,focus:wrongCheck.box,focusCheck:wrongCheck}],{label:'portrait'});
 ok(spot.verified&&spot.charm,'a verified size reports where the charm is');
 const c=spot.visible||spot.charm,over={...base,objects:[...base.objects,{type:'textbox',text:'Shop now',editorRole:'headline',id:'over',left:c.left,top:c.top,width:c.width,height:c.height,scaleX:1,scaleY:1,angle:0}]};
 const covered=P.checkDocument(over,boardP,[{id:'portrait',width:W,height:H,focus:wrongCheck.box,focusCheck:wrongCheck}],{label:'portrait'});
 ok(!covered.ok&&covered.issues.some(i=>i.kind==='covered'),'a headline placed over the charm is flagged as covering it');
 const said=P.describe([covered,spot]);ok(!said.ok&&said.failing.length===1&&said.failing[0]==='portrait'&&/portrait/.test(said.message),'the summary names the size that needs a fix');
 ok(P.describe([spot]).ok===true,'a clean set is summarised as fine');
 console.log('PASS '+n+' charm measurement and per-size placement checks');
})().catch(e=>{console.error(e);process.exit(1);});
