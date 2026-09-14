/* Shared semantic reflow for editable advertising artwork. No provider requests. */
(function(root){
  'use strict';
  const core=[['square',2048,2048],['landscape',2048,1072],['portrait',1638,2048]];
  const display=[[200,200],[240,400],[250,250],[250,360],[300,250],[336,280],[580,400],[120,600],[160,600],[300,600],[300,1050],[468,60],[728,90],[930,180],[970,90],[970,250],[980,120],[300,50],[320,50],[320,100]];
  const boards=core.concat(display.map(([w,h])=>['display_'+w+'x'+h,w,h])).map(([key,width,height])=>({key,width,height}));
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,Number(v)||0)),family=b=>b.width/b.height>3?'banner':b.width/b.height<.5?'skyscraper':b.width/b.height>1.3?'landscape':b.width/b.height<.9?'portrait':'square';
  function selectImage(plan,images,board){const f=family(board);return images.find(i=>i.forBoards?.includes(board.key))||images.find(i=>i.forFamilies?.includes(f))||images[0];}
  // Design at the actual viewing width, then export at the requested resolution.
  // A 2048px master must not turn a 36px CTA into a 6px mobile label.
  const layoutVersion=30;
  const brands=typeof module==='object'&&module.exports?require('./brites-brand-assets'):root.BritesBrandAssets;
  // The scene catalog matches meaningful crop families rather than charging for
  // every output size. Slim skyscrapers may receive an extra composition.
  const sceneCatalog=[
    {key:'landscape',format:{key:'landscape',width:2048,height:1072,requestSize:'2064x1088'},families:['landscape'],boards:[],direction:'Product on the right, within x 0.55–0.93 and y 0.08–0.92. The left 46 percent is continuous quiet photographic surface for the existing centered brand, title and action.'},
    {key:'square',format:{key:'square',width:2048,height:2048,requestSize:'2048x2048'},families:['square'],boards:[],direction:'Complete product centered horizontally within x 0.30–0.70 and y 0.04–0.42, occupying at most 38 percent of source height. The renderer zooms this into a dominant product view; this extra scene area is essential for both square and 300x250/336x280 crops. Keep the remaining lower surface quiet and continuous for captions. No separate footer.'},
    {key:'portrait',format:{key:'portrait',width:1638,height:2048,requestSize:'1648x2048'},families:['portrait'],boards:[],direction:'Complete product in the upper 58 percent, centered horizontally, with the lower third quiet for the existing brand, product name and action. Protect a 4:5 crop as well as 3:5; do not place props behind the captions.'},
    {key:'tall',format:{key:'portrait',width:1024,height:3072,requestSize:'1024x3072'},families:['skyscraper'],boards:['display_300x600'],direction:'A deliberate tall photograph. Keep the entire jewelry in x 0.27–0.73, y 0.18–0.60 so narrow horizontal crops retain the product. Lower third is an uninterrupted surface for the existing centered copy stack. Compose the vertical space deliberately with a subtle diagonal of peach-colored fabric, a small peach slice or a verified packaging edge above or below the charm. Keep props secondary, separated from the jewelry and outside the lower copy area. Do not add extra jewelry, chains or imply accessories are included. Avoid a large empty flat field; use depth and restrained editorial still-life context.'},
    {key:'banner',format:{key:'landscape',width:3072,height:1024,requestSize:'3072x1024'},families:['banner'],boards:[],direction:'Close jewelry view in the left third, with uninterrupted calm surface and matching illumination extending right. A product-free right-side surface supplies the tonal continuation for ultra-wide banners. Avoid horizontal seams, horizon lines or props in that continuation.'},
    {key:'slim',format:{key:'portrait',width:1024,height:3072,requestSize:'1024x3072'},families:[],boards:['display_120x600','display_160x600'],direction:'Dedicated slim crop. Keep complete jewelry including hardware in the central 42 percent of the source width and upper 60 percent of height. Keep the lower third quiet and continuous. This scene is specifically for 1:5 and 4:15 banners. Use an intentional vertical arrangement with a restrained peach or fabric accent above or below the product, never beside its narrow protected silhouette or behind the lower copy. No extra jewelry or invented included accessories.'}
  ];
  function cleanCrop(image,board){
    const ratio=board.width/board.height,w=image.width,h=image.height,cw=Math.min(w,h*ratio),ch=cw/ratio,f=image.focus;
    let x=(w-cw)/2,y=(h-ch)/2;
    if(f){
      const gap=Math.min(cw,ch)*.02,fx=f.x*w,fy=f.y*h,fr=(f.x+f.width)*w,fb=(f.y+f.height)*h;
      if(fr-fx>cw||fb-fy>ch)throw Error('The '+board.key+' scene cannot retain the complete product. Use its dedicated composition.');
      const origin=(start,end,size,total)=>{const lo=Math.max(0,end-size),hi=Math.min(total-size,start),pLo=Math.max(0,end+gap-size),pHi=Math.min(total-size,start-gap);return clamp((start+end-size)/2,pLo<=pHi?pLo:lo,pLo<=pHi?pHi:hi);};
      x=origin(fx,fr,cw,w);y=origin(fy,fb,ch,h);
    }
    return {x:x/w,y:y/h,width:cw/w,height:ch/h};
  }
  function atmosphericScene(objects,image,W,H,style,board){
    const p=objects.find(o=>o.editorRole==='photo'),f=image.focus;
    if(!p||!f||!['x','y','width','height'].every(k=>Number.isFinite(f[k]))||f.width<=0||f.height<=0)return {mode:'legacy',reason:'Product localization is required before extending the scene.'};
    const s=p.scaleX,subject={x:p.left+(f.x*image.width-(p.cropX||0))*s,y:p.top+(f.y*image.height-(p.cropY||0))*s,w:f.width*image.width*s,h:f.height*image.height*s};
    const layers=objects.filter(o=>['headline','description','brand','button'].includes(o.editorRole)),box=o=>({x:o.left,y:o.top,w:o.width*(o.scaleX||1),h:o.height*(o.scaleY||1)}),boxes=layers.map(box);
    const left=Math.min(...boxes.map(b=>b.x)),right=Math.max(...boxes.map(b=>b.x+b.w)),top=Math.min(...boxes.map(b=>b.y));
    const gap=Math.max(1,Math.min(W,H)*.005),axis=right<=subject.x-gap?'left':left>=subject.x+subject.w+gap?'right':top>=subject.y+subject.h+gap?'bottom':null;
    if(!axis)return {mode:'legacy',reason:'The protected product and the messaging region need separate space.'};
    const rgb=style.background.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16));
    // Choose opacity against both black and white underlying pixels, rather than
    // assuming that a light scene will always stay light under every letter.
    const lum=a=>a.map(v=>v/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4).reduce((n,v,i)=>n+v*[.2126,.7152,.0722][i],0),ink=lum(style.ink.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)));
    let opacity=.90;for(;opacity<1;opacity+=.005){if([0,255].every(v=>{const b=lum(rgb.map(c=>c*opacity+v*(1-opacity)));return (Math.max(ink,b)+.05)/(Math.min(ink,b)+.05)>=4.5;}))break;}opacity=Math.min(1,opacity);
    const span=axis==='bottom'?H:W,start=axis==='left'?right:axis==='right'?subject.x+subject.w+gap:subject.y+subject.h+gap,end=axis==='left'?subject.x-gap:axis==='right'?left:top;
    const stops=axis==='left'?[[0,opacity],[start/span,opacity],[end/span,0],[1,0]]:[[0,0],[start/span,0],[end/span,opacity],[1,Math.min(1,opacity+.02)]];
    // Prefer one complete photographic plane at the already-approved crop/scale.
    // Expanding the viewport never changes the jewelry's position or size.
    const vx=p.left-(p.cropX||0)*s,vy=p.top-(p.cropY||0)*s,x=Math.max(0,vx),y=Math.max(0,vy),r=Math.min(W,vx+image.width*s),b=Math.min(H,vy+image.height*s);
    const expanded={...p,left:x,top:y,cropX:(x-vx)/s,cropY:(y-vy)/s,width:(r-x)/s,height:(b-y)/s};
    // Continuations may only join beneath the protected messaging wash. An
    // uncovered edge in the clear product region needs a dedicated photograph,
    // not an obvious patched seam. Saved artwork keeps its earlier safe layout.
    const washAt=t=>{if(t<=stops[1][0])return stops[1][1];if(t>=stops[2][0])return stops[2][1];const u=(t-stops[1][0])/(stops[2][0]-stops[1][0]);return stops[1][1]+(stops[2][1]-stops[1][1])*(u*u*(3-2*u));};
    const insetBanner=['display_468x60','display_728x90','display_930x180','display_970x90','display_980x120'].includes(board.key)&&axis==='right'&&x<=W*.04&&f.x>.015;
    const edgeSurface=insetBanner&&x>.01?{...expanded,id:'ai_banner_edge',name:'Photographic edge margin',editorRole:'shape',left:0,width:1,cropX:0,scaleX:x,filters:[]}:null;
    if((x>.01&&!edgeSurface&&(axis!=='left'||washAt(x/W)<.88))||(r<W-.01&&(axis!=='right'||washAt(r/W)<.88))||y>.01||(b<H-.01&&(axis!=='bottom'||washAt(b/H)<.88)))return {mode:'legacy',reason:'This saved photo needs a dedicated '+family(board)+' scene to extend cleanly without a visible join.'};
    let background=null,mode='native';
    if(x>.01||y>.01||r<W-.01||b<H-.01){
      // Old photographs and ratios wider than the provider's 3:1 limit need a
      // quiet continuation. Sample only a product-free source region; never
      // stretch, clone or blur the jewelry to manufacture a background.
      const pad=.02,fx=Math.max(0,f.x-pad),fy=Math.max(0,f.y-pad),fr=Math.min(1,f.x+f.width+pad),fb=Math.min(1,f.y+f.height+pad);
      const patches=[{x:0,y:0,w:fx,h:1},{x:fr,y:0,w:1-fr,h:1},{x:0,y:0,w:1,h:fy},{x:0,y:fb,w:1,h:1-fb}].filter(a=>a.w>.025&&a.h>.025).sort((a,b)=>b.w*b.h-a.w*a.h),a=patches[0];
      if(!a)return {mode:'legacy',reason:'No product-free photographic surface is available for this crop.'};
      const sw=a.w*image.width,sh=a.h*image.height,scale=Math.max(W/sw,H/sh);
      background={...p,id:'ai_atmosphere_surface',name:'Continuous photographic surface',editorRole:'shape',left:0,top:0,width:W/scale,height:H/scale,cropX:a.x*image.width+(sw-W/scale)/2,cropY:a.y*image.height+(sh-H/scale)/2,scaleX:scale,scaleY:scale,filters:[]};mode='continued';
    }
    const fade={id:'ai_atmosphere_fade',name:'Protective translucent fade',editorRole:'shape',type:'Rect',originX:'left',originY:'top',left:0,top:0,width:W,height:H,scaleX:1,scaleY:1,strokeWidth:0,opacity:1,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:axis==='bottom'?0:1,y2:axis==='bottom'?1:0},colorStops:stops.flatMap((point,i)=>i===1?[point,...[.2,.4,.6,.8].map(t=>[point[0]+(stops[2][0]-point[0])*t,point[1]+(stops[2][1]-point[1])*(t*t*(3-2*t))])]:[point]).map(([offset,a])=>({offset:Math.max(0,Math.min(1,offset)),color:'rgba('+rgb.join(',')+','+a+')'}))}};
    objects.splice(0,objects.length,...(background?[background]:[]),...(edgeSurface?[edgeSurface]:[]),expanded,fade,...layers);
    return {mode,axis,subject:{x:subject.x/W,y:subject.y/H,w:subject.w/W,h:subject.h/H},opacity,sourceKey:image.id,reason:mode==='continued'?'A protected product-free surface continues the saved scene.':null};
  }

  function document(plan,image,board,device="mobile"){
    const master=['square','landscape','portrait'].includes(board.key),factor=master?board.width/Math.min(board.width,360):1;
    const W=board.width/factor,H=board.height/factor,f=family(board),layout=(plan.layouts||[]).find(l=>l.family===f&&l.device===device)||(plan.layouts||[]).find(l=>l.family===f)||{},style={...plan.style},copy=plan.copy;
    const rgb=style.background.match(/[a-f0-9]{2}/gi)?.map(x=>parseInt(x,16));if(style.treatment!=='soft-fade'&&rgb&&rgb[0]<90&&rgb[0]>=rgb[1]&&rgb[1]>=rgb[2])Object.assign(style,{background:'#F5F0E8',ink:'#34281E',accent:'#4B3825',buttonInk:'#FFF9F0',headlineFont:'Georgia'});
    if(style.treatment==='soft-fade'){
      const luminance=hex=>hex.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4).reduce((n,v,i)=>n+v*[.2126,.7152,.0722][i],0);
      const contrast=(a,b)=>(Math.max(luminance(a),luminance(b))+.05)/(Math.min(luminance(a),luminance(b))+.05);
      // Preserve the chosen hue while making typography readable over its tonal fade.
      const readable=(color,bg)=>{if(contrast(color,bg)>=4.5)return color;const rgb=color.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)),toward=luminance(bg)>.18?0:255;for(let n=1;n<=20;n++){const c='#'+rgb.map(v=>Math.round(v+(toward-v)*n/20).toString(16).padStart(2,'0')).join('');if(contrast(c,bg)>=4.5)return c;}return toward?'#ffffff':'#000000';};
      style.ink=readable(style.ink,style.background);style.buttonInk=readable(style.buttonInk,style.accent);
    }
    const base=(id,role,extra)=>({id:'ai_'+id,name:id,editorRole:role,originX:'left',originY:'top',angle:0,opacity:1,scaleX:1,scaleY:1,strokeWidth:0,...extra});
    const objects=[],margin=Math.max(3,Math.min(6,Math.min(W,H)*.03)),banner=f==='banner',narrow=f==='skyscraper';
    const typeScale=Math.max(1,Math.min(1.35,W/440)),description=copy.description.startsWith(copy.shortHeadline+'. ')?copy.description.slice(copy.shortHeadline.length+2):copy.description;
    const lineHeight=1.08;
    function lines(value,width,size){return String(value).split(/\n/).reduce((sum,line)=>sum+Math.max(1,line.split(/\s+/).reduce((a,word)=>{const n=word.length*size*.58;if(a.used&&a.used+size*.3+n>width){a.count++;a.used=n;}else a.used+=n+size*.3;return a;},{count:1,used:0}).count),0);}
    function text(id,value,x,y,width,height,size,role,font=style.bodyFont){if(value)objects.push(base(id,role,{type:'Textbox',text:value,left:x,top:y,width,height,aiBoxHeight:height,fontFamily:font,fontSize:size,fontWeight:role==='headline'?(style.headlineWeight||'700'):(style.bodyWeight||'400'),fill:style.ink,lineHeight,charSpacing:role==='brand'?35:0,textAlign:'left'}));}
    function brandRow(x,y,width,options={}){
      const icon=brands?.get('brites_brand_icon'),ih=options.iconHeight||rules.brand.iconHeight,iw=ih*789/592,gap=7,size=options.fontSize||(width<140?rules.brand.narrowTextSize:rules.brand.minimumTextSize);
      const single='BRITES JEWELRY',oneWidth=single.length*size*.64,wrapped=iw+gap+oneWidth>width,label=wrapped?'BRITES\nJEWELRY':single,tw=Math.min(width-iw-gap,(wrapped?7:single.length)*size*.64),total=iw+gap+tw,left=x+(width-total)/2;
      if(icon)objects.push(base('brand_icon','brand',{type:'Image',sourceKey:icon.id,left,top:y,width:icon.width,height:icon.height,scaleX:ih/icon.height,scaleY:ih/icon.height}));
      const th=wrapped?size*2.3:size*1.3;text('brand',label,left+iw+gap,y+(ih-th)/2,tw,th,size,'brand');
    }
    function button(x,y,width,height,label=copy.cta,size=14,flexible=false){
      if(!banner&&!flexible){const nw=Math.min(rules.button.maximumWidth,width);x+=(width-nw)/2;y+=(height-rules.button.height)/2;width=nw;height=rules.button.height;size=rules.button.fontSize;}

      // Select a short action before fitting; never squeeze a two-line label into a small banner.
      if(label.length*size*.65+12>width)label='Shop now';
      if(label.length*size*.65+12>width)label='Shop';
      objects.push(base('cta','button',{type:'Group',left:x,top:y,width,height,buttonPadding:4,objects:[{type:'Rect',originX:'left',originY:'top',left:-width/2,top:-height/2,width,height,fill:style.accent,strokeWidth:0,rx:style.preserveSavedStyle?height*style.buttonRadius:3,ry:style.preserveSavedStyle?height*style.buttonRadius:3},{type:'Textbox',originX:'center',originY:'center',left:0,top:0,width:width-8,height:16,text:label,fontFamily:style.buttonFont||style.bodyFont,fontWeight:style.buttonWeight||'700',fontSize:size,fill:style.buttonInk,textAlign:'center',lineHeight:1}]}));
    }
    // Frame the located charm, not the full chain or photograph. Small placements
    // receive tighter crops; larger ones retain a little photographic context.
    // Legacy unlocated photographs keep their conservative framing until located.
    function photograph(frame){
      const focus=image.focus,valid=focus&&['x','y','width','height'].every(k=>Number.isFinite(focus[k]))&&focus.width>0&&focus.height>0;
      const small=Math.min(frame.width,frame.height)<=100,padding=['display_120x600','display_160x600','display_300x600'].includes(board.key)?1.30:narrow||board.key==='landscape'?1.14:1.025;
      const cover=Math.max(frame.width/image.width,frame.height/image.height);
      const safeWidth=image.width/image.height>1.3?image.width*.30:image.width;
      const desired=valid?Math.min(frame.width/(image.width*focus.width*padding),frame.height/(image.height*focus.height*padding)):cover;
      const maximum=valid?Math.min(frame.width/(image.width*focus.width*padding),frame.height/(image.height*focus.height*padding)):cover;
      const scale=valid?Math.min(maximum,Math.max(cover,desired)):Math.min(cover,frame.width/safeWidth);
      const cw=Math.min(image.width,frame.width/scale),ch=Math.min(image.height,frame.height/scale);
      const cx=clamp(valid?image.width*(focus.x+focus.width/2)-cw/2:(image.width-cw)/2,0,image.width-cw),cy=clamp(valid?image.height*(focus.y+focus.height/2)-ch/2:(image.height-ch)/2,0,image.height-ch);
      const placed={left:frame.left+(frame.width-cw*scale)/2,top:frame.top+(frame.height-ch*scale)/2,width:cw*scale,height:ch*scale};
      objects.unshift(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:placed.left,top:placed.top,width:cw,height:ch,cropX:cx,cropY:cy,scaleX:scale,scaleY:scale}));
      if(valid&&placed.height<frame.height-1&&style.treatment==='soft-fade'){
        // Extend the photographic atmosphere through tall placements without
        // enlarging the foreground beyond the complete product's safe crop.
        const rgb=style.background.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)).join(','),edge=Math.min(32,placed.height*.14);
        for(const top of [true,false])objects.push(base('photo_feather_'+top,'shape',{type:'Rect',left:placed.left,top:top?placed.top:placed.top+placed.height-edge,width:placed.width,height:edge,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:0,y2:1},colorStops:[{offset:0,color:'rgba('+rgb+','+(top?1:0)+')'},{offset:1,color:'rgba('+rgb+','+(top?0:1)+')'}]}}));
        for(const top of [true,false]){const height=Math.min(edge*2,top?placed.top-frame.top:frame.top+frame.height-placed.top-placed.height);if(height>0)objects.push(base('scene_join_'+top,'shape',{type:'Rect',left:frame.left,top:top?placed.top-height:placed.top+placed.height,width:frame.width,height,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:0,y2:1},colorStops:[{offset:0,color:'rgba('+rgb+','+(top?0:1)+')'},{offset:1,color:'rgba('+rgb+','+(top?1:0)+')'}]}}));}

      }

      // Fill uncovered photo space from a product-free patch of the same scene.
      // This retains surface texture without stretching or repeating the charm.
      if(valid&&(placed.width<frame.width-1||placed.height<frame.height-1)){
        const patches=[{x:(focus.x+focus.width)*image.width,y:focus.y*image.height,w:(1-focus.x-focus.width)*image.width,h:focus.height*image.height},{x:0,y:focus.y*image.height,w:focus.x*image.width,h:focus.height*image.height},{x:0,y:(focus.y+focus.height)*image.height,w:image.width,h:(1-focus.y-focus.height)*image.height},{x:0,y:0,w:image.width,h:focus.y*image.height}].filter(p=>p.w>image.width*.025&&p.h>image.height*.025),patch=patches[0];
        if(patch){const s=Math.max(frame.width/patch.w,frame.height/patch.h);objects.unshift(base('scene_extension','shape',{type:'Image',sourceKey:image.id,left:frame.left,top:frame.top,width:frame.width/s,height:frame.height/s,cropX:patch.x,cropY:patch.y,scaleX:s,scaleY:s,filters:[{type:'Blur',blur:.12}]}));}
      }
      return placed;
    }
    function bottomFade(y,height){const rgb=style.background.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)).join(',');objects.push(base('photo_caption_fade','shape',{type:'Rect',left:0,top:y,width:W,height,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:0,y2:1},colorStops:[{offset:0,color:'rgba('+rgb+',0)'},{offset:.55,color:'rgba('+rgb+',.4)'},{offset:1,color:'rgba('+rgb+',1)'}]}}));}
    function fullBleedLandscape(){
      if(board.key!=='landscape'||style.treatment!=='soft-fade'||!image.focus)return false;
      const q=image.focus,scale=Math.max(W/image.width,H/image.height,Math.min(W*.43/(image.width*q.width*1.04),H*.86/(image.height*q.height*1.04))),cw=W/scale,ch=H/scale;
      const cx=clamp(image.width*(q.x+q.width/2)-cw*.66,0,image.width-cw),cy=clamp(image.height*(q.y+q.height/2)-ch/2,0,image.height-ch);
      const subject={left:(image.width*q.x-cx)*scale,top:(image.height*q.y-cy)*scale,width:image.width*q.width*scale,height:image.height*q.height*scale},pad=Math.max(6,W*.025),tw=Math.min(W*.44,subject.left-pad*2);
      if(tw<76||subject.top<0||subject.top+subject.height>H)return false;
      objects.push(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:0,top:0,width:cw,height:ch,cropX:cx,cropY:cy,scaleX:scale,scaleY:scale}));
      const rgb=style.background.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)).join(',');
      objects.push(base('image_fade','shape',{type:'Rect',left:0,top:0,width:W,height:H,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:1,y2:0},colorStops:[{offset:0,color:'rgba('+rgb+',1)'},{offset:Math.max(0,(tw-pad)/W),color:'rgba('+rgb+',.92)'},{offset:subject.left/W,color:'rgba('+rgb+',0)'},{offset:1,color:'rgba('+rgb+',0)'}]}}));
      const hs=Math.min(26,Math.max(20,W*.06),tw/(Math.max(...copy.shortHeadline.split(/\s+/).map(w=>w.length))*.67)),hh=lines(copy.shortHeadline,tw,hs)*hs*1.24,brandH=32,bh=32,gap=10,total=brandH+hh+bh+gap*2,y=(H-total)/2,bw=Math.min(108,tw*.84);
      brandRow(pad,y,tw);text('headline',copy.shortHeadline,pad,y+brandH+gap,tw,hh,hs,'headline',style.headlineFont);objects[objects.length-1].textAlign='center';button(pad+(tw-bw)/2,y+brandH+gap+hh+gap,bw,bh,copy.cta,14,true);return true;
    }
    function tallLayout(){
      if(!narrow)return false;
      const pad=Math.max(5,Math.min(12,W*.04)),photoHeight=H*2/3,available=H-photoHeight;
      photograph({left:0,top:0,width:W,height:photoHeight});bottomFade(photoHeight-Math.min(35,W*.2),Math.min(35,W*.2));
      const titleWidth=W-pad*2,brandH=W>=240?48:36,brandSize=W>=240?19:12,buttonH=W>=240?48:36,buttonW=Math.min(titleWidth,W>=240?210:128),buttonSize=W>=240?20:15;
      let titleSize=Math.min(44,Math.max(22,W*.15));
      titleSize=Math.min(titleSize,titleWidth/(Math.max(...copy.shortHeadline.split(/\s+/).map(w=>w.length))*.67));
      while(titleSize>18&&brandH+lines(copy.shortHeadline,titleWidth,titleSize)*titleSize*1.24+buttonH+24>available-16)titleSize-=1;
      const titleHeight=lines(copy.shortHeadline,titleWidth,titleSize)*titleSize*1.24,gap=Math.max(10,Math.min(30,(available-brandH-titleHeight-buttonH)*.25)),brandGap=board.key==='display_300x1050'?Math.min(24,gap):gap,actionGap=board.key==='display_300x1050'?Math.min(20,gap):gap,blockHeight=brandH+titleHeight+buttonH+brandGap+actionGap,y=photoHeight+(available-blockHeight)/2;
      brandRow(pad,y,titleWidth,{iconHeight:brandH,fontSize:brandSize});
      text('headline',copy.shortHeadline,pad,y+brandH+brandGap,titleWidth,titleHeight,titleSize,'headline',style.headlineFont);
      objects[objects.length-1].textAlign='center';button((W-buttonW)/2,y+brandH+brandGap+titleHeight+actionGap,buttonW,buttonH,copy.cta,buttonSize,true);return true;
    }

    function softFade(){
      const focus=image.focus,side=W/H>=1.3&&W>=300&&H>=160;
      if(style.treatment!=='soft-fade'||banner||(!side&&!(W>=240&&H>=280||narrow&&W>=120&&H>=400))||!focus||!['x','y','width','height'].every(k=>Number.isFinite(focus[k]))||focus.width<=0||focus.height<=0)return false;
      const pad=Math.max(6,Math.min(22,W*.045)),gap=Math.max(5,Math.min(12,H*.025));
      const bw=narrow?W-pad*2:Math.min(side?W*.42:W*.66,Math.max(96,copy.cta.length*(style.preserveSavedStyle?9:7)+24)),bh=Math.min(44,Math.max(32,H*.1));
      const row=false,tw=side?W*.47-pad*2:row?W-bw-pad*3:W-pad*2;
      let headline=copy.shortHeadline,hs=side?Math.min(48,Math.max(28,W*.083)):Math.min(narrow?40:30,Math.max(21,W*(narrow?.12:.085)));
      if(lines(headline,tw,hs)>3)headline=copy.shortHeadline;
      hs=Math.min(hs,tw/(Math.max(...headline.split(/\s+/).map(w=>w.length))*.67));
      const hh=lines(headline,tw,hs)*hs*1.24,brand=true,brandH=brand?(side?44:34):0,bodySize=side?Math.max(14,Math.min(18,W*.03)):14;
      const bodyH=lines(description,tw,bodySize)*bodySize*1.3,body=(!side&&style.preserveSavedStyle&&!narrow&&H>=400)&&bodyH<=H*.17&&brandH+hh+bodyH+bh+gap*4<H*.86;
      const total=brandH+(brand?gap:0)+hh+(body?bodyH+gap:0)+(row?0:bh+gap);let top=side?Math.max(pad,(H-total)/2):H-pad-Math.max(total,row?bh:0);
      if(!side&&top<H*.50)return false;
      const sideMargin=board.key==='display_300x600'?W*.10:pad;
      const region=side?{left:W*.55,top:pad,width:W*.45-pad,height:H-pad*2}:{left:sideMargin,top:pad,width:W-sideMargin*2,height:top-pad*2};
      const scale=Math.max(W/image.width,side?H/image.height:0,Math.min(region.width/(image.width*focus.width*1.04),region.height/(image.height*focus.height*1.04)));
      const cw=W/scale;let ch=Math.min(image.height,(side?H:top)/scale),cx=clamp(image.width*(focus.x+focus.width/2)-(region.left+region.width/2)/scale,0,image.width-cw),cy=clamp(image.height*(focus.y+focus.height/2)-(region.top+region.height/2)/scale,0,image.height-ch);
      const subject={left:(image.width*focus.x-cx)*scale,top:(image.height*focus.y-cy)*scale,width:image.width*focus.width*scale,height:image.height*focus.height*scale};
      if(subject.left<region.left||subject.top<region.top||subject.left+subject.width>region.left+region.width||subject.top+subject.height>region.top+region.height)return false;
      const photoTop=0;if(narrow){cy=clamp(cy+(subject.top-pad)/scale,0,image.height);subject.top=(image.height*focus.y-cy)*scale;top=subject.top+subject.height+gap*2;ch=Math.min(image.height-cy,top/scale);}
      if(style.preserveSavedStyle&&!narrow)ch=Math.min(image.height-cy,H/scale);
      objects.push(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:0,top:photoTop,width:cw,height:ch,cropX:cx,cropY:cy,scaleX:scale,scaleY:scale}));
      const stops=side?[[0,1],[.40,.98],[.55,0],[1,0]]:[...(photoTop>0?[[0,1],[photoTop/H,1],[(photoTop+Math.min(40,Math.max(1,subject.top-photoTop)))/H,0]]:[[0,0]]),[Math.max(0,(subject.top+subject.height)/H),0],[Math.min((top+(style.preserveSavedStyle&&!narrow?total*.75:0))/H,(photoTop+ch*scale)/H),1],[1,1]];
      objects.push(base('image_fade','shape',{type:'Rect',left:0,top:0,width:W,height:H,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:side?1:0,y2:side?0:1},colorStops:stops.map(([offset,opacity])=>({offset,color:'rgba('+style.background.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)).join(',')+','+opacity+')'}))}}));
      let y=top;if(brand){brandRow(pad,y,tw,side?{iconHeight:44,fontSize:18}:{});y+=brandH+gap;}
      text('headline',headline,pad,y,tw,hh,hs,'headline',style.headlineFont);y+=hh+gap;
      if(body){text('description',description,pad,y,tw,bodyH,bodySize,'description');y+=bodyH+gap;}
      if(side)button(pad+(tw-Math.min(172,tw))/2,y,Math.min(172,tw),44,copy.cta,20,true);else button(pad,y,tw,bh,copy.cta,14);
      for(const o of objects)if(o.editorRole==='headline'||o.editorRole==='description')o.textAlign='center';
      return true;
    }
    const compactHeadline=copy.shortHeadline;
    if(tallLayout()){
      // Fixed upper image region and lower messaging region for skyscrapers.
    }else if(fullBleedLandscape()){
      // Use the actual photograph edge to edge, with copy in its quiet side.
    }else if(board.key==='landscape'){
      const pad=8,tw=W*.46-pad*2,hs=22,hh=lines(copy.shortHeadline,tw,hs)*hs*1.24,gap=10,bh=32,total=32+hh+bh+gap*2,y=(H-total)/2,bw=Math.min(108,tw*.84);
      photograph({left:W*.48,top:0,width:W*.52,height:H});brandRow(pad,y,tw);text('headline',copy.shortHeadline,pad,y+32+gap,tw,hh,hs,'headline',style.headlineFont);objects[objects.length-1].textAlign='center';button(pad+(tw-bw)/2,y+32+gap+hh+gap,bw,bh,copy.cta,14,true);
    }else if(softFade()){
      // The photograph fills the artboard; the editable fade protects only the copy.
    }else if(banner){
      const inset=['display_728x90','display_970x90','display_980x120'].includes(board.key)?Math.max(14,Math.round(H*.15)):Math.max(5,H*.035);
      const outer=['display_468x60','display_728x90','display_930x180','display_970x90','display_980x120'].includes(board.key)?Math.min(W*.035,H*.30):0;
      const pw=Math.min(W*.27,H*1.08),tx=outer+pw+inset,right=Math.max(5,H*.035,outer);
      photograph({left:outer,top:0,width:pw,height:H});
      // A dedicated brand column lets the actual icon occupy most of a short
      // banner's height instead of becoming a tiny inline text decoration.
      const icon=brands?.get('brites_brand_icon'),ih=H<=60?H*.72:Math.min(W<=320?48:120,H*.65),iw=ih*789/592,logoGap=Math.max(8,H*.06),tw=W-tx-right-(icon?iw+logoGap:0);
      const bs=Math.max(14,Math.min(36,H*.22)),headline=compactHeadline,hs=Math.max(16,Math.min(H*.38,H>=200?64:42,tw/(Math.max(1,headline.length)*.59))),hh=lines(headline,tw,hs)*hs*1.18;
      const brandH=bs*1.3,gap=Math.max(3,H*.045),total=hh+brandH+gap,y=Math.max(2,(H-total)/2);
      text('headline',headline,tx,y,tw,hh,hs,'headline',style.headlineFont);
      text('brand','Brites Jewelry',tx,y+hh+gap,tw,brandH,bs,'brand');
      for(const o of objects)if(o.type==='Textbox')o.textAlign='center';
      if(icon)objects.push(base('brand_icon','brand',{type:'Image',sourceKey:icon.id,left:W-right-iw,top:(H-ih)/2,width:icon.width,height:icon.height,scaleX:iw/icon.width,scaleY:ih/icon.height}));
      // The ad itself is clickable. Product identity and branding take precedence
      // over a large button or optional claims in narrow placements.

    }else{
      // One compact caption and action share the footer. Optional selling copy
      // is admitted only when it leaves at least three quarters for photography.
      const bw=128,bh=34;
      let tw=W-bw-margin*3,hs=(W<280||H<240?20:24)*typeScale,headline=W<280||H<240?compactHeadline:copy.shortHeadline;
      const rowFits=false,showAction=!(W<=280&&H<=280);
      const fullWidth=W-margin*2;
      if(!rowFits)tw=fullWidth;
      let hh=lines(headline,tw,hs)*hs*1.25;
      // Legacy recipes sometimes contain a single long word. Reserve its width
      // before choosing the photo frame instead of letting Fabric clip it.
      const longest=Math.max(...headline.split(/\s+/).map(v=>v.length));
      hs=Math.min(hs,tw/(longest*.67));hh=lines(headline,tw,hs)*hs*1.25;
      const brand=true,brandHeight=34;
      const minimal=(rowFits?Math.max(brandHeight+hh,bh):brandHeight+hh+(showAction?bh+3:0))+margin*2;
      const dh=lines(description,fullWidth,13*typeScale)*17*typeScale;
      const benefit=W>=360&&H>=400&&minimal+dh+5<=H*.22;
      const footer=minimal+(benefit?dh+5:0),photo=photograph({left:0,top:0,width:W,height:H-footer});
      if(style.treatment==='soft-fade'){const fadeHeight=Math.min(30,(H-footer)*.16);bottomFade(H-footer-fadeHeight,fadeHeight);}
      // The caption and action occupy one footer, including tall placements.
      let y=H-footer+margin;
      if(brand){brandRow(margin,y,tw);y+=brandHeight;}
      text('headline',headline,margin,rowFits?H-footer+margin+(Math.max(brandHeight+hh,bh)-brandHeight-hh)/2+brandHeight:y,tw,hh,hs,'headline',style.headlineFont);
      objects[objects.length-1].textAlign='center';
      const actionY=rowFits?H-footer+margin+(Math.max(brandHeight+hh,bh)-bh)/2:y+hh+3;
      if(showAction)button(margin,actionY,fullWidth,bh,copy.cta,14);
      if(benefit)text('description',description,margin,H-margin-dh,fullWidth,dh,13*typeScale,'description');
    }
    function upscale(o){if(o.type==='Image'){o.left*=factor;o.top*=factor;o.scaleX*=factor;o.scaleY*=factor;return;}for(const k of ['left','top','width','height','fontSize','aiBoxHeight','buttonPadding','rx','ry'])if(typeof o[k]==='number')o[k]*=factor;(o.objects||[]).forEach(upscale);}
    const sceneFit=style.treatment==='soft-fade'&&style.atmospheric!==false?atmosphericScene(objects,image,W,H,style,board):null;
    objects.forEach(upscale);
    return {version:'7.4.0',background:style.background,objects,...(sceneFit?{sceneFit}: {})};
  }
  const variants=boards.flatMap(b=>['square','landscape','portrait'].includes(b.key)?['mobile','desktop'].map(device=>({...b,device})):[{...b,device:b.height<=100&&b.width<=320?'mobile':'desktop'}]);
  const rules={version:2,atmosphere:'Continuous edge-to-edge photography beneath a progressive translucent wash. Preserve the existing brand, headline, action positions and complete product. Derive the wash from measured product and copy bounds; opacity must protect at least 4.5:1 text contrast. Use dedicated ratio compositions before resorting to product-free surface continuation. No solid footer, repeated jewelry or distorted product.',priorities:['complete recognizable product','official brand lockup','product name','optional action and supporting copy'],brand:{iconHeight:32,compactBannerIconHeight:36,minimumWordmarkWidth:80,minimumTextSize:14,narrowTextSize:12,alignment:'Center icon and business name together as one lockup; never center the text independently of its icon.'},type:{bannerHeadlineMinimum:16,headlineMinimum:20,supportMinimum:14,maximumFontFamilies:2},button:{height:34,maximumWidth:128,fontSize:14,alignment:'centered beneath the headline within the copy region'},spacing:{minimumGap:3,preferredGap:8,skyscraper:'Use 10–30px gaps and scale the brand, heading and action to occupy the lower third; do not leave a tiny centered cluster'},sizeOverrides:{landscape:'Center the complete brand, headline and action group vertically; use a restrained 108px action and leave visible space around the charm.',skyscraper:'For 120x600, 160x600 and 300x600 keep approximately 10–12 percent clear on each side of the complete jewelry; use dedicated vertically composed scenes.',display_580x400:'Omit supporting copy. Large product headline, visible brand and action in the left region.',display_300x1050:'48px brand icon, up to 44px headline, 48px-high action with 20px label; keep brand-to-title gap at most 24px and title-to-action gap at most 20px as one centered lower-third stack.',display_160x600:'36px brand icon, readable headline, 36px action and balanced vertical gaps.',desktopBanners:'For 728x90, 970x90 and 980x120, reserve a 14–18px inset after the photograph; preserve headline size and the product-first hierarchy.',display_970x250:'Up to 64px headline, 36px business name and 120px brand icon; use the available banner height'},protection:['Whole jewelry including attachment hardware stays visible','No logo, copy or fade may cover the located product','No distortion or invented jewelry details','No microtext or arbitrary shrinking to fit','High-density PNG previews; exact-size upload and historical review pixels remain separate'],creativeFreedom:['Product-specific scene, venue, props and lighting','Photography and safe focal positioning','Supported product-specific messaging within text limits','Coordinated brand palette with readable contrast','Up to two coherent font families','Subtle decorative treatments in empty space']};
  const baseLayouts=boards.map(b=>({...b,family:family(b),composition:family(b)==='banner'?'edge product / centered message / dedicated logo':family(b)==='landscape'?'product beside a centered copy block':'product above a centered brand / headline / action stack',drawnAction:family(b)!=='banner'&&!(b.width<=280&&b.height<=280)}));
  const videoLayouts=[{key:'portrait',width:720,height:1280},{key:'square',width:720,height:720},{key:'landscape',width:1280,height:720}].map(b=>({...b,logo:{x:.065,y:.055,width:176,persistent:true},text:'Three distinct saved messages: hook, product, action. Only text transitions.',wash:'Persistent throughout; never covers protected jewelry.',composition:b.key==='portrait'?'product lower-middle, messaging above':'product toward right, messaging in measured free space'}));
  function baseLayout(board){return baseLayouts.find(b=>b.width===board.width&&b.height===board.height)||{...board,family:family(board),composition:'Adapt the nearest base without breaking shared ground rules'};}
  const api={layoutVersion,sceneCatalog,cleanCrop,atmosphericScene,boards,variants,family,selectImage,document,rules,baseLayouts,videoLayouts,baseLayout};if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesAdResponsive=api;
})(typeof window==='object'?window:globalThis);
