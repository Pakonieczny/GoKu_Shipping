/* Shared semantic reflow for editable advertising artwork. No provider requests. */
(function(root){
  'use strict';
  const core=[['square',2048,2048],['landscape',2048,1072],['portrait',1638,2048]];
  const display=[[200,200],[240,400],[250,250],[250,360],[300,250],[336,280],[580,400],[120,600],[160,600],[300,600],[300,1050],[468,60],[728,90],[930,180],[970,90],[970,250],[980,120],[300,50],[320,50],[320,100]];
  const boards=core.concat(display.map(([w,h])=>['display_'+w+'x'+h,w,h])).map(([key,width,height])=>({key,width,height}));
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,Number(v)||0)),family=b=>b.width/b.height>3?'banner':b.width/b.height<.5?'skyscraper':b.width/b.height>1.3?'landscape':b.width/b.height<.9?'portrait':'square';
  function selectImage(plan,images,board){const f=family(board);return images.find(i=>i.forFamilies?.includes(f))||images[0];}
  // Design at the actual viewing width, then export at the requested resolution.
  // A 2048px master must not turn a 36px CTA into a 6px mobile label.
  const layoutVersion=19;
  const brands=typeof module==='object'&&module.exports?require('./brites-brand-assets'):root.BritesBrandAssets;
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
    const objects=[],margin=Math.max(3,Math.min(8,Math.min(W,H)*.03)),banner=f==='banner',narrow=f==='skyscraper';
    const typeScale=Math.max(1,Math.min(1.35,W/440)),description=copy.description.startsWith(copy.shortHeadline+'. ')?copy.description.slice(copy.shortHeadline.length+2):copy.description;
    const lineHeight=1.08;
    function lines(value,width,size){return String(value).split(/\n/).reduce((sum,line)=>sum+Math.max(1,line.split(/\s+/).reduce((a,word)=>{const n=word.length*size*.58;if(a.used&&a.used+size*.3+n>width){a.count++;a.used=n;}else a.used+=n+size*.3;return a;},{count:1,used:0}).count),0);}
    function text(id,value,x,y,width,height,size,role,font=style.bodyFont){if(value)objects.push(base(id,role,{type:'Textbox',text:value,left:x,top:y,width,height,aiBoxHeight:height,fontFamily:font,fontSize:size,fontWeight:role==='headline'?(style.headlineWeight||'700'):(style.bodyWeight||'400'),fill:style.ink,lineHeight,charSpacing:role==='brand'?35:0,textAlign:'left'}));}
    function button(x,y,width,height,label=copy.cta,size=14){
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
      const small=Math.min(frame.width,frame.height)<=100,padding=1.025;
      const cover=Math.max(frame.width/image.width,frame.height/image.height);
      const safeWidth=image.width/image.height>1.3?image.width*.30:image.width;
      const desired=valid?Math.min(frame.width/(image.width*focus.width*padding),frame.height/(image.height*focus.height*padding)):cover;
      const maximum=valid?Math.min(frame.width/(image.width*focus.width*1.025),frame.height/(image.height*focus.height*1.025)):cover;
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
      const q=image.focus,scale=Math.max(W/image.width,H/image.height),cw=W/scale,ch=H/scale;
      const cx=clamp(image.width*(q.x+q.width/2)-cw*.66,0,image.width-cw),cy=clamp(image.height*(q.y+q.height/2)-ch/2,0,image.height-ch);
      const subject={left:(image.width*q.x-cx)*scale,top:(image.height*q.y-cy)*scale,width:image.width*q.width*scale,height:image.height*q.height*scale},pad=Math.max(6,W*.025),tw=Math.min(W*.44,subject.left-pad*2);
      if(tw<76||subject.top<0||subject.top+subject.height>H)return false;
      objects.push(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:0,top:0,width:cw,height:ch,cropX:cx,cropY:cy,scaleX:scale,scaleY:scale}));
      const rgb=style.background.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)).join(',');
      objects.push(base('image_fade','shape',{type:'Rect',left:0,top:0,width:W,height:H,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:1,y2:0},colorStops:[{offset:0,color:'rgba('+rgb+',1)'},{offset:Math.max(0,(tw-pad)/W),color:'rgba('+rgb+',.92)'},{offset:subject.left/W,color:'rgba('+rgb+',0)'},{offset:1,color:'rgba('+rgb+',0)'}]}}));
      const hs=Math.min(30,Math.max(20,W*.055),tw/(Math.max(...copy.shortHeadline.split(/\s+/).map(w=>w.length))*.67)),hh=lines(copy.shortHeadline,tw,hs)*hs*1.24,bh=Math.max(32,Math.min(42,H*.2)),gap=10,y=Math.max(pad,(H-hh-bh-gap)/2);
      text('brand','BRITES JEWELRY',pad,Math.max(3,y-19),tw,17,12,'brand');text('headline',copy.shortHeadline,pad,y,tw,hh,hs,'headline',style.headlineFont);button(pad,y+hh+gap,tw,bh,copy.cta,14);return true;
    }
    function tallLayout(){
      if(!narrow)return false;
      const pad=Math.max(5,Math.min(12,W*.04)),photoHeight=H*2/3,actionHeight=Math.min(52,Math.max(36,W*.24)),titleSize=Math.min(38,Math.max(21,W*.12));
      photograph({left:0,top:0,width:W,height:photoHeight});bottomFade(photoHeight-Math.min(35,W*.2),Math.min(35,W*.2));
      const titleWidth=W-pad*2,titleHeight=lines(copy.shortHeadline,titleWidth,titleSize)*titleSize*1.24;
      text('brand','BRITES JEWELRY',pad,photoHeight+3,W-pad*2,16,12,'brand');
      text('headline',copy.shortHeadline,pad,photoHeight+Math.max(28,(H-photoHeight-actionHeight-titleHeight-pad*3)/2),titleWidth,titleHeight,titleSize,'headline',style.headlineFont);
      objects[objects.length-1].textAlign='center';button(pad,H-pad-actionHeight,W-pad*2,actionHeight,copy.cta,Math.min(18,actionHeight*.4));return true;
    }
    function softFade(){
      const focus=image.focus,side=W/H>=1.3&&W>=300&&H>=160;
      if(style.treatment!=='soft-fade'||banner||(!side&&!(W>=240&&H>=280||narrow&&W>=120&&H>=400))||!focus||!['x','y','width','height'].every(k=>Number.isFinite(focus[k]))||focus.width<=0||focus.height<=0)return false;
      const pad=Math.max(6,Math.min(22,W*.045)),gap=Math.max(5,Math.min(12,H*.025));
      const bw=narrow?W-pad*2:Math.min(side?W*.42:W*.66,Math.max(96,copy.cta.length*(style.preserveSavedStyle?9:7)+24)),bh=Math.min(44,Math.max(32,H*.1));
      const row=false,tw=side?W*.47-pad*2:row?W-bw-pad*3:W-pad*2;
      let headline=(style.preserveSavedStyle&&!narrow&&W>=320)||side&&W>=320?copy.headline:copy.shortHeadline,hs=side?Math.min(36,Math.max(22,W*.055)):Math.min(narrow?40:30,Math.max(21,W*(narrow?.12:.085)));
      if(lines(headline,tw,hs)>3)headline=copy.shortHeadline;
      hs=Math.min(hs,tw/(Math.max(...headline.split(/\s+/).map(w=>w.length))*.67));
      const hh=lines(headline,tw,hs)*hs*1.24,brand=true,brandH=brand?16:0,bodySize=side?Math.min(18,W*.03):14;
      const bodyH=lines(description,tw,bodySize)*bodySize*1.3,body=(side||style.preserveSavedStyle&&!narrow&&H>=400)&&bodyH<=H*.17&&brandH+hh+bodyH+bh+gap*4<H*.86;
      const total=brandH+(brand?gap:0)+hh+(body?bodyH+gap:0)+(row?0:bh+gap);let top=side?Math.max(pad,(H-total)/2):H-pad-Math.max(total,row?bh:0);
      if(!side&&top<H*.50)return false;
      const region=side?{left:W*.55,top:pad,width:W*.45-pad,height:H-pad*2}:{left:pad,top:pad,width:W-pad*2,height:top-pad*2};
      const scale=Math.max(W/image.width,side?H/image.height:0,Math.min(region.width/(image.width*focus.width*(narrow?1.08:1.16)),region.height/(image.height*focus.height*1.16)));
      const cw=W/scale;let ch=Math.min(image.height,(side?H:top)/scale),cx=clamp(image.width*(focus.x+focus.width/2)-(region.left+region.width/2)/scale,0,image.width-cw),cy=clamp(image.height*(focus.y+focus.height/2)-(region.top+region.height/2)/scale,0,image.height-ch);
      const subject={left:(image.width*focus.x-cx)*scale,top:(image.height*focus.y-cy)*scale,width:image.width*focus.width*scale,height:image.height*focus.height*scale};
      if(subject.left<region.left||subject.top<region.top||subject.left+subject.width>region.left+region.width||subject.top+subject.height>region.top+region.height)return false;
      const photoTop=0;if(narrow){cy=clamp(cy+(subject.top-pad)/scale,0,image.height);subject.top=(image.height*focus.y-cy)*scale;top=subject.top+subject.height+gap*2;ch=Math.min(image.height-cy,top/scale);}
      if(style.preserveSavedStyle&&!narrow)ch=Math.min(image.height-cy,H/scale);
      objects.push(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:0,top:photoTop,width:cw,height:ch,cropX:cx,cropY:cy,scaleX:scale,scaleY:scale}));
      const stops=side?[[0,1],[.40,.98],[.55,0],[1,0]]:[...(photoTop>0?[[0,1],[photoTop/H,1],[(photoTop+Math.min(40,Math.max(1,subject.top-photoTop)))/H,0]]:[[0,0]]),[Math.max(0,(subject.top+subject.height)/H),0],[Math.min((top+(style.preserveSavedStyle&&!narrow?total*.75:0))/H,(photoTop+ch*scale)/H),1],[1,1]];
      objects.push(base('image_fade','shape',{type:'Rect',left:0,top:0,width:W,height:H,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:side?1:0,y2:side?0:1},colorStops:stops.map(([offset,opacity])=>({offset,color:'rgba('+style.background.match(/[a-f0-9]{2}/gi).map(v=>parseInt(v,16)).join(',')+','+opacity+')'}))}}));
      let y=top;if(brand){text('brand','BRITES JEWELRY',pad,y,tw,brandH,12,'brand');y+=brandH+gap;}
      text('headline',headline,pad,y,tw,hh,hs,'headline',style.headlineFont);y+=hh+gap;
      if(body){text('description',description,pad,y,tw,bodyH,bodySize,'description');y+=bodyH+gap;}
      button(side||style.preserveSavedStyle&&!narrow?pad:(W-Math.min(bw,tw))/2,y,Math.min(bw,tw),bh,copy.cta,Math.min(17,bh*.4));
      if(!side&&(!style.preserveSavedStyle||narrow))for(const o of objects)if('text'in o)o.textAlign='center';
      return true;
    }
    const compactHeadline=copy.shortHeadline;
    if(tallLayout()){
      // Fixed upper image region and lower messaging region for skyscrapers.
    }else if(fullBleedLandscape()){
      // Use the actual photograph edge to edge, with copy in its quiet side.
    }else if(softFade()){
      // The photograph fills the artboard; the editable fade protects only the copy.
    }else if(banner){
      const pw=Math.min(W*.27,H*1.08),tx=pw+Math.max(5,H*.035),right=Math.max(5,H*.035),tw=W-tx-right;
      photograph({left:0,top:0,width:pw,height:H});
      const bs=Math.max(12,Math.min(24,H*.15)),icon=brands?.get('brites_brand_icon'),ih=Math.min(32,Math.max(16,H*.26)),iw=ih*789/592;
      const headline=compactHeadline,hs=Math.max(16,Math.min(H*.38,42,tw/(Math.max(1,headline.length)*.59))),hh=lines(headline,tw,hs)*hs*1.18;
      const brandH=Math.max(ih,bs*1.3),gap=Math.max(3,H*.045),total=hh+brandH+gap,y=Math.max(2,(H-total)/2);
      text('headline',headline,tx,y,tw,hh,hs,'headline',style.headlineFont);
      const by=y+hh+gap;
      if(icon)objects.push(base('brand_icon','brand',{type:'Image',sourceKey:icon.id,left:tx,top:by,width:icon.width,height:icon.height,scaleX:iw/icon.width,scaleY:ih/icon.height}));
      text('brand','Brites Jewelry',tx+(icon?iw+5:0),by+(brandH-bs*1.3)/2,tw-(icon?iw+5:0),bs*1.3,bs,'brand');
      // The ad itself is clickable. Product identity and branding take precedence
      // over a large button or optional claims in narrow placements.

    }else{
      // One compact caption and action share the footer. Optional selling copy
      // is admitted only when it leaves at least three quarters for photography.
      const bw=Math.min(W*.37,Math.max(92,copy.cta.length*8+12)),bh=36*typeScale;
      let tw=W-bw-margin*3,hs=(W<280||H<240?20:24)*typeScale,headline=W<280||H<240?compactHeadline:copy.shortHeadline;
      const rowFits=!narrow&&tw>=72&&lines(headline,tw,hs)<=2;
      const fullWidth=W-margin*2;
      if(!rowFits)tw=fullWidth;
      let hh=lines(headline,tw,hs)*hs*1.25;
      // Legacy recipes sometimes contain a single long word. Reserve its width
      // before choosing the photo frame instead of letting Fabric clip it.
      const longest=Math.max(...headline.split(/\s+/).map(v=>v.length));
      hs=Math.min(hs,tw/(longest*.67));hh=lines(headline,tw,hs)*hs*1.25;
      const brand=true,brandHeight=17*typeScale;
      const minimal=(rowFits?Math.max(brandHeight+hh,bh):brandHeight+hh+bh+5)+margin*2;
      const dh=lines(description,fullWidth,13*typeScale)*17*typeScale;
      const benefit=W>=360&&H>=400&&minimal+dh+5<=H*.22;
      const footer=minimal+(benefit?dh+5:0),photo=photograph({left:0,top:0,width:W,height:H-footer});
      if(style.treatment==='soft-fade'){const fadeHeight=Math.min(30,(H-footer)*.16);bottomFade(H-footer-fadeHeight,fadeHeight);}
      // The caption and action occupy one footer, including tall placements.
      let y=H-footer+margin;
      if(brand){text('brand','BRITES JEWELRY',margin,y,tw,brandHeight,12*typeScale,'brand');y+=brandHeight;}
      text('headline',headline,margin,rowFits?H-footer+margin+(Math.max(brandHeight+hh,bh)-brandHeight-hh)/2+brandHeight:y,tw,hh,hs,'headline',style.headlineFont);
      const actionY=rowFits?H-footer+margin+(Math.max(brandHeight+hh,bh)-bh)/2:y+hh+5;
      button(rowFits?W-bw-margin:margin,actionY,rowFits?bw:Math.min(fullWidth,Math.max(100,copy.cta.length*8+12)),bh,copy.cta,14*typeScale);
      if(benefit)text('description',description,margin,H-margin-dh,fullWidth,dh,13*typeScale,'description');
    }
    if(!objects.some(o=>o.id==='ai_brand_icon'))for(const o of objects.filter(o=>o.editorRole==='brand'&&o.type==='Textbox')){const icon=brands?.get('brites_brand_icon');if(icon&&o.width>140){const h=Math.max(14,o.fontSize*1.2),w=h*icon.width/icon.height;objects.push(base('brand_icon','brand',{type:'Image',sourceKey:icon.id,left:o.left,top:o.top,width:icon.width,height:icon.height,scaleX:h/icon.height,scaleY:h/icon.height}));o.left+=w+5;o.width-=w+5;}}
    function upscale(o){if(o.type==='Image'){o.left*=factor;o.top*=factor;o.scaleX*=factor;o.scaleY*=factor;return;}for(const k of ['left','top','width','height','fontSize','aiBoxHeight','buttonPadding','rx','ry'])if(typeof o[k]==='number')o[k]*=factor;(o.objects||[]).forEach(upscale);}
    objects.forEach(upscale);
    return {version:'7.4.0',background:style.background,objects};
  }
  const variants=boards.flatMap(b=>['square','landscape','portrait'].includes(b.key)?['mobile','desktop'].map(device=>({...b,device})):[{...b,device:b.height<=100&&b.width<=320?'mobile':'desktop'}]);
  const api={layoutVersion,boards,variants,family,selectImage,document};if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesAdResponsive=api;
})(typeof window==='object'?window:globalThis);
