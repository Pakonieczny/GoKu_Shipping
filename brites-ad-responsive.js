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
  const layoutVersion=12;
  function document(plan,image,board,device="mobile"){
    const master=['square','landscape','portrait'].includes(board.key),factor=master?board.width/Math.min(board.width,device==='desktop'?600:360):1;
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
    function lines(value,width,size){return Math.max(1,String(value).split(/\s+/).reduce((a,word)=>{const n=word.length*size*.58;if(a.used&&a.used+size*.3+n>width){a.count++;a.used=n;}else a.used+=n+size*.3;return a;},{count:1,used:0}).count);}
    function text(id,value,x,y,width,height,size,role,font=style.bodyFont){if(value)objects.push(base(id,role,{type:'Textbox',text:value,left:x,top:y,width,height,aiBoxHeight:height,fontFamily:font,fontSize:size,fontWeight:role==='headline'?'700':'400',fill:style.ink,lineHeight,charSpacing:role==='brand'?35:0,textAlign:'left'}));}
    function button(x,y,width,height,label=copy.cta,size=14){
      // Select a short action before fitting; never squeeze a two-line label into a small banner.
      if(label.length*size*.65+12>width)label='Shop now';
      if(label.length*size*.65+12>width)label='Shop';
      objects.push(base('cta','button',{type:'Group',left:x,top:y,width,height,buttonPadding:4,objects:[{type:'Rect',originX:'left',originY:'top',left:-width/2,top:-height/2,width,height,fill:style.accent,strokeWidth:0,rx:3,ry:3},{type:'Textbox',originX:'center',originY:'center',left:0,top:0,width:width-8,height:16,text:label,fontFamily:style.bodyFont,fontWeight:'700',fontSize:size,fill:style.buttonInk,textAlign:'center',lineHeight:1}]}));
    }
    // Frame the located charm, not the full chain or photograph. Small placements
    // receive tighter crops; larger ones retain a little photographic context.
    // Legacy unlocated photographs keep their conservative framing until located.
    function photograph(frame){
      const focus=image.focus,valid=focus&&['x','y','width','height'].every(k=>Number.isFinite(focus[k]))&&focus.width>0&&focus.height>0;
      const small=Math.min(frame.width,frame.height)<=100,padding=small?1.22:narrow?1.35:1.62;
      const cover=Math.max(frame.width/image.width,frame.height/image.height);
      const safeWidth=image.width/image.height>1.3?image.width*.30:image.width;
      const scale=valid?Math.max(cover,Math.min(frame.width/(image.width*focus.width*padding),frame.height/(image.height*focus.height*padding))):Math.min(cover,frame.width/safeWidth);
      const cw=Math.min(image.width,frame.width/scale),ch=Math.min(image.height,frame.height/scale);
      const cx=clamp(valid?image.width*(focus.x+focus.width/2)-cw/2:(image.width-cw)/2,0,image.width-cw),cy=clamp(valid?image.height*(focus.y+focus.height/2)-ch/2:(image.height-ch)/2,0,image.height-ch);
      const placed={left:frame.left+(frame.width-cw*scale)/2,top:frame.top+(frame.height-ch*scale)/2,width:cw*scale,height:ch*scale};
      objects.unshift(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:placed.left,top:placed.top,width:cw,height:ch,cropX:cx,cropY:cy,scaleX:scale,scaleY:scale}));
      return placed;
    }
    function softFade(){
      const focus=image.focus,side=W/H>=1.3&&W>=300&&H>=160;
      if(style.treatment!=='soft-fade'||banner||(!side&&!(W>=240&&H>=280||narrow&&W>=120&&H>=400))||!focus||!['x','y','width','height'].every(k=>Number.isFinite(focus[k]))||focus.width<=0||focus.height<=0)return false;
      const pad=Math.max(6,Math.min(22,W*.045)),gap=Math.max(5,Math.min(12,H*.025));
      const bw=Math.min(side?W*.42:W*.36,Math.max(96,copy.cta.length*7+20)),bh=Math.min(44,Math.max(32,H*.1));
      const row=!side&&W>=320,tw=side?W*.47-pad*2:row?W-bw-pad*3:W-pad*2;
      let headline=W<320?copy.shortHeadline:copy.headline,hs=side?Math.min(36,Math.max(22,W*.055)):Math.min(30,Math.max(19,W*.065));
      if(lines(headline,tw,hs)>3)headline=copy.shortHeadline;
      hs=Math.min(hs,tw/(Math.max(...headline.split(/\s+/).map(w=>w.length))*.67));
      const hh=lines(headline,tw,hs)*hs*1.24,brand=W>=320,brandH=brand?16:0,bodySize=side?Math.min(18,W*.03):14;
      const bodyH=lines(description,tw,bodySize)*bodySize*1.3,body=side&&bodyH<=H*.17&&brandH+hh+bodyH+bh+gap*4<H*.86;
      const total=brandH+(brand?gap:0)+hh+(body?bodyH+gap:0)+(row?0:bh+gap),top=side?Math.max(pad,(H-total)/2):H-pad-Math.max(total,row?bh:0);
      if(!side&&top<H*.55)return false;
      const region=side?{left:W*.55,top:pad,width:W*.45-pad,height:H-pad*2}:{left:pad,top:pad,width:W-pad*2,height:top-pad*2};
      const scale=Math.max(W/image.width,H/image.height,Math.min(region.width/(image.width*focus.width*1.22),region.height/(image.height*focus.height*1.22)));
      const cw=W/scale,ch=H/scale,cx=clamp(image.width*(focus.x+focus.width/2)-(region.left+region.width/2)/scale,0,image.width-cw),cy=clamp(image.height*(focus.y+focus.height/2)-(region.top+region.height/2)/scale,0,image.height-ch);
      const subject={left:(image.width*focus.x-cx)*scale,top:(image.height*focus.y-cy)*scale,width:image.width*focus.width*scale,height:image.height*focus.height*scale};
      if(subject.left<region.left||subject.top<region.top||subject.left+subject.width>region.left+region.width||subject.top+subject.height>region.top+region.height)return false;
      objects.push(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:0,top:0,width:cw,height:ch,cropX:cx,cropY:cy,scaleX:scale,scaleY:scale}));
      const stops=side?[[0,1],[.40,.98],[.55,0],[1,0]]:[[0,0],[Math.max(0,(subject.top+subject.height)/H),0],[top/H,.98],[1,1]];
      objects.push(base('image_fade','shape',{type:'Rect',left:0,top:0,width:W,height:H,fill:{type:'linear',gradientUnits:'percentage',coords:{x1:0,y1:0,x2:side?1:0,y2:side?0:1},colorStops:stops.map(([offset,opacity])=>({offset,color:style.background,opacity}))}}));
      let y=top;if(brand){text('brand','BRITES JEWELRY',pad,y,tw,brandH,12,'brand');y+=brandH+gap;}
      text('headline',headline,pad,y,tw,hh,hs,'headline',style.headlineFont);y+=hh+gap;
      if(body){text('description',description,pad,y,tw,bodyH,bodySize,'description');y+=bodyH+gap;}
      button(row?W-pad-bw:pad,row?H-pad-bh:y,Math.min(bw,tw),bh,copy.cta,Math.min(17,bh*.4));
      return true;
    }
    const compactHeadline=copy.shortHeadline;
    if(softFade()){
      // The photograph fills the artboard; the editable fade protects only the copy.
    }else if(banner){
      const bw=Math.min(W*.26,Math.max(72,H*1.65)),bh=H<=60?H-6:Math.min(90,H*.72),bs=Math.min(28,Math.max(15,bh*.38));
      const pw=Math.min(W*.30,H*1.45),tx=pw+margin,tw=W-tx-bw-margin*3;
      photograph({left:0,top:0,width:pw,height:H});
      const headline=H<=60?compactHeadline:copy.shortHeadline,hs=Math.min(H*.46,tw/(Math.max(...headline.split(/\s+/).map(v=>v.length))*.68),tw/(headline.length*.59));
      const brand=H>=150,benefit=H>=180&&tw>=300&&lines(description,tw,18)<=2;
      const hh=lines(headline,tw,hs)*hs*1.25,dh=benefit?lines(description,tw,18)*23:0,total=hh+(brand?20:0)+(benefit?dh+6:0),y=Math.max(3,(H-total)/2);
      if(brand)text('brand','BRITES JEWELRY',tx,y,tw,17,13,'brand');
      text('headline',headline,tx,y+(brand?20:0),tw,hh,hs,'headline',style.headlineFont);
      if(benefit)text('description',description,tx,y+20+hh+6,tw,dh,18,'description');
      button(W-bw-margin,(H-bh)/2,bw,bh,'Shop now',bs);
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
      const brand=W>=360,brandHeight=brand?17*typeScale:0;
      const minimal=(rowFits?Math.max(brandHeight+hh,bh):brandHeight+hh+bh+5)+margin*2;
      const dh=lines(description,fullWidth,13*typeScale)*17*typeScale;
      const benefit=W>=360&&H>=400&&minimal+dh+5<=H*.22;
      const footer=minimal+(benefit?dh+5:0),photo=photograph({left:0,top:0,width:W,height:H-footer});
      // The caption and action occupy one footer, including tall placements.
      let y=H-footer+margin;
      if(brand){text('brand','BRITES JEWELRY',margin,y,tw,brandHeight,12*typeScale,'brand');y+=brandHeight;}
      text('headline',headline,margin,rowFits?H-footer+margin+(Math.max(brandHeight+hh,bh)-brandHeight-hh)/2+brandHeight:y,tw,hh,hs,'headline',style.headlineFont);
      const actionY=rowFits?H-footer+margin+(Math.max(brandHeight+hh,bh)-bh)/2:y+hh+5;
      button(rowFits?W-bw-margin:margin,actionY,rowFits?bw:Math.min(fullWidth,Math.max(100,copy.cta.length*8+12)),bh,copy.cta,14*typeScale);
      if(benefit)text('description',description,margin,H-margin-dh,fullWidth,dh,13*typeScale,'description');
    }
    function upscale(o){if(o.type==='Image'){o.left*=factor;o.top*=factor;o.scaleX*=factor;o.scaleY*=factor;return;}for(const k of ['left','top','width','height','fontSize','aiBoxHeight','buttonPadding','rx','ry'])if(typeof o[k]==='number')o[k]*=factor;(o.objects||[]).forEach(upscale);}
    objects.forEach(upscale);
    return {version:'7.4.0',background:style.background,objects};
  }
  const variants=boards.flatMap(b=>['square','landscape','portrait'].includes(b.key)?['mobile','desktop'].map(device=>({...b,device})):[{...b,device:b.height<=100&&b.width<=320?'mobile':'desktop'}]);
  const api={layoutVersion,boards,variants,family,selectImage,document};if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesAdResponsive=api;
})(typeof window==='object'?window:globalThis);
