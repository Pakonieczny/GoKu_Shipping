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
  const layoutVersion=2;
  function document(plan,image,board,device="mobile"){
    const master=['square','landscape','portrait'].includes(board.key),factor=master?board.width/Math.min(board.width,device==='desktop'?600:360):1;
    const W=board.width/factor,H=board.height/factor,f=family(board),layout=(plan.layouts||[]).find(l=>l.family===f&&l.device===device)||(plan.layouts||[]).find(l=>l.family===f)||{},style=plan.style,copy=plan.copy;
    const base=(id,role,extra)=>({id:'ai_'+id,name:id,editorRole:role,originX:'left',originY:'top',angle:0,opacity:1,scaleX:1,scaleY:1,strokeWidth:0,...extra});
    const objects=[],margin=Math.max(6,Math.min(W,H)*.045),banner=f==='banner',narrow=f==='skyscraper',horizontal=f==='landscape';let photo,area;
    const lineHeight=1.08;
    function lines(value,width,size){return Math.max(1,String(value).split(/\s+/).reduce((a,word)=>{const n=word.length*size*.56;if(a.used&&a.used+size*.28+n>width){a.count++;a.used=n;}else a.used+=n+size*.28;return a;},{count:1,used:0}).count);}
    function text(id,value,x,y,width,height,size,role,font=style.bodyFont){if(value)objects.push(base(id,role,{type:'Textbox',text:value,left:x,top:y,width,height,aiBoxHeight:height,fontFamily:font,fontSize:size,fontWeight:role==='headline'?'700':'400',fill:style.ink,lineHeight,charSpacing:role==='brand'?60:0,textAlign:layout.textAlign==='center'?'center':'left'}));}
    function button(x,y,width,height,label=copy.cta){objects.push(base('cta','button',{type:'Group',left:x,top:y,width,height,buttonPadding:6,objects:[{type:'Rect',originX:'left',originY:'top',left:-width/2,top:-height/2,width,height,fill:style.accent,strokeWidth:0,rx:4,ry:4},{type:'Textbox',originX:'center',originY:'center',left:0,top:0,width:width-12,height:16,text:label,fontFamily:style.bodyFont,fontWeight:'700',fontSize:13,fill:style.buttonInk,textAlign:'center',lineHeight:1}]}));}
    if(banner){
      const bw=H<90?76:Math.min(140,Math.max(100,W*.2)),bh=Math.min(H-12,34),pw=Math.min(H*1.15,W*.22),tw=W-pw-bw-margin*3;
      photo={left:0,top:0,width:pw,height:H};
      const hs=H<90?14:Math.min(24,H*.2),hh=Math.min(H-23,lines(copy.shortHeadline,tw,hs)*hs*1.25);
      text('headline',copy.shortHeadline,pw+margin,Math.max(5,(H-hh-13)/2),tw,hh,hs,'headline',style.headlineFont);
      text('brand','BRITES JEWELRY',pw+margin,H-15,tw,11,8,'brand');
      button(W-margin-bw,(H-bh)/2,bw,bh,copy.cta.length*7.4+12>bw?'Shop now':copy.cta);
    }else{
      const textWidth=horizontal?W*.52-margin*2:W-margin*2;
      const compact=W<280,hs=narrow?20:compact?20:horizontal?22:26,ds=13,brandSize=10;
      let headline=(narrow||compact||horizontal)?copy.shortHeadline:copy.headline;
      const headHeight=lines(headline,textWidth,hs)*hs*1.25,descHeight=lines(copy.description,textWidth,ds)*ds*1.3;
      const gap=compact?6:9,bh=32,brandHeight=13;
      const minimal=brandHeight+gap+headHeight+gap+bh;
      const showDescription=!compact&&minimal+gap+descHeight<=(horizontal?H-margin*2:H*.62);
      const contentHeight=minimal+(showDescription?gap+descHeight:0);
      if(horizontal){const pw=W*.48,right=layout.photoSide==='right';photo={left:right?W-pw:0,top:0,width:pw,height:H};area={left:right?margin:pw+margin,top:Math.max(margin,(H-contentHeight)/2),width:textWidth};}
      else{const desired=H*clamp(layout.photoFraction||.57,.4,.65),ph=Math.max(35,Math.min(desired,H-contentHeight-margin*2,narrow?W*1.5/clamp(layout.zoom||1,1,1.35):Infinity));photo={left:0,top:0,width:W,height:ph};area={left:margin,top:ph+margin,width:textWidth};}
      let y=area.top;
      text('brand','BRITES JEWELRY',area.left,y,area.width,brandHeight,brandSize,'brand');y+=brandHeight+gap;
      text('headline',headline,area.left,y,area.width,headHeight,hs,'headline',style.headlineFont);y+=headHeight+gap;
      if(showDescription){text('description',copy.description,area.left,y,area.width,descHeight,ds,'description');y+=descHeight+gap;}
      const bw=Math.min(area.width,Math.max(108,copy.cta.length*7.4+20)),x=layout.textAlign==='center'?area.left+(area.width-bw)/2:area.left;
      button(x,y,bw,bh,copy.cta.length*7.4+12>bw?'Shop now':copy.cta);
    }
    const scale=Math.max(photo.width/image.width,photo.height/image.height)*clamp(layout.zoom||1,1,1.35),cropWidth=photo.width/scale,cropHeight=photo.height/scale;
    objects.unshift(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:photo.left,top:photo.top,width:cropWidth,height:cropHeight,cropX:clamp(layout.focalX??image.focalX??.5,0,1)*(image.width-cropWidth),cropY:clamp(layout.focalY??image.focalY??.5,0,1)*(image.height-cropHeight),scaleX:scale,scaleY:scale}));
    function upscale(o){if(o.type==='Image'){o.left*=factor;o.top*=factor;o.scaleX*=factor;o.scaleY*=factor;return;}for(const k of ['left','top','width','height','fontSize','aiBoxHeight','buttonPadding','rx','ry'])if(typeof o[k]==='number')o[k]*=factor;(o.objects||[]).forEach(upscale);}
    objects.forEach(upscale);
    return {version:'7.4.0',background:style.background,objects};
  }
  const variants=boards.flatMap(b=>['square','landscape','portrait'].includes(b.key)?['mobile','desktop'].map(device=>({...b,device})):[{...b,device:b.height<=100&&b.width<=320?'mobile':'desktop'}]);
  const api={layoutVersion,boards,variants,family,selectImage,document};if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesAdResponsive=api;
})(typeof window==='object'?window:globalThis);
