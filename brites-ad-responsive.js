/* Shared semantic reflow for editable advertising artwork. No provider requests. */
(function(root){
  'use strict';
  const core=[['square',2048,2048],['landscape',2048,1072],['portrait',1638,2048]];
  const display=[[200,200],[240,400],[250,250],[250,360],[300,250],[336,280],[580,400],[120,600],[160,600],[300,600],[300,1050],[468,60],[728,90],[930,180],[970,90],[970,250],[980,120],[300,50],[320,50],[320,100]];
  const boards=core.concat(display.map(([w,h])=>['display_'+w+'x'+h,w,h])).map(([key,width,height])=>({key,width,height}));
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,Number(v)||0)),family=b=>b.width/b.height>3?'banner':b.width/b.height<.5?'skyscraper':b.width/b.height>1.3?'landscape':b.width/b.height<.9?'portrait':'square';
  function selectImage(plan,images,board){const f=family(board);return images.find(i=>i.forFamilies?.includes(f))||images[0];}
  function document(plan,image,board,device="mobile"){
    const W=board.width,H=board.height,f=family(board),layout=(plan.layouts||[]).find(l=>l.family===f&&l.device===device)||(plan.layouts||[]).find(l=>l.family===f)||{},style=plan.style,copy=plan.copy;
    const base=(id,role,extra)=>({id:'ai_'+id,name:id,editorRole:role,originX:'left',originY:'top',angle:0,opacity:1,scaleX:1,scaleY:1,strokeWidth:0,...extra});
    const objects=[],min=Math.min(W,H),margin=Math.max(5,min*.055),horizontal=f==='landscape',banner=f==='banner',narrow=f==='skyscraper';let photo,area;
    if(banner){photo={left:0,top:0,width:W*.22,height:H};area={left:photo.width+margin,top:margin,width:W-photo.width-margin*2,height:H-margin*2};}
    else if(horizontal){const width=W*clamp(layout.photoFraction||.53,.42,.62),right=layout.photoSide==='right';photo={left:right?W-width:0,top:0,width,height:H};area={left:right?margin:width+margin,top:margin,width:W-width-margin*2,height:H-margin*2};}
    else{const height=H*clamp(layout.photoFraction||(narrow?.49:.57),.4,.65);photo={left:0,top:0,width:W,height};area={left:margin,top:height+margin,width:W-margin*2,height:H-height-margin*2};}
    const scale=Math.max(photo.width/image.width,photo.height/image.height)*clamp(layout.zoom||1,1,1.35),cropWidth=photo.width/scale,cropHeight=photo.height/scale;
    objects.push(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:photo.left,top:photo.top,width:cropWidth,height:cropHeight,cropX:clamp(layout.focalX??image.focalX??.5,0,1)*(image.width-cropWidth),cropY:clamp(layout.focalY??image.focalY??.5,0,1)*(image.height-cropHeight),scaleX:scale,scaleY:scale}));
    const align=layout.textAlign==='center'?'center':'left';
    function text(id,value,x,y,width,height,size,role,font=style.bodyFont){if(layout['show'+role[0].toUpperCase()+role.slice(1)]===false)return;objects.push(base(id,role,{type:'Textbox',text:value,left:x,top:y,width,height,aiBoxHeight:height,fontFamily:font,fontSize:size,fontWeight:role==='headline'?'700':'400',fill:style.ink,lineHeight:1.08,charSpacing:role==='brand'?100:0,textAlign:align}));}
    function button(x,y,width,height){if(layout.showButton===false)return;objects.push(base('cta','button',{type:'Group',left:x,top:y,width,height,buttonPadding:width*.06,objects:[{type:'Rect',originX:'left',originY:'top',left:-width/2,top:-height/2,width,height,fill:style.accent,strokeWidth:0,rx:height*.13,ry:height*.13},{type:'Textbox',originX:'center',originY:'center',left:0,top:0,width:width*.88,height:height*.45,text:copy.cta,fontFamily:style.bodyFont,fontWeight:'700',fontSize:Math.max(8,Math.min(height*.36,width*.11)),fill:style.buttonInk,textAlign:'center',lineHeight:1}]}));}
    if(banner){
      const bw=Math.min(W*.2,Math.max(90,H*1.9)),bh=Math.max(22,Math.min(H*.58,48)),tw=area.width-bw-margin;
      if(H>=90)text('brand','BRITES JEWELRY',area.left,area.top,tw,H*.14,Math.max(7,H*.11),'brand');
      text('headline',copy.shortHeadline,area.left,H<90?H*.24:H*.3,tw,H*.45,Math.max(10,Math.min(H*.28,tw/Math.max(9,copy.shortHeadline.length*.52))),'headline',style.headlineFont);
      button(W-margin-bw,(H-bh)/2,bw,bh);
    }else{
      const brandSize=Math.max(8,Math.min(min*.018,area.height*.075)),headSize=Math.max(12,Math.min(area.width*(narrow?.12:.09),area.height*.18)),descSize=Math.max(9,Math.min(area.width*.045,area.height*.07));
      text('brand','BRITES JEWELRY',area.left,area.top,area.width,area.height*.08,brandSize,'brand');
      text('headline',narrow?copy.shortHeadline:copy.headline,area.left,area.top+area.height*.15,area.width,area.height*.34,headSize,'headline',style.headlineFont);
      if(area.height>85)text('description',copy.description,area.left,area.top+area.height*.54,area.width,area.height*.2,descSize,'description');
      const bw=Math.min(area.width,Math.max(area.width*(narrow?.95:.78),80)),bh=Math.min(area.height*.18,Math.max(24,min*.085)),x=align==='center'?area.left+(area.width-bw)/2:area.left;button(x,area.top+area.height-bh,bw,bh);
    }
    return {version:'7.4.0',background:style.background,objects};
  }
  const variants=boards.flatMap(b=>['square','landscape','portrait'].includes(b.key)?['mobile','desktop'].map(device=>({...b,device})):[{...b,device:b.height<=100&&b.width<=320?'mobile':'desktop'}]);
  const api={boards,variants,family,selectImage,document};if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesAdResponsive=api;
})(typeof window==='object'?window:globalThis);
