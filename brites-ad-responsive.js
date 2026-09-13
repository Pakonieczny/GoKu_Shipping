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
  const layoutVersion=6;
  function document(plan,image,board,device="mobile"){
    const master=['square','landscape','portrait'].includes(board.key),factor=master?board.width/Math.min(board.width,device==='desktop'?600:360):1;
    const W=board.width/factor,H=board.height/factor,f=family(board),layout=(plan.layouts||[]).find(l=>l.family===f&&l.device===device)||(plan.layouts||[]).find(l=>l.family===f)||{},style={...plan.style},copy=plan.copy;
    const rgb=style.background.match(/[a-f0-9]{2}/gi)?.map(x=>parseInt(x,16));if(rgb&&rgb[0]<90&&rgb[0]>=rgb[1]&&rgb[1]>=rgb[2])Object.assign(style,{background:'#F5F0E8',ink:'#34281E',accent:'#4B3825',buttonInk:'#FFF9F0',headlineFont:'Georgia'});
    const base=(id,role,extra)=>({id:'ai_'+id,name:id,editorRole:role,originX:'left',originY:'top',angle:0,opacity:1,scaleX:1,scaleY:1,strokeWidth:0,...extra});
    const objects=[],margin=Math.max(6,Math.min(W,H)*.045),banner=f==='banner',narrow=f==='skyscraper',horizontal=f==='landscape';let photo,area;
    // A wider master reserves its central third for the complete product.
    // Retain that region plus margin and the full height, including rings/chains.
    const cropWidth=image.width/image.height>1.3?image.width*.42:image.width,cropHeight=image.height;
    const typeScale=Math.max(1,Math.min(1.7,W/360)),description=copy.description.startsWith(copy.shortHeadline+'. ')?copy.description.slice(copy.shortHeadline.length+2):copy.description;
    const lineHeight=1.08;
    function lines(value,width,size){return Math.max(1,String(value).split(/\s+/).reduce((a,word)=>{const n=word.length*size*.56;if(a.used&&a.used+size*.28+n>width){a.count++;a.used=n;}else a.used+=n+size*.28;return a;},{count:1,used:0}).count);}
    function text(id,value,x,y,width,height,size,role,font=style.bodyFont){if(value)objects.push(base(id,role,{type:'Textbox',text:value,left:x,top:y,width,height,aiBoxHeight:height,fontFamily:font,fontSize:size,fontWeight:role==='headline'?'700':'400',fill:style.ink,lineHeight,charSpacing:role==='brand'?60:0,textAlign:layout.textAlign==='center'?'center':'left'}));}
    function button(x,y,width,height,label=copy.cta){objects.push(base('cta','button',{type:'Group',left:x,top:y,width,height,buttonPadding:6,objects:[{type:'Rect',originX:'left',originY:'top',left:-width/2,top:-height/2,width,height,fill:style.accent,strokeWidth:0,rx:4,ry:4},{type:'Textbox',originX:'center',originY:'center',left:0,top:0,width:width-12,height:16,text:label,fontFamily:style.bodyFont,fontWeight:'700',fontSize:13,fill:style.buttonInk,textAlign:'center',lineHeight:1}]}));}
    if(banner){
      const bw=H<90?76:Math.min(148,Math.max(108,W*.16)),bh=Math.min(H-12,38),pw=Math.min(H*1.12,W*.26),available=W-pw-bw-margin*4;
      const roomy=H>=120&&available>=240,hook=roomy||W>=700,headline=hook?copy.headline:copy.shortHeadline,hs=H<90?14:Math.min(30,H*.22);
      const tw=Math.min(available,Math.max(140,headline.length*hs*.62,roomy?description.length*7.3:0)),groupWidth=pw+tw+bw+margin*3,start=(W-groupWidth)/2;
      photo={left:start,top:4,width:pw-margin,height:H-8};
      const hh=lines(headline,tw,hs)*hs*1.25,benefit=roomy?description:'',dh=benefit?lines(benefit,tw,13)*17:0,total=hh+15+(benefit?dh+6:0),y=Math.max(3,(H-total)/2);
      text('brand','BRITES JEWELRY',start+pw,y,tw,13,10,'brand');
      text('headline',headline,start+pw,y+15,tw,hh,hs,'headline',style.headlineFont);
      if(benefit)text('description',benefit,start+pw,y+15+hh+6,tw,dh,13,'description');
      button(start+pw+tw+margin*2,(H-bh)/2,bw,bh,copy.cta.length*7.4+12>bw?'Shop now':copy.cta);
    }else{
      const textWidth=horizontal?W*.52-margin*2:W-margin*2;
      const compact=W<280,card=!horizontal&&H<=300,hs=(narrow?Math.min(48,Math.max(24,W*.17)):horizontal?22:card||compact?20:28)*typeScale,ds=(narrow?Math.min(18,Math.max(13,W*.055)):13)*typeScale,brandSize=10*typeScale;
      let headline=horizontal?copy.shortHeadline:copy.headline;
      if(card)headline=copy.shortHeadline;
      const headHeight=lines(headline,textWidth,hs)*hs*1.25,descHeight=lines(description,textWidth,ds)*ds*1.3;
      const gap=(compact?7:10)*typeScale,bh=32*typeScale,brandHeight=13*typeScale;
      const minimal=brandHeight+gap+headHeight+gap+bh;
      const showDescription=minimal+gap+descHeight<=(horizontal?H-margin*2:card?H*.46:H*.68);
      const contentHeight=minimal+(showDescription?gap+descHeight:0);
      if(horizontal){const pw=W*.48,right=layout.photoSide==='right';photo={left:right?W-pw:0,top:margin,width:pw,height:H-margin*2};area={left:right?margin:pw+margin,top:Math.max(margin,(H-contentHeight)/2),width:textWidth};}
      else{
        const ph=Math.max(35,Math.min(H-contentHeight-margin*3,W*cropHeight/cropWidth));
        const top=H/W>1.8?Math.max(margin,(H-ph-contentHeight-margin)/2):margin;
        photo={left:0,top,width:W,height:ph};
        area={left:margin,top:top+ph+margin,width:textWidth};
      }
      let y=area.top;
      text('brand','BRITES JEWELRY',area.left,y,area.width,brandHeight,brandSize,'brand');y+=brandHeight+gap;
      text('headline',headline,area.left,y,area.width,headHeight,hs,'headline',style.headlineFont);y+=headHeight+gap;
      if(showDescription){text('description',description,area.left,y,area.width,descHeight,ds,'description');y+=descHeight+gap;}
      const bw=Math.min(area.width,Math.max(108*typeScale,copy.cta.length*7.4+20)),x=layout.textAlign==='center'?area.left+(area.width-bw)/2:area.left;
      button(x,y,bw,bh,copy.cta.length*7.4+12>bw?'Shop now':copy.cta);
    }
    const scale=Math.min(photo.width/cropWidth,photo.height/cropHeight);
    objects.unshift(base('product_scene','photo',{type:'Image',sourceKey:image.id,left:photo.left+(photo.width-cropWidth*scale)/2,top:photo.top+(photo.height-cropHeight*scale)/2,width:cropWidth,height:cropHeight,cropX:(image.width-cropWidth)/2,cropY:0,scaleX:scale,scaleY:scale}));
    function upscale(o){if(o.type==='Image'){o.left*=factor;o.top*=factor;o.scaleX*=factor;o.scaleY*=factor;return;}for(const k of ['left','top','width','height','fontSize','aiBoxHeight','buttonPadding','rx','ry'])if(typeof o[k]==='number')o[k]*=factor;(o.objects||[]).forEach(upscale);}
    objects.forEach(upscale);
    return {version:'7.4.0',background:style.background,objects};
  }
  const variants=boards.flatMap(b=>['square','landscape','portrait'].includes(b.key)?['mobile','desktop'].map(device=>({...b,device})):[{...b,device:b.height<=100&&b.width<=320?'mobile':'desktop'}]);
  const api={layoutVersion,boards,variants,family,selectImage,document};if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesAdResponsive=api;
})(typeof window==='object'?window:globalThis);
