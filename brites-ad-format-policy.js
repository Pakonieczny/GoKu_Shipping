/* Verified Google asset requirements and Brites mobile-first creative framework. */
(function(root){
 'use strict';
 const policy={version:'2026-09-14',mobileTrafficShare:.8,
  video:{seconds:10,formats:[{key:'portrait',label:'Portrait · 9:16',width:720,height:1280},{key:'square',label:'Square · 1:1',width:720,height:720},{key:'landscape',label:'Landscape · 16:9',width:1280,height:720}],devices:['mobile','desktop'],maxBytes:500000000,googleMinimumSeconds:10,merchantMinimumSeconds:6},
  images:[
   {key:'landscape',fieldType:'MARKETING_IMAGE',ratio:1.91,tolerance:.02,minWidth:600,minHeight:314,recommendedWidth:1200,recommendedHeight:628,maxBytes:5242880,minCount:1,maxCount:20,required:true},
   {key:'square',fieldType:'SQUARE_MARKETING_IMAGE',ratio:1,tolerance:.02,minWidth:300,minHeight:300,recommendedWidth:1200,recommendedHeight:1200,maxBytes:5242880,minCount:1,maxCount:20,required:true},
   {key:'portrait',fieldType:'PORTRAIT_MARKETING_IMAGE',ratio:.8,tolerance:.02,minWidth:480,minHeight:600,recommendedWidth:960,recommendedHeight:1200,maxBytes:5242880,minCount:0,maxCount:20,required:false},
   {key:'logo',fieldType:'LOGO',ratio:1,tolerance:.02,minWidth:128,minHeight:128,recommendedWidth:1200,recommendedHeight:1200,maxBytes:5242880,minCount:1,maxCount:5,required:true},
   {key:'landscapeLogo',fieldType:'LANDSCAPE_LOGO',ratio:4,tolerance:.02,minWidth:512,minHeight:128,recommendedWidth:1200,recommendedHeight:300,maxBytes:5242880,minCount:0,maxCount:20,required:false}],
  principles:[
   'Treat the operator’s 80% mobile share as the planning priority, not a measured performance result.',
   'Show the exact jewelry immediately. Preserve silhouette, finish, construction and realistic scale throughout every frame.',
   'Mobile: clear close framing, an immediate visual hook and intentional motion, concise optional captions, strong contrast and generous space clear of placement controls.',
   'Desktop: retain useful scene context and allow supporting copy when legible. Reflow layouts; never rotate or stretch the product.',
   'Use image-only or video-only compositions when additional text would obscure the product. Google responsive copy and native CTA remain separate assets.',
   'Make the clip understandable without sound. No claims requiring music or voiceover; never invent product benefits.',
   'Keep Merchant photos and videos free of promotional graphics. Ads may use concise editorial captions; never draw pretend clickable controls.',
   'Generate two coordinated 10-second video masters: vertical and horizontal. Export exactly three formats: portrait, square and landscape, used across devices.',
   'Use a product-specific camera progression and realistic moving reflections, not morphing jewelry or invented rotating surfaces. Keep the whole item inside the crop-safe area.',
   'A provider receipt, Google upload acceptance, policy approval and actual serving are distinct states.'
  ],sources:[
   {title:'Performance Max asset requirements',url:'https://developers.google.com/google-ads/api/performance-max/asset-requirements'},
   {title:'Merchant Center video_link',url:'https://support.google.com/merchants/answer/15216925?hl=en'},
   {title:'Shorts creative guidance',url:'https://support.google.com/google-ads/answer/16041697?hl=en'},
   {title:'Responsive ad image guidance',url:'https://support.google.com/google-ads/answer/9823397?hl=en'},
   {title:'Google video uploads',url:'https://developers.google.com/google-ads/api/docs/assets/upload-videos'}]};

 policy.shapeFor=function(width,height){
  var w=Number(width),h=Number(height);
  if(!isFinite(w)||!isFinite(h)||w<=0||h<=0)return null;
  var ratio=w/h,best=null;
  for(var i=0;i<policy.images.length;i++){var spec=policy.images[i],off=Math.abs(ratio-spec.ratio);
   // LOGO and SQUARE_MARKETING_IMAGE share 1:1; the field type decides between
   // them, so the ratio alone resolves to the marketing shape.
   if(spec.key==='logo')continue;
   if(off<=spec.tolerance*spec.ratio&&(!best||off<best.off))best={key:spec.key,off:off};}
  return {ratio:Math.round(ratio*1000)/1000,width:w,height:h,shape:best?best.key:'other',
   orientation:ratio>1.02?'landscape':ratio<0.98?'portrait':'square'};
 };

 policy.videoFieldType='YOUTUBE_VIDEO';
 policy.fieldTypeFor=function(shape){
  if(shape==='video')return policy.videoFieldType;
  for(var i=0;i<policy.images.length;i++)if(policy.images[i].key===shape)return policy.images[i].fieldType;
  return null;
 };
 policy.shapeForFieldType=function(fieldType){
  var name=String(fieldType||'').toUpperCase();
  if(name===policy.videoFieldType)return 'video';
  for(var i=0;i<policy.images.length;i++)if(policy.images[i].fieldType===name)return policy.images[i].key;
  return null;
 };
 // A slot's plain-language name, for a report a person reads.
 policy.fieldTypeLabel=function(fieldType){
  var shape=policy.shapeForFieldType(fieldType);
  var labels={landscape:'Landscape 1.91:1',square:'Square 1:1',portrait:'Portrait 4:5',logo:'Logo 1:1',landscapeLogo:'Landscape logo 4:1',video:'Video'};
  return shape&&labels[shape]||String(fieldType||'').replace(/_/g,' ').toLowerCase().replace(/^./,function(c){return c.toUpperCase();});
 };
 if(typeof module==='object'&&module.exports)module.exports=policy;else root.BritesAdFormatPolicy=policy;
})(typeof window==='object'?window:globalThis);
