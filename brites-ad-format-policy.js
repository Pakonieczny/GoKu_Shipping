/* Verified Google asset requirements and Brites mobile-first creative framework. */
(function(root){
 'use strict';
 const policy={version:'2026-09-12',mobileTrafficShare:.8,
  video:{seconds:10,formats:[{key:'portrait',label:'Portrait · 9:16',width:720,height:1280},{key:'square',label:'Square · 1:1',width:720,height:720},{key:'landscape',label:'Landscape · 16:9',width:1280,height:720}],devices:['mobile','desktop'],maxBytes:500000000,googleMinimumSeconds:10,merchantMinimumSeconds:6},
  principles:[
   'Treat the operator’s 80% mobile share as the planning priority, not a measured performance result.',
   'Show the exact jewelry immediately. Preserve silhouette, finish, construction and realistic scale throughout every frame.',
   'Mobile: clear close framing, restrained motion, concise optional captions, strong contrast and generous space clear of placement controls.',
   'Desktop: retain useful scene context and allow supporting copy when legible. Reflow layouts; never rotate or stretch the product.',
   'Use image-only or video-only compositions when additional text would obscure the product. Google responsive copy and native CTA remain separate assets.',
   'Make the clip understandable without sound. No claims requiring music or voiceover; never invent product benefits.',
   'Keep Merchant photos and videos free of promotional graphics. Ads may use concise editorial captions; never draw pretend clickable controls.',
   'Generate two coordinated 10-second video masters: vertical and horizontal. Reuse these for six device/ratio exports instead of six paid video generations.',
   'Use subtle camera movement and lighting, not morphing jewelry or invented rotating surfaces. Keep the whole item inside the crop-safe area.',
   'A provider receipt, Google upload acceptance, policy approval and actual serving are distinct states.'
  ],sources:[
   {title:'Performance Max asset requirements',url:'https://developers.google.com/google-ads/api/performance-max/asset-requirements'},
   {title:'Merchant Center video_link',url:'https://support.google.com/merchants/answer/15216925?hl=en'},
   {title:'Shorts creative guidance',url:'https://support.google.com/google-ads/answer/16041697?hl=en'},
   {title:'Responsive ad image guidance',url:'https://support.google.com/google-ads/answer/9823397?hl=en'},
   {title:'Google video uploads',url:'https://developers.google.com/google-ads/api/docs/assets/upload-videos'}]};
 if(typeof module==='object'&&module.exports)module.exports=policy;else root.BritesAdFormatPolicy=policy;
})(typeof window==='object'?window:globalThis);
