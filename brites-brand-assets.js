/* Immutable, user-supplied brand artwork. Source PNGs are preserved byte-for-byte. */
(function(root){const assets=[
 {id:'brites_brand_square',file:'logo-square.png',title:'Brites wordmark, square canvas',width:1024,height:1024,crop:{x:74,y:268,width:875,height:490}},
 {id:'brites_brand_wide',file:'logo-wide.png',title:'Brites wordmark, wide canvas',width:1024,height:576,crop:{x:200,y:114,width:624,height:349}},
 {id:'brites_brand_icon',file:'icon-blue.png',title:'Brites blue heart icon',width:789,height:592,crop:{x:0,y:0,width:789,height:592}}
].map(a=>({...a,url:'/assets/brites-brand/'+a.file}));
const api={assets,
 dataUrl:id=>{const a=assets.find(a=>a.id===id);if(!a)throw Error("Unknown brand asset");const fs=require("fs"),path=require("path");for(const base of [__dirname,process.cwd(),path.resolve(__dirname,"../..")]){const file=path.join(base,"assets/brites-brand",a.file);if(fs.existsSync(file))return "data:image/png;base64,"+fs.readFileSync(file).toString("base64");}throw Error("The official brand asset is missing from this deployment: "+a.file);},
get:id=>assets.find(a=>a.id===id),guidance:'Use only these official Brites brand assets; never redraw or invent a logo. Product name and recognizable Brites branding outrank optional promotional copy and oversized calls to action when space is tight. Use the blue heart icon with readable Brites Jewelry text in compact placements, or the actual wordmark where it remains recognizable. Keep logos separate from product identity reference photos. Never alter product geometry to make room for branding.'};if(typeof module==='object'&&module.exports)module.exports=api;else root.BritesBrandAssets=api;})(typeof window==='object'?window:globalThis);
