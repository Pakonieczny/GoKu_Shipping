/* Dedicated initial-fit worker. Fonts arrive once, already hash-verified by
 * the page. No cloud calls, DOM work or production writes happen here. */
"use strict";
importScripts("vendor/opentype-1.3.4.min.js", "charm-nest-text.js", "charm-nest-geom.js", "charm-nest-engrave-fit.js");
let fonts=null, fontError=null;
self.onmessage=({data})=>{
  if(data.type === "fonts") {
    try {
      fonts={};
      for(const weight of ["Regular","Semibold"]) if(data.fonts[weight]) fonts[weight]=opentype.parse(data.fonts[weight]);
      if(data.fonts.emoji && data.fonts.emojiMap) {
        const emoji=opentype.parse(data.fonts.emoji);
        for(const weight of ["Regular","Semibold"]) if(fonts[weight]) fonts[weight]=CharmNestText.withEmoji(fonts[weight],emoji,data.fonts.emojiMap,opentype.Path);
      }
      if(!fonts.Regular)throw new Error("Engraving font is unavailable.");
      fontError=null;
    } catch(error) {fontError=error;}
    return;
  }
  if(data.type !== "fit")return;
  try {
    if(fontError)throw fontError;
    if(!fonts)throw new Error("Engraving fonts are not ready.");
    self.postMessage({id:data.id,result:CharmNestEngraveFit.calculate(data.input,fonts,CharmNestGeom)});
  } catch(error) {
    self.postMessage({id:data.id,error:{message:error.message,stage:error.stage,checks:error.checks,images:error.images}});
  }
};
