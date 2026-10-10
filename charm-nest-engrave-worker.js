/* Dedicated initial-fit worker. Fonts arrive once, already hash-verified by
 * the page. No cloud calls, DOM work or production writes happen here. */
"use strict";
importScripts("vendor/opentype-1.3.4.min.js", "charm-nest-text.js", "charm-nest-geom.js?v=20261009-flat-col", "charm-nest-engrave-fit.js");
let fonts=null, fontError=null, emojiFont=null, emojiMap=null; const sets={};   // sets: the other engraving fonts by id (sent once, before the first piece that uses one)
self.onmessage=({data})=>{
  if(data.type === "fonts" && data.key) {
    try {
      const set={};
      for(const weight of ["Regular","Semibold"]) if(data.fonts[weight]) set[weight]=opentype.parse(data.fonts[weight]);
      if(!set.Regular)throw new Error("Engraving font is unavailable.");
      if(emojiFont && emojiMap) for(const weight of ["Regular","Semibold"]) if(set[weight]) set[weight]=CharmNestText.withEmoji(set[weight],emojiFont,emojiMap,opentype.Path);
      sets[data.key]=set;
    } catch(error) {sets[data.key]={error};}
    return;
  }
  if(data.type === "fonts") {
    try {
      fonts={};
      for(const weight of ["Regular","Semibold"]) if(data.fonts[weight]) fonts[weight]=opentype.parse(data.fonts[weight]);
      if(data.fonts.emoji && data.fonts.emojiMap) {
        const emoji=opentype.parse(data.fonts.emoji); emojiFont=emoji; emojiMap=data.fonts.emojiMap;
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
    const key=data.input && data.input.fontKey, own=!!key && key !== "source-sans-3", set=own ? sets[key] : null;   // (Source Sans 3 is the default set, always here)
    if(own && (!set || set.error))throw new Error("The engraving font "+key+" is unavailable.");
    self.postMessage({id:data.id,result:CharmNestEngraveFit.calculate(data.input,set || fonts,CharmNestGeom)});
  } catch(error) {
    self.postMessage({id:data.id,error:{message:error.message,stage:error.stage,checks:error.checks,images:error.images}});
  }
};
