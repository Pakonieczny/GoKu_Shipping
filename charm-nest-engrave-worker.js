/* Dedicated initial-fit worker. Fonts arrive once, already hash-verified by
 * the page. No cloud calls, DOM work or production writes happen here. */
"use strict";
importScripts("vendor/opentype-1.3.4.min.js", "charm-nest-text.js?v=20261019-wd1", "charm-nest-geom.js?v=20261009-flat-col-fb1-fb2-fr1-m3-wd1-cb1", "charm-nest-engrave-fit.js?v=20261019-wd1");
let fonts=null, fontError=null, emojiFont=null, emojiMap=null, sansRaw=null, symbolFont=null; const sets={};   // sets: the other engraving fonts by id (sent once, before the first piece that uses one)
// (WORLD, Paul 19 Oct 2026: Arabic, Hebrew, Indic, CJK lines) the script fonts and the shaper arrive on demand, in a "scripts" message before the first fit that needs them: HarfBuzz starts asynchronously, so the messages are handled one at a time, in order,
// and a fit never runs before the init that precedes it has finished. Every engraving font set is wrapped with the same (still empty) script set (CharmNestText.sharedScripts, the one withEmoji takes); the shaper script itself is only loaded when the first script font comes.
let scriptSet=CharmNestText.sharedScripts || null, scriptError=null, shapeLoaded=false;
let inbox=Promise.resolve();
self.onmessage=event=>{inbox=inbox.then(()=>handle(event)).catch(()=>{});};
async function handle({data}){
  if(data.type === "scripts") {
    try {
      if(!scriptSet)throw new Error("Script fonts are not supported by this worker.");
      if(!shapeLoaded){importScripts(data.shapeUrl || "charm-nest-shape.js");shapeLoaded=true;}
      if(!CharmNestShape.ready){if(!data.wasm)throw new Error("The shaping engine was not sent.");await CharmNestShape.init({wasm:data.wasm});}
      scriptSet.attach(CharmNestShape);
      for(const f of data.fonts || []) if(!scriptSet.isLoaded(f.id)) {const m=CharmNestShape.addFont(f.id,f.bytes);scriptSet.use(Object.assign({},f.entry,{capEm:m.capHeight/m.unitsPerEm}));}
      scriptError=null;
    } catch(error) {scriptError=error;}
    return;
  }
  if(data.type === "fonts" && data.key) {
    try {
      const set={};
      for(const weight of ["Regular","Semibold"]) if(data.fonts[weight]) set[weight]=opentype.parse(data.fonts[weight]);
      if(!set.Regular)throw new Error("Engraving font is unavailable.");
      if(emojiFont && emojiMap) for(const weight of ["Regular","Semibold"]) if(set[weight]) set[weight]=CharmNestText.withEmoji(set[weight],emojiFont,emojiMap,opentype.Path,[sansRaw,symbolFont].filter(Boolean));   // (as the page sets it: a letter this face lacks in Source Sans 3, a ♡ in the symbol font)
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
        sansRaw=fonts.Regular; symbolFont=data.fonts.symbol ? opentype.parse(data.fonts.symbol) : null;   // (the face itself, before it is wrapped, is every other font's fallback)
        for(const weight of ["Regular","Semibold"]) if(fonts[weight]) fonts[weight]=CharmNestText.withEmoji(fonts[weight],emoji,data.fonts.emojiMap,opentype.Path,symbolFont ? [symbolFont] : []);
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
    // a line in a script the page loaded a font for: the worker has that font and the shaper, or says so (never a cut set in the wrong font)
    for(const id of (data.input && data.input.scriptIds) || [])if(!scriptSet || !scriptSet.isLoaded(id))throw new Error("The script font "+id+" is unavailable in the background engraving worker"+(scriptError ? ": "+scriptError.message : "")+".");
    const key=data.input && data.input.fontKey, own=!!key && key !== "source-sans-3", set=own ? sets[key] : null;   // (Source Sans 3 is the default set, always here)
    if(own && (!set || set.error))throw new Error("The engraving font "+key+" is unavailable.");
    // (the words are set in exactly the script fonts the page chose for them: one regional font for a Chinese line, however it wraps)
    const ids=(data.input && data.input.scriptIds) || [],use=set || fonts,pin=f=>f && ids.length && f.pin ? f.pin(ids) : f;
    self.postMessage({id:data.id,result:CharmNestEngraveFit.calculate(data.input,ids.length ? {Regular:pin(use.Regular),Semibold:pin(use.Semibold)} : use,CharmNestGeom)});
  } catch(error) {
    self.postMessage({id:data.id,error:{message:error.message,stage:error.stage,checks:error.checks,images:error.images}});
  }
}
