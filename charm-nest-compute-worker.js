/* Dedicated PDF/geometry worker. No network APIs or production state live here. */
importScripts('vendor/pdf-lib-1.17.1.min.js','vendor/clipper-6.4.2.js','charm-nest-vector.js','charm-nest-geom.js?v=20260921-material','charm-nest-pdf.js?v=20260924-followup','charm-nest-rose.js?v=20260923-edges','charm-nest-export.js');
const P=self.CharmNestPDF,parsedCache=new Map();
async function hydrate(parsed){
  if(parsed.doc)return parsed;
  let doc=parsedCache.get(parsed.computeKey);
  if(!doc){doc=await P.parseSource(parsed.bytes,parsed.name);if(parsed.computeKey){parsedCache.set(parsed.computeKey,doc);while(parsedCache.size>16)parsedCache.delete(parsedCache.keys().next().value);}}
  return {...parsed,doc:doc.doc,page:doc.page};
}
async function compute(type,a,progress){
  if(type==='parse'){
    const result=await P.parseSource(a.bytes,a.name);result.computeKey=a.key;parsedCache.set(a.key,result);while(parsedCache.size>16)parsedCache.delete(parsedCache.keys().next().value);
    const {doc,page,...plain}=result;return plain;
  }
  if(type==='indexGeometry'){const G=self.CharmNestGeom,up=G.upAngleOf(a.charm);G.backView(a.charm,{res:6,upAngle:up.angle});return up;}
  if(type==='group')return P.groupForTransfer(a.parsed,a.opts);
  if(type==='silhouettes'){
    await P.buildSilhouettes(null,a.charms,a.scale,progress);
    return a.charms.map(c=>Object.fromEntries(['bits','w','h','scale','bboxOuter','areaPt2','open','hash','thumb','widthPt','heightPt','centerPt'].map(k=>[k,c[k]])));
  }
  if(type==='thumbnail')return P.thumbnail(a.charm,a.size);
  if(type==='front'){
    const c=a.charm,b=c.bbox,pad=3*72/25.4,w=b[2]-b[0]+2*pad,h=b[3]-b[1]+2*pad,k=a.size/Math.max(w,h),cv=new OffscreenCanvas(Math.max(1,Math.round(w*k)),Math.max(1,Math.round(h*k))),ctx=cv.getContext('2d');
    ctx.fillStyle='#fff';ctx.fillRect(0,0,cv.width,cv.height);P.drawCharm(ctx,c,(x,y)=>[(x-b[0]+pad)*k,(b[3]+pad-y)*k],k);
    return new FileReaderSync().readAsDataURL(await cv.convertToBlob({type:'image/png'}));
  }
  if(type==='sheet'){
    const sources=new Map();for(const [key,p] of a.spec.sources)sources.set(key,await hydrate(p));
    const spec={...a.spec,sources},bytes=await P.buildSheet(spec);return {bytes,layers:spec.placements.map(p=>p.layerName)};
  }
  if(type==='single')return P.buildSingleCharm(a.charm,await hydrate(a.parsed));
  if(type==='back')return P.buildBackFile({...a.spec,parsed:await hydrate(a.spec.parsed)});
  if(type==='compose')return self.CharmNestExport.compose(a.front,a.sheet,a.backs);
  if(type==='backIndex')return self.CharmNestExport.backIndexPdf(a);
  if(type==='dxf'){const parsed=await P.parseSource(a.bytes,'Production sheet'),E=self.CharmNestExport;return E.dxf(E.productionPaths(parsed),E.layerNames(parsed));}
  if(type==='roseShapes')return self.CharmNestRose.shapes(a.charms,a.placements);
  throw new Error('Unknown calculation: '+type);
}
let chain=Promise.resolve();
self.onmessage=({data:m})=>{chain=chain.then(async()=>{
  try{const result=await compute(m.type,m.input,(done,total)=>self.postMessage({id:m.id,progress:[done,total]}));self.postMessage({id:m.id,result});}
  catch(e){self.postMessage({id:m.id,error:String(e.message||e)});}
});};
