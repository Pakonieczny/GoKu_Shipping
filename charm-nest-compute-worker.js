/* Dedicated PDF/geometry worker. No network APIs or production state live here. */
importScripts('vendor/pdf-lib-1.17.1.min.js','vendor/clipper-6.4.2.js','charm-nest-vector.js','charm-nest-geom.js?v=20261009-flat-col','charm-nest-pdf.js?v=20261009-cutfill-gt1-bx2-hoops-col-rv71-pt1-plb2-bc2-pe2-bf1-bf2-bf3-as1-mi6-bf4-mi7-bb1-ba1-sx1','charm-nest-rose.js?v=20261007-edges-gc1','charm-nest-export.js?v=20261009-cutfill-plb1-bf1-ba1','charm-nest-pair.js?v=20261009-pr1-mi1-mi3-mi4-mi5-mi6-mi7-pm8-pm9-cg1','charm-nest-pair-thumb.js?v=20261009-pt3-pt2-mi1-mi2-mi5-mi6-mi7-rlp1-rlp3');
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
    // a mismatched pair design: both bodies side by side, Left / Right chips (charm-nest-pair-thumb.js); a highlight ("L" or "R") washes the other body out, body 0 | 1 draws that ear alone, mirror true draws the Right piece of a pair turned over, side adds its chip, pair true draws a matching pair's Left and Right side by side (an earring order line). Any other charm: the drawing below, unchanged.
    const PT=self.CharmNestPairThumb,pairCv=PT?PT.canvasFor(P,a.charm,{size:a.size,padPt:3*72/25.4,bg:'#fff',highlight:a.opts&&a.opts.highlight,body:a.opts&&a.opts.body,mirror:a.opts&&a.opts.mirror,side:a.opts&&a.opts.side,pair:a.opts&&a.opts.pair,facing:a.opts&&a.opts.facing,sku:a.opts&&a.opts.sku,other:a.opts&&a.opts.other,turnRight:a.opts&&a.opts.turnRight,makeCanvas:(w,h)=>new OffscreenCanvas(w,h)}):null;
    if(pairCv)return new FileReaderSync().readAsDataURL(await pairCv.convertToBlob({type:'image/png'}));
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
