// Which saved photo is the product's physical truth for an AI request.
// An operator crop of a verified photo IS that photo, trimmed on purpose, so the
// reference must show exactly what they framed. Generated artwork never qualifies.
const productKey=id=>String(id||'').split('/').pop();
const token=value=>/^[a-zA-Z0-9_-]{1,100}$/.test(String(value||''));
const owns=(ids,productId)=>(ids||[]).some(id=>productKey(id)===productKey(productId));
// A saved library record qualifies only when it is a crop of a verified photo.
function cropQualifies(saved,productId){
 return !!saved&&saved.kind==='crop'&&saved.artwork!==true&&(owns(saved.productIds,productId)||saved.rootSource?.kind==='product'&&productKey(saved.rootSource.productId)===productKey(productId));
}
async function verified(source,productId,library){
 if(source?.source?.kind==='product')return productKey(source.source.productId)===productKey(productId);
 if(source?.source?.kind==='upload')return owns(source.productIds,productId);
 if(source?.source?.kind==='library'&&token(source.source.imageId))return cropQualifies(await library(source.source.imageId),productId);
 return false;
}
// The artboard layer carries the operator's framing in cropX/cropY/width/height.
function frameOf(source,objects){
 const layer=(objects||[]).find(o=>o&&o.sourceKey===source.id),W=Number(source.width)||0,H=Number(source.height)||0;
 if(!layer||!W||!H)return null;
 const x=Math.max(0,Math.round(Number(layer.cropX)||0)),y=Math.max(0,Math.round(Number(layer.cropY)||0));
 const width=Math.max(1,Math.min(W-x,Math.round(Number(layer.width)||W))),height=Math.max(1,Math.min(H-y,Math.round(Number(layer.height)||H)));
 if(x<1&&y<1&&width>=W-1&&height>=H-1)return null;
 return {x,y,width,height};
}
// Trimmed bytes are saved under a deterministic key, so re-resolving the same
// frame reuses the same asset instead of storing another copy.
async function framed(source,objects,D){
 const frame=frameOf(source,objects);if(!frame)return source;
 const bytes=await D.loadAsset(source.asset);
 const out=await require('sharp')(bytes,{limitInputPixels:40000000}).extract({left:frame.x,top:frame.y,width:frame.width,height:frame.height}).png().toBuffer({resolveWithObject:true});
 const asset=await D.saveAsset(D.workspaceId,out.data,source.id+'_framed_'+D.sha([frame.x,frame.y,frame.width,frame.height]).slice(0,16),{width:out.info.width,height:out.info.height,mimeType:'image/png',kind:'operator-framed identity reference'});
 return {...source,asset,width:out.info.width,height:out.info.height,framedFrom:source.id,frame};
}
async function resolve({sources,objects,productId},D){
 const identity=[];
 for(const source of sources||[])if(await verified(source,productId,D.library))identity.push(await framed(source,objects,D));
 return identity;
}
module.exports={POLICY:'verified-product-v2',productKey,owns,cropQualifies,verified,frameOf,framed,resolve};
