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
// True when a saved photo is declared to belong to a different listing than productId.
// A film is only ever made from the photographs of the ad's own listing.
function foreign(source,productId){
 if(!source)return true;
 if(source.source?.kind==='product')return productKey(source.source.productId)!==productKey(productId);
 if((source.productIds||[]).length)return !owns(source.productIds,productId);
 if(source.productId)return productKey(source.productId)!==productKey(productId);
 return false;
}
async function resolve({sources,objects,productId},D){
 const identity=[];
 for(const source of sources||[])if(await verified(source,productId,D.library))identity.push({...await framed(source,objects,D),identityVerified:module.exports.POLICY,identityProductId:productId});
 return identity;
}
// Per-format identity of a film. Every format is filmed as its own take from the one identity reference, so the charm can drift in a single
// format: a feature added that the reference lacks (an eye, wing lines, engraving, a moulded look) or one it shows taken away. The set-level
// identity boolean and the 5% product-recognition weight do not catch that; the review returns one verdict per format and a failing format is named.
// Both directions are judged against the reference alone: a plain blank reference must stay plain and blank.
const CHECKS=[
 {field:'sameOutline',when:'the silhouette, beak or proportions differ from the SOURCE',fault:'a different outline, beak or proportions',need:'the same outline and proportions'},
 {field:'noAddedDetail',when:'the charm shows any feature the SOURCE lacks: an eye, wing or feather lines, a beak line, engraving, texture, pattern, raised or recessed detail, or a thicker, moulded or bevelled look',fault:'a feature the reference lacks (an eye, wing or feather lines, engraving, texture, or a thicker, moulded look)',need:'no feature the reference lacks'},
 {field:'noMissingDetail',when:'a detail the SOURCE clearly shows is missing or simplified',fault:'a detail the reference clearly shows that is missing or simplified',need:'every detail the reference clearly shows'},
 {field:'sameFeatures',when:'a cutout, ring, bail or loop the SOURCE shows is missing or changed, or one it does not show is added',fault:'cutouts, ring or loop that are missing, changed or added',need:'the same cutouts, ring and loop'}
];
const familyOf=key=>String(key||'').split('_').pop();
function formatIdentitySchema(keys){
 const verdict=()=>({type:'object',additionalProperties:false,properties:{...Object.fromEntries(CHECKS.map(c=>[c.field,{type:'boolean',description:'False when '+c.when+'.'}])),evidence:{type:'string',description:'The exact difference, or that the format matches.'}},required:[...CHECKS.map(c=>c.field),'evidence']});
 return {type:'object',additionalProperties:false,properties:Object.fromEntries(keys.map(k=>[k,verdict()])),required:[...keys]};
}
const formatRule=keys=>' PER-FORMAT CHARM IDENTITY. Each rendered format was filmed as its own separate take. FINAL n is renderedFormats[n-1] and each names its format key ('+keys.join(', ')+'). Compare the charm in EVERY sampled frame of EACH format, one format at a time, with the SOURCE, and return formatIdentity with one verdict for every one of those keys. Judge each format on its own: an exact charm in one format never excuses another. Each film must show EXACTLY the SOURCE, nothing added and nothing removed; a plain flat blank SOURCE must stay plain, flat and blank. '+CHECKS.map(c=>'Set '+c.field+'=false when '+c.when+'.').join(' ')+' Any chain, cord or hardware supplied as part of the referenced jewelry belongs to its identity: preserve its type, construction and relative scale. Natural flexible motion is allowed. Compare physical geometry, not pixel alignment under a change of angle. Write in evidence the exact difference, or that the format matches.';
// Turns those verdicts into a failed review that names the format, so the existing targeted fix regenerates only that film. Idempotent.
function applyFormatIdentity(quality,keys){
 const verdicts=quality&&quality.formatIdentity;
 if(!verdicts||typeof verdicts!=='object'||Array.isArray(verdicts)||quality.formatIdentityChecked)return quality;
 quality.formatIdentityChecked=true;
 const failed=[];
 for(const key of (keys&&keys.length?keys:Object.keys(verdicts))){
  const v=verdicts[key];if(!v||typeof v!=='object')continue;
  const found=CHECKS.filter(c=>v[c.field]===false);
  if(found.length)failed.push({key,found,evidence:String(v.evidence||'').replace(/\s+/g,' ').trim().slice(0,300)});
 }
 quality.formatIdentityFailures=failed.map(f=>f.key);
 if(!failed.length)return quality;
 const detail=f=>({
  reason:'The '+familyOf(f.key)+' film shows a charm that differs from the catalog reference',
  evidence:'Per-format identity check of '+f.key+': '+f.found.map(c=>c.fault).join('; ')+'.'+(f.evidence?' '+f.evidence:''),
  correction:'Regenerate this film from the same catalog photograph. The charm must have '+f.found.map(c=>c.need).join(', ')+': nothing added and nothing removed.'
 });
 quality.pass=false;quality.productFaithful=false;quality.exactProductIdentity=false;
 quality.issues=[...(Array.isArray(quality.issues)?quality.issues:[]),...failed.map(f=>{const d=detail(f);return d.reason+' ('+f.key+'): '+f.found.map(c=>c.fault).join('; ')+'.';})];
 // The named deduction is what the targeted fix reads: it carries the format key, so only that film is regenerated.
 const category=quality.categoryReviews&&quality.categoryReviews.productRecognition,scores=quality.scores;
 if(category&&Array.isArray(category.deductions)&&scores&&Number.isFinite(scores.productRecognition)){
  let left=scores.productRecognition;
  const added=failed.map(f=>{const points=Math.min(left,20);left-=points;return {points,...detail(f),kind:'required',formats:[f.key]};});
  quality.categoryReviews={...quality.categoryReviews,productRecognition:{...category,deductions:[...category.deductions,...added]}};
  quality.scores={...scores,productRecognition:left};
  const weights=require('./googleAdsAdQuality').WEIGHTS,total=Object.entries(weights).reduce((n,[k,w])=>n+(Number(quality.scores[k])||0)*w/100,0);
  if(Number.isFinite(quality.score))quality.score=Math.round(total*100)/100;
 }
 return quality;
}
module.exports={POLICY:'verified-product-v2',productKey,owns,cropQualifies,verified,foreign,frameOf,framed,resolve,CHECKS,formatIdentitySchema,formatRule,applyFormatIdentity};
