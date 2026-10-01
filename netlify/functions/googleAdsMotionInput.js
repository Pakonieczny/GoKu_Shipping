// Saved per-job input contract. A missing field always means the historical
// reference mode, so a deployment or preference change cannot alter a resume.
const PHOTO='image_to_video',REFERENCE='reference_to_video',DEFAULT=PHOTO;
const PRODUCT_POLICY=require('./googleAdsMotionProductLock').POLICY;
const fidelity=job=>forJob(job)===PHOTO&&job.photoFidelityPolicy===PRODUCT_POLICY;
const newPolicy=mode=>mode===PHOTO?PRODUCT_POLICY:null;
function validate(value){if(![PHOTO,REFERENCE].includes(value))throw Error('Choose Start from photo or New scene.');return value;}
const forJob=job=>job.generationMode==null?REFERENCE:validate(job.generationMode);
const revision=job=>forJob(job)===PHOTO?(fidelity(job)?'design-photo-product-attachment-v4':'original-first-frame-attachment-v3'):null;
function designPhoto(document,sources,productId){
 const layers=[];function visit(items){for(const o of items||[]){if(o.visible===false||o.opacity===0)continue;if(o.sourceKey)layers.push(o);visit(o.objects);}}visit(document?.objects);
 const layer=layers.find(o=>o.editorRole==='photo')||layers.find(o=>String(o.type).toLowerCase()==='image'&&o.editorRole!=='brand'&&o.editorRole!=='shape');
 const source=layer&&(sources||[]).find(s=>s.id===layer.sourceKey);
 if(!source?.asset||require('./googleAdsAdIdentity').foreign(source,productId))return null;
 return {...source,designLayer:layer};
}
function prompt(job,orientation,identityRules){
 const locked=fidelity(job)?require('./googleAdsMotionProductLock').prompt(job.productLock,job.firstFrameHash):'';
 const framing=orientation==='square'?'Deliver a 16:9 video whose central square contains the complete decorative jewelry and attachment.':orientation==='portrait'?'Deliver a 9:16 video.':'Deliver a 16:9 video.';
 return `Use the supplied photograph from the first saved design preview as the FIRST FRAME of one continuous ten-second live-action product video. Continue the photographed scene forward in time. The exact photographed jewelry, its supplied item count, support, background, placement, viewing angle, scale and illumination establish the opening shot. Preserve that specific manufactured design throughout.

${identityRules}
${require('./googleAdsMotionReferences').CONTINUITY_RULES}
${require('./googleAdsMotionReferences').ATTACHMENT_RULES}
${locked?locked+'\n\nTHROUGHOUT 0–10 SECONDS: Preserve the photographed pose, support, placement and reference-facing silhouette continuously. The original item remains the same visible physical object during every moment, especially the middle of the film. Gentle surrounding motion and small camera translation develop within this one view; never restage, hide or rotate the decorative piece.':''}

MOTION: Bring only elements already visible in this photograph to life. A freely suspended assembly may make one small natural rigid-body sway; a resting item stays supported while its visible flexible chain, fabric or other flexible surroundings move gently where physically plausible. ${locked?'If no flexible surroundings are visible, keep supported jewelry stable and use the camera translation and real moving-light reflections for physical depth; never force an unsupported object to settle or move.':'For rigid surroundings use a very small physically plausible in-plane settling movement of the jewelry.'} Keep movement restrained but clearly visible, with natural contact, depth, parallax and changing reflections. The decorative design itself never articulates. A short, slow camera translation stays close to the photographed view; the complete detailed face and its engraving remain sharp and readable. Preserve the light direction and material finish. Avoid a new angle that requires inventing hidden geometry.

${framing} Adapt the canvas by extending only the existing background where necessary. Keep the photographed product's proportions and complete silhouette; do not stretch, crop, rearrange or replace it to fit the canvas. Preserve the source placement as closely as the canvas allows. A long chain may continue out of frame exactly as in the photograph. Do not move the product to create a text zone; captions are composed later.

Keep the photographed setting. Add no new props, jewelry, display furniture, people or scene changes. Preserve every existing product marking and engraving; add no captions or lettering. No cuts or transitions. Generate real physical motion throughout, never a still photograph with only zoom or animated words. Do not extract, mask or paste a jewelry layer. The source photograph, rather than a written reconstruction of its motif, defines the product. FINAL CONTINUITY: The complete original item stays visible without interruption from the first frame through every middle frame to the last. End on that same item; never substitute another charm or reset its identity.`;
}
module.exports={PHOTO,REFERENCE,DEFAULT,PRODUCT_POLICY,fidelity,newPolicy,validate,forJob,revision,prompt,designPhoto};
