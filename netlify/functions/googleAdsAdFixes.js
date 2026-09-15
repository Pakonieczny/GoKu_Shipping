// Targeted corrections for reviewed ads. Every plan changes one reviewed
// defect in the named ad only; unaffected paid images, films and captions
// are reused byte-for-byte. Review text is evidence, never an instruction.
const ANIMATED_FORMATS=['mobile_portrait','mobile_square','desktop_landscape'];
const text=d=>[d.correction,d.reason,d.evidence].map(v=>String(v||'')).join(' ').toLowerCase();
const words=(re,s)=>re.test(s);
// Resolve which rendered formats a deduction names. The reviewer may list exact
// keys; older reviews only describe them in prose.
function resolveFormats(deduction,keys){
 const known=(keys||[]).map(String);
 if(Array.isArray(deduction.formats)&&deduction.formats.length){const listed=deduction.formats.map(String).filter(f=>known.includes(f));if(listed.length)return listed;}
 const s=text(deduction),found=new Set();
 for(const key of known){
  const lower=key.toLowerCase(),size=lower.match(/(\d+)x(\d+)$/),family=lower.split('_').pop();
  if(s.includes(lower)||s.includes(lower.replace(/_/g,' ')))found.add(key);
  else if(size&&new RegExp('(?<![0-9])'+size[1]+'\\s*[x×]\\s*'+size[2]+'(?![0-9])').test(s))found.add(key);
  else if(['portrait','square','landscape'].includes(family)&&new RegExp('\\b'+family+'\\b|\\b'+({portrait:'9:16',square:'1:1',landscape:'16:9'})[family].replace(':','\\s*:\\s*')).test(s))found.add(key);
 }
 return [...found];
}
function deductionAt(quality,category,index){
 const list=quality?.categoryReviews?.[category]?.deductions;if(!Array.isArray(list)||!Number.isInteger(index)||!list[index])return null;
 return {...list[index],category,index};
}
// Animated: caption (free re-composition), copy (text revision, free re-composition)
// or master (one regenerated film feeding the named format; the other film stays).
function classifyAnimated(deduction,ctx={}){
 const keys=ctx.formatKeys||ANIMATED_FORMATS,formats=resolveFormats(deduction,keys),s=text(deduction),correction=String(deduction.correction||'').toLowerCase();
 const scene=/\b(scene|background|backdrop|prop|props|lighting|light|setting|jewel\w*|product|charm|pendant|geometry|engrav\w*|distort\w*|morph\w*|blur\w*|motion|camera|framing|reframe|crop\w*|sharp\w*|colou?r|shine|reflection|identity|invented|stones?|metal|chain|leaf|film|footage|video|shot)\b/;
 const caption=/\b(text|type|typograph\w*|font|caption\w*|legib\w*|readab\w*|small|tiny|larger|bigger|size|overlap\w*|cover\w*|obscur\w*|collid\w*|collision|contrast|wash|fade|band|placement|position\w*|align\w*|margin|spacing|crowd\w*|logo|wordmark|hierarchy)\b/;
 const copy=/\b(copy|wording|word\w*|headline|message|messaging|call to action|cta|claim\w*|tagline|phrase|verb|benefit|hook|repeat\w*|redundan\w*|generic|vague|persuasi\w*|grammar|spell\w*)\b/;
 const rewrite=/\b(rewrite|reword|rephrase|replace the|shorten|clarify|stronger|specific|distinct|persuasi\w*|claim\w*|wording|verb|benefit|hook|tagline|repeat\w*|redundan\w*|generic|vague|grammar|spell\w*)\b/;
 const captionAction=/\b(enlarge|bigger|larger|shrink|reduce|move|shift|nudge|reposition|raise|lower|relocate|increase|decrease|strengthen|darken|lighten)\b.*\b(text|type|typograph\w*|font|caption\w*|headline|message|messaging|copy|wash|fade|logo|wordmark|call to action|cta)\b|\b(text|caption\w*|headline|copy|wash|logo)\b.*\b(away from|off the|clear of|overlap\w*|cover\w*)\b/;
 let kind;
 if(deduction.category==='messaging')kind='copy';
 else if(words(captionAction,correction))kind='caption';
 else if(words(caption,correction)&&!words(scene,correction)&&!words(rewrite,correction))kind='caption';
 else if(words(copy,correction)&&!words(scene,correction))kind='copy';
 else if(deduction.category==='layout'&&words(caption,s)&&!words(scene,correction))kind='caption';
 else kind='master';
 const targets=formats.length?formats:kind==='master'?[keys.find(k=>k.endsWith((ctx.squareMaster||'landscape')==='landscape'?'landscape':'portrait'))||keys[0]]:keys;
 const orientation=kind==='master'?masterFor(targets[0],ctx.squareMaster):null;
 const affected=kind==='master'?keys.filter(k=>masterFor(k,ctx.squareMaster)===orientation):targets;
 const hints=kind==='caption'?captionHints(s):null;
 const usd=kind==='master'?Number(ctx.masterUsd)||0:0;
 const label=kind==='copy'?'Fix · revise this message and re-compose the captions (no new video)':kind==='caption'?'Fix · re-compose '+affected.join(', ')+' captions (no new video)':'Fix · regenerate only the '+orientation+' film'+(usd?' ≈ $'+usd.toFixed(2):'')+' · '+affected.join(', ')+' re-composed, the other film kept';
 return {kind,category:deduction.category,index:deduction.index,formats:affected,orientation,hints,estimatedUsd:usd,label,reason:deduction.reason,evidence:deduction.evidence,correction:deduction.correction};
}
function masterFor(key,squareMaster){const family=String(key||'').split('_').pop();return family==='landscape'?'landscape':family==='portrait'?'portrait':squareMaster||'landscape';}
function captionHints(s){
 const h={};
 if(/\b(small|tiny|larger|bigger|size|legib\w*|readab\w*|cramp\w*|crowd\w*)\b/.test(s))h.preferBand=true;
 if(/\b(overlap\w*|cover\w*|obscur\w*|collid\w*|collision|behind|touch\w*|clearance)\b/.test(s))h.clearance=.06;
 if(/\b(contrast|wash|fade|busy|competing|legib\w*|readab\w*)\b/.test(s))h.wash='strong';
 if(!Object.keys(h).length){h.preferBand=true;h.wash='strong';}
 return h;
}
// Static: scene (one regenerated photograph for the named format's scene) or
// plan (copy/style revision through the text model, every image reused).
function classifyStatic(deduction,ctx={}){
 const keys=ctx.formatKeys||[],formats=resolveFormats(deduction,keys),s=text(deduction),correction=String(deduction.correction||'').toLowerCase();
 const visual=/\b(photo\w*|image|imagery|scene|background|backdrop|prop|props|lighting|light|shadow|setting|jewel\w*|product|charm|pendant|geometry|engrav\w*|distort\w*|blur\w*|sharp\w*|crop\w*|framing|colou?r|palette|seam|texture|invented|stones?|reflection|metal|chain|leaf|duplicate\w*|generic|recolou?r\w*)\b/;
 const textual=/\b(copy|wording|word\w*|headline|description|message|messaging|call to action|cta|button|claim\w*|text|font|typograph\w*|size|legib\w*|readab\w*|spacing|align\w*|hierarchy|margin|balance|brand|logo|name|price|benefit|hook|persuasi\w*)\b/;
 let kind;
 if(deduction.category==='messaging')kind='plan';
 else if(deduction.category==='layout')kind=words(visual,correction)&&!words(textual,correction)?'scene':'plan';
 else kind=words(textual,correction)&&!words(visual,correction)?'plan':'scene';
 let sceneKey=null;
 if(kind==='scene'){
  const map=ctx.sceneFor||(()=>null),counts=new Map();for(const f of (formats.length?formats:keys)){const k=map(f);if(k)counts.set(k,(counts.get(k)||0)+1);}
  sceneKey=[...counts.entries()].sort((a,b)=>b[1]-a[1])[0]?.[0]||ctx.masterScene||null;
  if(!sceneKey)kind='plan';
 }
 const usd=kind==='scene'?Number(ctx.sceneUsd?.(sceneKey))||0:0;
 const label=kind==='scene'?'Fix · regenerate only the '+sceneKey+' scene'+(usd?' ≈ $'+usd.toFixed(2):'')+' · other scenes kept':'Fix · revise the copy/layout plan for this defect (no new images)';
 return {kind,category:deduction.category,index:deduction.index,formats:formats.length?formats:keys,sceneKey,estimatedUsd:usd,label,reason:deduction.reason,evidence:deduction.evidence,correction:deduction.correction};
}
function options(kind,quality,ctx){
 const out=[];if(!quality?.categoryReviews)return out;
 for(const [category,review]of Object.entries(quality.categoryReviews))for(let index=0;index<(review?.deductions||[]).length;index++){const d=deductionAt(quality,category,index);if(!d)continue;out.push(kind==='animated'?classifyAnimated(d,ctx):classifyStatic(d,ctx));}
 return out;
}
module.exports={ANIMATED_FORMATS,resolveFormats,deductionAt,classifyAnimated,classifyStatic,captionHints,masterFor,options};
