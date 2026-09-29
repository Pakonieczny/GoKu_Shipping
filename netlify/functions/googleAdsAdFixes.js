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
 const targets=formats.length?formats:kind==='master'?[keys.find(k=>k.endsWith((ctx.squareMaster||'portrait')==='landscape'?'landscape':'portrait'))||keys[0]]:keys; // a version 3 job defaults to its portrait film
 const orientation=kind==='master'?masterFor(targets[0],ctx.squareMaster):null;
 const affected=kind==='master'?keys.filter(k=>masterFor(k,ctx.squareMaster)===orientation):targets;
 const hints=kind==='caption'?captionHints(s):null;
 const usd=kind==='master'?Number(ctx.masterUsd)||0:0;
 const label=kind==='copy'?'Fix · revise this message and re-compose the captions (no new video)':kind==='caption'?'Fix · re-compose '+affected.join(', ')+' captions (no new video)':'Fix · regenerate only the '+orientation+' film'+(usd?' ≈ US$'+usd.toFixed(2):'')+' · '+affected.join(', ')+' re-composed, '+(ctx.squareMaster==='square'?'the other films':'the other film')+' kept';
 return {kind,category:deduction.category,index:deduction.index,formats:affected,orientation,hints,estimatedUsd:usd,label,reason:deduction.reason,evidence:deduction.evidence,correction:deduction.correction};
}
// A version 3 job passes squareMaster 'square': its square format has a master of its own. Earlier jobs crop
// the square from whichever of their two masters the job chose, portrait by default.
function masterFor(key,squareMaster){const family=String(key||'').split('_').pop();return family==='landscape'?'landscape':family==='portrait'?'portrait':squareMaster||'portrait';}
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
 const label=kind==='scene'?'Fix · regenerate only the '+sceneKey+' scene'+(usd?' ≈ US$'+usd.toFixed(2):'')+' · other scenes kept':'Fix · revise the copy/layout plan for this defect (no new images)';
 return {kind,category:deduction.category,index:deduction.index,formats:formats.length?formats:keys,sceneKey,estimatedUsd:usd,label,reason:deduction.reason,evidence:deduction.evidence,correction:deduction.correction};
}
function options(kind,quality,ctx){
 const out=[];if(!quality?.categoryReviews)return out;
 for(const [category,review]of Object.entries(quality.categoryReviews))for(let index=0;index<(review?.deductions||[]).length;index++){const d=deductionAt(quality,category,index);if(!d)continue;out.push(kind==='animated'?classifyAnimated(d,ctx):classifyStatic(d,ctx));}
 return out;
}
// Placement (a saved static set): a size whose photo cuts the charm, or that
// still cuts it when laid out around the measured charm, needs a new photo; a
// size that is fine once laid out again only needs a new layout. Text over the
// charm is not fixed by new photos. A specialised photo the set lacks takes
// over its sizes. Everything a person reads is in plain words, never ids.
const SPECIAL=['midLandscape','midPortrait'],EDGE_WORDS={top:'top',right:'right side',bottom:'bottom',left:'left side'};
const joinWords=list=>list.length<2?list.join(''):list.slice(0,-1).join(', ')+' and '+list[list.length-1],plural=(n,word)=>n+' '+word+(n===1?'':'s');
// A size in plain words: '250x360', or 'portrait on phones' for the master
// shapes. `active` is the saved design (its own artboard and device).
function sizeName(key,active){const m=/^(mobile|desktop)_(.+)$/.exec(key),board=m?m[2]:active?.artboard?.key,device=m?m[1]:active?.device==='desktop'?'desktop':'mobile',d=/^display_(\d+x\d+)$/.exec(board||'');return d?d[1]:String(board||key)+' on '+(device==='desktop'?'desktop':'phones');}
const sizeCount=(keys,active)=>new Set(keys.map(k=>sizeName(k,active))).size;
// results: every size's check (browser findings merged in); relayout: the same
// sizes laid out again around the measured charm; declared: the checks before
// the browser findings. sizes: [{key,boardKey}]; planned: scene keys in the
// saved plan; sceneFor(key): the scene behind a size; special: {sceneKey:
// {boards,shape}} for the specialised shapes; order: the scene catalog order.
function placementPlan({results=[],relayout={},declared=[],sizes=[],planned=new Set(),sceneFor=()=>null,special={},order=[],active}={}){
 const scenes=new Map(),relayoutFormats=[],unresolved=[],want=(key,size,issues=[])=>{const s=scenes.get(key)||{sizes:[],issues:[]};if(!s.sizes.includes(size))s.sizes.push(size);s.issues.push(...issues.filter(i=>i.kind!=='covered').map(i=>({...i,size})));scenes.set(key,s);};
 for(const key of SPECIAL)if(!planned.has(key)&&special[key])for(const size of sizes)if((special[key].boards||[]).includes(size.boardKey))want(key,size.key);
 for(const r of results){
  if(r.ok)continue;
  const taker=[...scenes.keys()].find(k=>!planned.has(k)&&scenes.get(k).sizes.includes(r.key));if(taker){want(taker,r.key,r.issues);continue;}
  const kinds=new Set((r.issues||[]).map(i=>i.kind)),again=relayout[r.key]||{ok:false,kinds:[]},scene=sceneFor(r.key);
  if(kinds.has('source-cut')||(kinds.has('cut')||kinds.has('missing'))&&!again.ok&&again.kinds.some(k=>k!=='covered')){if(scene)want(scene,r.key,r.issues);else unresolved.push(r.key);}
  else if(again.ok&&(kinds.has('cut')||kinds.has('missing')||declared.find(d=>d.key===r.key)?.issues.some(i=>i.kind==='covered')))relayoutFormats.push(r.key);
  else unresolved.push(r.key);
 }
 const rank=k=>{const i=order.concat(SPECIAL).indexOf(k);return i<0?99:i;},sceneKeys=[...scenes.keys()].sort((a,b)=>rank(a)-rank(b)||a.localeCompare(b)),corrections={};
 for(const key of sceneKeys){
  const s=scenes.get(key),named=list=>joinWords([...new Set(list.map(k=>sizeName(k,active)))]),edges=list=>joinWords([...new Set(list.flatMap(i=>i.edges||[]))].map(e=>EDGE_WORDS[e]||e)),own=s.issues.filter(i=>i.kind==='source-cut'),cut=s.issues.filter(i=>i.kind==='cut'),parts=[];
  if(!planned.has(key))parts.push('A new '+(special[key]?.shape?special[key].shape+' ':'')+'photo made for '+named(s.sizes)+'.');
  if(own.length)parts.push('The earlier photo itself cut off the charm at the '+edges(own)+', in '+named(own.map(i=>i.size))+'.');
  if(cut.length)parts.push('The charm was cut off at the '+edges(cut)+' in '+named(cut.map(i=>i.size))+'.');
  corrections[key]=parts.concat('Keep the complete charm and bail inside the photo with a clear margin.').join(' ');
 }
 const photoFormats=sceneKeys.flatMap(k=>scenes.get(k).sizes),formats=sizes.map(s=>s.key).filter(k=>photoFormats.includes(k)||relayoutFormats.includes(k));
 let reason=null;
 if(!sceneKeys.length&&!relayoutFormats.length){
  const names=joinWords([...new Set(unresolved.map(k=>sizeName(k,active)))]);
  reason=!unresolved.length?'Nothing to fix: every size shows the complete charm, and the set already has every photo shape.':unresolved.every(k=>results.find(r=>r.key===k)?.issues.some(i=>i.kind==='covered'))?'Text or the logo covers the charm in '+names+'. New photos will not fix that; move the text in the editor.':'The charm is not fully clear in '+names+', and new photos or a new layout would not fix it. Adjust '+(unresolved.length>1?'those sizes':'that size')+' in the editor.';
 }
 return {sceneKeys,relayoutFormats,corrections,formats,photoFormats,unresolved,reason};
}
function placementLabel({sceneKeys=[],relayoutFormats=[],photoFormats=[],estimatedUsd=0},active){
 const reframe=sizeCount(relayoutFormats,active);
 return (reframe?'Reframe '+plural(reframe,'size')+(sceneKeys.length?' and make '+plural(sceneKeys.length,'new photo'):''):'Make '+plural(sceneKeys.length,'new photo')+' for '+plural(sizeCount(photoFormats,active),'size'))+' · about US$'+Number(estimatedUsd||0).toFixed(2);
}
function placementProgress(fix,active){
 const reframe=sizeCount(fix.relayoutFormats||[],active),photos=sizeCount((fix.formats||[]).filter(k=>!(fix.relayoutFormats||[]).includes(k)),active);
 return 'Saved · '+[reframe?plural(reframe,'size')+' will be reframed':'',photos?'new photos will be made for '+plural(photos,'size'):''].filter(Boolean).join(' and ');
}
module.exports={ANIMATED_FORMATS,resolveFormats,deductionAt,classifyAnimated,classifyStatic,captionHints,masterFor,options,SPECIAL,sizeName,placementPlan,placementLabel,placementProgress};
