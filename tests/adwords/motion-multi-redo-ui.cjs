const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
(async()=>{
 const names=['portrait','square','landscape'],keys=['mobile_portrait','mobile_square','desktop_landscape'];
 const films=names.map((format,i)=>({format,key:keys[i],device:i===2?'desktop':'mobile',width:720,height:720,seconds:10,asset:{hash:'old-'+format},url:'https://example.test/old-'+format}));
 const options=names.map((format,i)=>({format,orientation:format,formats:[keys[i]],kept:keys.filter(k=>k!==keys[i]),hasFilm:true,estimatedUsd:1.01}));
 for(const selection of [['portrait'],['portrait','landscape'],names]){
  const dom=new JSDOM('<div id="host"></div>',{runScripts:'outside-only',url:'https://example.test'}),w=dom.window;w.setTimeout=()=>0;w.clearTimeout=()=>{};w.HTMLMediaElement.prototype.pause=function(){};w.HTMLMediaElement.prototype.load=function(){};
  w.eval(fs.readFileSync('brites-ad-motion.js','utf8'));let state={ok:true,jobId:'old',workspaceId:'w',phase:'ready',variants:films,redoOptions:options},sent=[];
  const request=async(action,payload)=>{if(action==='adDesignMotionStatus')return JSON.parse(JSON.stringify(state));sent.push(payload);state={ok:true,jobId:'new',workspaceId:'w',phase:'running',making:selection,variants:films.filter(v=>!selection.includes(v.format)),displayVariants:films.map(v=>({...v,previous:selection.includes(v.format)})),videoStates:names.map(format=>({format,stage:selection.includes(format)?'generating':'ready',previous:selection.includes(format),progress:selection.includes(format)?null:100})),redoOptions:[]};return {ok:true,jobId:'new',queued:true,making:selection};};
  w.BritesAdMotion.mount(w.document.getElementById('host'),{scope:{workspaceId:'w',productId:'p',groupRef:'g'},request});await new Promise(setImmediate);const q=s=>w.document.querySelector(s),card=f=>q('[data-video-card="'+f+'"]');q('[data-sizes]').click();const players=Object.fromEntries(names.map(f=>[f,card(f).querySelector('video')]));
  for(const f of selection){const checkbox=card(f).querySelector('input');checkbox.checked=true;checkbox.dispatchEvent(new w.Event('change'));}
  assert.equal(q('[data-redo-selected]').textContent,'Redo selected ('+selection.length+')');q('[data-redo-selected]').click();assert.equal(sent.length,0);await q('[data-confirm] .bam-primary').onclick();assert.equal(sent.length,1);assert.deepEqual(JSON.parse(JSON.stringify(sent[0].redoFormats||[sent[0].redoFormat])),selection);assert.equal(sent[0].generationMode,'image_to_video');
  for(const f of names){assert.equal(card(f).querySelector('video'),players[f],'queued/running status never replaces the old player');if(selection.includes(f)){const bar=card(f).querySelector('progress');assert.ok(bar);assert.equal(bar.hasAttribute('value'),false,'unknown provider progress stays indeterminate');assert.match(card(f).textContent,/Current version stays visible/);}}
  const first=selection[0],fresh={...films.find(v=>v.format===first),asset:{hash:'new-'+first},url:'https://example.test/new-'+first};
  state={...state,variants:[...state.variants,fresh],displayVariants:state.displayVariants.map(v=>v.format===first?fresh:v),videoStates:state.videoStates.map(v=>v.format===first?{...v,stage:'ready',progress:100,updated:true,previous:false}:v)};
  await q('[data-refresh]').onclick();assert.notEqual(card(first).querySelector('video'),players[first]);assert.match(card(first).textContent,/✓ Updated/);assert.equal(card(first).querySelector('progress'),null);
  for(const f of names.filter(f=>f!==first))assert.equal(card(f).querySelector('video'),players[f],'each replacement leaves the other players alone');
  state={...state,phase:'needs_attention',canResume:true,error:'Provider paused',videoStates:state.videoStates.map(v=>v.stage==='generating'?{...v,stage:'attention'}:v)};await q('[data-refresh]').onclick();
  for(const f of selection.filter(f=>f!==first)){assert.equal(card(f).querySelector('video'),players[f]);assert.match(card(f).textContent,/Paused · saved video retained/);}
  // Reloading reads server fallbacks and completion states, rather than depending on local state.
  const host=w.document.createElement('div');w.document.body.appendChild(host);w.BritesAdMotion.mount(host,{scope:{workspaceId:'other',productId:'p',groupRef:'g'},request});await new Promise(setImmediate);host.querySelector('[data-sizes]').click();assert.equal(host.querySelectorAll('video').length,4);assert.match(host.querySelector('[data-video-card="'+first+'"]').textContent,/✓ Updated/);
  dom.window.close();
 }
 console.log('PASS one/two/three selections, atomic redo, per-video progress, retained previews, independent replacement, completion and reload feedback');
})().catch(e=>{console.error(e.stack);process.exit(1);});
