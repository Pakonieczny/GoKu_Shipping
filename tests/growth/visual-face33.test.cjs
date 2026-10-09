'use strict';

// Actual SVG controller and real Three geometry under a synthetic renderer.
// No physical microphone, provider inference, GPU appearance or trust claim.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const {JSDOM} = require('jsdom'), THREE = require('three');
const avatar = require('../../brites-concierge-avatar.js');
const zero = () => ({amplitude:0,bands:[0,0,0,0,0,0],brightness:0,valid:false});
const measured = (amplitude=.65,bands=[.8,.65,.4,.22,.1,.02],brightness=.3) => ({amplitude,bands,brightness,valid:true});

async function rig(t,loadScene) {
  const dom = new JSDOM('<main></main>',{url:'https://sandbox.example/',pretendToBeVisual:true}), win=dom.window;
  let clock=1000,hidden=false,job=0;const timers=new Map();
  Object.defineProperty(win.performance,'now',{value:()=>clock});Object.defineProperty(win.document,'hidden',{get:()=>hidden});
  win.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
  win.requestAnimationFrame=()=>0;win.cancelAnimationFrame=()=>{};
  win.setTimeout=(callback,delay)=>{timers.set(++job,{callback,at:clock+delay});return job;};win.clearTimeout=id=>timers.delete(id);
  const guide=avatar.create({container:win.document.querySelector('main'),visible:true,greetingOnOpen:false,loadScene:loadScene||(async()=>{throw Error('Synthetic no WebGL');})});
  t.after(()=>{guide.destroy();win.close();});await guide.ready;
  return {guide,advance(ms,source){for(let i=0;i<ms;i+=16){clock+=16;for(const [id,value] of [...timers])if(value.at<=clock){timers.delete(id);value.callback();}source?.();guide.lookAt(0,0,false);}},hide(value){hidden=value;win.document.dispatchEvent(new win.Event('visibilitychange'));},get clock(){return clock;}};
}
const svg=guide=>Object.fromEntries(['brow--left','brow--right','ribbon--left','ribbon--right','smile-signal','speech-mouth'].map(name=>[name,guide.element.querySelector('.brites-avatar__'+name).getAttribute('d')]));

function geometryRig(t) {
  const dom=new JSDOM('<main></main>',{pretendToBeVisual:true}),win=dom.window;let renderer,clock=1000;
  Object.defineProperty(win.performance,'now',{value:()=>clock});const stage=win.document.querySelector('main');stage.getBoundingClientRect=()=>({width:480,height:440});
  win.HTMLCanvasElement.prototype.getContext=type=>type==='2d'?{createImageData(w,h){return {data:new Uint8ClampedArray(w*h*4)};},putImageData(){}}:null;
  class Renderer{constructor(){renderer=this;this.domElement=win.document.createElement('canvas');this.shadowMap={};this.capabilities={getMaxAnisotropy:()=>1};this.info={render:{calls:0,triangles:0},reset(){}};}setClearColor(){}setPixelRatio(v){this.ratio=v;}getPixelRatio(){return this.ratio;}setSize(){}setAnimationLoop(){}render(scene,camera){this.scene=scene;this.camera=camera;}dispose(){}forceContextLoss(){}}
  class Pmrem{fromEquirectangular(texture){return {texture,dispose(){}};}dispose(){}}
  const module={exports:{}},source=fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'),'utf8').replace(/^import [^\n]+\n/gm,'').replace(/export (const|function) /g,'$1 ');
  vm.runInNewContext(source+'\nmodule.exports={createAvatarScene};',{module,THREE:{...THREE,WebGLRenderer:Renderer,PMREMGenerator:Pmrem}});
  const engine=module.exports.createAvatarScene({container:stage,quality:{...avatar.qualityFor({width:1440,bloom:false}),textureSize:16},onFrame:()=>avatar.poseFor()});
  t.after(()=>{engine.destroy();win.close();});return {engine,pose(value){engine.render({...avatar.poseFor(value),...value},true);renderer.scene.updateMatrixWorld(true);},get(name){return renderer.scene.getObjectByName(name);},advance(ms){clock+=ms;}};
}
const vertices=mesh=>Array.from(mesh.geometry.attributes.position.array);

test('normal explanation, reflective repair, inquiry, support and appreciation change the complete SVG face',async t=>{
  const h=await rig(t),g=h.guide;g.setState('speaking');const samples=[];
  for(const [kind,intensity] of [['explain',.66],['reflect',.62],['inquiry',.78],['support',.8],['appreciate',.76],['celebrate',.86]]){g.setExpression({kind,intensity});h.advance(800);samples.push({kind,paths:svg(g),pose:g.snapshot().facePose});}
  for(const key of ['brow--left','brow--right','ribbon--left','ribbon--right','smile-signal'])assert.ok(new Set(samples.map(value=>value.paths[key])).size>=5,key+' changes by authored meaning, independent of colour');
  const explanation=samples[0].pose,repair=samples[1].pose,inquiry=samples[2].pose,support=samples[3].pose;
  assert.ok(repair.mouthCurve>0&&repair.mouthCurve<explanation.mouthCurve-.005,'considerate repair keeps a small softened closed smile');assert.ok(repair.browConcern>explanation.browConcern+.1);assert.ok(repair.mouthTension>explanation.mouthTension+.02);
  assert.ok(samples[4].pose.mouthCurve>repair.mouthCurve+.025,'appreciation is visibly distinct from repair');
  for(const pose of [repair,inquiry,support]){assert.ok(Math.abs(pose.faceBrowTilt)<=.01);assert.equal(pose.eyeAsymmetry,0);assert.equal(pose.mouthSkew,0);}
  assert.ok(inquiry.faceBrowLift>explanation.faceBrowLift+.07);assert.ok(inquiry.eyeRoundness>explanation.eyeRoundness+.02);assert.ok(support.browConcern>explanation.browConcern+.12&&support.browConcern<=.2,'support has bounded considerate concern');
  for(const {pose} of samples){assert.equal(pose.mouthOpen,0);assert.equal(pose.speechEnergy,0);assert.ok(Object.values(pose).every(Number.isFinite));}
});

test('native incoming signal makes attentive eyes and brows responsive without a speaking mouth or guessed mood',async t=>{
  const h=await rig(t),g=h.guide;g.setState('listening');g.setExpression({kind:'attentive',intensity:.45});h.advance(1200);const before=g.snapshot().facePose,curve=svg(g)['smile-signal'];
  assert.equal(g.setInputSignal(measured(.8)),true);h.advance(600,()=>g.setInputSignal(measured(.8)));const after=g.snapshot().facePose;
  assert.ok(after.inputEnergy>.79);assert.ok(after.faceBrowLift>before.faceBrowLift+.055);assert.ok(after.eyeRoundness>before.eyeRoundness+.03);assert.ok(after.eyeOpen>before.eyeOpen);
  assert.equal(svg(g)['smile-signal'],curve);assert.equal(g.snapshot().expression.kind,'attentive');assert.equal(g.snapshot().facePose.speechEnergy,0);assert.equal(g.element.querySelector('.brites-avatar__speech-mouth').getAttribute('opacity'),'0');
  assert.ok([...g.element.querySelectorAll('.brites-avatar__speech-band,.brites-avatar__speech-ripple')].every(node=>Number(node.getAttribute('opacity'))===0));
});

test('incoming silence, stale source, malformed packets and every stop boundary clear listening energy',async t=>{
  const h=await rig(t),g=h.guide;g.setState('listening');
  const active=()=>{g.setState('listening');g.setInputSignal(measured(.8));h.advance(150,()=>g.setInputSignal(measured(.8)));assert.ok(g.snapshot().facePose.inputEnergy>.6);};
  active();h.advance(300);assert.equal(g.snapshot().inputSignal.valid,false);assert.equal(g.snapshot().facePose.inputEnergy,0,'source expiry clears without fabricating another packet');
  active();assert.equal(g.setInputSignal({...measured(),bands:[1,2,3]}),false);assert.deepEqual(g.snapshot().inputSignal,zero());
  for(const [stop,resume] of [[()=>g.setPaused(true),()=>g.setPaused(false)],[()=>h.hide(true),()=>h.hide(false)],[()=>g.setReducedMotion(true),()=>g.setReducedMotion(false)],[()=>g.setState('speaking'),()=>g.setState('listening')]]){active();stop();assert.deepEqual(g.snapshot().inputSignal,zero());assert.equal(g.setInputSignal(measured()),false);resume();assert.equal(g.snapshot().facePose.inputEnergy,0);}
  active();g.destroy();assert.deepEqual(g.snapshot().inputSignal,zero());assert.equal(g.setInputSignal(measured()),false);
});

test('measured spectrum is integrated into the closed emotional mouth in SVG and never invents an idle waveform',async t=>{
  const h=await rig(t),g=h.guide;g.setState('speaking');g.setExpression({kind:'reflect',intensity:.8});h.advance(1200);const semantic=svg(g)['smile-signal'];
  g.setSpeechSignal(measured(.7));h.advance(500);const low=svg(g)['speech-mouth'];assert.notEqual(low,semantic,'measured sound crests are on the expressive curve itself');
  g.setSpeechSignal(measured(.7,[.01,.04,.15,.4,.65,.9],.8));h.advance(1000);const high=svg(g)['speech-mouth'];assert.notEqual(high,low);assert.equal(svg(g)['smile-signal'],semantic);
  const held=svg(g);h.advance(900);assert.deepEqual(svg(g),held,'the same spectrum holds the same shape; wall clock invents no sound');
  g.setSpeechSignal(zero());assert.equal(g.snapshot().facePose.mouthOpen,0);assert.equal(g.element.querySelector('.brites-avatar__speech-mouth').getAttribute('opacity'),'0');assert.equal(svg(g)['smile-signal'],semantic);
});

test('meter callbacks paint the actual active Three scene even with no animation frames',async t=>{
  const frames=[],engine={setMotion(){},render(value){frames.push({...value});},invalidate(){},snapshot(){return {}},destroy(){}};
  const h=await rig(t,async()=>({createAvatarScene:()=>engine})),g=h.guide;assert.equal(g.snapshot().mode,'webgl');g.setState('speaking');
  const before=frames.length;g.setSpeechSignal(measured(.8));h.advance(80);g.setSpeechSignal(measured(.8));assert.ok(frames.length>=before+2);assert.ok(frames.at(-1).speechEnergy>.6,'current measured output is explicitly painted, not only hidden SVG');
  g.setSpeechSignal(zero());assert.equal(frames.at(-1).speechEnergy,0);g.setState('listening');g.setInputSignal(measured(.6));h.advance(96);g.setInputSignal(measured(.6));assert.ok(frames.at(-1).inputEnergy>.35);assert.equal(frames.at(-1).speechEnergy,0);
  g.setPaused(true);const count=frames.length;g.setInputSignal(measured());g.setSpeechSignal(measured());assert.equal(frames.length,count,'blocked meters cannot draw behind a pause');
});

test('real Three geometry has semantic brows, eyes and smile/frown with on-curve measured sound contours',t=>{
  const h=geometryRig(t),samples=[];
  for(const kind of ['explain','reflect','inquiry','support','appreciate','celebrate']){h.pose({state:'speaking',expression:{kind,intensity:.8},speechSignal:measured(.65),time:4});samples.push({kind,eye:vertices(h.get('expression-eye-left')),brow:vertices(h.get('expression-brow-left')),smile:vertices(h.get('expression-smile-glyph')),mouth:vertices(h.get('expression-speech-mouth'))});}
  for(const key of ['eye','brow','smile','mouth'])assert.ok(new Set(samples.map(value=>JSON.stringify(value[key]))).size>=5,'actual '+key+' topology changes with meaningful cues');
  h.pose({state:'speaking',expression:{kind:'reflect',intensity:.8},speechSignal:measured(.65),time:4});const base=vertices(h.get('expression-smile-glyph')),audio=vertices(h.get('expression-speech-mouth')),bar=h.get('expression-speech-bar-2');assert.ok(Math.abs(bar.position.y-h.get('expression-speech-mouth').position.y)<.001,'spectrum segments sit on the mouth, not disconnected beneath it');
  h.pose({state:'speaking',expression:{kind:'reflect',intensity:.8},speechSignal:measured(.65,[.01,.04,.15,.4,.65,.9],.8),time:4});assert.notDeepEqual(vertices(h.get('expression-speech-mouth')),audio);assert.deepEqual(vertices(h.get('expression-smile-glyph')),base,'measured spectrum cannot turn thoughtful repair into a smile');
  const held=vertices(h.get('expression-speech-mouth'));h.advance(7000);h.pose({state:'speaking',expression:{kind:'reflect',intensity:.8},speechSignal:measured(.65,[.01,.04,.15,.4,.65,.9],.8),time:4});assert.deepEqual(vertices(h.get('expression-speech-mouth')),held);
  for(const {eye,brow,smile,mouth} of samples)assert.ok([...eye,...brow,...smile,...mouth].every(Number.isFinite));assert.equal(h.get('sculpted-porcelain-torso').parent.rotation.z,0);assert.equal(h.engine.snapshot().character.faceGeometry.mouthClosed,true);
});

test('amplitude-only legacy and silent sources never manufacture spectral energy',()=>{
  assert.equal(avatar.spectrumContour(.25,[0,0,0,0,0,0],1),0);assert.equal(avatar.spectrumContour(.25,[1,1,1,1,1,1],0),0);
  const p=avatar.poseFor({state:'speaking',level:.7,time:4});assert.deepEqual(p.speechBands,[0,0,0,0,0,0]);assert.equal(p.mouthOpen,0);
  for(const state of ['idle','thinking','speaking','success','error']){const v=avatar.poseFor({state,inputSignal:measured(),time:4});assert.equal(v.inputEnergy,0,'incoming analysis is confined to listening');}
  for(const kind of avatar.EXPRESSION_KINDS){const p=avatar.poseFor({state:'speaking',expression:{kind,intensity:1},speechSignal:measured(1),time:4});assert.equal(p.mouthOpen,0);assert.ok(Math.abs(p.mouthCurve)<.08);assert.ok(p.lidClosure<.05);}
});

test('quiet audible voice retains visible contour energy without double-multiplying amplitude-scaled frequency bins',()=>{
  const lowBands=[.016,.043,.094,.079,.03,.01],peak=Math.max(...lowBands),proportions=lowBands.map(value=>value/peak),samples=Array.from({length:81},(_,i)=>-1+i/40);
  const low=samples.map(h=>avatar.spectrumContour(h,lowBands,.12)),normalized=samples.map(h=>avatar.spectrumContour(h,proportions,.12));
  for(let i=0;i<samples.length;i++)assert.ok(Math.abs(low[i]-normalized[i])<1e-12,'shape follows measured proportions at the same actual RMS');
  assert.ok(Math.max(...low.map(Math.abs))*6.8>.75,'quiet source gives at least a readable subpixel-to-pixel crest in SVG design space');
  assert.equal(avatar.spectrumContour(.2,[0,0,0,0,0,0],.12),0);
});
