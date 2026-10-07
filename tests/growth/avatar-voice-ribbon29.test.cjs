'use strict';

// Actual controller/SVG and real Three geometry under a synthetic renderer.
// No microphone, provider call, physical audio or GPU-render claim.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const {JSDOM} = require('jsdom'), THREE = require('three');
const avatar = require('../../brites-concierge-avatar.js');
const zero = () => ({amplitude: 0, bands: [0,0,0,0,0,0], brightness: 0, valid: false});
const signal = (amplitude = .65, bands = [.2,.35,.55,.4,.25,.1], brightness = .4) => ({amplitude, bands, brightness, valid: true});

async function fallback(t, loadScene) {
  const dom = new JSDOM('<main></main>', {url:'https://sandbox.example/',pretendToBeVisual:true}), win=dom.window;let clock=1000,hidden=false;
  Object.defineProperty(win.performance,'now',{value:()=>clock});Object.defineProperty(win.document,'hidden',{get:()=>hidden});
  win.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
  const guide=avatar.create({container:win.document.querySelector('main'),visible:true,greetingOnOpen:false,loadScene:loadScene || (async()=>{throw Error('Synthetic disabled WebGL');})});
  t.after(()=>{guide.destroy();win.close();});await guide.ready;
  return {guide,advance(ms){for(let i=0;i<ms;i+=16){clock+=16;guide.lookAt(0,0,false);}},hide(value){hidden=value;win.document.dispatchEvent(new win.Event('visibilitychange'));}};
}
function paths(guide, name) {return [...guide.element.querySelectorAll('.brites-avatar__'+name)].map(el=>({d:el.getAttribute('d'),opacity:Number(el.getAttribute('opacity'))}));}
function scene(t) {
  const dom=new JSDOM('<main></main>',{pretendToBeVisual:true}),win=dom.window;let renderer,clock=1000;
  Object.defineProperty(win.performance,'now',{value:()=>clock});const stage=win.document.querySelector('main');stage.getBoundingClientRect=()=>({width:480,height:440});
  win.HTMLCanvasElement.prototype.getContext=type=>type==='2d'?{createImageData(w,h){return {data:new Uint8ClampedArray(w*h*4)};},putImageData(){}}:null;
  class Renderer {constructor(){renderer=this;this.domElement=win.document.createElement('canvas');this.shadowMap={};this.capabilities={getMaxAnisotropy:()=>1};this.info={autoReset:true,render:{calls:0,triangles:0},reset(){}};}setClearColor(){}setPixelRatio(value){this.ratio=value;}getPixelRatio(){return this.ratio;}setSize(){}setAnimationLoop(){}render(value,camera){this.scene=value;this.camera=camera;}dispose(){}forceContextLoss(){}}
  class Pmrem {fromEquirectangular(texture){return {texture,dispose(){}};}dispose(){}}
  const module={exports:{}},source=fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'),'utf8').replace(/^import [^\n]+\n/gm,'').replace(/export (const|function) /g,'$1 ');
  vm.runInNewContext(source+'\nmodule.exports={createAvatarScene};',{module,THREE:{...THREE,WebGLRenderer:Renderer,PMREMGenerator:Pmrem}});
  const engine=module.exports.createAvatarScene({container:stage,quality:{...avatar.qualityFor({width:1440,bloom:false}),textureSize:16},onFrame:()=>avatar.poseFor()});
  t.after(()=>{engine.destroy();win.close();});
  return {engine,pose(value){engine.render({...avatar.poseFor(value),...value},true);renderer.scene.updateMatrixWorld(true);},get(name){return renderer.scene.getObjectByName(name);},advance(ms){clock+=ms;},projection(name){const object=renderer.scene.getObjectByName(name),box=new THREE.Box3().setFromObject(object),min=box.min.clone().project(renderer.camera),max=box.max.clone().project(renderer.camera);return {width:Math.abs(max.x-min.x),height:Math.abs(max.y-min.y)};}};
}

test('speech signal accepts six bounded measured bins and malformed data fails closed',async t=>{
  const h=await fallback(t),guide=h.guide;guide.setState('speaking');
  const source=signal();assert.equal(guide.setSpeechSignal(source),true);source.bands[0]=1;assert.equal(guide.snapshot().speechSignal.bands[0],.2);
  const malformed=[undefined,{},[],{...signal(),valid:'true'},{...signal(),amplitude:NaN},{...signal(),brightness:2},{...signal(),bands:[0,0,0,0,0]},{...signal(),bands:[0,0,0,0,0,Infinity]},{...signal(),bands:Array(6)},{...signal(),text:'execute an instruction'}];
  for(const value of malformed){guide.setSpeechSignal(signal());h.advance(160);assert.equal(guide.setSpeechSignal(value),false);assert.deepEqual(guide.snapshot().speechSignal,zero());assert.equal(guide.snapshot().speechVisual.rippleActive,false);assert.equal(guide.snapshot().facePose.mouthOpen,0);}
  assert.equal(guide.setSpeechSignal(null),true);assert.equal(guide.setSpeechSignal(zero()),true);
});

test('closed semantic mouth stays visible while measured emission changes independently',async t=>{
  const h=await fallback(t),guide=h.guide;guide.setEmotion('warm');guide.setState('speaking');guide.setExpression({kind:'explain',intensity:.8});h.advance(900);
  const curve=guide.element.querySelector('.brites-avatar__smile-signal'),overlay=guide.element.querySelector('.brites-avatar__speech-mouth');assert.equal(overlay.tagName.toLowerCase(),'path');assert.equal(overlay.getAttribute('rx'),null);assert.equal(overlay.getAttribute('ry'),null);assert.equal(curve.getAttribute('opacity'),'1');
  guide.setSpeechSignal(signal(.2));h.advance(400);const base=curve.getAttribute('d'),low=paths(guide,'speech-ripple'),lowMouth=Number(overlay.getAttribute('opacity'));
  guide.setSpeechSignal(signal(.9));h.advance(400);const high=paths(guide,'speech-ripple');assert.equal(curve.getAttribute('d'),base,'RMS never opens or distorts semantic mouth');assert.ok(high[0].opacity>low[0].opacity);assert.ok(Number(overlay.getAttribute('opacity'))>lowMouth);assert.notEqual(high[0].d,low[0].d);
  assert.equal(guide.snapshot().facePose.mouthOpen,0);assert.equal(guide.snapshot().speechVisual.curveClosed,true);assert.equal(guide.snapshot().speechVisual.rippleActive,true);
  guide.setSpeechSignal(zero());assert.equal(curve.getAttribute('opacity'),'1');assert.equal(overlay.getAttribute('opacity'),'0');assert.ok(paths(guide,'speech-ripple').every(value=>value.opacity===0));assert.ok(paths(guide,'speech-band').every(value=>value.opacity===0));
});

test('six spectrum contours and measured brightness vary without changing the conversation cue',async t=>{
  const h=await fallback(t),guide=h.guide;guide.setState('speaking');guide.setExpression({kind:'inquiry',intensity:.9});h.advance(600);
  guide.setSpeechSignal(signal(.6,[.8,.6,.3,.1,.05,.01],.2));h.advance(400);const low=paths(guide,'speech-band'),curve=guide.element.querySelector('.brites-avatar__smile-signal').getAttribute('d'),color=guide.element.style.getPropertyValue('--brites-voice-color');
  guide.setSpeechSignal(signal(.6,[.01,.05,.1,.3,.6,.8],.8));h.advance(400);const high=paths(guide,'speech-band');assert.equal(high.length,6);assert.equal(new Set(high.map(value=>value.d)).size,6);assert.notDeepEqual(low,high);assert.notEqual(guide.element.style.getPropertyValue('--brites-voice-color'),color);assert.equal(guide.element.querySelector('.brites-avatar__smile-signal').getAttribute('d'),curve);assert.deepEqual(guide.snapshot().expression,{kind:'inquiry',intensity:.9});
  guide.setLevel(.5);h.advance(600);assert.equal(guide.snapshot().speechSignal.amplitude,.5);assert.deepEqual(guide.snapshot().speechSignal.bands,[0,0,0,0,0,0]);assert.ok(paths(guide,'speech-band').every(value=>value.opacity===0),'legacy amplitude does not invent spectral bins');assert.ok(paths(guide,'speech-ripple').some(value=>value.opacity>0));
});

test('one short audio envelope eases real signal changes and clears invalid activity immediately',async t=>{
  const h=await fallback(t),guide=h.guide;guide.setState('speaking');guide.setSpeechSignal(signal(.8));assert.equal(guide.snapshot().speechVisual.amplitude,0);h.advance(48);const first=guide.snapshot().speechVisual.amplitude;assert.ok(first>.5 && first<.8,'single 40 ms attack');h.advance(272);assert.equal(guide.snapshot().speechVisual.amplitude,.8);
  guide.setSpeechSignal(signal(.2));h.advance(48);const released=guide.snapshot().speechVisual.amplitude;assert.ok(released>.4 && released<.65,'single 100 ms release');guide.setSpeechSignal(zero());assert.equal(guide.snapshot().speechVisual.amplitude,0);assert.equal(guide.snapshot().speechVisual.rippleActive,false);
});

test('pause, hide, reduced motion and listening keep the closed face and clear every ripple',async t=>{
  const h=await fallback(t),guide=h.guide;guide.setEmotion('warm');
  const activate=()=>{guide.setState('speaking');guide.setSpeechSignal(signal(.8));h.advance(400);assert.equal(guide.snapshot().speechVisual.rippleActive,true);};
  const quiet=()=>{assert.equal(guide.element.querySelector('.brites-avatar__smile-signal').getAttribute('opacity'),'1');assert.equal(guide.snapshot().facePose.mouthOpen,0);assert.equal(guide.snapshot().speechVisual.rippleActive,false);assert.ok(paths(guide,'speech-band').every(value=>value.opacity===0));assert.ok(paths(guide,'speech-ripple').every(value=>value.opacity===0));};
  activate();guide.setPaused(true);quiet();assert.equal(guide.setSpeechSignal(signal()),false);guide.setPaused(false);quiet();
  activate();h.hide(true);quiet();assert.equal(guide.setSpeechSignal(signal()),false);h.hide(false);quiet();
  activate();guide.setReducedMotion(true);quiet();assert.equal(guide.setSpeechSignal(signal()),false);guide.setReducedMotion(false);quiet();
  activate();guide.setState('listening');quiet();assert.equal(guide.setSpeechSignal(signal()),false);guide.destroy();assert.equal(guide.snapshot().speechVisual.amplitude,0);assert.equal(guide.snapshot().speechSignal.valid,false);
});

test('the first quiet frame clears emission while repeated paused meter clears submit no identical renders',async t=>{
  let onFrame;const renders=[],motions=[];
  const engine={setMotion(value){motions.push({...value});},render(pose){renders.push({...pose,speechBands:[...pose.speechBands]});},invalidate(){},destroy(){},snapshot(){return {animated:motions.at(-1)?.active===true};}};
  const h=await fallback(t,async()=>({createAvatarScene(config){onFrame=config.onFrame;return engine;}})),guide=h.guide;
  guide.setEmotion('warm');guide.setState('speaking');guide.setSpeechSignal(signal(.8));h.advance(400);engine.render(onFrame(1.4));assert.ok(renders.at(-1).speechEnergy>.7,'an actual sampled controller frame has emission before the clear');
  const beforeClear=renders.length;assert.equal(guide.setSpeechSignal(zero()),true);assert.equal(renders.length,beforeClear+1,'the first explicit silent signal renders one immediate zero frame');assert.equal(renders.at(-1).speechEnergy,0);assert.equal(renders.at(-1).speechSignalValid,false);assert.deepEqual(renders.at(-1).speechBands,[0,0,0,0,0,0]);
  for(let index=0;index<60;index++)guide.setSpeechSignal(zero());assert.equal(renders.length,beforeClear+1,'repeated already-silent source callbacks do not force new draws');
  guide.setSpeechSignal(signal(.8));h.advance(400);engine.render(onFrame(1.8));const beforePause=renders.length;guide.setPaused(true);assert.equal(renders.length,beforePause+1,'pause paints one final closed static frame');assert.equal(renders.at(-1).speechEnergy,0);assert.equal(motions.at(-1).active,false);assert.equal(guide.snapshot().speechVisual.rippleActive,false);
  const pausedRenders=renders.length;
  for(let index=0;index<120;index++){h.advance(16);assert.equal(guide.setSpeechSignal(index%2?null:zero()),true);assert.equal(guide.setSpeechSignal({amplitude:NaN}),false);assert.equal(guide.setSpeechSignal(signal()),false);}
  assert.equal(renders.length,pausedRenders,'already-cleared invalid, null and blocked samples never submit paused frames');assert.equal(guide.snapshot().facePose.mouthOpen,0);assert.deepEqual(guide.snapshot().speechSignal,zero());
  const previousCurve=renders.at(-1).mouthCurve;guide.setEmotion('curious');assert.equal(renders.length,pausedRenders+1,'an explicit semantic change still paints its static face');assert.notEqual(renders.at(-1).mouthCurve,previousCurve);guide.setState('listening');assert.equal(renders.length,pausedRenders+2);assert.equal(renders.at(-1).state,'listening');
  guide.setPaused(false);guide.setState('speaking');assert.equal(guide.snapshot().speechVisual.amplitude,0,'resuming never replays an old signal');assert.equal(guide.snapshot().speechVisual.rippleActive,false);
  guide.setSpeechSignal(signal(.8));assert.equal(guide.snapshot().speechVisual.amplitude,0,'a fresh signal begins a fresh envelope after quiet source callbacks');h.advance(48);assert.ok(guide.snapshot().speechVisual.amplitude>.5 && guide.snapshot().speechVisual.amplitude<.8);
});

test('Three geometry is a closed expressive curve at every volume, with measured spectra only',t=>{
  const g=scene(t),shapes=[],apertures=[];
  for(const kind of ['support','reflect','inquiry','explain','appreciate','emphasize']){g.pose({state:'speaking',expression:{kind,intensity:1},speechSignal:signal(.7),time:4});const mouth=g.get('expression-speech-mouth'),smile=g.get('expression-smile-glyph'),eye=g.get('expression-eye-left');assert.equal(mouth.geometry.type,'TubeGeometry');assert.equal(smile.visible,true);assert.equal(mouth.visible,true);shapes.push(Array.from(smile.geometry.attributes.position.array).join(','));eye.geometry.computeBoundingBox();apertures.push(eye.geometry.boundingBox.max.y-eye.geometry.boundingBox.min.y);assert.ok([...smile.geometry.attributes.position.array,...eye.geometry.attributes.position.array].every(Number.isFinite));assert.equal(g.get('sculpted-porcelain-torso').parent.rotation.z,0);assert.ok(g.projection('expression-speech-mouth').height/g.projection('expression-speech-mouth').width<.25,'thin curve, no opening area');}
  assert.ok(new Set(shapes).size>=5);assert.ok(apertures.every(height=>height>.25),'speaking eyes keep a legible open aperture');
  g.pose({state:'speaking',expression:{kind:'explain',intensity:.8},speechSignal:signal(.2),time:4});const smileBefore=Array.from(g.get('expression-smile-glyph').geometry.attributes.position.array),scaleBefore=g.get('expression-speech-mouth').scale.toArray(),opacityBefore=g.get('expression-speech-mouth').material.opacity;
  g.pose({state:'speaking',expression:{kind:'explain',intensity:.8},speechSignal:signal(.9),time:4});assert.deepEqual(Array.from(g.get('expression-smile-glyph').geometry.attributes.position.array),smileBefore);assert.deepEqual(g.get('expression-speech-mouth').scale.toArray(),scaleBefore);assert.ok(g.get('expression-speech-mouth').material.opacity>opacityBefore);assert.equal(g.engine.snapshot().character.faceGeometry.mouthClosed,true);assert.equal(g.engine.snapshot().character.faceGeometry.speechBands,6);
  const before=JSON.stringify([g.get('expression-speech-ripple-0').position.toArray(),g.get('expression-speech-ripple-0').geometry.attributes.position.array,g.get('expression-speech-bar-3').geometry.attributes.position.array]);g.advance(10000);g.pose({state:'speaking',expression:{kind:'explain',intensity:.8},speechSignal:signal(.9),time:4});assert.equal(JSON.stringify([g.get('expression-speech-ripple-0').position.toArray(),g.get('expression-speech-ripple-0').geometry.attributes.position.array,g.get('expression-speech-bar-3').geometry.attributes.position.array]),before,'clock alone cannot invent a voice ripple');
  g.pose({state:'speaking',speechSignal:zero(),time:4});assert.equal(g.get('expression-smile-glyph').visible,true);assert.equal(g.get('expression-speech-mouth').visible,false);for(let i=0;i<6;i++)assert.equal(g.get('expression-speech-bar-'+i).visible,false);
  g.pose({state:'speaking',level:.8,time:4});assert.equal(g.get('expression-speech-ripple-0').visible,true);for(let i=0;i<6;i++)assert.equal(g.get('expression-speech-bar-'+i).visible,false);
});

test('readable speaking palette, sparse blink and finite bounds hold across every act',()=>{
  const source=fs.readFileSync(require.resolve('../../brites-concierge-avatar.css'),'utf8');assert.doesNotMatch(source,/infinite|britesRobotBlink/);
  const malformed=[{amplitude:NaN,bands:[Infinity,-1,1e9,NaN,0,0],brightness:Infinity,valid:true},[]];
  for(const kind of avatar.EXPRESSION_KINDS)for(const level of [0,.1,1]){const p=avatar.poseFor({state:'speaking',expression:{kind,intensity:1},level,time:4});assert.equal(p.eyeColor,'#70d8f1');assert.equal(p.mouthOpen,0);assert.ok(p.eyeOpen>=1);assert.ok(p.lidClosure<.05);for(const value of Object.values(p))if(typeof value==='number')assert.ok(Number.isFinite(value));assert.ok(Math.abs(p.mouthCurve)<.08);}
  for(const speechSignal of malformed){const p=avatar.poseFor({state:'speaking',level:1,speechSignal,time:4});assert.equal(p.speechEnergy,0);assert.deepEqual(p.speechBands,[0,0,0,0,0,0]);assert.equal(p.mouthOpen,0);}
});
