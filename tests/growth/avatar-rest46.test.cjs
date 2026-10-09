'use strict';
// Real face controller and Three.js vertices, deterministic local time and
// synthetic renderer. These checks do not certify GPU pixels or human taste.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {JSDOM}=require('jsdom'),THREE=require('three');
const Avatar=require('../../brites-concierge-avatar.js');

function productionScene(win,container,onFrame){
  let renderer;
  container.getBoundingClientRect=()=>({width:480,height:440});
  win.HTMLCanvasElement.prototype.getContext=type=>type==='2d'?{createImageData(width,height){return {data:new Uint8ClampedArray(width*height*4)};},putImageData(){}}:null;
  class Renderer{constructor(){renderer=this;this.domElement=win.document.createElement('canvas');this.shadowMap={};this.capabilities={getMaxAnisotropy:()=>1};this.info={autoReset:true,render:{calls:0,triangles:0},reset(){}};}setClearColor(){}setPixelRatio(value){this.ratio=value;}getPixelRatio(){return this.ratio;}setSize(){}setAnimationLoop(){}render(scene){this.scene=scene;}dispose(){}forceContextLoss(){}}
  class Pmrem{fromEquirectangular(texture){return {texture,dispose(){}};}dispose(){}}
  const module={exports:{}},source=fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'),'utf8').replace(/^import [^\n]+\n/gm,'').replace(/export (const|function) /g,'$1 ');
  vm.runInNewContext(source+'\nmodule.exports={createAvatarScene};',{module,THREE:{...THREE,WebGLRenderer:Renderer,PMREMGenerator:Pmrem}});
  const engine=module.exports.createAvatarScene({container,quality:{...Avatar.qualityFor({width:1440,bloom:false}),textureSize:16},onFrame});
  return {engine,get:name=>renderer.scene.getObjectByName(name)};
}
async function fixture(t,fallback=false){
  const dom=new JSDOM('<main></main>',{url:'https://sandbox.example/',pretendToBeVisual:true}),win=dom.window,doc=win.document,timers=new Map(),frames=new Map();
  let clock=1000,serial=0,hidden=false,intersect,sceneFrame,scene;
  Object.defineProperty(win.performance,'now',{value:()=>clock});Object.defineProperty(doc,'hidden',{get:()=>hidden});
  win.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
  win.setTimeout=(fn,delay)=>{timers.set(++serial,{fn,due:clock+delay});return serial;};win.clearTimeout=id=>timers.delete(id);
  win.requestAnimationFrame=fn=>{frames.set(++serial,fn);return serial;};win.cancelAnimationFrame=id=>frames.delete(id);
  win.IntersectionObserver=class{constructor(fn){intersect=fn;}observe(){}disconnect(){}};
  const guide=Avatar.create({container:doc.querySelector('main'),visible:true,greetingOnOpen:false,loadScene:async()=>{if(fallback)throw Error('Synthetic unavailable GPU');return {createAvatarScene(config){sceneFrame=config.onFrame;scene=productionScene(win,config.container,sceneFrame);return scene.engine;}};}});
  await guide.ready;
  function advance(milliseconds){const end=clock+milliseconds;while(clock<end){clock=Math.min(end,clock+16);const ready=[...frames.values()];frames.clear();ready.forEach(fn=>fn(clock));for(const[id,timer]of[...timers])if(timer.due<=clock){timers.delete(id);timer.fn();}if(scene&&guide.snapshot().visible&&!guide.snapshot().paused&&!hidden)scene.engine.render(sceneFrame(clock/1000),true);}}
  t.after(()=>{guide.destroy();win.close();});return {guide,advance,timers,frames,get:name=>scene.get(name),hide(value){hidden=value;doc.dispatchEvent(new win.Event('visibilitychange'));},intersect(value){intersect([{isIntersecting:value}]);},scenePose(pose){scene.engine.render(pose,true);}};
}
function smileDepth(mesh){const geometry=mesh.geometry,p=geometry.attributes.position,segments=geometry.parameters.tubularSegments,radial=geometry.parameters.radialSegments;
  const ring=at=>{let sum=0;for(let i=0;i<radial;i++)sum+=p.getY(at*(radial+1)+i);return sum/radial;};return (ring(0)+ring(segments))/2-ring(Math.floor(segments/2));}
function friendly(h){const pose=h.guide.snapshot().facePose;assert.ok(Math.abs(pose.faceBrowTilt)<.01);assert.ok(Math.abs(pose.eyeAsymmetry)<.005);assert.ok(Math.abs(pose.mouthSkew)<.005);assert.ok(pose.mouthCurve>.01);}
function symmetricGeometry(h){const left=h.get('expression-eye-left'),right=h.get('expression-eye-right');left.geometry.computeBoundingBox();right.geometry.computeBoundingBox();const a=left.geometry.boundingBox.getSize(new THREE.Vector3()),b=right.geometry.boundingBox.getSize(new THREE.Vector3());assert.ok(Math.abs(a.y-b.y)<.002,'matching actual eye heights');assert.ok(Math.abs(h.get('expression-brow-left').position.y-h.get('expression-brow-right').position.y)<.002,'matching actual brow heights');assert.ok(smileDepth(h.get('expression-smile-glyph'))>.01,'actual mouth center sits below its corners');}

test('READY idle stays symmetric and smiling after a question, performance and appreciation restore',async t=>{
  for(const fallback of[false,true]){const h=await fixture(t,fallback);h.guide.setEmotion('curious');h.guide.setState('thinking');h.advance(2000);h.guide.setState('idle');h.advance(750);friendly(h);if(!fallback)symmetricGeometry(h);
    h.guide.perform({mood:'curious',gesture:'explain',intensity:.8,durationMs:800});h.advance(1600);friendly(h);
    h.guide.setEmotion('curious');h.guide.setEmotion('appreciated');h.advance(2100);friendly(h);assert.notEqual(h.guide.snapshot().emotion,'curious','an expired presentation cannot restore a stale question mood');if(!fallback)symmetricGeometry(h);
    h.advance(12000);friendly(h);assert.equal(h.guide.snapshot().ambiguityCue,null);}
});
test('real 3D idle geometry also rejects legacy crooked eye and frown pose values',async t=>{const h=await fixture(t);h.scenePose({...Avatar.poseFor({state:'idle'}),faceBrowTilt:.66,eyeAsymmetry:.14,mouthSkew:.36,smileCurve:.16,mouthCurve:-.0132});symmetricGeometry(h);});
test('ordinary spoken inquiry and a sustained think remain balanced, warm and distinct from true ambiguity',()=>{
  const question=Avatar.poseFor({state:'speaking',expression:{kind:'inquiry',intensity:1}});assert.equal(question.eyeAsymmetry,0);assert.equal(question.mouthSkew,0);assert.equal(question.faceBrowTilt,0);assert.ok(question.mouthCurve>.01);
  for(let elapsed=0;elapsed<12;elapsed+=.05){const pose=Avatar.poseFor({state:'thinking',emotion:'curious',expression:{kind:'inquiry',intensity:1},elapsed,time:elapsed+100});assert.ok(Math.abs(pose.eyeAsymmetry)<.005);assert.ok(Math.abs(pose.faceBrowTilt)<.01);assert.ok(pose.mouthCurve>0);assert.equal(pose.mouthOpen,0);}
});
test('actual ambiguity has one subsecond confusion, considerate recovery, reassurance, then friendly rest',async t=>{
  for(const fallback of[false,true]){const h=await fixture(t,fallback);assert.equal(h.guide.cue('ambiguity'),true);h.advance(320);assert.equal(h.guide.snapshot().ambiguityCue.phase,'confused');assert.ok(h.guide.snapshot().facePose.faceBrowTilt>.03);assert.equal(h.guide.cue('ambiguity'),false,'duplicate callbacks cannot prolong confusion');
    h.advance(500);assert.equal(h.guide.snapshot().ambiguityCue.phase,'considerate');h.advance(180);assert.ok(Math.abs(h.guide.snapshot().facePose.eyeAsymmetry)<.01);assert.ok(Math.abs(h.guide.snapshot().facePose.faceBrowTilt)<.04,'confusion is gone within one second');
    h.advance(400);assert.equal(h.guide.snapshot().ambiguityCue.phase,'reassuring');h.advance(1300);assert.equal(h.guide.snapshot().ambiguityCue,null);friendly(h);if(!fallback)symmetricGeometry(h);h.advance(12000);assert.equal(h.guide.snapshot().ambiguityCue,null);friendly(h);}
});
test('ambiguity timers retire on interruption, hidden/offscreen state, pause, close and reduced motion without replay',async t=>{
  for(const mode of['state','emotion','cancel','hidden','offscreen','pause','close','reduce','destroy']){const h=await fixture(t,true);h.guide.cue('ambiguity');h.advance(300);
    const stop={state:()=>h.guide.setState('thinking'),emotion:()=>h.guide.setEmotion('warm'),cancel:()=>h.guide.cancelPerformance(),hidden:()=>h.hide(true),offscreen:()=>h.intersect(false),pause:()=>h.guide.setPaused(true),close:()=>h.guide.setVisible(false),reduce:()=>h.guide.setReducedMotion(true),destroy:()=>h.guide.destroy()};stop[mode]();assert.equal(h.guide.snapshot().ambiguityCue,null,mode);
    if(mode==='hidden')h.hide(false);if(mode==='offscreen')h.intersect(true);if(mode==='pause')h.guide.setPaused(false);if(mode==='close')h.guide.setVisible(true);if(mode==='reduce'){assert.equal(h.guide.cue('ambiguity'),false);assert.equal(h.guide.perform({mood:'curious',gesture:'explain',intensity:.8,durationMs:800}),false);assert.equal(h.guide.snapshot().performance,null);h.guide.setReducedMotion(false);}h.advance(5000);assert.equal(h.guide.snapshot().ambiguityCue,null,mode+' never revives a retired cue');
  }
});
