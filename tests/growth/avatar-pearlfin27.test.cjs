'use strict';

// Execute production geometry, material declarations and articulation with real
// Three.js classes and a synthetic renderer. These certify CPU contracts only;
// they do not certify WebGL pixels, real shadows, frame-rate or voice capture.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const {JSDOM} = require('jsdom'), THREE = require('three');
const avatar = require('../../brites-concierge-avatar.js');
const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');
const measured = amplitude => ({amplitude, bands: [.2, .35, .55, .4, .25, .1], brightness: .4, valid: amplitude > 0});

function harness(t) {
  const dom = new JSDOM('<div id="stage"></div>', {pretendToBeVisual: true}), win = dom.window, stage = win.document.getElementById('stage');
  let renderer, resizeCallback, time = 0, box = {width: 480, height: 440};
  Object.defineProperty(win.performance, 'now', {value: () => time});
  stage.getBoundingClientRect = () => box;
  win.ResizeObserver = class {constructor(callback) {resizeCallback = callback;} observe() {} disconnect() {}};
  win.HTMLCanvasElement.prototype.getContext = type => type === '2d' ? {createImageData(width, height) {return {data: new Uint8ClampedArray(width * height * 4)};}, putImageData() {}} : null;
  class SyntheticRenderer {
    constructor() {renderer = this; this.domElement = win.document.createElement('canvas'); this.shadowMap = {}; this.capabilities = {getMaxAnisotropy: () => 1}; this.info = {autoReset: true, render: {calls: 0, triangles: 0}, reset() {}};}
    setClearColor() {}
    setPixelRatio(value) {this.ratio = value;}
    getPixelRatio() {return this.ratio;}
    setSize() {}
    setAnimationLoop(callback) {this.callback = callback;}
    render(scene, camera) {this.scene = scene; this.camera = camera;}
    dispose() {}
    forceContextLoss() {}
  }
  class SyntheticPmrem {fromEquirectangular(texture) {return {texture, dispose() {}};} dispose() {}}
  const module = {exports: {}};
  vm.runInNewContext(source.replace(/^import [^\n]+\n/gm, '').replace(/export (const|function) /g, '$1 ') + '\nmodule.exports={createAvatarScene,AVATAR_SCENE_DECLARATIONS};', {module, THREE: {...THREE, WebGLRenderer: SyntheticRenderer, PMREMGenerator: SyntheticPmrem}}, {filename: 'avatar-face-replacement-production-scene.cjs'});
  const engine = module.exports.createAvatarScene({container: stage, quality: {...avatar.qualityFor({width: 1440, bloom: false}), textureSize: 16}, onFrame: value => avatar.poseFor({time: value, state: 'idle'})});
  t.after(() => {engine.destroy(); win.close();});
  const h = {
    engine, declarations: module.exports.AVATAR_SCENE_DECLARATIONS,
    get scene() {return renderer.scene;}, get camera() {return renderer.camera;},
    pose(fields = {}) {engine.render({...avatar.poseFor({state: fields.state || 'idle', time: time / 1000, ...fields}), ...fields}, true); renderer.scene.updateMatrixWorld(true);},
    get(name) {return renderer.scene.getObjectByName(name);},
    resize(width, height) {box = {width, height}; resizeCallback();},
    advance(milliseconds) {time = milliseconds;},
    motion(fields) {engine.setMotion(fields);}
  };
  h.pose(); return h;
}
const eyeNames = ['expression-eye-left', 'expression-eye-right'];
const faceNames = [...eyeNames, 'expression-brow-left', 'expression-brow-right', 'expression-cheek-left', 'expression-cheek-right', 'expression-smile-glyph', 'expression-heart-left', 'expression-heart-right'];
function geometrySize(mesh) {mesh.geometry.computeBoundingBox(); return mesh.geometry.boundingBox.getSize(new THREE.Vector3());}
function validGeometry(mesh) {
  for (const [name, attribute] of Object.entries(mesh.geometry.attributes)) assert.ok([...attribute.array].every(Number.isFinite), mesh.name + ' ' + name + ' is finite');
  assert.ok(mesh.geometry.attributes.position.count > 0, mesh.name + ' real vertices');
  if (mesh.geometry.index) assert.ok([...mesh.geometry.index.array].every(index => index >= 0 && index < mesh.geometry.attributes.position.count), mesh.name + ' valid indices');
  for (const vector of [mesh.position, mesh.rotation, mesh.scale]) for (const axis of ['x', 'y', 'z']) assert.ok(Number.isFinite(vector[axis]), mesh.name + ' ' + axis + ' transform is finite');
}


test('the new silhouette has one continuous body and sculpted fins rather than a booted robot', t => {
  const h = harness(t), body = h.get('sculpted-porcelain-torso'), head = h.get('original-pebble-shell');
  assert.equal(body.geometry.type, 'LatheGeometry'); assert.equal(head.geometry.type, 'SphereGeometry');
  const bodyBox = new THREE.Box3().setFromObject(body), headBox = new THREE.Box3().setFromObject(head);
  const ratio = headBox.getSize(new THREE.Vector3()).x / bodyBox.getSize(new THREE.Vector3()).x;
  assert.ok(ratio >= 1.35 && ratio <= 1.7, 'head no longer overwhelms a tiny torso');
  assert.ok(bodyBox.getSize(new THREE.Vector3()).y > headBox.getSize(new THREE.Vector3()).y);
  for (const name of ['grounded-foot--1','grounded-foot-1','grounded-sole--1','grounded-sole-1','retired-chain-ornament']) assert.equal(h.get(name), undefined);
  for (const side of [-1,1]) {const fin=h.get('porcelain-side-fin-'+side); assert.equal(fin.geometry.type,'ExtrudeGeometry'); validGeometry(fin); assert.equal(fin.parent.name,side<0?'articulated-fin-left':'articulated-fin-right');}
  assert.equal(h.engine.snapshot().grounding.separateFeet, false);
});

test('body contact and full silhouette remain within the camera across mobile and desktop shapes', t => {
  const h=harness(t);
  for(const [width,height] of [[390,440],[480,440],[980,440],[180,440],[390,280]]){
    h.resize(width,height); h.pose({headPitch:0,headYaw:0,headRoll:0});
    for(const name of ['sculpted-porcelain-torso','original-pebble-shell']){
      const box=new THREE.Box3().setFromObject(h.get(name));
      for(const x of [box.min.x,box.max.x])for(const y of [box.min.y,box.max.y])for(const z of [box.min.z,box.max.z]){
        const point=new THREE.Vector3(x,y,z).project(h.camera); assert.ok(Math.abs(point.x)<1 && Math.abs(point.y)<1,name+' stays framed at '+width+'x'+height);
      }
    }
    for(const state of ['idle','speaking','success']){
      h.pose({state,level:1,bob:.8,stanceScale:2,lean:1,bodyRoll:1,bodyYaw:1});
      const lower=new THREE.Box3().setFromObject(h.get('sculpted-porcelain-torso')).min.y;
      const top=new THREE.Box3().setFromObject(h.get('grounding-platform')).max.y;
      assert.ok(Math.abs(lower-top)<1e-6,'speech and gestures never move the contact edge');
    }
  }
});

test('studio keeps a readable white silhouette against a constant cool backdrop and a separate shadow receiver', t => {
  const h=harness(t); assert.equal(h.scene.background.isColor,true);
  // This verifies authored sRGB colours only. Lighting, tone mapping and real
  // WebGL pixels need a physical-browser check; this is not a rendered ratio.
  const luminance=color=>{const rgb=color.getHexString().match(/../g).map(channel=>parseInt(channel,16)/255).map(channel=>channel<=.04045?channel/12.92:Math.pow((channel+.055)/1.055,2.4));return .2126*rgb[0]+.7152*rgb[1]+.0722*rgb[2];};
  const backdrop=h.scene.background,body=h.get('original-pebble-shell').material.color;
  assert.ok(luminance(backdrop)>.45 && luminance(backdrop)<.75,'the studio remains bright without disappearing into white ceramic');
  assert.ok((luminance(body)+.05)/(luminance(backdrop)+.05)>=1.5,'authored silhouette/background separation survives a palette change');
  const [r,g,b]=backdrop.getHexString().match(/../g).map(channel=>parseInt(channel,16));
  assert.ok(b>=g && g>r,'a restrained cool backdrop separates the warm ceramic and gold');
  assert.equal(h.get('continuous-studio-sweep').visible,false,'the huge PBR sweep cannot darken the entire background');
  const floor=h.scene.children.find(p=>p.isMesh&&p.material.isShadowMaterial); assert.ok(floor?.receiveShadow); assert.ok(floor.material.opacity<=.15);
  const lights=h.scene.children.filter(p=>p.isLight); assert.ok(lights.some(p=>p.isHemisphereLight && p.intensity>=1)); assert.ok(lights.some(p=>p.isDirectionalLight && p.intensity>=1));
  assert.ok(h.scene.environmentIntensity>=1);
  assert.equal(h.get('original-pebble-shell').material.color.getHexString(),'ffffff');
  assert.ok(new THREE.Box3().setFromObject(h.get('grounding-platform')).getSize(new THREE.Vector3()).x<1.6);
});

test('closed speech curve, spectral contours and cheek lights use measured output and settle on silence', t => {
  const h=harness(t), mouth=h.get('expression-speech-mouth'), smile=h.get('expression-smile-glyph');
  h.pose({state:'speaking',speechSignal:measured(0)}); assert.equal(mouth.visible,false); assert.equal(smile.visible,true);
  h.pose({state:'speaking',speechSignal:measured(.12)}); assert.equal(mouth.visible,true); assert.equal(smile.visible,true); const softOpacity=mouth.material.opacity,softCheek=h.get('expression-cheek-left').scale.x,softVertices=Array.from(smile.geometry.attributes.position.array);
  h.pose({state:'speaking',speechSignal:measured(.88)}); assert.ok(mouth.material.opacity>softOpacity*2); assert.deepEqual(Array.from(smile.geometry.attributes.position.array),softVertices); assert.equal(mouth.scale.y,1); assert.ok(h.get('expression-cheek-left').scale.x>softCheek); assert.equal(h.get('expression-speech-bar-2').visible,true);
  const capture=()=>JSON.stringify([mouth.geometry.attributes.position.array,h.get('expression-speech-ripple-0').position.toArray(),h.get('expression-speech-bar-2').geometry.attributes.position.array]);const snapshot=capture();
  h.advance(10000);h.pose({state:'speaking',speechSignal:measured(.88)}); assert.equal(capture(),snapshot,'clock alone cannot invent an audio waveform');
  h.pose({state:'listening',speechSignal:measured(1)}); assert.equal(mouth.visible,false); assert.equal(smile.visible,true); assert.equal(h.get('expression-speech-bar-2').visible,false);
  h.pose({state:'speaking',speechSignal:measured(0)}); assert.equal(mouth.visible,false);
  assert.equal(mouth.geometry.type,'TubeGeometry');
  validGeometry(mouth);
});

test('the 2-D fallback retains a closed expressive curve while measured emission changes', async t => {
  const dom=new JSDOM('<div id="mount"></div>',{url:'https://sandbox.example/',pretendToBeVisual:true}),win=dom.window;
  let clock=1000;Object.defineProperty(win.performance,'now',{value:()=>clock});
  win.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
  const guide=avatar.create({container:win.document.getElementById('mount'),visible:true,greetingOnOpen:false,loadScene:async()=>{throw Error('Synthetic disabled WebGL');}});
  t.after(()=>{guide.destroy();win.close();});await guide.ready;
  const advance=()=>{for(let i=0;i<25;i++){clock+=16;guide.lookAt(0,0,false);}};
  const body=guide.element.querySelector('.brites-avatar__porcelain-body'),mouth=guide.element.querySelector('.brites-avatar__speech-mouth');assert.ok(body && mouth);assert.equal(guide.element.querySelector('.brites-avatar__antenna'),null);
  guide.setState('speaking');advance();guide.setLevel(.2);advance();const quiet=Number(mouth.getAttribute('opacity')),curve=mouth.getAttribute('d');guide.setLevel(.8);advance();assert.ok(Number(mouth.getAttribute('opacity'))>quiet);assert.equal(mouth.getAttribute('d'),curve);assert.equal(mouth.tagName.toLowerCase(),'path');assert.equal(mouth.getAttribute('ry'),null);assert.equal(guide.element.querySelector('.brites-avatar__smile-signal').getAttribute('opacity'),'1');
  guide.setLevel(0);assert.equal(mouth.getAttribute('opacity'),'0');guide.setState('listening');guide.setLevel(1);assert.equal(mouth.getAttribute('opacity'),'0');
  assert.equal(guide.snapshot().fallback.format,'animated_svg_2d');
});

test('happy-to-friendly curious shapes interpolate rather than snapping or cycling emotions on an idle timer', async t => {
  const dom=new JSDOM('<div id="mount"></div>',{url:'https://sandbox.example/',pretendToBeVisual:true}),win=dom.window;let clock=1000,onFrame;
  Object.defineProperty(win.performance,'now',{value:()=>clock});win.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
  const engine={setMotion(){},render(){},invalidate(){},destroy(){},snapshot(){return {};}};
  const guide=avatar.create({container:win.document.getElementById('mount'),visible:true,greetingOnOpen:false,loadScene:async()=>({createAvatarScene(config){onFrame=config.onFrame;return engine;}})});
  t.after(()=>{guide.destroy();win.close();});await guide.ready;
  const pose=()=>onFrame(clock/1000);guide.setEmotion('celebrate');for(let i=0;i<60;i++){clock+=16;pose();}const happy=pose();
  guide.setEmotion('curious');const instant=pose();assert.equal(instant.smileCurve,happy.smileCurve,'no instantaneous geometry jump');clock+=60;const middle=pose();assert.ok(middle.smileCurve<instant.smileCurve && middle.smileCurve>avatar.FACE_EXPRESSIONS.curious.smileCurve);
  for(let i=0;i<60;i++){clock+=16;pose();}const curious=pose();assert.ok(curious.smileCurve<middle.smileCurve);assert.ok(Math.abs(curious.smileCurve-avatar.FACE_EXPRESSIONS.curious.smileCurve)<.001);assert.equal(curious.eyeAsymmetry,0);assert.equal(curious.mouthSkew,0);assert.ok(curious.mouthCurve>0);
  clock+=5000;const stable=pose();assert.equal(stable.faceExpression,'curious');assert.ok(Math.abs(stable.smileCurve-curious.smileCurve)<.001,'no unrelated emotional loop');
});


test('reduced motion keeps speech geometry still across changing remote audio energy',t=>{
  const h=harness(t);h.motion({active:true,reducedMotion:true});h.pose({state:'speaking',speechEnergy:.1,reducedMotion:true});const mouth=h.get('expression-speech-mouth');
  const first=JSON.stringify([mouth.scale.toArray(),h.get('expression-cheek-left').scale.toArray(),h.get('expression-eye-left').geometry.attributes.position.array]);
  h.pose({state:'speaking',speechEnergy:.9,reducedMotion:true});assert.equal(mouth.visible,false);assert.equal(h.get('expression-speech-bar-2').visible,false);assert.equal(JSON.stringify([mouth.scale.toArray(),h.get('expression-cheek-left').scale.toArray(),h.get('expression-eye-left').geometry.attributes.position.array]),first);
  assert.equal(avatar.poseFor({state:'speaking',level:1,reducedMotion:true}).speechEnergy,0);
  const dom=new JSDOM('<div id="mount"></div>',{url:'https://sandbox.example/',pretendToBeVisual:true});dom.window.matchMedia=()=>({matches:true,addEventListener(){},removeEventListener(){}});
  const guide=avatar.create({container:dom.window.document.getElementById('mount'),visible:true,greetingOnOpen:false,loadScene:async()=>{throw Error('Synthetic disabled WebGL');}});t.after(()=>{guide.destroy();dom.window.close();});guide.setState('speaking');guide.setLevel(.1);const before=guide.element.querySelector('.brites-avatar__speech-mouth').outerHTML;guide.setLevel(.9);assert.equal(guide.element.querySelector('.brites-avatar__speech-mouth').outerHTML,before);assert.equal(guide.snapshot().reducedMotion,true);
});
