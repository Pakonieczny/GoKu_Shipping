'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const THREE = require('three'), avatar = require('../../brites-concierge-avatar.js');
const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');
const declarationsStart = source.indexOf('export const AVATAR_SCENE_DECLARATIONS'), declarationsEnd = source.indexOf('export function createAvatarScene', declarationsStart);
assert.ok(declarationsStart >= 0 && declarationsEnd > declarationsStart);
const declarations = vm.runInNewContext(source.slice(declarationsStart, declarationsEnd).replace('export const', 'const') + '\nAVATAR_SCENE_DECLARATIONS;');
// Execute the production mesh/pose blocks with actual Three.js classes on CPU.
// These tests intentionally do not claim WebGL pixels, PBR appearance or FPS.
function fixture(profile = 'high') {
  const begin = source.indexOf('  const head = new THREE.Group();'), end = source.indexOf('  const decoration = mesh(', begin);
  const poseBegin = source.indexOf('  const whiteColor = new THREE.Color('), poseEnd = source.indexOf('  function render(pose', poseBegin);
  assert.ok(begin > 0 && end > begin && poseBegin > end && poseEnd > poseBegin);
  const materialNames = ['ivory','gold','paleGold','face','lidMaterial','eyeMaterial','pupilMaterial','glint','gemMaterial','corneaMaterial','irisMaterial','mouthMaterial'];
  const materials = Object.fromEntries(materialNames.map(name => [name, ['irisMaterial', 'mouthMaterial', 'glint'].includes(name) ? new THREE.MeshBasicMaterial({toneMapped: false}) : new THREE.MeshPhysicalMaterial()]));
  const quality = avatar.qualityFor(profile === 'adaptive' ? {width:390,memory:4,pixelRatio:2} : {width:1440,memory:8,pixelRatio:2});
  const script = '(()=>{const geometries=new Set(),geometry=value=>{geometries.add(value);return value;},segments=(high,minimum=16)=>Math.max(minimum,Math.round(high*quality.geometryScale)),avatar=new THREE.Group();' + source.slice(begin,end) + '\nconst key=new THREE.SpotLight(),eyeLight=new THREE.PointLight();let sampleTime=1,reducedMotion=false;' + source.slice(poseBegin,poseEnd) + '\nreturn {avatar,head,eyes,arms,footPads,eye,statusBars,heartGlyphs,geometries,pose:(value,time=1,reduced=false)=>{sampleTime=time;reducedMotion=reduced;applyPose(value);},dispose:()=>geometries.forEach(value=>value.dispose())};})()';
  const model = vm.runInNewContext(script, {THREE,quality,...materials,AVATAR_SCENE_DECLARATIONS:declarations});
  return {...model,materials,destroy(){model.dispose();Object.values(materials).forEach(value=>value.dispose());}};
}
test('original robot has two filled luminous ribbons, mechanical limbs and no organic facial rig',()=>{
  const f=fixture();try {
    assert.equal(f.eyes.length,2); assert.equal(f.arms.length,2); assert.equal(f.footPads.length,4); assert.equal(f.statusBars.length,5);
    for (const eye of f.eyes) {assert.equal(eye.digital,true); assert.equal(eye.aperture.geometry.type,'ExtrudeGeometry'); assert.equal(eye.topLid,undefined); assert.equal(eye.pupil,undefined);}
    assert.equal(f.eye.getObjectByName('retired-aperture-ornament').children.length,0);
    assert.doesNotMatch(source,/function irisTexture|const lashCurve|const smile =|const openMouth/);
    let triangles=0;f.avatar.traverse(o=>{if(o.isMesh)triangles+=(o.geometry.index?o.geometry.index.count:o.geometry.attributes.position.count)/3*(o.isInstancedMesh?o.count:1);});assert.ok(triangles>100000);assert.ok(triangles<600000);
  }finally{f.destroy();}
});
test('both real light ribbons blink and deform while preserving finite normals',()=>{
  const f=fixture();try {
    f.pose(avatar.poseFor({state:'idle',time:1}),1);const open=f.eyes.map(eye=>eye.apertureGeometry.attributes.position.array.slice());
    const p={...avatar.poseFor({state:'speaking',time:2,level:.8}),eyeOpen:.035,mouthOpen:.8};f.pose(p,2);
    f.eyes.forEach((eye,index)=>{const current=eye.apertureGeometry.attributes.position.array;assert.notDeepEqual(current,open[index]);let maxY=0;for(let i=1;i<current.length;i+=3)maxY=Math.max(maxY,Math.abs(current[i]));assert.ok(maxY<.025);assert.ok([...current].every(Number.isFinite));assert.ok([...eye.apertureGeometry.attributes.normal.array].every(Number.isFinite));});
  }finally{f.destroy();}
});
test('state colour is smoothed in live mode and immediate in reduced motion',()=>{
  const f=fixture();try {
    const blue=new THREE.Color(declarations.stateColors.listening);f.pose(avatar.poseFor({state:'listening',reducedMotion:true}),1,true);assert.equal(f.materials.irisMaterial.color.getHex(),blue.getHex());
    const before=f.materials.irisMaterial.color.clone();f.pose(avatar.poseFor({state:'thinking',time:1.05}),1.05);const midway=f.materials.irisMaterial.color.clone();assert.notEqual(midway.getHex(),before.getHex());assert.notEqual(midway.getHex(),new THREE.Color(declarations.stateColors.thinking).getHex());
    f.pose(avatar.poseFor({state:'thinking',time:1.4}),1.4);assert.equal(f.materials.irisMaterial.color.getHex(),new THREE.Color(declarations.stateColors.thinking).getHex(),'colour reaches its actual declared target');
    f.pose(avatar.poseFor({state:'speaking',reducedMotion:true}),2,true);assert.equal(f.materials.irisMaterial.color.getHex(),new THREE.Color(declarations.stateColors.speaking).getHex());
    for(const eye of f.eyes) assert.equal(eye.aperture.material,f.materials.irisMaterial,'both visible ribbons use the tested colour');
  }finally{f.destroy();}
});
test('whole-character camera adapts to narrow and wide stages; skybox and PBR declarations are explicit',async()=>{
  const module=await import('../../brites-concierge-avatar-scene.mjs');const declarations=module.AVATAR_SCENE_DECLARATIONS;assert.equal(declarations.environment.faces,6);assert.equal(declarations.environment.hdr,true);assert.equal(declarations.environment.hdri,false);assert.ok(declarations.textures.some(row=>row.kind==='albedo'));assert.ok(declarations.textures.some(row=>row.kind==='normal'));assert.ok(Object.isFrozen(declarations.stateColors));assert.match(source,/new THREE\.CubeTexture\(cubeFaces\)/);assert.match(source,/new THREE\.DataTexture\(hdrPixels, hdrWidth, hdrHeight, THREE\.RGBAFormat, THREE\.FloatType\)/);assert.match(source,/hdrEnvironment\.colorSpace = THREE\.LinearSRGBColorSpace/);assert.match(source,/pmrem\.fromEquirectangular\(hdrEnvironment\)/);assert.match(source,/scene\.background = skybox/);assert.match(source,/scene\.environment = environmentTarget\.texture/);assert.match(source,/key\.castShadow = true/);assert.match(source,/camera\.position\.z = Math\.max\(5\.8/);assert.match(source,/environmentTarget\.dispose\(\)/);
});
