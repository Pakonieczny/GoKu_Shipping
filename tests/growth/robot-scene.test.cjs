'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const THREE = require('three'), avatar = require('../../brites-concierge-avatar.js');
const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8');
// Execute the production mesh/pose blocks with actual Three.js classes on CPU.
// These tests intentionally do not claim WebGL pixels, PBR appearance or FPS.
function fixture(profile = 'high') {
  const begin = source.indexOf('  const head = new THREE.Group();'), end = source.indexOf('  const decoration = mesh(', begin);
  const poseBegin = source.indexOf('  const whiteColor = new THREE.Color('), poseEnd = source.indexOf('  function render(pose', poseBegin);
  assert.ok(begin > 0 && end > begin && poseBegin > end && poseEnd > poseBegin);
  const materialNames = ['ivory','gold','paleGold','face','lidMaterial','eyeMaterial','pupilMaterial','glint','gemMaterial','corneaMaterial','irisMaterial','mouthMaterial'];
  const materials = Object.fromEntries(materialNames.map(name => [name, new THREE.MeshPhysicalMaterial()]));
  const quality = avatar.qualityFor(profile === 'adaptive' ? {width:390,memory:4,pixelRatio:2} : {width:1440,memory:8,pixelRatio:2});
  const script = '(()=>{const geometries=new Set(),geometry=value=>{geometries.add(value);return value;},segments=(high,minimum=16)=>Math.max(minimum,Math.round(high*quality.geometryScale)),avatar=new THREE.Group();' + source.slice(begin,end) + '\nconst key=new THREE.SpotLight(),eyeLight=new THREE.PointLight();let sampleTime=1,reducedMotion=false;' + source.slice(poseBegin,poseEnd) + '\nreturn {avatar,head,eyes,arms,footPads,apertureGeometry,apertureRest,eye,halo,statusBars,geometries,pose:(value,time=1,reduced=false)=>{sampleTime=time;reducedMotion=reduced;applyPose(value);},dispose:()=>geometries.forEach(value=>value.dispose())};})()';
  const model = vm.runInNewContext(script, {THREE,quality,...materials,AVATAR_SCENE_DECLARATIONS:{stateColors:{idle:'#66cfff',listening:'#319dff',thinking:'#b29bff',speaking:'#ffc179',success:'#7ce3c3',error:'#e9c8a9'}}});
  return {...model,materials,destroy(){model.dispose();Object.values(materials).forEach(value=>value.dispose());}};
}
test('original robot has one luminous aperture, mechanical limbs and no organic facial rig',()=>{
  const f=fixture();try {assert.equal(f.eyes.length,1);assert.equal(f.eyes[0].digital,true);assert.equal(f.arms.length,2);assert.equal(f.footPads.length,2);assert.equal(f.statusBars.length,5);assert.equal(f.eyes[0].topLid,undefined);assert.equal(f.eyes[0].pupil,undefined);assert.doesNotMatch(source,/function irisTexture|const lashCurve|const smile =|const openMouth/);let triangles=0;f.avatar.traverse(o=>{if(o.isMesh)triangles+=(o.geometry.index?o.geometry.index.count:o.geometry.attributes.position.count)/3*(o.isInstancedMesh?o.count:1);});assert.ok(triangles>100000);assert.ok(triangles<600000);}finally{f.destroy();}
});
test('real aperture geometry blinks and elastically deforms while preserving finite normals',()=>{
  const f=fixture();try {f.pose(avatar.poseFor({state:'idle',time:1}),1);const open=f.apertureGeometry.attributes.position.array.slice();const p={...avatar.poseFor({state:'speaking',time:2,level:.8}),eyeOpen:.035,mouthOpen:.8};f.pose(p,2);const current=f.apertureGeometry.attributes.position.array;assert.notDeepEqual(current,open);let maxY=0;for(let i=1;i<current.length;i+=3)maxY=Math.max(maxY,Math.abs(current[i]));assert.ok(maxY<.025);assert.ok([...current].every(Number.isFinite));assert.ok([...f.apertureGeometry.attributes.normal.array].every(Number.isFinite));assert.ok(f.halo.scale.y<.04);}finally{f.destroy();}
});
test('state colour is smoothed in live mode and immediate in reduced motion',()=>{
  const f=fixture();try {const blue=new THREE.Color('#319dff');f.pose(avatar.poseFor({state:'listening',reducedMotion:true}),1,true);assert.equal(f.materials.irisMaterial.emissive.getHex(),blue.getHex());const before=f.materials.irisMaterial.emissive.clone();f.pose(avatar.poseFor({state:'thinking',time:1.05}),1.05);const midway=f.materials.irisMaterial.emissive.clone();assert.notEqual(midway.getHex(),before.getHex());assert.notEqual(midway.getHex(),new THREE.Color('#b29bff').getHex());f.pose(avatar.poseFor({state:'speaking',reducedMotion:true}),2,true);assert.equal(f.materials.irisMaterial.emissive.getHex(),new THREE.Color('#ffc179').getHex());}finally{f.destroy();}
});
test('whole-character camera adapts to narrow and wide stages; skybox and PBR declarations are explicit',async()=>{
  const module=await import('../../brites-concierge-avatar-scene.mjs');const declarations=module.AVATAR_SCENE_DECLARATIONS;assert.equal(declarations.environment.faces,6);assert.equal(declarations.environment.hdr,true);assert.equal(declarations.environment.hdri,false);assert.ok(declarations.textures.some(row=>row.kind==='albedo'));assert.ok(declarations.textures.some(row=>row.kind==='normal'));assert.ok(Object.isFrozen(declarations.stateColors));assert.match(source,/new THREE\.CubeTexture\(cubeFaces\)/);assert.match(source,/new THREE\.DataTexture\(hdrPixels, hdrWidth, hdrHeight, THREE\.RGBAFormat, THREE\.FloatType\)/);assert.match(source,/hdrEnvironment\.colorSpace = THREE\.LinearSRGBColorSpace/);assert.match(source,/pmrem\.fromEquirectangular\(hdrEnvironment\)/);assert.match(source,/scene\.background = skybox/);assert.match(source,/scene\.environment = environmentTarget\.texture/);assert.match(source,/key\.castShadow = true/);assert.match(source,/camera\.position\.z = Math\.max\(5\.8/);assert.match(source,/environmentTarget\.dispose\(\)/);
});
