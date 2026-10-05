'use strict';
// CPU-only diagnostics execute the frozen production geometry-construction
// block with the actual local Three.js classes. This is not a browser or GPU
// render and does not certify shader appearance, lighting, shadows or FPS.
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm'), crypto = require('node:crypto');
const THREE = require('three');
const avatar = require('../brites-concierge-avatar.js');
const sourcePath = path.resolve(__dirname, '..', 'brites-concierge-avatar-scene.mjs');
function buildModel(profile = 'high') {
  const source = fs.readFileSync(sourcePath, 'utf8'), begin = source.indexOf('  const head = new THREE.Group();'), end = source.indexOf('  const decoration = mesh(', begin);
  if (begin < 0 || end < 0 || end <= begin) throw Error('Frozen production model boundaries changed; review the diagnostic harness.');
  const names = ['ivory', 'gold', 'paleGold', 'face', 'lidMaterial', 'eyeMaterial', 'pupilMaterial', 'glint', 'gemMaterial', 'corneaMaterial', 'irisMaterial', 'mouthMaterial'];
  const materials = Object.fromEntries(names.map(name => [name, new THREE.MeshPhysicalMaterial()]));
  const quality = avatar.qualityFor(profile === 'adaptive' ? {width: 390, memory: 4, pixelRatio: 2} : {width: 1440, memory: 8, pixelRatio: 2});
  const built = vm.runInNewContext('(()=>{const geometries=new Set();const geometry=value=>{geometries.add(value);return value;};const segments=(high,minimum=16)=>Math.max(minimum,Math.round(high*quality.geometryScale));const avatar=new THREE.Group();' + source.slice(begin, end) + '\nreturn {avatar,geometries,head,eyes,arms};})()', {THREE, quality, ...materials}, {timeout: 10000, filename: 'frozen-brites-avatar-geometry.js'});
  return {...built, materials, quality, sourceHash: crypto.createHash('sha256').update(source).digest('hex')};
}
function inspect(model) {
  const result = {profile: model.quality.name, triangles: 0, vertices: 0, meshes: 0, instancedCopies: 0, uniqueGeometries: model.geometries.size, materialSlots: Object.keys(model.materials).length, rig: {eyeAssemblies: model.eyes.length, brows: model.eyes.filter(eye => eye.brow).length, upperAndLowerLids: model.eyes.reduce((n, eye) => n + (eye.topLid ? 1 : 0) + (eye.bottomLid ? 1 : 0), 0), armAssemblies: model.arms.length, head: true}, finiteBuffers: true, validIndices: true};
  result.graphicFace = {brows: 0, shutters: 0, cheekFacets: 0, smileGlyph: false, signalMarkers: 0};
  model.avatar.traverse(object => {if (!object.isMesh) return; const position = object.geometry.attributes.position, index = object.geometry.index, copies = object.isInstancedMesh ? object.count : 1; result.triangles += (index ? index.count : position.count) / 3 * copies; result.vertices += position.count * copies; result.meshes++; if (object.isInstancedMesh) result.instancedCopies += copies;
    if (/^expression-brow-/.test(object.name)) result.graphicFace.brows++;
    if (/^expression-(?:upper|lower)-shutter$/.test(object.name)) result.graphicFace.shutters++;
    if (/^expression-cheek-/.test(object.name)) result.graphicFace.cheekFacets++;
    if (object.name === 'expression-smile-glyph') result.graphicFace.smileGlyph = true;
    if (/^expression-signal-/.test(object.name)) result.graphicFace.signalMarkers++;
  });
  result.rig.brows = result.graphicFace.brows;
  for (const geometry of model.geometries) {const position = geometry.attributes.position; for (const name of ['position', 'normal', 'uv']) {const attribute = geometry.attributes[name]; if (attribute && !Array.from(attribute.array).every(Number.isFinite)) result.finiteBuffers = false;} const index = geometry.index; if (index && Array.from(index.array).some(value => value < 0 || value >= position.count || !Number.isInteger(value))) result.validIndices = false;}
  result.triangles = Math.round(result.triangles); return result;
}
function dispose(model) {model.geometries.forEach(value => value.dispose()); Object.values(model.materials).forEach(value => value.dispose());}
function report() {
  const profiles = []; let sourceHash;
  for (const profile of ['high', 'adaptive']) {const model = buildModel(profile); sourceHash = model.sourceHash; profiles.push(inspect(model)); dispose(model);}
  return {schema: 1, at: new Date().toISOString(), sourceHash, threeRevision: THREE.REVISION, verification: 'CPU-only actual production mesh construction; no WebGL or GPU visual certification', includesHiddenExpressionMeshes: true, profiles, renderingAcceptance: {status: 'requires WebGL-capable browser', shadowsAndLights: 'Declared in production source, not visually certified here', textures: 'Six 2048px procedural maps and a six-face 1024px studio cubemap declared in production source, not uploaded or rendered by this diagnostic', fps: 'Not measured'}};
}
if (require.main === module) {const data = report(), destination = process.argv[2]; if (destination) {fs.mkdirSync(path.dirname(path.resolve(destination)), {recursive: true}); fs.writeFileSync(destination, JSON.stringify(data, null, 2) + '\n');} console.log(JSON.stringify(data, null, 2));}
module.exports = {buildModel, inspect, dispose, report};
