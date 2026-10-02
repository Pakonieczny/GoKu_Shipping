import * as THREE from 'three';
import {RoomEnvironment} from 'three/addons/environments/RoomEnvironment.js';
import {EffectComposer} from 'three/addons/postprocessing/EffectComposer.js';
import {RenderPass} from 'three/addons/postprocessing/RenderPass.js';
import {UnrealBloomPass} from 'three/addons/postprocessing/UnrealBloomPass.js';
import {OutputPass} from 'three/addons/postprocessing/OutputPass.js';

// Original Brites guide, generated as real mesh geometry. No raster character,
// remotely hosted model, camera, microphone, tracking or external texture URL.
export const AVATAR_SCENE_DECLARATIONS = Object.freeze({
  schema: 1,
  textures: Object.freeze([
    Object.freeze({name: 'Porcelain micro-surface', kind: 'porcelain'}),
    Object.freeze({name: 'Champagne brushed grain', kind: 'brush'}),
    Object.freeze({name: 'Champagne roughness', kind: 'roughness'}),
    Object.freeze({name: 'Aquamarine iris radial fibres', kind: 'color'})
  ])
});
export function createAvatarScene({container, quality, onFrame, onContext, onError}) {
  const doc = container.ownerDocument, win = doc.defaultView;
  const renderer = new THREE.WebGLRenderer({alpha: true, antialias: true, powerPreference: 'high-performance'});
  renderer.setPixelRatio(quality.pixelRatio);
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.08;
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  renderer.info.autoReset = false;
  renderer.domElement.setAttribute('aria-hidden', 'true');
  container.appendChild(renderer.domElement);
  const scene = new THREE.Scene();
  scene.background = new THREE.Color('#f4eee4');
  const camera = new THREE.PerspectiveCamera(34, 1, .1, 35);
  camera.position.set(.45, .8, 6.4); camera.lookAt(0, -.18, 0);
  const materials = new Set(), textures = new Set(), geometries = new Set();
  const material = params => {const value = new THREE.MeshPhysicalMaterial(params); materials.add(value); return value;};
  const basic = params => {const value = new THREE.MeshBasicMaterial(params); materials.add(value); return value;};
  const geometry = value => {geometries.add(value); return value;};
  const segments = (high, minimum = 16) => Math.max(minimum, Math.round(high * quality.geometryScale));
  const maps = [];
  function textureMap(name, kind, size = quality.textureSize) {
    const canvas = doc.createElement('canvas'); canvas.width = canvas.height = size;
    const ctx = canvas.getContext('2d', {alpha: false});
    if (!ctx) throw Error('Material textures could not be created.');
    const data = ctx.createImageData(size, size), pixels = data.data;
    let seed = kind === 'porcelain' ? 982451653 : 961748941;
    const noise = () => {seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296;};
    for (let y = 0; y < size; y++) {
      const brush = (noise() - .5) * 18 + Math.sin(y * .9) * 3;
      for (let x = 0; x < size; x++) {
        let value = kind === 'porcelain' ? 128 + (noise() - .5) * 14 : kind === 'roughness' ? 215 + brush * .4 + (noise() - .5) * 8 : 128 + brush + (noise() - .5) * 6;
        const index = (y * size + x) * 4; value = Math.max(0, Math.min(255, Math.round(value)));
        pixels[index] = pixels[index + 1] = pixels[index + 2] = value; pixels[index + 3] = 255;
      }
    }
    ctx.putImageData(data, 0, 0);
    const texture = new THREE.CanvasTexture(canvas); texture.wrapS = texture.wrapT = THREE.RepeatWrapping; texture.anisotropy = Math.min(8, renderer.capabilities.getMaxAnisotropy()); texture.colorSpace = THREE.NoColorSpace;
    texture.repeat.set(kind === 'porcelain' ? 2 : 1, kind === 'porcelain' ? 2 : 1);
    textures.add(texture); maps.push({name, width: size, height: size, kind}); return texture;
  }
  const porcelainMap = textureMap(AVATAR_SCENE_DECLARATIONS.textures[0].name, AVATAR_SCENE_DECLARATIONS.textures[0].kind), goldMap = textureMap(AVATAR_SCENE_DECLARATIONS.textures[1].name, AVATAR_SCENE_DECLARATIONS.textures[1].kind), roughnessMap = textureMap(AVATAR_SCENE_DECLARATIONS.textures[2].name, AVATAR_SCENE_DECLARATIONS.textures[2].kind);
  const ivory = material({color: '#f9f2df', roughness: .23, metalness: .03, clearcoat: .94, clearcoatRoughness: .16, bumpMap: porcelainMap, bumpScale: .006, sheen: .2, sheenColor: new THREE.Color('#fff7e3'), iridescence: .06, iridescenceIOR: 1.32, iridescenceThicknessRange: [180, 260]});
  const gold = material({color: '#dcc091', metalness: .91, roughness: .3, roughnessMap, bumpMap: goldMap, bumpScale: .0025, anisotropy: .72, anisotropyRotation: Math.PI / 2, clearcoat: .28, clearcoatRoughness: .24});
  const paleGold = material({color: '#e8d5b3', metalness: .74, roughness: .25, roughnessMap, bumpMap: goldMap, bumpScale: .002, clearcoat: .5});
  const face = material({color: '#07171d', metalness: .28, roughness: .22, clearcoat: 1, clearcoatRoughness: .08});
  const lidMaterial = material({color: '#0a2027', metalness: .08, roughness: .35, clearcoat: .45});
  const eyeMaterial = material({color: '#fffaf0', roughness: .18, metalness: .02, clearcoat: 1, clearcoatRoughness: .045});
  const pupilMaterial = material({color: '#061719', roughness: .065, clearcoat: 1});
  const mouthMaterial = material({color: '#f9d898', emissive: '#e8aa48', emissiveIntensity: 1.15, roughness: .2, metalness: .15});
  const corneaMaterial = material({color: '#f0ffff', roughness: .055, metalness: 0, transmission: .97, thickness: .06, ior: 1.38, clearcoat: 1, depthWrite: false});
  const glint = basic({color: '#e5ffff', toneMapped: false});
  const gemMaterial = material({color: '#3b72d7', roughness: .065, metalness: .02, transmission: .75, thickness: .45, ior: 1.77, dispersion: .035, clearcoat: 1, attenuationColor: new THREE.Color('#acd9ff'), attenuationDistance: .85});
  const stageMaterial = material({color: '#f2e8d7', roughness: .5, metalness: .05, clearcoat: .15, bumpMap: porcelainMap, bumpScale: .005});
  const floorMaterial = material({color: '#f4eee4', roughness: .87, metalness: 0});
  const avatar = new THREE.Group(); scene.add(avatar);
  function irisTexture() {
    const size = quality.textureSize, canvas = doc.createElement('canvas'); canvas.width = canvas.height = size; const ctx = canvas.getContext('2d');
    const mid = size / 2, radius = size * .49, gradient = ctx.createRadialGradient(mid, mid, 0, mid, mid, radius);
    gradient.addColorStop(0, '#03151a'); gradient.addColorStop(.25, '#071d24'); gradient.addColorStop(.29, '#46babb'); gradient.addColorStop(.43, '#80e9dd'); gradient.addColorStop(.72, '#39949f'); gradient.addColorStop(.91, '#164454'); gradient.addColorStop(1, '#061d27'); ctx.fillStyle = gradient; ctx.fillRect(0, 0, size, size);
    let seed = 65537; const random = () => {seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296;};
    for (let i = 0; i < 2800; i++) {const angle = random() * Math.PI * 2, start = (.29 + random() * .22) * radius, end = (.69 + random() * .27) * radius; ctx.strokeStyle = i % 4 ? 'rgba(179,255,237,' + (.04 + random() * .19) + ')' : 'rgba(9,44,69,.22)'; ctx.lineWidth = 1 + random() * 2.4; ctx.beginPath(); ctx.moveTo(mid + Math.cos(angle) * start, mid + Math.sin(angle) * start); ctx.quadraticCurveTo(mid + Math.cos(angle + .014) * radius * .62, mid + Math.sin(angle + .014) * radius * .62, mid + Math.cos(angle + .025) * end, mid + Math.sin(angle + .025) * end); ctx.stroke();}
    for (let i = 0; i < 800; i++) {const angle = random() * Math.PI * 2, distance = (.36 + random() * .45) * radius; ctx.fillStyle = 'rgba(222,255,228,' + (.18 + random() * .28) + ')'; ctx.beginPath(); ctx.arc(mid + Math.cos(angle) * distance, mid + Math.sin(angle) * distance, .6 + random() * 2.3, 0, Math.PI * 2); ctx.fill();}
    const texture = new THREE.CanvasTexture(canvas); texture.colorSpace = THREE.SRGBColorSpace; texture.anisotropy = Math.min(8, renderer.capabilities.getMaxAnisotropy()); textures.add(texture); maps.push({name: AVATAR_SCENE_DECLARATIONS.textures[3].name, width: size, height: size, kind: AVATAR_SCENE_DECLARATIONS.textures[3].kind}); return texture;
  }
  const irisMap = irisTexture(), irisMaterial = material({map: irisMap, color: '#d2ffff', emissive: '#2b959d', emissiveMap: irisMap, emissiveIntensity: .48, roughness: .22, metalness: .1, clearcoat: 1});
  const head = new THREE.Group(); head.position.set(0, .59, .03); avatar.add(head);
  function roundedGeometry(width, height, depth, radius, bevel = .065) {
    const x = -width / 2, y = -height / 2, s = new THREE.Shape();
    s.moveTo(x + radius, y); s.lineTo(x + width - radius, y); s.quadraticCurveTo(x + width, y, x + width, y + radius); s.lineTo(x + width, y + height - radius); s.quadraticCurveTo(x + width, y + height, x + width - radius, y + height); s.lineTo(x + radius, y + height); s.quadraticCurveTo(x, y + height, x, y + height - radius); s.lineTo(x, y + radius); s.quadraticCurveTo(x, y, x + radius, y);
    const result = geometry(new THREE.ExtrudeGeometry(s, {depth, bevelEnabled: bevel > 0, bevelSegments: segments(16, 10), steps: 1, bevelSize: bevel, bevelThickness: bevel, curveSegments: segments(72, 40)})); result.translate(0, 0, -depth / 2); result.computeVertexNormals(); return result;
  }
  function mesh(g, m, parent, x = 0, y = 0, z = 0) {const value = new THREE.Mesh(g, m); value.position.set(x, y, z); value.castShadow = true; value.receiveShadow = true; parent.add(value); return value;}
  const shell = mesh(roundedGeometry(2.06, 1.3, .5, .36, .09), gold, head);
  mesh(roundedGeometry(1.98, 1.23, .5, .34, .075), ivory, head, 0, .015, .035);
  mesh(roundedGeometry(1.73, .94, .085, .28, .035), face, head, 0, .015, .37);
  // Separate edges, inlaid side caps and gem give the head a crafted jewellery
  // object silhouette rather than a copied movie robot body.
  const sphere = geometry(new THREE.SphereGeometry(1, segments(144, 80), segments(96, 56)));
  const smallSphere = geometry(new THREE.SphereGeometry(1, segments(88, 56), segments(64, 40)));
  for (const side of [-1, 1]) {
    const cap = mesh(sphere, paleGold, head, side * 1.065, -.02, -.045); cap.scale.set(.12, .27, .22);
    const bead = mesh(smallSphere, ivory, head, side * 1.105, -.1, .115); bead.scale.set(.052, .1, .05);
  }
  const forehead = mesh(geometry(new THREE.OctahedronGeometry(.075, 0)), gemMaterial, head, .67, .56, .32); forehead.rotation.z = .1;
  const foreheadBezel = mesh(geometry(new THREE.TorusGeometry(.068, .012, 16, 80)), gold, head, .67, .56, .31); foreheadBezel.rotation.z = Math.PI / 4;
  const torso = mesh(sphere, ivory, avatar, 0, -.63, 0); torso.scale.set(.6, .73, .44);
  const neck = mesh(geometry(new THREE.CylinderGeometry(.24, .2, .22, segments(96, 64))), gold, avatar, 0, -.08, 0);
  for (const y of [-.16, -.06]) {const jointRing = mesh(geometry(new THREE.TorusGeometry(.23, .017, 20, 128)), paleGold, avatar, 0, y, 0); jointRing.rotation.x = Math.PI / 2;}
  const waist = mesh(geometry(new THREE.TorusGeometry(.49, .046, 32, segments(192, 128))), paleGold, avatar, 0, -.88, 0); waist.rotation.x = Math.PI / 2; waist.scale.set(1.16, .89, 1);
  const lower = mesh(sphere, paleGold, avatar, 0, -1.245, 0); lower.scale.set(.285, .09, .235);
  const chestGem = mesh(geometry(new THREE.OctahedronGeometry(.18, 0)), gemMaterial, avatar, 0, -.44, .445); chestGem.scale.set(.78, 1.15, .55); chestGem.rotation.z = .015;
  const jewelBezel = mesh(geometry(new THREE.TorusGeometry(.17, .017, 24, 100)), gold, avatar, 0, -.435, .42); jewelBezel.scale.set(.78, 1.14, .5); jewelBezel.rotation.z = Math.PI / 4;
  const chainCurve = new THREE.CatmullRomCurve3([new THREE.Vector3(-.16, -.03, .18), new THREE.Vector3(-.2, -.17, .35), new THREE.Vector3(0, -.36, .45), new THREE.Vector3(.2, -.17, .35), new THREE.Vector3(.16, -.03, .18)]);
  mesh(geometry(new THREE.TubeGeometry(chainCurve, 96, .012, 12, false)), paleGold, avatar);
  // Small alternating links are instanced, so detail costs one draw call.
  const linkGeometry = geometry(new THREE.TorusGeometry(.021, .006, 10, 24)), links = new THREE.InstancedMesh(linkGeometry, gold, 28), transform = new THREE.Object3D();
  for (let i = 0; i < 28; i++) {transform.position.copy(chainCurve.getPoint(i / 27)); transform.rotation.set(0, i % 2 ? Math.PI / 2 : 0, (i - 14) * .035); transform.updateMatrix(); links.setMatrixAt(i, transform.matrix);} links.castShadow = true; avatar.add(links);
  const arms = [];
  for (const side of [-1, 1]) {
    const arm = new THREE.Group(); arm.position.set(side * .65, -.4, 0); avatar.add(arm);
    const joint = mesh(smallSphere, gold, arm, 0, 0, 0); joint.scale.set(.1, .115, .11);
    const hand = mesh(sphere, ivory, arm, side * .12, -.24, .07); hand.scale.set(.135, .21, .16); hand.rotation.z = side * .22;
    const cuff = mesh(geometry(new THREE.TorusGeometry(.1, .018, 20, 96)), paleGold, arm, side * .1, -.16, .07); cuff.rotation.x = Math.PI / 2;
    const palm = mesh(smallSphere, paleGold, arm, side * .155, -.4, .105); palm.scale.set(.095, .105, .07);
    for (let finger = 0; finger < 4; finger++) {
      const x = side * .155 + (finger - 1.5) * .039, length = finger === 0 || finger === 3 ? .08 : .105;
      const digit = mesh(geometry(new THREE.CapsuleGeometry(.017, length, 8, 24)), gold, arm, x, -.47 - length / 2, .11 + finger * .006); digit.rotation.x = -.18;
      const knuckle = mesh(smallSphere, paleGold, arm, x, -.44, .118 + finger * .006); knuckle.scale.set(.019, .021, .021);
    }
    const thumb = mesh(geometry(new THREE.CapsuleGeometry(.021, .067, 8, 24)), gold, arm, side * .27, -.416, .13); thumb.rotation.z = -side * .7;
    arms.push({group: arm, side});
  }
  const eyes = [];
  for (const side of [-1, 1]) {
    const group = new THREE.Group(); group.position.set(side * .405, .055, .48); head.add(group);
    const eye = mesh(sphere, eyeMaterial, group); eye.scale.set(.215, .278, .06);
    const iris = mesh(geometry(new THREE.CircleGeometry(.145, segments(128, 80))), irisMaterial, group, side * -.013, -.01, .067);
    const pupil = mesh(smallSphere, pupilMaterial, group, side * -.013, -.01, .077); pupil.scale.set(.052, .057, .013);
    const cornea = mesh(sphere, corneaMaterial, group, side * -.013, -.01, .085); cornea.scale.set(.147, .147, .037); cornea.castShadow = false;
    const catchlight = mesh(smallSphere, glint, group, -.043, .077, .125); catchlight.scale.set(.025, .031, .006); catchlight.castShadow = false;
    const lowerGlint = mesh(smallSphere, glint, group, .042, -.063, .121); lowerGlint.scale.set(.009, .012, .004); lowerGlint.castShadow = false;
    const lidGeometry = roundedGeometry(.46, .26, .022, .1, .005), topLid = mesh(lidGeometry, lidMaterial, group, 0, .44, .09), bottomLid = mesh(lidGeometry, lidMaterial, group, 0, -.43, .09);
    const browCurve = new THREE.QuadraticBezierCurve3(new THREE.Vector3(-.17, 0, 0), new THREE.Vector3(0, .045, .002), new THREE.Vector3(.17, .008, 0));
    const brow = mesh(geometry(new THREE.TubeGeometry(browCurve, 56, .021, 16, false)), paleGold, head, side * .405, .37, .472);
    const lashCurve = new THREE.QuadraticBezierCurve3(new THREE.Vector3(-.185, .12, .025), new THREE.Vector3(0, .285, .035), new THREE.Vector3(.185, .12, .025)), lash = mesh(geometry(new THREE.TubeGeometry(lashCurve, 64, .009, 12, false)), gold, group, 0, .025, .088);
    eyes.push({group, eye, iris, pupil, cornea, catchlight, lowerGlint, topLid, bottomLid, brow, lash, side});
  }
  const mouth = new THREE.Group(); mouth.position.set(0, -.278, .476); head.add(mouth);
  const smile = mesh(geometry(new THREE.TubeGeometry(new THREE.QuadraticBezierCurve3(new THREE.Vector3(-.135, .025, 0), new THREE.Vector3(0, -.085, .012), new THREE.Vector3(.135, .025, 0)), 64, .015, 16, false)), mouthMaterial, mouth);
  const concern = mesh(geometry(new THREE.TubeGeometry(new THREE.QuadraticBezierCurve3(new THREE.Vector3(-.105, -.005, 0), new THREE.Vector3(0, .05, .012), new THREE.Vector3(.105, -.005, 0)), 64, .013, 16, false)), mouthMaterial, mouth); concern.visible = false;
  const openMouth = mesh(smallSphere, mouthMaterial, mouth); openMouth.scale.set(.08, .06, .013); openMouth.visible = false;
  const decoration = mesh(geometry(new THREE.TorusGeometry(1.52, .009, 12, 220, Math.PI * 1.06)), paleGold, scene, 0, -.05, -.75); decoration.rotation.z = -.35; decoration.rotation.x = .12; decoration.castShadow = false;
  const floor = mesh(geometry(new THREE.PlaneGeometry(200, 200)), floorMaterial, scene, 0, -1.655, 0); floor.rotation.x = -Math.PI / 2; floor.castShadow = false;
  const pedestal = mesh(geometry(new THREE.CylinderGeometry(1.2, 1.25, .11, segments(180, 120))), stageMaterial, scene, 0, -1.575, 0);
  const stageRing = mesh(geometry(new THREE.TorusGeometry(1.22, .014, 16, segments(192, 128))), paleGold, scene, 0, -1.553, 0); stageRing.rotation.x = Math.PI / 2;
  const hemisphere = new THREE.HemisphereLight('#fdf7ee', '#b9c7bf', 1.85); scene.add(hemisphere);
  const key = new THREE.SpotLight('#fff0d9', 78, 20, Math.PI / 5, .7, 2); key.position.set(3.2, 4.8, 5.6); key.target.position.set(0, -.4, 0); key.castShadow = true; key.shadow.mapSize.set(quality.shadowSize, quality.shadowSize); key.shadow.bias = -.00016; key.shadow.normalBias = .025; key.shadow.radius = 3; key.shadow.camera.near = .5; key.shadow.camera.far = 18; scene.add(key, key.target);
  const fill = new THREE.DirectionalLight('#c8eceb', 1.7); fill.position.set(-3, 1.1, 4); scene.add(fill);
  const rim = new THREE.DirectionalLight('#ffe3b5', 2.8); rim.position.set(-1.5, 3.8, -3); scene.add(rim);
  const eyeLight = new THREE.PointLight('#79e9e5', .28, 2.7, 2); eyeLight.position.set(0, .4, .8); avatar.add(eyeLight);
  const pmrem = new THREE.PMREMGenerator(renderer), environment = new RoomEnvironment();
  const environmentTarget = pmrem.fromScene(environment, .045); scene.environment = environmentTarget.texture; scene.environmentIntensity = .78; environment.dispose(); pmrem.dispose();
  let composer = null, bloom = null;
  if (quality.bloom) {
    const target = new THREE.WebGLRenderTarget(1, 1, {type: THREE.HalfFloatType, samples: quality.name === 'high' ? 4 : 0});
    composer = new EffectComposer(renderer, target); composer.addPass(new RenderPass(scene, camera)); bloom = new UnrealBloomPass(new THREE.Vector2(1, 1), .18, .3, 1.15); composer.addPass(bloom); composer.addPass(new OutputPass());
  }
  let active = false, reducedMotion = false, disposed = false, lost = false, last = 0, frames = 0, lastPose = null, renderMs = 0, drawCalls = 0, renderedTriangles = 0, sampleTime = 0, width = 1, height = 1;
  const geometryStats = root => {let triangles = 0, vertices = 0, meshes = 0; root.traverse(object => {if (!object.isMesh) return; const multiplier = object.isInstancedMesh ? object.count : 1; triangles += (object.geometry.index ? object.geometry.index.count : object.geometry.attributes.position.count) / 3 * multiplier; vertices += object.geometry.attributes.position.count * multiplier; meshes += 1;}); return {triangles: Math.round(triangles), vertices, meshes};};
  const modelStats = geometryStats(avatar), sceneStats = geometryStats(scene);
  function resize() {
    if (disposed) return; const box = container.getBoundingClientRect(); width = Math.max(1, Math.round(box.width)); height = Math.max(1, Math.round(box.height));
    renderer.setSize(width, height, false); camera.aspect = width / height; camera.updateProjectionMatrix(); composer?.setSize(width, height); if (lastPose) render(lastPose, true);
  }
  function applyPose(pose) {
    avatar.position.y = pose.bob; avatar.rotation.z = pose.bodyRoll;
    head.rotation.set(pose.headPitch, pose.headYaw, pose.headRoll);
    for (const eye of eyes) {
      eye.eye.scale.y = .278 * pose.eyeOpen; eye.iris.scale.y = pose.eyeOpen; eye.pupil.scale.y = .057 * pose.eyeOpen; eye.cornea.scale.y = .147 * pose.eyeOpen; eye.catchlight.scale.y = .031 * pose.eyeOpen; eye.lowerGlint.scale.y = .012 * pose.eyeOpen; eye.lash.scale.y = pose.eyeOpen;
      for (const part of [eye.iris, eye.pupil, eye.cornea, eye.catchlight, eye.lowerGlint]) {part.position.x = ([eye.iris, eye.pupil, eye.cornea].includes(part) ? eye.side * -.013 : part === eye.catchlight ? -.043 : .042) + pose.gazeX; part.position.y = ([eye.iris, eye.pupil, eye.cornea].includes(part) ? -.01 : part === eye.catchlight ? .077 : -.063) * pose.eyeOpen + pose.gazeY;}
      const closure = Math.max(0, 1 - pose.eyeOpen); eye.topLid.visible = eye.bottomLid.visible = closure > .02; eye.topLid.scale.y = eye.bottomLid.scale.y = Math.max(.02, closure); eye.topLid.position.y = .278 * pose.eyeOpen + .12 * closure; eye.bottomLid.position.y = -.278 * pose.eyeOpen - .12 * closure;
      eye.brow.position.y = .37 + pose.browLift; eye.brow.rotation.z = pose.browAngle * eye.side;
    }
    smile.visible = pose.mouth === 'smile'; concern.visible = pose.mouth === 'concern'; openMouth.visible = pose.mouth === 'open'; openMouth.scale.y = .025 + pose.mouthOpen * .065; openMouth.scale.x = .07 + pose.mouthOpen * .015;
    arms.forEach(({group, side}) => {group.rotation.z = -side * (pose.armLift + pose.gesture); group.rotation.x = -pose.armLift * .4;});
    irisMaterial.emissiveIntensity = .45 + pose.lightPulse * .2; eyeLight.intensity = .24 + pose.lightPulse * .18;
    key.position.x = 3.2 + Math.sin(sampleTime * .25) * .11 * (reducedMotion ? 0 : 1);
  }
  function render(pose, force = false) {
    if (disposed || lost || (!active && !force)) return; const start = win.performance.now(); lastPose = pose; applyPose(pose); renderer.info.reset(); if (composer) composer.render(); else renderer.render(scene, camera); renderMs = win.performance.now() - start; frames += 1; drawCalls = renderer.info.render.calls; renderedTriangles = renderer.info.render.triangles;
  }
  function tick(milliseconds) {const time = milliseconds / 1000; if (time - last < 1 / quality.fps) return; last = time; sampleTime = time; try {render(onFrame(time));} catch {active = false; renderer.setAnimationLoop(null); onError?.();}}
  function setMotion(value) {active = value.active === true; reducedMotion = value.reducedMotion === true; renderer.setAnimationLoop(active && !reducedMotion && !lost ? tick : null);}
  function invalidate() {last = 0;}
  function contextLost(event) {event.preventDefault(); lost = true; renderer.setAnimationLoop(null); onContext?.(true);}
  function contextRestored() {lost = false; onContext?.(false);}
  renderer.domElement.addEventListener('webglcontextlost', contextLost); renderer.domElement.addEventListener('webglcontextrestored', contextRestored);
  const resizeObserver = win.ResizeObserver ? new win.ResizeObserver(resize) : null; resizeObserver?.observe(container); if (!resizeObserver) win.addEventListener('resize', resize);
  resize();
  function snapshot() {return {revision: THREE.REVISION, animated: active && !reducedMotion && !lost, frames, frameRenderMs: Math.round(renderMs * 100) / 100, drawCalls, renderedTriangles, geometry: {model: modelStats, scene: sceneStats}, textures: maps.map(value => ({...value})), shadow: {enabled: true, size: quality.shadowSize, type: 'PCF soft', casts: true, receivingStage: true}, materials: {physical: materials.size - 1, metallicAnisotropy: true, transmission: true, clearcoat: true, environmentReflection: true}, lights: 5, bloom: !!composer, width, height, pixelRatio: renderer.getPixelRatio(), contextLost: lost};}
  function destroy() {if (disposed) return; disposed = true; renderer.setAnimationLoop(null); resizeObserver?.disconnect(); win.removeEventListener('resize', resize); renderer.domElement.removeEventListener('webglcontextlost', contextLost); renderer.domElement.removeEventListener('webglcontextrestored', contextRestored); composer?.passes.forEach(pass => pass.dispose?.()); composer?.dispose(); geometries.forEach(value => value.dispose()); materials.forEach(value => value.dispose()); textures.forEach(value => value.dispose()); environmentTarget.dispose(); renderer.dispose(); renderer.forceContextLoss(); renderer.domElement.remove();}
  return {setMotion, invalidate, render, snapshot, destroy};
}
