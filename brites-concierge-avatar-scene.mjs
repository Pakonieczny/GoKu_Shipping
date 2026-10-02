import * as THREE from 'three';
import {EffectComposer} from 'three/addons/postprocessing/EffectComposer.js';
import {RenderPass} from 'three/addons/postprocessing/RenderPass.js';
import {UnrealBloomPass} from 'three/addons/postprocessing/UnrealBloomPass.js';
import {OutputPass} from 'three/addons/postprocessing/OutputPass.js';

// Original Brites guide, generated as real mesh geometry. No raster character,
// remotely hosted model, camera, microphone, tracking or external texture URL.
export const AVATAR_SCENE_DECLARATIONS = Object.freeze({
  schema: 2,
  identity: 'original single-eye pebble robot',
  expressionRig: 'deformable luminous aperture, no human iris or mouth',
  stateColors: Object.freeze({idle: '#4aa8ff', listening: '#49c9ff', thinking: '#ab87ff', speaking: '#ffcb79', success: '#72ddd1', error: '#ffc28e'}),
  textures: Object.freeze([
    Object.freeze({name: 'Ivory ceramic micro-surface', kind: 'porcelain'}),
    Object.freeze({name: 'Champagne brushed metal grain', kind: 'brush'}),
    Object.freeze({name: 'Champagne metal roughness', kind: 'roughness'}),
    Object.freeze({name: 'Sapphire glass micro-etch', kind: 'glass'}),
    Object.freeze({name: 'Ivory ceramic base colour', kind: 'albedo'}),
    Object.freeze({name: 'Ivory ceramic tangent-space normal', kind: 'normal'})
  ]),
  environment: Object.freeze({kind: 'procedural studio cubemap', faces: 6, dynamicLights: true, hdri: false})
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
  camera.position.set(.26, .65, 6.2); camera.lookAt(0, -.16, 0);
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
        if (kind === 'albedo') {const grain = (value - 128) * .16; pixels[index] = 249 + grain; pixels[index + 1] = 244 + grain; pixels[index + 2] = 232 + grain;}
        else if (kind === 'normal') {pixels[index] = 128 + (noise() - .5) * 8; pixels[index + 1] = 128 + (noise() - .5) * 8; pixels[index + 2] = 255;}
        else pixels[index] = pixels[index + 1] = pixels[index + 2] = value; pixels[index + 3] = 255;
      }
    }
    ctx.putImageData(data, 0, 0);
    const texture = new THREE.CanvasTexture(canvas); texture.wrapS = texture.wrapT = THREE.RepeatWrapping; texture.anisotropy = Math.min(8, renderer.capabilities.getMaxAnisotropy()); texture.colorSpace = kind === 'albedo' ? THREE.SRGBColorSpace : THREE.NoColorSpace;
    texture.repeat.set(kind === 'porcelain' ? 2 : 1, kind === 'porcelain' ? 2 : 1);
    textures.add(texture); maps.push({name, width: size, height: size, kind}); return texture;
  }
  const porcelainMap = textureMap(AVATAR_SCENE_DECLARATIONS.textures[0].name, AVATAR_SCENE_DECLARATIONS.textures[0].kind), goldMap = textureMap(AVATAR_SCENE_DECLARATIONS.textures[1].name, AVATAR_SCENE_DECLARATIONS.textures[1].kind), roughnessMap = textureMap(AVATAR_SCENE_DECLARATIONS.textures[2].name, AVATAR_SCENE_DECLARATIONS.textures[2].kind);
  const ceramicAlbedo = textureMap(AVATAR_SCENE_DECLARATIONS.textures[4].name, 'albedo'), ceramicNormal = textureMap(AVATAR_SCENE_DECLARATIONS.textures[5].name, 'normal');
  const ivory = material({color: '#ffffff', map: ceramicAlbedo, normalMap: ceramicNormal, normalScale: new THREE.Vector2(.25, .25), roughness: .23, metalness: .03, clearcoat: .94, clearcoatRoughness: .16, bumpMap: porcelainMap, bumpScale: .006, sheen: .2, sheenColor: new THREE.Color('#fff7e3'), iridescence: .06, iridescenceIOR: 1.32, iridescenceThicknessRange: [180, 260]});
  const gold = material({color: '#dcc091', metalness: .91, roughness: .3, roughnessMap, bumpMap: goldMap, bumpScale: .0025, anisotropy: .72, anisotropyRotation: Math.PI / 2, clearcoat: .28, clearcoatRoughness: .24});
  const paleGold = material({color: '#e8d5b3', metalness: .74, roughness: .25, roughnessMap, bumpMap: goldMap, bumpScale: .002, clearcoat: .5});
  const glassMap = textureMap(AVATAR_SCENE_DECLARATIONS.textures[3].name, 'glass');
  const face = material({color: '#071526', metalness: .16, roughness: .17, clearcoat: 1, clearcoatRoughness: .075, bumpMap: glassMap, bumpScale: .0008});
  // These retained material names keep diagnostics compatible; they describe
  // machined parts and light, never skin, human eyes or organic features.
  const lidMaterial = material({color: '#162b40', metalness: .7, roughness: .26});
  const eyeMaterial = material({color: '#76cfff', metalness: .1, roughness: .2, emissive: '#4bbaff', emissiveIntensity: 1.4, clearcoat: 1});
  const pupilMaterial = material({color: '#030e1c', roughness: .19, metalness: .1, clearcoat: 1});
  const mouthMaterial = material({color: '#9cddff', emissive: '#52bfff', emissiveIntensity: 1.2, roughness: .25});
  const corneaMaterial = material({color: '#bbdfff', roughness: .06, transmission: .72, thickness: .055, ior: 1.45, clearcoat: 1, clearcoatRoughness: .035, opacity: .2, transparent: true, depthWrite: false, bumpMap: glassMap, bumpScale: .0006});
  const glint = basic({color: '#bfeaff', toneMapped: false});
  const gemMaterial = material({color: '#3b72d7', roughness: .065, metalness: .02, transmission: .75, thickness: .45, ior: 1.77, dispersion: .035, clearcoat: 1, attenuationColor: new THREE.Color('#acd9ff'), attenuationDistance: .85});
  const stageMaterial = material({color: '#f2e8d7', roughness: .5, metalness: .05, clearcoat: .15, bumpMap: porcelainMap, bumpScale: .005});
  const floorMaterial = material({color: '#f4eee4', roughness: .87, metalness: 0});
  const avatar = new THREE.Group(); scene.add(avatar);
  const irisMaterial = material({color: '#78d7ff', emissive: '#52bfff', emissiveIntensity: 1.35, roughness: .2, metalness: .15, clearcoat: .7});
  const head = new THREE.Group(); head.position.set(0, .65, .01); avatar.add(head);
  function roundedGeometry(width, height, depth, radius, bevel = .065) {
    const x = -width / 2, y = -height / 2, s = new THREE.Shape();
    s.moveTo(x + radius, y); s.lineTo(x + width - radius, y); s.quadraticCurveTo(x + width, y, x + width, y + radius); s.lineTo(x + width, y + height - radius); s.quadraticCurveTo(x + width, y + height, x + width - radius, y + height); s.lineTo(x + radius, y + height); s.quadraticCurveTo(x, y + height, x, y + height - radius); s.lineTo(x, y + radius); s.quadraticCurveTo(x, y, x + radius, y);
    const result = geometry(new THREE.ExtrudeGeometry(s, {depth, bevelEnabled: bevel > 0, bevelSegments: segments(16, 10), steps: 1, bevelSize: bevel, bevelThickness: bevel, curveSegments: segments(72, 40)})); result.translate(0, 0, -depth / 2); result.computeVertexNormals(); return result;
  }
  function mesh(g, m, parent, x = 0, y = 0, z = 0) {const value = new THREE.Mesh(g, m); value.position.set(x, y, z); value.castShadow = true; value.receiveShadow = true; parent.add(value); return value;}
  const sphere = geometry(new THREE.SphereGeometry(1, segments(128, 88), segments(96, 64)));
  const smallSphere = geometry(new THREE.SphereGeometry(1, segments(72, 48), segments(48, 32)));
  const shell = mesh(sphere, ivory, head); shell.scale.set(.99, .85, .58);
  const rearSeam = mesh(geometry(new THREE.TorusGeometry(.81, .014, 24, segments(256, 160))), paleGold, head, 0, 0, -.22); rearSeam.scale.set(1.13, 1, 1);
  const bezel = mesh(geometry(new THREE.TorusGeometry(.672, .034, 40, segments(256, 160))), gold, head, 0, .015, .61); bezel.scale.set(1.07, 1, 1);
  const visor = mesh(sphere, face, head, 0, .015, .52); visor.scale.set(.727, .681, .14);
  // One broad, abstract light aperture. It blinks by deforming into a soft
  // horizontal ribbon rather than using eyelids, eyelashes or a fleshy iris.
  const eye = new THREE.Group(); eye.position.set(0, .022, .685); head.add(eye);
  const apertureGeometry = geometry(new THREE.TorusGeometry(.338, .047, 36, segments(192, 112)));
  const aperture = mesh(apertureGeometry, irisMaterial, eye); aperture.castShadow = false;
  const apertureRest = apertureGeometry.attributes.position.array.slice();
  const halo = mesh(geometry(new THREE.TorusGeometry(.387, .008, 16, segments(192, 112))), eyeMaterial, eye, 0, 0, -.009); halo.castShadow = false;
  const innerHalo = mesh(geometry(new THREE.TorusGeometry(.28, .007, 16, segments(192, 112))), glint, eye, 0, 0, .001); innerHalo.castShadow = false;
  const lens = mesh(sphere, corneaMaterial, head, 0, .015, .677); lens.scale.set(.72, .674, .062); lens.castShadow = false; lens.receiveShadow = false; lens.renderOrder = 3;
  const eyes = [{group: eye, aperture, halo, innerHalo, apertureRest, lens, digital: true}];
  const orbit = mesh(geometry(new THREE.TorusGeometry(.447, .011, 20, 128, Math.PI * .38)), paleGold, eye, 0, 0, -.025); orbit.rotation.z = -.8;
  const statusBars = [], statusBarGeometry = geometry(new THREE.CapsuleGeometry(.012, .016, 4, 16));
  for (let i = 0; i < 5; i++) {
    const bar = mesh(statusBarGeometry, mouthMaterial, head, (i - 2) * .056, -.516, .666); bar.castShadow = false; bar.scale.z = .3; statusBars.push(bar);
  }
  for (const side of [-1, 1]) {
    const hinge = mesh(smallSphere, paleGold, head, side * .935, -.14, -.09); hinge.scale.set(.074, .144, .17);
    const cap = mesh(smallSphere, ivory, head, side * .974, -.14, -.06); cap.scale.set(.045, .099, .105);
    const earPin = mesh(geometry(new THREE.TorusGeometry(.064, .009, 16, 72)), gold, head, side * .994, -.14, -.048); earPin.rotation.y = Math.PI / 2;
  }
  const torso = mesh(sphere, ivory, avatar, 0, -.86, -.01); torso.scale.set(.48, .5, .36);
  const chestInset = mesh(smallSphere, face, avatar, 0, -.77, .33); chestInset.scale.set(.185, .166, .021);
  const chestGem = mesh(geometry(new THREE.OctahedronGeometry(.098, 1)), gemMaterial, avatar, 0, -.765, .362); chestGem.rotation.z = Math.PI / 4;
  const chestRing = mesh(geometry(new THREE.TorusGeometry(.158, .01, 20, 112)), gold, avatar, 0, -.765, .352);
  const neck = mesh(geometry(new THREE.CylinderGeometry(.15, .18, .2, segments(96, 64))), gold, avatar, 0, -.29, 0);
  const neckRing = mesh(geometry(new THREE.TorusGeometry(.161, .016, 20, 112)), paleGold, avatar, 0, -.25, 0); neckRing.rotation.x = Math.PI / 2;
  const waist = mesh(geometry(new THREE.TorusGeometry(.385, .023, 28, segments(192, 128))), paleGold, avatar, 0, -1.12, -.01); waist.rotation.x = Math.PI / 2; waist.scale.set(1.1, .88, 1);
  const footPads = [];
  for (const side of [-1, 1]) {
    const foot = mesh(sphere, ivory, avatar, side * .24, -1.4, .06); foot.scale.set(.205, .12, .27); footPads.push(foot);
    const sole = mesh(smallSphere, paleGold, avatar, side * .24, -1.457, .064); sole.scale.set(.203, .036, .259);
  }
  // Jewellery-like chain detail is an original craft cue; smooth robot paddles
  // are visibly mechanical and helpful without suggesting human hands.
  const chainCurve = new THREE.CatmullRomCurve3([new THREE.Vector3(-.135, -.43, .23), new THREE.Vector3(-.18, -.60, .325), new THREE.Vector3(0, -.765, .37), new THREE.Vector3(.18, -.60, .325), new THREE.Vector3(.135, -.43, .23)]);
  const linkGeometry = geometry(new THREE.TorusGeometry(.013, .0035, 8, 20)), links = new THREE.InstancedMesh(linkGeometry, gold, 28), transform = new THREE.Object3D();
  for (let i = 0; i < 28; i++) {transform.position.copy(chainCurve.getPoint(i / 27)); transform.rotation.set(0, i % 2 ? Math.PI / 2 : 0, (i - 14) * .04); transform.updateMatrix(); links.setMatrixAt(i, transform.matrix);} links.castShadow = true; avatar.add(links);
  const arms = [];
  for (const side of [-1, 1]) {
    const arm = new THREE.Group(); arm.position.set(side * .495, -.66, -.03); avatar.add(arm);
    const shoulder = mesh(smallSphere, gold, arm); shoulder.scale.set(.094, .105, .094);
    const forearm = mesh(sphere, ivory, arm, side * .096, -.202, .03); forearm.scale.set(.105, .226, .108); forearm.rotation.z = side * .2;
    const wrist = mesh(smallSphere, paleGold, arm, side * .149, -.383, .057); wrist.scale.set(.09, .077, .095);
    const palm = mesh(sphere, ivory, arm, side * .173, -.466, .074); palm.scale.set(.14, .145, .075); palm.rotation.z = -side * .15;
    const palmInset = mesh(smallSphere, paleGold, arm, side * .176, -.469, .139); palmInset.scale.set(.056, .065, .011);
    arms.push({group: arm, side, palm});
  }
  const decoration = mesh(geometry(new THREE.TorusGeometry(1.52, .009, 12, 220, Math.PI * 1.06)), paleGold, scene, 0, -.05, -.75); decoration.rotation.z = -.35; decoration.rotation.x = .12; decoration.castShadow = false;
  const floor = mesh(geometry(new THREE.PlaneGeometry(200, 200)), floorMaterial, scene, 0, -1.655, 0); floor.rotation.x = -Math.PI / 2; floor.castShadow = false;
  const pedestal = mesh(geometry(new THREE.CylinderGeometry(1.2, 1.25, .11, segments(180, 120))), stageMaterial, scene, 0, -1.575, 0);
  const stageRing = mesh(geometry(new THREE.TorusGeometry(1.22, .014, 16, segments(192, 128))), paleGold, scene, 0, -1.553, 0); stageRing.rotation.x = Math.PI / 2;
  const hemisphere = new THREE.HemisphereLight('#fdf7ee', '#b9c7bf', 1.85); scene.add(hemisphere);
  const key = new THREE.SpotLight('#fff0d9', 78, 20, Math.PI / 5, .7, 2); key.position.set(3.2, 4.8, 5.6); key.target.position.set(0, -.4, 0); key.castShadow = true; key.shadow.mapSize.set(quality.shadowSize, quality.shadowSize); key.shadow.bias = -.00016; key.shadow.normalBias = .025; key.shadow.radius = 3; key.shadow.camera.near = .5; key.shadow.camera.far = 18; scene.add(key, key.target);
  const fill = new THREE.DirectionalLight('#c8eceb', 1.7); fill.position.set(-3, 1.1, 4); scene.add(fill);
  const rim = new THREE.DirectionalLight('#ffe3b5', 2.8); rim.position.set(-1.5, 3.8, -3); scene.add(rim);
  const eyeLight = new THREE.PointLight('#79e9e5', .28, 2.7, 2); eyeLight.position.set(0, .4, .8); avatar.add(eyeLight);
  // Six locally generated faces form a complete, seamless studio skybox.
  // It is a procedural LDR studio environment, not a claimed photographic HDRI.
  const skyboxSize = Math.min(1024, quality.textureSize), cubeFaces = [];
  const faceDirections = [(u,v)=>[1,-v,-u],(u,v)=>[-1,-v,u],(u,v)=>[u,1,v],(u,v)=>[u,-1,-v],(u,v)=>[u,-v,1],(u,v)=>[-u,-v,-1]];
  const lightDirections = [new THREE.Vector3(.55,.72,.42).normalize(), new THREE.Vector3(-.65,.35,.68).normalize(), new THREE.Vector3(-.3,.55,-.78).normalize()];
  for (let faceIndex = 0; faceIndex < 6; faceIndex++) {
    const canvas = doc.createElement('canvas'); canvas.width = canvas.height = skyboxSize;
    const context = canvas.getContext('2d', {alpha: false}), image = context.createImageData(skyboxSize, skyboxSize), direction = new THREE.Vector3();
    for (let y = 0; y < skyboxSize; y++) for (let x = 0; x < skyboxSize; x++) {
      direction.fromArray(faceDirections[faceIndex]((x + .5) / skyboxSize * 2 - 1, (y + .5) / skyboxSize * 2 - 1)).normalize();
      const ceiling = (direction.y + 1) * .5, panel = Math.pow(Math.max(0, direction.dot(lightDirections[0])), 48) * .44 + Math.pow(Math.max(0, direction.dot(lightDirections[1])), 72) * .35 + Math.pow(Math.max(0, direction.dot(lightDirections[2])), 36) * .28, index = (y * skyboxSize + x) * 4;
      image.data[index] = Math.min(255, 207 + ceiling * 25 + panel * 54); image.data[index + 1] = Math.min(255, 216 + ceiling * 13 + panel * 40); image.data[index + 2] = Math.min(255, 226 - ceiling * 5 + panel * 27); image.data[index + 3] = 255;
    }
    context.putImageData(image, 0, 0); cubeFaces.push(canvas);
  }
  const skybox = new THREE.CubeTexture(cubeFaces); skybox.colorSpace = THREE.SRGBColorSpace; skybox.needsUpdate = true; textures.add(skybox);
  const pmrem = new THREE.PMREMGenerator(renderer), environmentTarget = pmrem.fromCubemap(skybox);
  scene.background = skybox; scene.backgroundBlurriness = .55; scene.backgroundIntensity = 1; scene.environment = environmentTarget.texture; scene.environmentIntensity = .85; pmrem.dispose();
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
    renderer.setSize(width, height, false); camera.aspect = width / height; camera.position.z = Math.max(5.8, 2.85 / (camera.aspect * 2 * Math.tan(THREE.MathUtils.degToRad(camera.fov / 2)))); camera.lookAt(0, -.16, 0); camera.updateProjectionMatrix(); composer?.setSize(width, height); if (lastPose) render(lastPose, true);
  }
  const whiteColor = new THREE.Color('#ffffff'), targetColor = new THREE.Color(), displayedColor = new THREE.Color(AVATAR_SCENE_DECLARATIONS.stateColors.idle);
  let previousState = 'idle', transitionAt = 0, lastColorTime = 0;
  function applyPose(pose) {
    const state = Object.hasOwn(AVATAR_SCENE_DECLARATIONS.stateColors, pose.state) ? pose.state : 'idle';
    if (state !== previousState) {previousState = state; transitionAt = sampleTime;}
    const delta = Math.max(0, Math.min(.1, sampleTime - lastColorTime)); lastColorTime = sampleTime;
    targetColor.set(AVATAR_SCENE_DECLARATIONS.stateColors[state]); displayedColor.lerp(targetColor, reducedMotion ? 1 : 1 - Math.exp(-delta * 8));
    const speaking = state === 'speaking', thinking = state === 'thinking', calm = pose.emotion === 'calm' || pose.emotion === 'reassuring', happy = state === 'success' && !calm, reassuring = state === 'error' || calm;
    avatar.position.y = pose.bob; avatar.rotation.z = pose.bodyRoll; avatar.rotation.x = Number.isFinite(pose.lean) ? pose.lean : 0;
    head.rotation.set(pose.headPitch, pose.headYaw, pose.headRoll + (reassuring ? -.025 : pose.emotion === 'curious' ? .035 : 0));
    const eyeOpen = THREE.MathUtils.clamp(pose.eyeOpen, .035, 1.08), eyeScaleX = THREE.MathUtils.clamp(pose.eyeScaleX || 1, .8, 1.2), eyeScaleY = THREE.MathUtils.clamp(pose.eyeScaleY || 1, .6, 1.2), deformation = THREE.MathUtils.clamp(pose.eyeDeformation || 0, -.3, .3), amplitude = speaking ? pose.mouthOpen : thinking ? .12 : happy ? .18 : 0;
    eye.position.x = THREE.MathUtils.clamp(pose.gazeX, -.12, .12); eye.position.y = .022 + THREE.MathUtils.clamp(pose.gazeY, -.08, .08);
    const attribute = apertureGeometry.attributes.position;
    for (let i = 0; i < attribute.count; i++) {
      const offset = i * 3, x = apertureRest[offset], y = apertureRest[offset + 1], z = apertureRest[offset + 2], angle = Math.atan2(y, x);
      // Local mesh deformation gives speech an elastic four-lobed pulse,
      // success an uplifted arc, and errors a gentle flattened listening shape.
      const wave = reducedMotion ? 0 : Math.sin(angle * 4 + sampleTime * (speaking ? 7 : 2)) * amplitude * .025;
      attribute.setXYZ(i, x * eyeScaleX * (1 + wave) * (1 + (1 - eyeOpen) * .055), (y * (1 + wave) + Math.abs(x) * deformation * .2) * eyeOpen * eyeScaleY, z);
    }
    attribute.needsUpdate = true; apertureGeometry.computeVertexNormals();
    halo.scale.set(eyeScaleX * (1 + amplitude * .025), eyeOpen * eyeScaleY, 1); innerHalo.scale.set(eyeScaleX, eyeOpen * eyeScaleY, 1); orbit.scale.y = eyeOpen * eyeScaleY;
    orbit.rotation.z = -.8 + (thinking && !reducedMotion ? sampleTime * .8 : happy ? .35 : 0);
    irisMaterial.color.copy(displayedColor); irisMaterial.emissive.copy(displayedColor); eyeMaterial.color.copy(displayedColor); eyeMaterial.emissive.copy(displayedColor); mouthMaterial.color.copy(displayedColor); mouthMaterial.emissive.copy(displayedColor); glint.color.copy(displayedColor).lerp(whiteColor, .6);
    irisMaterial.emissiveIntensity = 1.05 + pose.lightPulse * .55 + (speaking ? pose.mouthOpen * .22 : 0);
    eyeLight.color.copy(displayedColor); eyeLight.intensity = .22 + pose.lightPulse * .16;
    statusBars.forEach((bar, index) => {bar.scale.y = speaking ? .7 + pose.mouthOpen * (.8 + .35 * Math.sin(sampleTime * 8 + index)) : thinking && !reducedMotion ? .8 + .3 * Math.sin(sampleTime * 3 - index * .7) : 1;});
    arms.forEach(({group, side}) => {const invitation = state === 'listening' ? .065 : speaking ? .04 : reassuring ? .035 : 0, offer = side === 1 && Number.isFinite(pose.offer) ? pose.offer : 0; group.rotation.z = -side * (pose.armLift + invitation + offer * .24); group.rotation.x = -pose.armLift * .4 - offer * .065; group.rotation.y = side * offer * .12;});
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
  function snapshot() {return {revision: THREE.REVISION, animated: active && !reducedMotion && !lost, frames, frameRenderMs: Math.round(renderMs * 100) / 100, drawCalls, renderedTriangles, geometry: {model: modelStats, scene: sceneStats}, textures: maps.map(value => ({...value})), shadow: {enabled: true, size: quality.shadowSize, type: 'PCF soft', casts: true, receivingStage: true}, mannerism: {name: lastPose?.mannerism || null, active: lastPose?.mannerismActive === true, eventBound: true}, character: {identity: AVATAR_SCENE_DECLARATIONS.identity, digitalEyes: 1, humanFeatures: false, meshDeformation: true, state: previousState, color: '#' + displayedColor.getHexString()}, environment: {kind: AVATAR_SCENE_DECLARATIONS.environment.kind, faces: 6, size: skyboxSize, hdri: false}, materials: {physical: [...materials].filter(value => value.isMeshPhysicalMaterial).length, metallicAnisotropy: true, transmission: true, clearcoat: true, environmentReflection: true}, lights: 5, bloom: !!composer, width, height, pixelRatio: renderer.getPixelRatio(), contextLost: lost};}
  function destroy() {if (disposed) return; disposed = true; renderer.setAnimationLoop(null); resizeObserver?.disconnect(); win.removeEventListener('resize', resize); renderer.domElement.removeEventListener('webglcontextlost', contextLost); renderer.domElement.removeEventListener('webglcontextrestored', contextRestored); composer?.passes.forEach(pass => pass.dispose?.()); composer?.dispose(); geometries.forEach(value => value.dispose()); materials.forEach(value => value.dispose()); textures.forEach(value => value.dispose()); environmentTarget.dispose(); renderer.dispose(); renderer.forceContextLoss(); renderer.domElement.remove();}
  return {setMotion, invalidate, render, snapshot, destroy};
}
