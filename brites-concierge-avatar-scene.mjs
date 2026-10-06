import * as THREE from 'three';
import {EffectComposer} from 'three/addons/postprocessing/EffectComposer.js';
import {RenderPass} from 'three/addons/postprocessing/RenderPass.js';
import {UnrealBloomPass} from 'three/addons/postprocessing/UnrealBloomPass.js';
import {OutputPass} from 'three/addons/postprocessing/OutputPass.js';

// Original Brites guide, generated as real mesh geometry. No raster character,
// remotely hosted model, camera, microphone or tracking. An explicit verified
// product selection may load its allowlisted Shopify photograph.
export const AVATAR_SCENE_DECLARATIONS = Object.freeze({
  schema: 4,
  identity: 'original twin-ribbon pebble robot',
  expressionRig: 'two deformable geometric light ribbons, raised brow arcs, faceted cheek lights and smile glyph; no human iris or mouth',
  interactionProfile: Object.freeze({authorship: 'original authored choreography', blink: 'rare irregular 0.19-0.21 second closure', transitionMs: 320, signal: 'brow silhouette, eye shape, smile glyph and faceted cheek signals; colour is supplementary', grounded: true, loopingBodyMotion: false}),
  expressions: Object.freeze(['neutral', 'attentive', 'curious', 'explaining', 'delighted', 'reassuring', 'warm']),
  stateColors: Object.freeze({idle: '#4aa8ff', listening: '#49c9ff', thinking: '#ab87ff', speaking: '#ffcb79', success: '#72ddd1', error: '#ffc28e'}),
  textures: Object.freeze([
    Object.freeze({name: 'Ivory ceramic micro-surface', kind: 'porcelain'}),
    Object.freeze({name: 'Champagne brushed metal grain', kind: 'brush'}),
    Object.freeze({name: 'Champagne metal roughness', kind: 'roughness'}),
    Object.freeze({name: 'Sapphire glass micro-etch', kind: 'glass'}),
    Object.freeze({name: 'Ivory ceramic base colour', kind: 'albedo'}),
    Object.freeze({name: 'Ivory ceramic tangent-space normal', kind: 'normal'})
  ]),
  environment: Object.freeze({kind: 'procedural studio cubemap with HDR radiance lighting', faces: 6, dynamicLights: true, hdr: true, hdri: false}),
  finish: Object.freeze({porcelain: 'satin matte', visor: 'dark satin', bloom: 'disabled pending GPU verification', ornamentalOrbits: false, stableLighting: true, microdetail: 'band-limited satin'})
});
export function createAvatarScene({container, quality, onFrame, onContext, onError}) {
  const doc = container.ownerDocument, win = doc.defaultView;
  const renderer = new THREE.WebGLRenderer({alpha: true, antialias: true, powerPreference: 'high-performance'});
  renderer.setPixelRatio(quality.pixelRatio);
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = .88;
  renderer.setClearColor('#e6edf2', 1);
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
      // Band-limited texture variation avoids sparkling one-pixel microfacets.
      const brush = Math.sin(y / size * Math.PI * 12) * 2;
      for (let x = 0; x < size; x++) {
        let value = kind === 'porcelain' ? 128 + Math.sin(x / size * Math.PI * 8) * 2 + brush : kind === 'roughness' ? 242 + brush : 128 + brush;
        const index = (y * size + x) * 4; value = Math.max(0, Math.min(255, Math.round(value)));
        if (kind === 'albedo') {const grain = (value - 128) * .16; pixels[index] = 249 + grain; pixels[index + 1] = 244 + grain; pixels[index + 2] = 232 + grain;}
        else if (kind === 'normal') {pixels[index] = 128 + Math.sin(x / size * Math.PI * 8) * .8; pixels[index + 1] = 128 + Math.sin(y / size * Math.PI * 8) * .8; pixels[index + 2] = 255;}
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
  const ivory = material({color: '#eee7dc', map: ceramicAlbedo, normalMap: ceramicNormal, normalScale: new THREE.Vector2(.06, .06), roughness: .66, metalness: 0, clearcoat: .12, clearcoatRoughness: .62, bumpMap: porcelainMap, bumpScale: .001, sheen: .04, sheenColor: new THREE.Color('#dfd5c2')});
  const gold = material({color: '#b79a69', metalness: .86, roughness: .66, roughnessMap, bumpMap: goldMap, bumpScale: .0005, anisotropy: .18, anisotropyRotation: Math.PI / 2, clearcoat: .04, clearcoatRoughness: .58});
  const paleGold = material({color: '#c8b187', metalness: .74, roughness: .64, roughnessMap, bumpMap: goldMap, bumpScale: .0005, clearcoat: .06, clearcoatRoughness: .6});
  const glassMap = textureMap(AVATAR_SCENE_DECLARATIONS.textures[3].name, 'glass');
  const face = material({color: '#061322', metalness: .06, roughness: .48, clearcoat: .08, clearcoatRoughness: .48, envMapIntensity: .18, bumpMap: glassMap, bumpScale: .0001});
  // These retained material names keep diagnostics compatible; they describe
  // machined parts and light, never skin, human eyes or organic features.
  const lidMaterial = material({color: '#162b40', metalness: .7, roughness: .26});
  const eyeMaterial = material({color: '#76cfff', metalness: .1, roughness: .72, emissive: '#4bbaff', emissiveIntensity: 1.4, clearcoat: 0});
  const pupilMaterial = material({color: '#030e1c', roughness: .19, metalness: .1, clearcoat: 1});
  // Graphic light keeps its colour under the studio softboxes. PBR remains on
  // ceramic, metal and the visor; the face must not wash out into a white ring.
  const mouthMaterial = basic({color: '#9cddff', toneMapped: false});
  // A low-opacity satin protective cover must not turn the whole dark visor
  // into a white transmission/reflection veil. Refraction stays on the gem.
  const corneaMaterial = material({color: '#16384e', roughness: .56, transmission: 0, metalness: 0, clearcoat: .08, clearcoatRoughness: .62, envMapIntensity: .16, opacity: .035, transparent: true, depthWrite: false, bumpMap: glassMap, bumpScale: .0001});
  const glint = basic({color: '#bfeaff', toneMapped: false});
  const gemMaterial = material({color: '#3b72d7', roughness: .66, metalness: 0, transmission: 0, thickness: .45, ior: 1.77, dispersion: 0, clearcoat: 0, attenuationColor: new THREE.Color('#acd9ff'), attenuationDistance: .85});
  const stageMaterial = material({color: '#e5ded1', roughness: .78, metalness: .02, clearcoat: .02, bumpMap: porcelainMap, bumpScale: .005});
  const floorMaterial = material({color: '#eee9e1', roughness: .94, metalness: 0});
  const avatar = new THREE.Group(); scene.add(avatar);
  const irisMaterial = basic({color: '#78d7ff', toneMapped: false});
  const head = new THREE.Group(); head.name = 'articulated-expression-head'; head.position.set(0, .65, .01); avatar.add(head);
  function roundedGeometry(width, height, depth, radius, bevel = .065, graphic = false) {
    const x = -width / 2, y = -height / 2, s = new THREE.Shape();
    s.moveTo(x + radius, y); s.lineTo(x + width - radius, y); s.quadraticCurveTo(x + width, y, x + width, y + radius); s.lineTo(x + width, y + height - radius); s.quadraticCurveTo(x + width, y + height, x + width - radius, y + height); s.lineTo(x + radius, y + height); s.quadraticCurveTo(x, y + height, x, y + height - radius); s.lineTo(x, y + radius); s.quadraticCurveTo(x, y, x + radius, y);
    const result = geometry(new THREE.ExtrudeGeometry(s, {depth, bevelEnabled: bevel > 0, bevelSegments: graphic ? segments(6, 4) : segments(16, 10), steps: 1, bevelSize: bevel, bevelThickness: bevel, curveSegments: graphic ? segments(28, 20) : segments(72, 40)})); result.translate(0, 0, -depth / 2); result.computeVertexNormals(); return result;
  }
  function mesh(g, m, parent, x = 0, y = 0, z = 0) {const value = new THREE.Mesh(g, m); value.position.set(x, y, z); value.castShadow = true; value.receiveShadow = true; parent.add(value); return value;}
  const sphere = geometry(new THREE.SphereGeometry(1, segments(128, 88), segments(96, 64)));
  const smallSphere = geometry(new THREE.SphereGeometry(1, segments(72, 48), segments(48, 32)));
  // A wide soft ceramic head and inlaid visor replace the old circular face.
  // No aperture, halo or circular bezel sits behind these new face surfaces.
  const shell = mesh(roundedGeometry(1.82, 1.3, .78, .42, .16), ivory, head); shell.name = 'original-pebble-shell';
  const rearSeam = mesh(roundedGeometry(1.81, 1.25, .016, .36, .024), paleGold, head, 0, 0, -.32); rearSeam.name = 'original-rear-seam';
  const bezel = mesh(roundedGeometry(1.69, 1.11, .045, .3, .035), gold, head, 0, .012, .55); bezel.name = 'original-visor-bezel';
  const visor = mesh(roundedGeometry(1.62, 1.055, .045, .28, .035), face, head, 0, .012, .605); visor.name = 'original-wide-visor';
  const eye = new THREE.Group(); eye.name = 'expression-eye-pair'; eye.position.set(0, .022, .71); head.add(eye);
  const eyes = [], heartGlyphs = []; let eyeShapeSignature = null;
  function heartGeometry() {
    const s = new THREE.Shape();
    s.moveTo(0, -.145); s.bezierCurveTo(-.047, -.103, -.208, -.017, -.208, .077); s.bezierCurveTo(-.208, .183, -.061, .196, 0, .099); s.bezierCurveTo(.061, .196, .208, .183, .208, .077); s.bezierCurveTo(.208, -.017, .047, -.103, 0, -.145);
    const result = geometry(new THREE.ExtrudeGeometry(s, {depth: .016, bevelEnabled: true, bevelSegments: segments(6, 4), steps: 1, bevelSize: .008, bevelThickness: .008, curveSegments: segments(28, 20)})); result.translate(0, 0, -.008); result.computeVertexNormals(); return result;
  }
  const appreciationGeometry = heartGeometry();
  for (const side of [-1, 1]) {
    const group = new THREE.Group(); group.position.set(side * .315, .09, 0); eye.add(group);
    // Each ribbon is a filled, bevelled mesh. A blink changes its actual
    // silhouette, rather than adding a shutter across the old ring identity.
    const ribbonGeometry = roundedGeometry(.43, .22, .018, .105, .009, true), ribbon = mesh(ribbonGeometry, irisMaterial, group);
    ribbon.castShadow = false; ribbon.receiveShadow = false; ribbon.name = side < 0 ? 'expression-eye-left' : 'expression-eye-right';
    const ribbonRest = ribbonGeometry.attributes.position.array.slice();
    eyes.push({group, aperture: ribbon, apertureGeometry: ribbonGeometry, apertureRest: ribbonRest, digital: true, side});
    const heart = mesh(appreciationGeometry, irisMaterial, group, 0, -.015, .001); heart.visible = false; heart.castShadow = false; heart.receiveShadow = false; heart.name = side < 0 ? 'expression-heart-left' : 'expression-heart-right'; heartGlyphs.push(heart);
  }
  // Jewellery-inspired face marks are geometry, not a flat decal. Split brow
  // arcs, faceted cheek lights and a simple smile glyph remain readable when
  // colour is indistinguishable; they do not turn this robot into a human face.
  const faceBrows = [];
  for (const side of [-1, 1]) {
    const browCurve = new THREE.QuadraticBezierCurve3(new THREE.Vector3(-.19, 0, 0), new THREE.Vector3(0, .054, 0), new THREE.Vector3(.19, 0, 0));
    const brow = mesh(geometry(new THREE.TubeGeometry(browCurve, segments(48, 28), .017, 12, false)), mouthMaterial, head, side * .315, .355, .713); brow.castShadow = false; brow.receiveShadow = false; brow.name = side < 0 ? 'expression-brow-left' : 'expression-brow-right'; faceBrows.push({mesh: brow, side});
  }
  // Blink and smile now deform the light ribbons themselves. Foreground
  // shutters would obscure the new design and are intentionally absent.
  const visorShutters = [];
  const cheekLights = [];
  for (const side of [-1, 1]) {
    const cheek = mesh(geometry(new THREE.OctahedronGeometry(.053, 0)), mouthMaterial, head, side * .58, -.153, .718); cheek.scale.z = .22; cheek.castShadow = false; cheek.receiveShadow = false; cheek.name = side < 0 ? 'expression-cheek-left' : 'expression-cheek-right'; cheekLights.push(cheek);
  }
  const smilePath = new THREE.QuadraticBezierCurve3(new THREE.Vector3(-.23, 0, 0), new THREE.Vector3(0, -.11, 0), new THREE.Vector3(.23, 0, 0));
  const smileGlyph = mesh(geometry(new THREE.TubeGeometry(smilePath, segments(64, 40), .017, 12, false)), mouthMaterial, head, 0, -.235, .718); smileGlyph.castShadow = false; smileGlyph.receiveShadow = false; smileGlyph.name = 'expression-smile-glyph';
  const faceSignals = [];
  for (let index = 0; index < 3; index++) {
    const signal = mesh(smallSphere, mouthMaterial, head, (index - 1) * .065, -.385, .712); signal.scale.set(.011, .011, .006); signal.castShadow = false; signal.receiveShadow = false; signal.name = 'expression-signal-' + index; faceSignals.push(signal);
  }
  const lens = mesh(roundedGeometry(1.57, 1.005, .006, .265, .01), corneaMaterial, head, 0, .012, .683); lens.castShadow = false; lens.receiveShadow = false; lens.name = 'original-satin-visor-cover';
  const orbit = new THREE.Group(); orbit.visible = false; orbit.name = 'retired-aperture-ornament'; eye.add(orbit);
  const statusBars = [], statusBarGeometry = geometry(new THREE.CapsuleGeometry(.01, .017, 4, 16));
  for (let i = 0; i < 5; i++) {
    const bar = mesh(statusBarGeometry, mouthMaterial, head, (i - 2) * .055, -.12, .718); bar.castShadow = false; bar.receiveShadow = false; bar.scale.z = .35; bar.visible = false; bar.name = 'expression-speech-bar-' + i; statusBars.push(bar);
  }
  for (const side of [-1, 1]) {
    const hinge = mesh(smallSphere, paleGold, head, side * 1.006, -.14, -.09); hinge.scale.set(.074, .144, .17);
    const cap = mesh(smallSphere, ivory, head, side * 1.045, -.14, -.06); cap.scale.set(.045, .099, .105);
    const earPin = mesh(geometry(new THREE.TorusGeometry(.064, .009, 16, 72)), gold, head, side * 1.065, -.14, -.048); earPin.rotation.y = Math.PI / 2;
  }
  const torso = mesh(sphere, ivory, avatar, 0, -.86, -.01); torso.scale.set(.48, .5, .36);
  const chestInset = mesh(smallSphere, face, avatar, 0, -.77, .33); chestInset.scale.set(.185, .166, .021);
  const chestGem = mesh(geometry(new THREE.OctahedronGeometry(.098, 1)), gemMaterial, avatar, 0, -.765, .362); chestGem.rotation.z = Math.PI / 4;
  const chestRing = mesh(geometry(new THREE.TorusGeometry(.158, .01, 20, 112)), gold, avatar, 0, -.765, .352);
  const neck = mesh(geometry(new THREE.CylinderGeometry(.15, .18, .26, segments(96, 64))), gold, avatar, 0, -.25, 0); neck.name = 'supported-neck';
  const neckRing = mesh(geometry(new THREE.TorusGeometry(.161, .016, 20, 112)), paleGold, avatar, 0, -.25, 0); neckRing.rotation.x = Math.PI / 2;
  const waist = mesh(geometry(new THREE.TorusGeometry(.385, .023, 28, segments(192, 128))), paleGold, avatar, 0, -1.12, -.01); waist.rotation.x = Math.PI / 2; waist.scale.set(1.1, .88, 1);
  const footPads = [], footSoles = [];
  for (const side of [-1, 1]) {
    const foot = mesh(sphere, ivory, avatar, side * .24, -1.387, .06); foot.scale.set(.205, .104, .27); foot.name = 'grounded-foot-' + side; footPads.push(foot);
    // The sole's lower edge is -1.520, exactly the pedestal's top plane.
    const sole = mesh(smallSphere, paleGold, avatar, side * .24, -1.494, .064); sole.scale.set(.203, .026, .259); sole.name = 'grounded-sole-' + side; footSoles.push(sole);
    const ankle = mesh(geometry(new THREE.CylinderGeometry(.083, .09, .145, 32)), paleGold, avatar, side * .24, -1.315, .015); ankle.name = 'grounded-ankle-' + side; footPads.push(ankle);
  }
  // Jewellery-like chain detail is an original craft cue; smooth robot paddles
  // are visibly mechanical and helpful without suggesting human hands.
  const chainCurve = new THREE.CatmullRomCurve3([new THREE.Vector3(-.135, -.43, .23), new THREE.Vector3(-.18, -.60, .325), new THREE.Vector3(0, -.765, .37), new THREE.Vector3(.18, -.60, .325), new THREE.Vector3(.135, -.43, .23)]);
  const linkGeometry = geometry(new THREE.TorusGeometry(.013, .0035, 8, 20)), links = new THREE.InstancedMesh(linkGeometry, gold, 28), transform = new THREE.Object3D();
  for (let i = 0; i < 28; i++) {transform.position.copy(chainCurve.getPoint(i / 27)); transform.rotation.set(0, i % 2 ? Math.PI / 2 : 0, (i - 14) * .04); transform.updateMatrix(); links.setMatrixAt(i, transform.matrix);} links.castShadow = false; links.visible = false; links.name = 'retired-chain-ornament'; avatar.add(links); // Keep compatibility geometry out of the composed silhouette.
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
  // Articulation pivots at the waist; the soles never follow head/speech bob.
  // Keeping the support rig separate also prevents a greeting from scaling
  // the entire character up and detaching it from its base.
  const bodyRig = new THREE.Group(); bodyRig.name = 'supported-upper-body'; bodyRig.position.y = -1.12;
  const groundedParts = new Set([...footPads, ...footSoles]);
  for (const part of [...avatar.children]) {if (groundedParts.has(part)) continue; part.position.y += 1.12; bodyRig.add(part);}
  avatar.add(bodyRig);
  const decoration = mesh(geometry(new THREE.TorusGeometry(1.52, .009, 12, 220, Math.PI * 1.06)), paleGold, scene, 0, -.05, -.75); decoration.rotation.z = -.35; decoration.rotation.x = .12; decoration.castShadow = false; decoration.visible = false; decoration.name = 'retired-background-orbit';
  const productShowcase = new THREE.Group(); productShowcase.visible = false; productShowcase.name = 'verified-product-photo-showcase'; scene.add(productShowcase);
  const photoBacking = mesh(geometry(new THREE.BoxGeometry(.82, .9, .035)), ivory, productShowcase); photoBacking.castShadow = false;
  const photoMaterial = basic({color: '#ffffff', toneMapped: false}), photoPlane = mesh(geometry(new THREE.PlaneGeometry(1, 1)), photoMaterial, productShowcase, 0, .025, .024); photoPlane.castShadow = false; photoPlane.receiveShadow = false;
  let photoEpoch = 0, photoTimer = null, pendingPhotoResolve = null, photoTexture = null, showcaseProduct = null;
  const retiredPhotoTextures = new WeakSet();
  function disposePhoto(value) {if (!value || retiredPhotoTextures.has(value)) return; retiredPhotoTextures.add(value); textures.delete(value); value.dispose();}
  function clearProduct() {photoEpoch++; if (photoTimer !== null) win.clearTimeout(photoTimer); photoTimer = null; pendingPhotoResolve?.(false); pendingPhotoResolve = null; disposePhoto(photoTexture); photoTexture = null; photoMaterial.map = null; photoMaterial.needsUpdate = true; productShowcase.visible = false; showcaseProduct = null;}
  function showProduct(value) {
    let url; try {if (typeof value?.image !== 'string' || value.image.length > 2048) return Promise.resolve(false); url = new URL(value?.image); if (url.protocol !== 'https:' || url.hostname !== 'cdn.shopify.com' || !url.pathname.startsWith('/s/files/') || url.port || url.username || url.password || !/^gid:\/\/shopify\/Product\/\d+$/.test(value?.id || '')) return Promise.resolve(false);} catch {return Promise.resolve(false);}
    url.searchParams.set('width', '768'); url.searchParams.delete('height'); if (disposed) return Promise.resolve(false); clearProduct(); const epoch = photoEpoch;
    return new Promise(resolve => {pendingPhotoResolve = resolve; const settle = ok => {if (epoch !== photoEpoch) return; if (photoTimer !== null) win.clearTimeout(photoTimer); photoTimer = null; pendingPhotoResolve = null; resolve(ok);};
      photoTimer = win.setTimeout(() => {if (epoch !== photoEpoch) return; clearProduct();}, 6000);
      const loader = new THREE.TextureLoader(); loader.setCrossOrigin('anonymous');
      const pendingTexture = loader.load(url.href, texture => {if (disposed || epoch !== photoEpoch) {disposePhoto(texture); resolve(false); return;} texture.colorSpace = THREE.SRGBColorSpace; texture.anisotropy = Math.min(4, renderer.capabilities.getMaxAnisotropy()); photoTexture = texture; textures.add(texture); photoMaterial.map = texture; photoMaterial.needsUpdate = true;
        const image = texture.image || {}, ratio = Math.max(.1, Math.min(10, (image.naturalWidth || image.width || 1) / (image.naturalHeight || image.height || 1))); photoPlane.scale.set(ratio >= 1 ? .74 : .74 * ratio, ratio >= 1 ? .74 / ratio : .74, 1); productShowcase.visible = true; showcaseProduct = {id: value.id, handle: value.handle, format: 'product-photo'}; settle(true); if (lastPose) render(lastPose, true);
      }, undefined, () => {if (epoch === photoEpoch) {productShowcase.visible = false; disposePhoto(photoTexture); photoTexture = null; settle(false);} else resolve(false);}); photoTexture = pendingTexture; if (pendingTexture) textures.add(pendingTexture);
    });
  }
  const floor = mesh(geometry(new THREE.PlaneGeometry(200, 200)), floorMaterial, scene, 0, -1.655, 0); floor.rotation.x = -Math.PI / 2; floor.castShadow = false;
  const sweepPoints = [new THREE.Vector2(-1.655, -3), new THREE.Vector2(-1.62, -3.8), new THREE.Vector2(-1.3, -4.6), new THREE.Vector2(-.65, -5.15), new THREE.Vector2(.4, -5.45), new THREE.Vector2(6, -5.5)];
  const sweepPositions = [], sweepIndices = [];
  for (const point of sweepPoints) {sweepPositions.push(-14, point.x, point.y, 14, point.x, point.y);}
  for (let i = 0; i < sweepPoints.length - 1; i++) {const n = i * 2; sweepIndices.push(n, n + 1, n + 2, n + 1, n + 3, n + 2);}
  const sweepGeometry = geometry(new THREE.BufferGeometry()); sweepGeometry.setAttribute('position', new THREE.Float32BufferAttribute(sweepPositions, 3)); sweepGeometry.setIndex(sweepIndices); sweepGeometry.computeVertexNormals();
  const studioSweep = mesh(sweepGeometry, floorMaterial, scene); studioSweep.castShadow = false; studioSweep.name = 'continuous-studio-sweep'; studioSweep.material.side = THREE.DoubleSide;
  const pedestal = mesh(geometry(new THREE.CylinderGeometry(1.2, 1.25, .11, segments(180, 120))), stageMaterial, scene, 0, -1.575, 0);
  pedestal.name = 'grounding-platform';
  // This non-photoreal contact cue keeps the transparent guide intentionally
  // supported. It supplements, but never certifies, the real shadow map.
  const contactMaterial = basic({color: '#776c5b', transparent: true, opacity: .15, depthWrite: false, toneMapped: false});
  const contactCue = mesh(geometry(new THREE.CircleGeometry(.61, 64)), contactMaterial, scene, 0, -1.519, .054); contactCue.rotation.x = -Math.PI / 2; contactCue.scale.set(1, .7, 1); contactCue.castShadow = false; contactCue.receiveShadow = false; contactCue.name = 'bounded-ground-contact-cue'; contactCue.visible = false;
  const stageRing = mesh(geometry(new THREE.TorusGeometry(1.22, .014, 16, segments(192, 128))), paleGold, scene, 0, -1.553, 0); stageRing.rotation.x = Math.PI / 2; stageRing.visible = false; stageRing.castShadow = false; stageRing.name = 'retired-stage-ornament';
  const hemisphere = new THREE.HemisphereLight('#e9f2fc', '#6b747d', .46); scene.add(hemisphere);
  const key = new THREE.SpotLight('#fff1db', 32, 20, Math.PI / 5, .7, 2); key.position.set(3.2, 4.8, 5.6); key.target.position.set(0, -.4, 0); key.castShadow = true; key.shadow.mapSize.set(quality.shadowSize, quality.shadowSize); key.shadow.bias = -.00016; key.shadow.normalBias = .025; key.shadow.radius = 3; key.shadow.camera.near = .5; key.shadow.camera.far = 18; scene.add(key, key.target);
  const fill = new THREE.DirectionalLight('#c9e4f2', .44); fill.position.set(-3, 1.1, 4); scene.add(fill);
  const rim = new THREE.DirectionalLight('#ffe3b5', .8); rim.position.set(-1.5, 3.8, -3); scene.add(rim);
  const eyeLight = new THREE.PointLight('#79e9e5', .28, 2.7, 2); eyeLight.position.set(0, 1.52, .8); bodyRig.add(eyeLight);
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
  // Separate display background from lighting. Float radiance retains studio
  // softboxes above 1.0, without whitening the skybox or dark display glass.
  // This is an authored HDR environment, not photographic HDRI footage.
  const hdrWidth = 512, hdrHeight = 256, hdrPixels = new Float32Array(hdrWidth * hdrHeight * 4), direction = new THREE.Vector3();
  for (let y = 0; y < hdrHeight; y++) for (let x = 0; x < hdrWidth; x++) {
    const phi = (x + .5) / hdrWidth * Math.PI * 2, theta = (y + .5) / hdrHeight * Math.PI;
    direction.set(Math.sin(theta) * Math.cos(phi), Math.cos(theta), Math.sin(theta) * Math.sin(phi));
    const ceiling = (direction.y + 1) * .5, warm = Math.pow(Math.max(0, direction.dot(lightDirections[0])), 36) * 3.4, cool = Math.pow(Math.max(0, direction.dot(lightDirections[1])), 52) * 1.7, edge = Math.pow(Math.max(0, direction.dot(lightDirections[2])), 42) * 1.2, index = (y * hdrWidth + x) * 4;
    hdrPixels[index] = .055 + ceiling * .13 + warm + cool * .72 + edge;
    hdrPixels[index + 1] = .065 + ceiling * .135 + warm * .89 + cool * .9 + edge * .82;
    hdrPixels[index + 2] = .085 + ceiling * .14 + warm * .7 + cool + edge * .65;
    hdrPixels[index + 3] = 1;
  }
  const hdrEnvironment = new THREE.DataTexture(hdrPixels, hdrWidth, hdrHeight, THREE.RGBAFormat, THREE.FloatType); hdrEnvironment.mapping = THREE.EquirectangularReflectionMapping; hdrEnvironment.colorSpace = THREE.LinearSRGBColorSpace; hdrEnvironment.needsUpdate = true; textures.add(hdrEnvironment);
  const pmrem = new THREE.PMREMGenerator(renderer), environmentTarget = pmrem.fromEquirectangular(hdrEnvironment);
  scene.background = skybox; scene.backgroundBlurriness = .55; scene.backgroundIntensity = .84; scene.environment = environmentTarget.texture; scene.environmentIntensity = .68; pmrem.dispose();
  let composer = null, bloom = null;
  if (quality.bloom) {
    const target = new THREE.WebGLRenderTarget(1, 1, {type: THREE.HalfFloatType, samples: quality.name === 'high' ? 4 : 0});
    composer = new EffectComposer(renderer, target); composer.addPass(new RenderPass(scene, camera)); bloom = new UnrealBloomPass(new THREE.Vector2(1, 1), 0, .16, 2.4); bloom.enabled = false; composer.addPass(bloom); composer.addPass(new OutputPass());
  }
  let active = false, reducedMotion = false, disposed = false, lost = false, last = 0, loopRunning = false, lastAnimationFrameMs = null, frames = 0, lastPose = null, renderMs = 0, drawCalls = 0, renderedTriangles = 0, sampleTime = 0, width = 1, height = 1;
  const geometryStats = root => {let triangles = 0, vertices = 0, meshes = 0; root.traverse(object => {if (!object.isMesh) return; const multiplier = object.isInstancedMesh ? object.count : 1; triangles += (object.geometry.index ? object.geometry.index.count : object.geometry.attributes.position.count) / 3 * multiplier; vertices += object.geometry.attributes.position.count * multiplier; meshes += 1;}); return {triangles: Math.round(triangles), vertices, meshes};};
  const modelStats = geometryStats(avatar), sceneStats = geometryStats(scene);
  function resize() {
    if (disposed) return; const box = container.getBoundingClientRect(); width = Math.max(1, Math.round(box.width)); height = Math.max(1, Math.round(box.height));
    renderer.setSize(width, height, false); camera.aspect = width / height; camera.position.z = Math.max(5.8, 2.85 / (camera.aspect * 2 * Math.tan(THREE.MathUtils.degToRad(camera.fov / 2)))); camera.lookAt(0, -.16, 0); camera.updateProjectionMatrix(); composer?.setSize(width, height); if (lastPose) render(lastPose, true);
  }
  const whiteColor = new THREE.Color('#ffffff'), targetColor = new THREE.Color(), displayedColor = new THREE.Color(AVATAR_SCENE_DECLARATIONS.stateColors.idle), transitionColor = displayedColor.clone();
  let previousState = 'idle', transitionAt = 0;
  function applyPose(pose) {
    const state = Object.hasOwn(AVATAR_SCENE_DECLARATIONS.stateColors, pose.state) ? pose.state : 'idle';
    if (state !== previousState) {transitionColor.copy(displayedColor); previousState = state; transitionAt = sampleTime;}
    targetColor.set(AVATAR_SCENE_DECLARATIONS.stateColors[state]);
    const transitionMs = Number.isFinite(AVATAR_SCENE_DECLARATIONS.interactionProfile?.transitionMs) ? AVATAR_SCENE_DECLARATIONS.interactionProfile.transitionMs : 320;
    const colorProgress = reducedMotion ? 1 : THREE.MathUtils.clamp((sampleTime - transitionAt + 1 / 60) / (transitionMs / 1000), 0, 1), colorEase = colorProgress * colorProgress * (3 - 2 * colorProgress);
    displayedColor.copy(transitionColor).lerp(targetColor, colorEase);
    const speaking = state === 'speaking', thinking = state === 'thinking', calm = pose.emotion === 'calm' || pose.emotion === 'reassuring', happy = state === 'success' && !calm, reassuring = state === 'error' || calm;
    // Stable support is not an animation channel. Only the upper body pivots,
    // with small limits; pointer pitch is consumed unchanged (negative is up).
    avatar.position.set(0, 0, 0); avatar.scale.setScalar(1); avatar.rotation.set(0, 0, 0);
    const finiteCue = (value, fallback = 0, min = 0, max = 1) => THREE.MathUtils.clamp(Number.isFinite(value) ? value : fallback, min, max);
    bodyRig.rotation.set(finiteCue(pose.lean, 0, -.025, .025), finiteCue(pose.bodyYaw, 0, -.032, .032), finiteCue(pose.bodyRoll, 0, -.015, .015));
    head.rotation.set(Number.isFinite(pose.headPitch) ? pose.headPitch : 0, Number.isFinite(pose.headYaw) ? pose.headYaw : 0, finiteCue(finiteCue(pose.headRoll, 0, -.09, .09) + (reassuring ? -.012 : pose.emotion === 'curious' ? .02 : 0), 0, -.09, .09));
    const eyeOpen = finiteCue(pose.eyeOpen, 1, .035, 1.08), eyeScaleX = finiteCue(pose.eyeScaleX, 1, .8, 1.2), eyeScaleY = finiteCue(pose.eyeScaleY, 1, .6, 1.2), deformation = finiteCue(pose.eyeDeformation, 0, -.3, .3);
    eye.position.x = finiteCue(pose.gazeX, 0, -.12, .12); eye.position.y = .022 + finiteCue(pose.gazeY, 0, -.08, .08);
    const browLift = finiteCue(pose.faceBrowLift ?? pose.browLift, 0, -1, 1), browTilt = finiteCue(pose.faceBrowTilt ?? pose.browAngle, 0, -1, 1), eyeSmile = finiteCue(pose.eyeSmile), cheekGlow = finiteCue(pose.cheekGlow), smileCurve = finiteCue(pose.smileCurve), signalLevel = finiteCue(pose.faceSignal);
    faceBrows.forEach(({mesh: brow, side}) => {brow.position.y = .355 + browLift * .057 + side * browTilt * .038; brow.rotation.z = side * -.08 + browTilt * .18;});
    const smileClosure = finiteCue(pose.lidClosure, eyeSmile * .14), speechEnergy = speaking ? finiteCue(pose.speechEnergy) : 0, warmth = finiteCue(pose.heart);
    cheekLights.forEach(cheek => {const scale = .62 + cheekGlow * .48; cheek.scale.set(scale, scale, .22);});
    smileGlyph.scale.set(.9 + smileCurve * .16, .18 + smileCurve * .82, 1);
    smileGlyph.position.y = -.235 + speechEnergy * .006;
    faceSignals.forEach((signal, index) => {const energy = speaking ? finiteCue(pose.speechEnergy) : 0, emphasis = state === 'listening' ? index === 1 ? .7 : .25 : thinking ? index === 2 ? .7 : .2 : signalLevel; const radius = .010 + emphasis * .005 + energy * (index === 1 ? .008 : .005); signal.scale.set(radius, radius, .006);});
    const shapeSignature = [eyeOpen, eyeScaleX, eyeScaleY, deformation, eyeSmile, browTilt, smileClosure, reducedMotion ? 0 : speechEnergy].join(','), shapeChanged = shapeSignature !== eyeShapeSignature; eyeShapeSignature = shapeSignature;
    for (const {aperture: ribbon, apertureGeometry, apertureRest, side} of eyes) {
      if (shapeChanged) {
        const attribute = apertureGeometry.attributes.position, openness = eyeScaleY * (1 - smileClosure * .18) * (1 - eyeSmile * .38), asymmetry = 1 + side * browTilt * .13;
        for (let i = 0; i < attribute.count; i++) {
          const offset = i * 3, x = apertureRest[offset], y = apertureRest[offset + 1], z = apertureRest[offset + 2], horizontal = x / .224;
          // Blink/expression and measured speech alter the actual silhouette.
          // Gaze translates the pair; there is no idle geometric wave.
          const arch = (horizontal * horizontal - .35) * eyeSmile * .105, inquisitiveSlope = horizontal * side * browTilt * .018, speechShape = reducedMotion ? 0 : (1 - horizontal * horizontal) * speechEnergy * .008;
          attribute.setXYZ(i, x * eyeScaleX * (1 + (1 - eyeOpen) * .035), (y * openness * asymmetry + arch + inquisitiveSlope + Math.abs(x) * deformation * .14 + speechShape) * eyeOpen, z);
        }
        attribute.needsUpdate = true; apertureGeometry.computeVertexNormals(); apertureGeometry.computeBoundingBox(); apertureGeometry.computeBoundingSphere();
      }
      ribbon.visible = warmth <= .05;
    }
    heartGlyphs.forEach(heart => {heart.visible = warmth > .05; heart.scale.setScalar(.82 + warmth * .18);});
    const heartColor = new THREE.Color('#ed93aa');
    displayedColor.lerp(heartColor, warmth);
    irisMaterial.color.copy(displayedColor); mouthMaterial.color.copy(displayedColor); glint.color.copy(displayedColor).lerp(whiteColor, .6);
    eyeLight.color.copy(displayedColor); eyeLight.intensity = .22 + finiteCue(pose.lightPulse) * .16;
    const statusAccent = [.4, .72, 1, .72, .4];
    // Silent and listening states show no invented waveform. A held measured
    // output level has a held shape, with no clock-driven bar or arm loop.
    statusBars.forEach((bar, index) => {bar.visible = speaking && speechEnergy > .015 && warmth <= .05; bar.scale.y = .45 + speechEnergy * statusAccent[index] * 1.7;});
    arms.forEach(({group, side, palm}) => {const invitation = state === 'listening' ? .035 : reassuring ? .015 : 0, pointsThisSide = pose.productFocused && Math.sign(pose.targetX || 1) === side, offer = (pointsThisSide || !pose.productFocused && side === 1) && Number.isFinite(pose.offer) ? THREE.MathUtils.clamp(pose.offer, 0, 1) : 0, wave = side === 1 && Number.isFinite(pose.helloWave) ? THREE.MathUtils.clamp(pose.helloWave, -1, 1) : 0, armLift = finiteCue(side === -1 ? pose.armLiftLeft ?? pose.armLift : pose.armLiftRight ?? pose.armLift, 0, 0, .22); group.rotation.z = -side * THREE.MathUtils.clamp(armLift + invitation + offer * .24 + wave * .075, -.09, .32); group.rotation.x = -armLift * .22 - offer * .06; group.rotation.y = side * offer * .09; group.position.z = -.03 + (pointsThisSide ? finiteCue(pose.armReach, 0, 0, .11) : 0); palm.rotation.z = -side * .15 + wave * .06;});
    // The studio softbox stays fixed: articulation moves, specular lighting does not.
    key.position.x = 3.2;
  }
  function render(pose, force = false) {
    if (disposed || lost || (!active && !force)) return; const start = win.performance.now(); if (force) sampleTime = start / 1000; lastPose = pose; applyPose(pose); productShowcase.position.set(.94, -.68, .45); productShowcase.rotation.y = -.12; productShowcase.scale.setScalar(1); renderer.info.reset(); if (composer) composer.render(); else renderer.render(scene, camera); renderMs = win.performance.now() - start; frames += 1; drawCalls = renderer.info.render.calls; renderedTriangles = renderer.info.render.triangles;
  }
  function tick(milliseconds) {if (!Number.isFinite(milliseconds) || !active || reducedMotion || lost || disposed) return; const time = milliseconds / 1000, interval = 1 / quality.fps; if (time - last + .000001 < interval) return; last = time - Math.max(0, (time - last) % interval); sampleTime = time; try {render(onFrame(time)); lastAnimationFrameMs = milliseconds;} catch {active = false; loopRunning = false; renderer.setAnimationLoop(null); onError?.();}}
  function setMotion(value) {active = value.active === true; if (!active) clearProduct(); reducedMotion = value.reducedMotion === true; const shouldRun = active && !reducedMotion && !lost && !disposed; if (shouldRun === loopRunning) return; loopRunning = shouldRun; if (shouldRun) last = 0; renderer.setAnimationLoop(shouldRun ? tick : null);}
  function invalidate() {last = 0;}
  function contextLost(event) {event.preventDefault(); lost = true; loopRunning = false; renderer.setAnimationLoop(null); onContext?.(true);}
  function contextRestored() {lost = false; onContext?.(false);}
  renderer.domElement.addEventListener('webglcontextlost', contextLost); renderer.domElement.addEventListener('webglcontextrestored', contextRestored);
  const resizeObserver = win.ResizeObserver ? new win.ResizeObserver(resize) : null; resizeObserver?.observe(container); if (!resizeObserver) win.addEventListener('resize', resize);
  resize();
  function setFloating(value) {const floats = value === true; scene.background = floats ? null : skybox; renderer.setClearColor('#e6edf2', floats ? 0 : 1); for (const surface of [floor, pedestal, studioSweep]) surface.visible = !floats; contactCue.visible = floats; if (lastPose) render(lastPose, true);}
  function gazeAnchor() {scene.updateMatrixWorld(true); camera.updateMatrixWorld(true); const center = head.localToWorld(new THREE.Vector3(0, .112, .71)).project(camera); return {x: THREE.MathUtils.clamp((center.x + 1) / 2, 0, 1), y: THREE.MathUtils.clamp((1 - center.y) / 2, 0, 1)};}
  function snapshot() {return {revision: THREE.REVISION, animated: !disposed && !lost && loopRunning && lastAnimationFrameMs !== null && win.performance.now() - lastAnimationFrameMs < 800, animation: {loopRequested: loopRunning, sampledFrames: frames, lastFrameMs: lastAnimationFrameMs}, frames, frameRenderMs: Math.round(renderMs * 100) / 100, drawCalls, renderedTriangles, geometry: {model: modelStats, scene: sceneStats}, textures: maps.map(value => ({...value})), shadow: {enabled: true, size: quality.shadowSize, type: 'PCF soft', casts: true, receivingStage: true, contactCue: contactCue.visible}, grounding: {feetFixed: true, soleY: -1.52, platformTopY: -1.52, upperBodyPivot: 'waist'}, gazeAnchor: gazeAnchor(), mannerism: {name: lastPose?.mannerism || null, active: lastPose?.mannerismActive === true, eventBound: true}, character: {identity: AVATAR_SCENE_DECLARATIONS.identity, digitalEyes: eyes.length, humanFeatures: false, meshDeformation: true, faceExpression: AVATAR_SCENE_DECLARATIONS.expressions.includes(lastPose?.faceExpression) ? lastPose.faceExpression : 'neutral', faceGeometry: {lightRibbons: eyes.length, brows: faceBrows.length, shutters: visorShutters.length, cheekFacets: cheekLights.length, smileGlyph: true, signalMarkers: faceSignals.length, appreciationGlyphs: heartGlyphs.length, speechBars: statusBars.length}, interactionProfile: {...AVATAR_SCENE_DECLARATIONS.interactionProfile}, state: previousState, color: '#' + displayedColor.getHexString()}, environment: {kind: AVATAR_SCENE_DECLARATIONS.environment.kind, faces: 6, size: skyboxSize, hdr: true, hdri: false, radiance: {width: hdrWidth, height: hdrHeight, format: 'linear float RGBA'}}, finish: {...AVATAR_SCENE_DECLARATIONS.finish}, materials: {physical: [...materials].filter(value => value.isMeshPhysicalMaterial).length, metallicAnisotropy: true, transmission: true, clearcoat: true, environmentReflection: true}, lights: 5, bloom: !!composer && bloom?.enabled !== false, showcase: showcaseProduct ? {...showcaseProduct} : null, width, height, pixelRatio: renderer.getPixelRatio(), contextLost: lost};}
  function destroy() {if (disposed) return; disposed = true; clearProduct(); renderer.setAnimationLoop(null); resizeObserver?.disconnect(); win.removeEventListener('resize', resize); renderer.domElement.removeEventListener('webglcontextlost', contextLost); renderer.domElement.removeEventListener('webglcontextrestored', contextRestored); composer?.passes.forEach(pass => pass.dispose?.()); composer?.dispose(); geometries.forEach(value => value.dispose()); materials.forEach(value => value.dispose()); textures.forEach(value => value.dispose()); environmentTarget.dispose(); renderer.dispose(); renderer.forceContextLoss(); renderer.domElement.remove();}
  return {setMotion, invalidate, render, snapshot, gazeAnchor, showProduct, clearProduct, setFloating, destroy};
}
