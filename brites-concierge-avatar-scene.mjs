import * as THREE from 'three';
import {EffectComposer} from 'three/addons/postprocessing/EffectComposer.js';
import {RenderPass} from 'three/addons/postprocessing/RenderPass.js';
import {UnrealBloomPass} from 'three/addons/postprocessing/UnrealBloomPass.js';
import {OutputPass} from 'three/addons/postprocessing/OutputPass.js';

// Original Brites guide, generated as real mesh geometry. No raster character,
// remotely hosted model, camera, microphone or tracking. An explicit verified
// product selection may load its allowlisted Shopify photograph.
export const AVATAR_SCENE_DECLARATIONS = Object.freeze({
  schema: 5,
  identity: 'original pearlfin porcelain guide',
  expressionRig: 'two open deformable light ribbons, raised brow arcs, faceted cheek lights, continuously closed expressive mouth curve and measured sound emission ripples; no human iris, opening aperture or phoneme claim',
  interactionProfile: Object.freeze({authorship: 'original authored choreography', blink: 'rare irregular 0.19-0.21 second closure', transitionMs: 320, signal: 'brow silhouette, eye shape, smile glyph and faceted cheek signals; colour is supplementary', grounded: true, loopingBodyMotion: false, gaze: 'one continuous acceleration and velocity bounded cursor and product trajectory'}),
  expressions: Object.freeze(['neutral', 'attentive', 'curious', 'explaining', 'delighted', 'reassuring', 'warm']),
  stateColors: Object.freeze({idle: '#4aa8ff', listening: '#49c9ff', thinking: '#ab87ff', speaking: '#70d8f1', success: '#72ddd1', error: '#ffc28e'}),
  textures: Object.freeze([
    Object.freeze({name: 'Ivory ceramic micro-surface', kind: 'porcelain'}),
    Object.freeze({name: 'Champagne brushed metal grain', kind: 'brush'}),
    Object.freeze({name: 'Champagne metal roughness', kind: 'roughness'}),
    Object.freeze({name: 'Sapphire glass micro-etch', kind: 'glass'}),
    Object.freeze({name: 'Ivory ceramic base colour', kind: 'albedo'}),
    Object.freeze({name: 'Ivory ceramic tangent-space normal', kind: 'normal'})
  ]),
  environment: Object.freeze({kind: 'procedural HDR studio radiance', dynamicLights: true, hdr: true, hdri: false}),
  finish: Object.freeze({silhouette: 'continuous pear-shaped porcelain body with articulated side fins and oval helmet', studio: 'bright neutral backdrop with separate real shadow receiver', porcelain: 'satin matte', visor: 'dark satin', bloom: 'disabled pending GPU verification', ornamentalOrbits: false, stableLighting: true, microdetail: 'band-limited satin'})
});
export function createAvatarScene({container, quality, onFrame, onContext, onError}) {
  const doc = container.ownerDocument, win = doc.defaultView;
  const renderer = new THREE.WebGLRenderer({alpha: true, antialias: true, powerPreference: 'high-performance'});
  renderer.setPixelRatio(quality.pixelRatio);
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.12;
  renderer.setClearColor('#e6edf2', 1);
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  renderer.info.autoReset = false;
  renderer.domElement.setAttribute('aria-hidden', 'true');
  container.appendChild(renderer.domElement);
  const scene = new THREE.Scene();
  scene.background = new THREE.Color('#bccdd6');
  const camera = new THREE.PerspectiveCamera(34, 1, .1, 35);
  camera.position.set(.26, .65, 6.2); camera.lookAt(0, .03, 0);
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
  const ivory = material({color: '#ffffff', map: ceramicAlbedo, normalMap: ceramicNormal, normalScale: new THREE.Vector2(.06, .06), roughness: .66, metalness: 0, clearcoat: .12, clearcoatRoughness: .62, bumpMap: porcelainMap, bumpScale: .001, sheen: .04, sheenColor: new THREE.Color('#dfd5c2')});
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
  const stageMaterial = material({color: '#bdcbd1', roughness: .78, metalness: .02, clearcoat: .02, bumpMap: porcelainMap, bumpScale: .005});
  const floorMaterial = new THREE.ShadowMaterial({color: '#52636b', opacity: .15, depthWrite: false}); materials.add(floorMaterial);
  const avatar = new THREE.Group(); scene.add(avatar);
  const irisMaterial = basic({color: '#78d7ff', toneMapped: false});
  const head = new THREE.Group(); head.name = 'articulated-expression-head'; head.position.set(0, .87, .01); avatar.add(head);
  function roundedGeometry(width, height, depth, radius, bevel = .065, graphic = false) {
    const x = -width / 2, y = -height / 2, s = new THREE.Shape();
    s.moveTo(x + radius, y); s.lineTo(x + width - radius, y); s.quadraticCurveTo(x + width, y, x + width, y + radius); s.lineTo(x + width, y + height - radius); s.quadraticCurveTo(x + width, y + height, x + width - radius, y + height); s.lineTo(x + radius, y + height); s.quadraticCurveTo(x, y + height, x, y + height - radius); s.lineTo(x, y + radius); s.quadraticCurveTo(x, y, x + radius, y);
    const result = geometry(new THREE.ExtrudeGeometry(s, {depth, bevelEnabled: bevel > 0, bevelSegments: graphic ? segments(6, 4) : segments(16, 10), steps: 1, bevelSize: bevel, bevelThickness: bevel, curveSegments: graphic ? segments(28, 20) : segments(72, 40)})); result.translate(0, 0, -depth / 2); result.computeVertexNormals(); return result;
  }
  function mesh(g, m, parent, x = 0, y = 0, z = 0) {const value = new THREE.Mesh(g, m); value.position.set(x, y, z); value.castShadow = true; value.receiveShadow = true; parent.add(value); return value;}
  const sphere = geometry(new THREE.SphereGeometry(1, segments(128, 88), segments(96, 64)));
  const smallSphere = geometry(new THREE.SphereGeometry(1, segments(72, 48), segments(48, 32)));
  // An oval porcelain helmet flows into the continuous body. The face is an
  // inset graphic surface, not a square monitor attached to the old robot.
  const shell = mesh(sphere, ivory, head); shell.scale.set(.96, .73, .59); shell.name = 'original-pebble-shell';
  const rearSeam = mesh(geometry(new THREE.TorusGeometry(.63, .006, 12, segments(192, 128))), paleGold, head, 0, -.02, -.405); rearSeam.scale.x = 1.24; rearSeam.name = 'original-rear-seam';
  const bezel = mesh(roundedGeometry(1.65, 1.13, .024, .49, .018), paleGold, head, 0, .012, .604); bezel.name = 'original-visor-bezel';
  const visor = mesh(roundedGeometry(1.61, 1.09, .025, .475, .018), face, head, 0, .012, .637); visor.name = 'original-wide-visor';
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
    const ribbonGeometry = roundedGeometry(.43, .29, .018, .13, .009, true), ribbon = mesh(ribbonGeometry, irisMaterial, group);
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
    const brow = mesh(geometry(new THREE.TubeGeometry(browCurve, segments(48, 28), .019, 12, false)), mouthMaterial, head, side * .315, .355, .713); brow.castShadow = false; brow.receiveShadow = false; brow.name = side < 0 ? 'expression-brow-left' : 'expression-brow-right'; faceBrows.push({mesh: brow, side, rest: brow.geometry.attributes.position.array.slice()});
  }
  // Blink and smile now deform the light ribbons themselves. Foreground
  // shutters would obscure the new design and are intentionally absent.
  const visorShutters = [];
  const cheekLights = [];
  for (const side of [-1, 1]) {
    const cheek = mesh(geometry(new THREE.OctahedronGeometry(.053, 0)), mouthMaterial, head, side * .58, -.153, .718); cheek.scale.z = .22; cheek.castShadow = false; cheek.receiveShadow = false; cheek.name = side < 0 ? 'expression-cheek-left' : 'expression-cheek-right'; cheekLights.push(cheek);
  }
  const closedCurve = (halfWidth, radius, steps = 48) => geometry(new THREE.TubeGeometry(new THREE.QuadraticBezierCurve3(new THREE.Vector3(-halfWidth,0,0), new THREE.Vector3(0,0,0), new THREE.Vector3(halfWidth,0,0)), segments(steps,32), radius, 8, false));
  const smileGlyph = mesh(closedCurve(.23,.013,64), mouthMaterial, head, 0, -.235, .718); smileGlyph.castShadow = false; smileGlyph.receiveShadow = false; smileGlyph.name = 'expression-smile-glyph';
  // Sound changes emission and adjacent restrained contours. The expressive
  // mouth remains a single closed-lip curve; energy never opens an area.
  const voiceMaterial = basic({color:'#b0f3ff',toneMapped:false,transparent:true,opacity:0,depthWrite:false});
  const speechMouth = mesh(closedCurve(.23,.019,64), voiceMaterial, head, 0, -.235, .722); speechMouth.castShadow = false; speechMouth.receiveShadow = false; speechMouth.visible = false; speechMouth.name = 'expression-speech-mouth';
  const mouthCurves = [smileGlyph,speechMouth].map(value => ({mesh:value,rest:value.geometry.attributes.position.array.slice()}));
  const speechRipples = [];
  for (let index=0; index<2; index++) {const material=basic({color:'#86e8fa',toneMapped:false,transparent:true,opacity:0,depthWrite:false}), ripple=mesh(closedCurve(.25+index*.02,.006),material,head,0,-.27-index*.025,.721); ripple.castShadow=false;ripple.receiveShadow=false;ripple.visible=false;ripple.name='expression-speech-ripple-'+index;speechRipples.push({mesh:ripple,rest:ripple.geometry.attributes.position.array.slice()});}
  let speechMouthShape = null;
  const faceSignals = [];
  for (let index = 0; index < 3; index++) {
    const signal = mesh(smallSphere, mouthMaterial, head, (index - 1) * .065, -.385, .712); signal.scale.set(.011, .011, .006); signal.castShadow = false; signal.receiveShadow = false; signal.visible = false; signal.name = 'expression-signal-' + index; faceSignals.push(signal);
  }
  const lens = mesh(roundedGeometry(1.56, 1.04, .006, .455, .006), corneaMaterial, head, 0, .012, .683); lens.castShadow = false; lens.receiveShadow = false; lens.name = 'original-satin-visor-cover';
  const orbit = new THREE.Group(); orbit.visible = false; orbit.name = 'retired-aperture-ornament'; eye.add(orbit);
  const statusBars = [];
  for (let i = 0; i < 6; i++) {
    const material=basic({color:'#8de6f7',toneMapped:false,transparent:true,opacity:0,depthWrite:false}), bar=mesh(closedCurve(.034,.0085,40),material,head,(i-2.5)*.075,-.235,.724); bar.castShadow=false;bar.receiveShadow=false;bar.visible=false;bar.name='expression-speech-bar-'+i;bar.userData.spectralBand=i;statusBars.push({mesh:bar,rest:bar.geometry.attributes.position.array.slice()});
  }
  // The sculpted body replaces the sphere, brass neck, ball joints, mittens
  // and separate boots. Its flat contact edge stays fixed under every gesture.
  const bodyProfile = [[0,-1.52],[.20,-1.52],[.31,-1.46],[.45,-1.25],[.56,-.88],[.60,-.48],[.57,-.13],[.44,.13],[.25,.29],[0,.30]].map(([radius,y]) => new THREE.Vector3(radius,y,0));
  const bodyCurve = new THREE.CatmullRomCurve3(bodyProfile, false, 'centripetal'), bodyPoints = bodyCurve.getPoints(segments(160, 112)).map(point => new THREE.Vector2(Math.max(0,point.x),Math.max(-1.52,point.y)));
  const torso = mesh(geometry(new THREE.LatheGeometry(bodyPoints, segments(160, 112))), ivory, avatar); torso.scale.z = .74; torso.name = 'sculpted-porcelain-torso';
  const neck = mesh(geometry(new THREE.CylinderGeometry(.21, .26, .23, segments(96, 64))), ivory, avatar, 0, .225, -.025); neck.name = 'supported-neck';
  // A tiny inlaid cut-stone mark is the only chest ornament; no dark circular
  // porthole or waist ring remains in the composed silhouette.
  const chestGem = mesh(geometry(new THREE.OctahedronGeometry(.081, 2)), gemMaterial, avatar, 0, -.47, .454); chestGem.scale.z = .34; chestGem.rotation.z = Math.PI / 4; chestGem.name = 'inlaid-jewel-signature';
  const signature = mesh(geometry(new THREE.TorusGeometry(.093, .004, 12, 96)), paleGold, avatar, 0, -.47, .454); signature.rotation.z = Math.PI / 4; signature.scale.set(.72,1,1); signature.name = 'fine-jewel-inlay';
  const arms = [];
  for (const side of [-1, 1]) {
    const arm = new THREE.Group(); arm.position.set(side * .54, -.08, -.005); arm.name = side < 0 ? 'articulated-fin-left' : 'articulated-fin-right'; avatar.add(arm);
    const finShape = new THREE.Shape();
    finShape.moveTo(0,.10); finShape.bezierCurveTo(side*.17,.12,side*.35,-.24,side*.29,-.69); finShape.bezierCurveTo(side*.26,-.94,side*.10,-1.02,side*.02,-.78); finShape.bezierCurveTo(-side*.03,-.47,-side*.06,-.12,0,.10);
    const finGeometry = geometry(new THREE.ExtrudeGeometry(finShape,{depth:.055,bevelEnabled:true,bevelSegments:segments(16,10),steps:1,bevelSize:.045,bevelThickness:.06,curveSegments:segments(96,64)})); finGeometry.translate(0,0,-.028); finGeometry.computeVertexNormals();
    const palm = mesh(finGeometry, ivory, arm); palm.name = 'porcelain-side-fin-' + side;
    const seamCurve = new THREE.CubicBezierCurve3(new THREE.Vector3(side*.12,-.18,.08),new THREE.Vector3(side*.22,-.4,.08),new THREE.Vector3(side*.24,-.58,.08),new THREE.Vector3(side*.19,-.74,.08));
    const seam = mesh(geometry(new THREE.TubeGeometry(seamCurve,segments(64,40),.004,8,false)),paleGold,arm); seam.name = 'fine-fin-inlay-' + side;
    arms.push({group: arm, side, palm});
  }
  // Only the head and fins articulate. The entire continuous ceramic body
  // stays planted; conversation never tilts/scales the support into midair.
  const bodyRig = new THREE.Group(); bodyRig.name = 'supported-upper-body';
  bodyRig.add(head); for (const arm of arms) bodyRig.add(arm.group); avatar.add(bodyRig);
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
  const floor = mesh(geometry(new THREE.PlaneGeometry(200, 200)), floorMaterial, scene, 0, -1.581, 0); floor.rotation.x = -Math.PI / 2; floor.castShadow = false; floor.receiveShadow = true; floor.name = 'ground-shadow-receiver';
  const sweepPoints = [new THREE.Vector2(-1.655, -3), new THREE.Vector2(-1.62, -3.8), new THREE.Vector2(-1.3, -4.6), new THREE.Vector2(-.65, -5.15), new THREE.Vector2(.4, -5.45), new THREE.Vector2(6, -5.5)];
  const sweepPositions = [], sweepIndices = [];
  for (const point of sweepPoints) {sweepPositions.push(-14, point.x, point.y, 14, point.x, point.y);}
  for (let i = 0; i < sweepPoints.length - 1; i++) {const n = i * 2; sweepIndices.push(n, n + 1, n + 2, n + 1, n + 3, n + 2);}
  const sweepGeometry = geometry(new THREE.BufferGeometry()); sweepGeometry.setAttribute('position', new THREE.Float32BufferAttribute(sweepPositions, 3)); sweepGeometry.setIndex(sweepIndices); sweepGeometry.computeVertexNormals();
  const studioSweep = mesh(sweepGeometry, basic({color: '#faf8f2', toneMapped: false}), scene); studioSweep.castShadow = false; studioSweep.receiveShadow = false; studioSweep.visible = false; studioSweep.name = 'continuous-studio-sweep';
  const pedestal = mesh(geometry(new THREE.CylinderGeometry(.70, .74, .06, segments(180, 120))), stageMaterial, scene, 0, -1.55, 0);
  pedestal.name = 'grounding-platform';
  // This non-photoreal contact cue keeps the transparent guide intentionally
  // supported. It supplements, but never certifies, the real shadow map.
  const contactMaterial = basic({color: '#776c5b', transparent: true, opacity: .15, depthWrite: false, toneMapped: false});
  const contactCue = mesh(geometry(new THREE.CircleGeometry(.61, 64)), contactMaterial, scene, 0, -1.519, .054); contactCue.rotation.x = -Math.PI / 2; contactCue.scale.set(1, .7, 1); contactCue.castShadow = false; contactCue.receiveShadow = false; contactCue.name = 'bounded-ground-contact-cue'; contactCue.visible = false;
  const stageRing = mesh(geometry(new THREE.TorusGeometry(1.22, .014, 16, segments(192, 128))), paleGold, scene, 0, -1.553, 0); stageRing.rotation.x = Math.PI / 2; stageRing.visible = false; stageRing.castShadow = false; stageRing.name = 'retired-stage-ornament';
  const hemisphere = new THREE.HemisphereLight('#ffffff', '#eee7dd', 1.15); scene.add(hemisphere);
  const key = new THREE.SpotLight('#fff8eb', 100, 20, Math.PI / 5, .7, 2); key.position.set(3.2, 4.8, 5.6); key.target.position.set(0, -.4, 0); key.castShadow = true; key.shadow.mapSize.set(quality.shadowSize, quality.shadowSize); key.shadow.bias = -.00016; key.shadow.normalBias = .025; key.shadow.radius = 3; key.shadow.camera.near = .5; key.shadow.camera.far = 18; scene.add(key, key.target);
  const fill = new THREE.DirectionalLight('#e9f5ff', 1.15); fill.position.set(-3, 1.1, 4); scene.add(fill);
  const rim = new THREE.DirectionalLight('#fff0d7', 1.25); rim.position.set(-1.5, 3.8, -3); scene.add(rim);
  const eyeLight = new THREE.PointLight('#79e9e5', .28, 2.7, 2); eyeLight.position.set(0, .92, .8); bodyRig.add(eyeLight);
  const lightDirections = [new THREE.Vector3(.55,.72,.42).normalize(), new THREE.Vector3(-.65,.35,.68).normalize(), new THREE.Vector3(-.3,.55,-.78).normalize()];
  // Separate display background from lighting. Float radiance retains studio
  // softboxes above 1.0, without whitening the backdrop or dark display glass.
  // This is an authored HDR environment, not photographic HDRI footage.
  const hdrWidth = 512, hdrHeight = 256, hdrPixels = new Float32Array(hdrWidth * hdrHeight * 4), direction = new THREE.Vector3();
  for (let y = 0; y < hdrHeight; y++) for (let x = 0; x < hdrWidth; x++) {
    const phi = (x + .5) / hdrWidth * Math.PI * 2, theta = (y + .5) / hdrHeight * Math.PI;
    direction.set(Math.sin(theta) * Math.cos(phi), Math.cos(theta), Math.sin(theta) * Math.sin(phi));
    const ceiling = (direction.y + 1) * .5, warm = Math.pow(Math.max(0, direction.dot(lightDirections[0])), 36) * 3.4, cool = Math.pow(Math.max(0, direction.dot(lightDirections[1])), 52) * 1.7, edge = Math.pow(Math.max(0, direction.dot(lightDirections[2])), 42) * 1.2, index = (y * hdrWidth + x) * 4;
    hdrPixels[index] = .31 + ceiling * .18 + warm + cool * .72 + edge;
    hdrPixels[index + 1] = .32 + ceiling * .18 + warm * .89 + cool * .9 + edge * .82;
    hdrPixels[index + 2] = .34 + ceiling * .18 + warm * .7 + cool + edge * .65;
    hdrPixels[index + 3] = 1;
  }
  const hdrEnvironment = new THREE.DataTexture(hdrPixels, hdrWidth, hdrHeight, THREE.RGBAFormat, THREE.FloatType); hdrEnvironment.mapping = THREE.EquirectangularReflectionMapping; hdrEnvironment.colorSpace = THREE.LinearSRGBColorSpace; hdrEnvironment.needsUpdate = true; textures.add(hdrEnvironment);
  const pmrem = new THREE.PMREMGenerator(renderer), environmentTarget = pmrem.fromEquirectangular(hdrEnvironment);
  const studioBackground = new THREE.Color('#bccdd6'); scene.background = studioBackground; scene.environment = environmentTarget.texture; scene.environmentIntensity = 1.05; pmrem.dispose();
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
    renderer.setSize(width, height, false); camera.aspect = width / height; camera.position.z = Math.max(6.65, 2.85 / (camera.aspect * 2 * Math.tan(THREE.MathUtils.degToRad(camera.fov / 2)))); camera.lookAt(0, .03, 0); camera.updateProjectionMatrix(); composer?.setSize(width, height); if (lastPose) render(lastPose, true);
  }
  const whiteColor = new THREE.Color('#ffffff'), targetColor = new THREE.Color(), voiceColor = new THREE.Color('#b0f3ff'), displayedColor = new THREE.Color(AVATAR_SCENE_DECLARATIONS.stateColors.idle), transitionColor = displayedColor.clone();
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
    bodyRig.rotation.set(0, 0, 0);
    // The shared controller has already smoothed gaze, emotional roll and
    // gesture offsets. Do not introduce an independent emotion tilt here.
    head.rotation.set(finiteCue(pose.headPitch, 0, -.09, .09), finiteCue(pose.headYaw, 0, -.1, .1), finiteCue(finiteCue(pose.headRoll, 0, -.09, .09) + finiteCue(pose.expressionHeadRoll, 0, -.025, .025), 0, -.09, .09));
    const eyeOpen = finiteCue(pose.eyeOpen, 1, .035, 1.12), eyeScaleX = finiteCue(pose.eyeScaleX, 1, .8, 1.2), eyeScaleY = finiteCue(pose.eyeScaleY, 1, .6, 1.2), deformation = finiteCue(pose.eyeDeformation, 0, -.3, .3), eyeRoundness = finiteCue(pose.eyeRoundness,1,.86,1.2), eyeAsymmetry = finiteCue(pose.eyeAsymmetry,0,-.18,.18);
    eye.position.x = finiteCue(pose.gazeX, 0, -.12, .12); eye.position.y = .022 + finiteCue(pose.gazeY, 0, -.08, .08);
    const browLift = finiteCue(pose.faceBrowLift ?? pose.browLift, 0, -1, 1), browTilt = finiteCue(pose.faceBrowTilt ?? pose.browAngle, 0, -1, 1), eyeSmile = finiteCue(pose.eyeSmile), cheekGlow = finiteCue(pose.cheekGlow), smileCurve = finiteCue(pose.smileCurve), signalLevel = finiteCue(pose.faceSignal), browConcern = finiteCue(pose.browConcern), browArch = finiteCue(pose.browArch,.28), mouthSkew = finiteCue(pose.mouthSkew,0,-1,1), mouthTension = finiteCue(pose.mouthTension);
    faceBrows.forEach(({mesh: brow, side, rest}) => {brow.position.y = .355 + browLift * .082 + side * browTilt * .052; brow.rotation.z = side * -.04 + browTilt * .22 - side * browConcern * .14; const attribute = brow.geometry.attributes.position; for (let i=0;i<attribute.count;i++){const at=i*3,x=rest[at],h=THREE.MathUtils.clamp(x/.19,-1,1); attribute.setXYZ(i,x,rest[at+1]+(1-h*h)*(browArch-.28)*.084-side*h*browConcern*.028,rest[at+2]);} attribute.needsUpdate=true; brow.geometry.computeBoundingSphere();});
    const smileClosure = finiteCue(pose.lidClosure, eyeSmile * .08), speechEnergy = speaking && !reducedMotion && pose.reducedMotion !== true && pose.speechSignalValid !== false ? finiteCue(pose.speechEnergy) : 0, warmth = speaking ? 0 : finiteCue(pose.heart);
    const speechActive = speaking && speechEnergy > .015, speechBrightness=finiteCue(pose.speechBrightness), speechBands=Array.from({length:6},(_,index) => speechActive && Array.isArray(pose.speechBands) ? finiteCue(pose.speechBands[index]) : 0), mouthCurve=finiteCue(pose.mouthCurve,(smileCurve-.28)*.11,-.031,.08);
    cheekLights.forEach(cheek => {const scale = .62 + cheekGlow * .48 + speechEnergy * .12; cheek.scale.set(scale, scale, .22);});
    const mouthWidth = .94+smileCurve*.24-mouthTension*.13;
    const semanticY = h => -(1-h*h)*mouthCurve + h*(1-h*h)*mouthSkew*.032;
    const spectralPeak = Math.max(...speechBands);
    const spectrumY = h => {if (!speechActive || spectralPeak === 0) return 0; const a=THREE.MathUtils.clamp(h,-1,1); return THREE.MathUtils.clamp((1-a*a)*Math.sqrt(speechEnergy)*speechBands.reduce((sum,band,index)=>sum+band/spectralPeak*Math.sin((a+1)*Math.PI*(index+1)),0)/2,-.35,.35)*.065;};
    smileGlyph.scale.set(mouthWidth,1,1);speechMouth.scale.copy(smileGlyph.scale);smileGlyph.visible=true;speechMouth.visible=speechActive;
    voiceMaterial.opacity=speechActive ? .18+speechEnergy*.52 : 0;
    const mouthShape = [mouthCurve,mouthSkew,mouthTension,speechActive,speechEnergy,...speechBands].join(',');
    if (mouthShape !== speechMouthShape) {
      speechMouthShape=mouthShape;
      for (const {mesh:value,rest} of mouthCurves) {const attribute=value.geometry.attributes.position;for(let i=0;i<attribute.count;i++){const offset=i*3,x=rest[offset],horizontal=THREE.MathUtils.clamp(x/.23,-1,1);attribute.setXYZ(i,x,rest[offset+1]+semanticY(horizontal)+(value===speechMouth?spectrumY(horizontal):0),rest[offset+2]);}attribute.needsUpdate=true;value.geometry.computeBoundingBox();value.geometry.computeBoundingSphere();}
    }
    faceSignals.forEach(signal => {signal.visible=false;});
    for (const [index,{mesh:ripple,rest}] of speechRipples.entries()) {ripple.visible=speechActive;ripple.material.opacity=speechActive ? speechEnergy*(index ? .2 : .32) : 0;ripple.position.y=-.235+(index?1:-1)*(.026+speechEnergy*.018);ripple.scale.x=mouthWidth;const attribute=ripple.geometry.attributes.position;for(let i=0;i<attribute.count;i++){const offset=i*3,x=rest[offset],h=THREE.MathUtils.clamp(x/(.25+index*.02),-1,1);attribute.setXYZ(i,x,rest[offset+1]+semanticY(h)+spectrumY(h),rest[offset+2]);}attribute.needsUpdate=true;ripple.geometry.computeBoundingSphere();}
    const shapeSignature = [eyeOpen, eyeScaleX, eyeScaleY, deformation, eyeSmile, browTilt, smileClosure, eyeRoundness, eyeAsymmetry, browConcern].join(','), shapeChanged = shapeSignature !== eyeShapeSignature; eyeShapeSignature = shapeSignature;
    for (const {aperture: ribbon, apertureGeometry, apertureRest, side} of eyes) {
      if (shapeChanged) {
        const attribute = apertureGeometry.attributes.position, openness = eyeScaleY * eyeRoundness * (1 - smileClosure * .12) * (1 - eyeSmile * .05), asymmetry = 1 + side * browTilt * .1 + side * eyeAsymmetry;
        for (let i = 0; i < attribute.count; i++) {
          const offset = i * 3, x = apertureRest[offset], y = apertureRest[offset + 1], z = apertureRest[offset + 2], horizontal = x / .224;
          // Blink and semantic expression alter the actual silhouette.
          // Gaze translates the pair; there is no idle geometric wave.
          const arch = (horizontal * horizontal - .35) * eyeSmile * .105, inquisitiveSlope = horizontal * side * browTilt * .025, concernedInner = -side*horizontal*browConcern*.012;
          attribute.setXYZ(i, x * eyeScaleX * (1 + (1 - eyeOpen) * .035), (y * openness * asymmetry + arch + inquisitiveSlope + concernedInner + Math.abs(x) * deformation * .14) * eyeOpen + eyeSmile*.016*eyeOpen, z);
        }
        attribute.needsUpdate = true; apertureGeometry.computeVertexNormals(); apertureGeometry.computeBoundingBox(); apertureGeometry.computeBoundingSphere();
      }
      ribbon.visible = warmth <= .05;
    }
    heartGlyphs.forEach(heart => {heart.visible = warmth > .05; heart.scale.setScalar(.82 + warmth * .18);});
    const heartColor = new THREE.Color('#ed93aa');
    displayedColor.lerp(heartColor, warmth);
    irisMaterial.color.copy(displayedColor); mouthMaterial.color.copy(displayedColor); glint.color.copy(displayedColor).lerp(whiteColor, .6);
    voiceColor.setHSL((183+speechBrightness*23)/360,.85,.72+speechBrightness*.08);voiceMaterial.color.copy(voiceColor);for(const {mesh:ripple} of speechRipples)ripple.material.color.copy(voiceColor);
    eyeLight.color.copy(displayedColor); eyeLight.intensity = 0; // Face graphics must not project a distracting blue spot onto the visor.
    // Six actual spectral bands drive restrained curved light segments. An
    // amplitude-only legacy caller gets no fabricated spectrum. A held signal
    // has a held shape: there is no wall-clock oscillator or phoneme mapping.
    statusBars.forEach(({mesh:bar,rest},index) => {const band=speechBands[index],center=(index-2.5)*.075,wasVisible=bar.visible;bar.visible=speechActive && band>.002;bar.material.opacity=bar.visible ? .18+Math.min(.5,band*.45+speechEnergy*.12) : 0;const attribute=bar.geometry.attributes.position;if(!bar.visible){if(wasVisible){attribute.array.set(rest);attribute.needsUpdate=true;bar.position.x=center;bar.scale.set(1,1,1);bar.geometry.computeBoundingSphere();}return;}bar.material.color.copy(voiceColor).lerp(whiteColor,band*.12);bar.position.x=center*mouthWidth;bar.scale.set(mouthWidth,1+band*speechEnergy*.65,1);for(let i=0;i<attribute.count;i++){const offset=i*3,x=rest[offset],h=THREE.MathUtils.clamp((center+x)/.23,-1,1);attribute.setXYZ(i,x,rest[offset+1]+semanticY(h)+spectrumY(h),rest[offset+2]);}attribute.needsUpdate=true;bar.geometry.computeBoundingSphere();});
    arms.forEach(({group, side, palm}) => {const invitation = state === 'listening' ? .035 : reassuring ? .015 : 0, pointsThisSide = pose.productFocused && Math.sign(pose.targetX || 1) === side, offer = (pointsThisSide || !pose.productFocused && side === 1) && Number.isFinite(pose.offer) ? THREE.MathUtils.clamp(pose.offer, 0, 1) : 0, wave = side === 1 && Number.isFinite(pose.helloWave) ? THREE.MathUtils.clamp(pose.helloWave, -1, 1) : 0, armLift = finiteCue(side === -1 ? pose.armLiftLeft ?? pose.armLift : pose.armLiftRight ?? pose.armLift, 0, 0, .22); group.rotation.z = -side * THREE.MathUtils.clamp(armLift + invitation + offer * .24 + wave * .075, -.09, .32); group.rotation.x = -armLift * .22 - offer * .06; group.rotation.y = side * offer * .09; group.position.z = -.005 + (pointsThisSide ? finiteCue(pose.armReach, 0, 0, .11) : 0); palm.rotation.z = wave * .06;});
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
  function setFloating(value) {const floats = value === true; scene.background = floats ? null : studioBackground; renderer.setClearColor('#e6edf2', floats ? 0 : 1); floor.visible = true; pedestal.visible = !floats; studioSweep.visible = false; contactCue.visible = floats; if (lastPose) render(lastPose, true);}
  function gazeAnchor() {scene.updateMatrixWorld(true); camera.updateMatrixWorld(true); const center = head.localToWorld(new THREE.Vector3(0, .112, .71)).project(camera); return {x: THREE.MathUtils.clamp((center.x + 1) / 2, 0, 1), y: THREE.MathUtils.clamp((1 - center.y) / 2, 0, 1)};}
  function snapshot() {return {revision: THREE.REVISION, animated: !disposed && !lost && loopRunning && lastAnimationFrameMs !== null && win.performance.now() - lastAnimationFrameMs < 800, animation: {loopRequested: loopRunning, sampledFrames: frames, lastFrameMs: lastAnimationFrameMs}, frames, frameRenderMs: Math.round(renderMs * 100) / 100, drawCalls, renderedTriangles, geometry: {model: modelStats, scene: sceneStats}, textures: maps.map(value => ({...value})), shadow: {enabled: true, size: quality.shadowSize, type: 'PCF soft', casts: true, receivingStage: [floor, pedestal].some(value => value.visible && value.receiveShadow && value.material.visible), contactCue: contactCue.visible}, grounding: {bodyFixed: true, contactY: -1.52, platformTopY: -1.52, upperBodyPivot: 'head and fins', separateFeet: false}, gazeAnchor: gazeAnchor(), mannerism: {name: lastPose?.mannerism || null, active: lastPose?.mannerismActive === true, eventBound: true}, character: {identity: AVATAR_SCENE_DECLARATIONS.identity, digitalEyes: eyes.length, humanFeatures: false, meshDeformation: true, faceExpression: AVATAR_SCENE_DECLARATIONS.expressions.includes(lastPose?.faceExpression) ? lastPose.faceExpression : 'neutral', faceGeometry: {lightRibbons: eyes.length, brows: faceBrows.length, shutters: visorShutters.length, cheekFacets: cheekLights.length, smileGlyph: true, signalMarkers: 0, appreciationGlyphs: heartGlyphs.length, speechBars: 0, speechBands: statusBars.length, speechRipples: speechRipples.length, speechMouth: true, mouthClosed: true}, interactionProfile: {...AVATAR_SCENE_DECLARATIONS.interactionProfile}, state: previousState, color: '#' + displayedColor.getHexString()}, environment: {kind: AVATAR_SCENE_DECLARATIONS.environment.kind, hdr: true, hdri: false, radiance: {width: hdrWidth, height: hdrHeight, format: 'linear float RGBA'}}, finish: {...AVATAR_SCENE_DECLARATIONS.finish}, materials: {physical: [...materials].filter(value => value.isMeshPhysicalMaterial).length, metallicAnisotropy: true, transmission: [...materials].some(value => value.isMeshPhysicalMaterial && value.transmission > 0), clearcoat: true, environmentReflection: true}, lights: 5, bloom: !!composer && bloom?.enabled !== false, showcase: showcaseProduct ? {...showcaseProduct} : null, width, height, pixelRatio: renderer.getPixelRatio(), contextLost: lost};}
  function destroy() {if (disposed) return; disposed = true; clearProduct(); renderer.setAnimationLoop(null); resizeObserver?.disconnect(); win.removeEventListener('resize', resize); renderer.domElement.removeEventListener('webglcontextlost', contextLost); renderer.domElement.removeEventListener('webglcontextrestored', contextRestored); composer?.passes.forEach(pass => pass.dispose?.()); composer?.dispose(); geometries.forEach(value => value.dispose()); materials.forEach(value => value.dispose()); textures.forEach(value => value.dispose()); environmentTarget.dispose(); renderer.dispose(); renderer.forceContextLoss(); renderer.domElement.remove();}
  return {setMotion, invalidate, render, snapshot, gazeAnchor, showProduct, clearProduct, setFloating, destroy};
}
