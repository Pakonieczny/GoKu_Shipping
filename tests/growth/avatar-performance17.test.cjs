'use strict';

// CPU/static and synthetic-DOM safeguards only. These tests do not create a
// WebGL context and must not be used as evidence of rendered quality or FPS.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');
const diagnostic = require('../../scripts/inspect-concierge-avatar.cjs');

const root = path.resolve(__dirname, '../..');
const fileBytes = relative => fs.statSync(path.join(root, relative)).size;
const tick = () => new Promise(resolve => setImmediate(resolve));

function rawTextureBytes(quality, materialMaps = 6, cubeFaces = 6) {
  const cubeSize = Math.min(1024, quality.textureSize);
  return (materialMaps * quality.textureSize ** 2 + cubeFaces * cubeSize ** 2) * 4;
}

function harness(t, {visible = false, reducedMotion = false, loader} = {}) {
  const dom = new JSDOM('<!doctype html><main id="avatar"></main>', {
    url: 'https://growth-sandbox.example/concierge-sandbox.html',
    pretendToBeVisual: true
  });
  const {window} = dom;
  let hidden = false;
  Object.defineProperty(window.document, 'hidden', {configurable: true, get: () => hidden});
  const motionListeners = new Set();
  const media = {
    matches: reducedMotion,
    addEventListener(type, callback) {if (type === 'change') motionListeners.add(callback);},
    removeEventListener(type, callback) {if (type === 'change') motionListeners.delete(callback);}
  };
  window.matchMedia = () => media;
  let intersectionCallback;
  window.IntersectionObserver = class {
    constructor(callback) {intersectionCallback = callback;}
    observe() {}
    disconnect() {}
  };
  const calls = {loads: 0, motions: [], renders: 0, destroys: 0};
  const engine = {
    setMotion(value) {calls.motions.push({...value});},
    render() {calls.renders++;},
    invalidate() {},
    snapshot() {const last = calls.motions.at(-1) || {}; return {animated: last.active === true && last.reducedMotion !== true};},
    destroy() {calls.destroys++;}
  };
  const sceneModule = {
    AVATAR_SCENE_DECLARATIONS: {schema: 2, textures: Array.from({length: 6}, (_, index) => ({name: `map-${index}`, kind: 'procedural'}))},
    createAvatarScene() {return engine;}
  };
  const api = avatar.create({
    container: window.document.querySelector('#avatar'),
    visible,
    greetingOnOpen: false,
    loadScene: async () => {calls.loads++; return loader ? loader(sceneModule, calls.loads) : sceneModule;}
  });
  t.after(() => {api.destroy(); window.close();});
  return {
    api,
    calls,
    intersect(value) {intersectionCallback([{isIntersecting: value}]);},
    hide(value) {hidden = value; window.document.dispatchEvent(new window.Event('visibilitychange'));},
    reduce(value) {media.matches = value; for (const callback of motionListeners) callback({matches: value});}
  };
}

test('quality tiers preserve high detail while bounding adaptive allocation work', () => {
  const high = avatar.qualityFor({width: 1440, memory: 8, pixelRatio: 3});
  const narrow = avatar.qualityFor({width: 390, memory: 8, pixelRatio: 3});
  const lowMemory = avatar.qualityFor({width: 1440, memory: 4, pixelRatio: 2});
  const explicitMobile = avatar.qualityFor({width: 1440, memory: 8, pixelRatio: 2, mobile: true});

  assert.deepEqual(high, {name: 'high', pixelRatio: 2, shadowSize: 2048, textureSize: 2048, fps: 60, geometryScale: 1, bloom: true});
  for (const tier of [narrow, lowMemory, explicitMobile]) {
    assert.deepEqual(tier, {name: 'adaptive', pixelRatio: 1.5, shadowSize: 1024, textureSize: 1024, fps: 30, geometryScale: .8, bloom: false});
  }
  assert.equal(avatar.qualityFor({width: 1440, memory: 8, bloom: false}).bloom, false);
  assert.equal(rawTextureBytes(high), 125829120);
  assert.equal(rawTextureBytes(narrow), 50331648);
  assert.ok(rawTextureBytes(narrow) <= rawTextureBytes(high) * .4);
});

test('actual production mesh construction stays finite and adaptive tessellation is materially lower', () => {
  const high = diagnostic.buildModel('high');
  const adaptive = diagnostic.buildModel('adaptive');
  try {
    const highResult = diagnostic.inspect(high), adaptiveResult = diagnostic.inspect(adaptive);
    assert.equal(highResult.finiteBuffers, true);
    assert.equal(highResult.validIndices, true);
    assert.equal(adaptiveResult.finiteBuffers, true);
    assert.equal(adaptiveResult.validIndices, true);
    // Preserve the previous face's allocation ceilings while replacing its
    // circular aperture and shutters with two filled expressive ribbons.
    assert.ok(highResult.meshes <= 54);
    assert.equal(adaptiveResult.meshes, highResult.meshes);
    assert.ok(highResult.triangles > 100000 && highResult.triangles <= 455776);
    assert.ok(adaptiveResult.triangles > 100000 && adaptiveResult.triangles <= 313308);
    assert.deepEqual(highResult.graphicFace, {brows: 2, shutters: 0, cheekFacets: 2, smileGlyph: true, signalMarkers: 3});
    assert.deepEqual(adaptiveResult.graphicFace, highResult.graphicFace);
    for (const model of [high, adaptive]) {
      assert.equal(model.eyes.length, 2);
      for (const eye of model.eyes) {assert.equal(eye.digital, true); assert.equal(eye.aperture.geometry.type, 'ExtrudeGeometry');}
      assert.equal(model.head.getObjectByName('original-wide-visor').geometry.type, 'ExtrudeGeometry');
      assert.equal(model.head.getObjectByName('retired-aperture-ornament').children.length, 0);
    }
    assert.ok(adaptiveResult.triangles <= highResult.triangles * .7);
  } finally {
    diagnostic.dispose(high);
    diagnostic.dispose(adaptive);
  }
});

test('scene network and source budgets prevent accidental eager or oversized avatar regressions', () => {
  const controller = fs.readFileSync(path.join(root, 'brites-concierge-avatar.js'), 'utf8');
  assert.ok(fileBytes('assets/brites-concierge-avatar-scene.mjs') <= 640 * 1024);
  // Allow the semantic facial rig, measured speech aperture and bounded expression easing while
  // retaining the same on-demand compiled-scene download limit.
  assert.ok(fileBytes('brites-concierge-avatar.js') <= 56 * 1024);
  assert.ok(fileBytes('brites-concierge-avatar.css') <= 12 * 1024);
  assert.doesNotMatch(controller, /avatar-concept\.png/);
  assert.match(controller, /await import\(moduleUrl\)/);
});

test('scene import remains on demand and loaded motion obeys offscreen, pause, reduced-motion and hidden guards', async t => {
  const h = harness(t);
  await tick();
  assert.equal(h.calls.loads, 0);
  h.intersect(false);
  h.api.setVisible(true);
  await tick();
  assert.equal(h.calls.loads, 0);
  h.intersect(true);
  await h.api.ready;
  assert.equal(h.calls.loads, 1);
  assert.equal(h.api.snapshot().quality.textureSize, 2048);
  assert.equal(h.calls.motions.at(-1).active, true);

  h.api.setPaused(true);
  assert.equal(h.calls.motions.at(-1).active, false);
  h.api.setPaused(false);
  assert.equal(h.calls.motions.at(-1).active, true);
  h.reduce(true);
  assert.deepEqual(h.calls.motions.at(-1), {active: true, reducedMotion: true});
  assert.equal(h.api.snapshot().animated, false);
  h.hide(true);
  assert.equal(h.calls.motions.at(-1).active, false);
  h.hide(false);
  h.reduce(false);
  assert.equal(h.calls.motions.at(-1).active, true);
  assert.equal(h.calls.loads, 1);
});

test('load failure uses the 2-D fallback and retries only after an explicit request', async t => {
  const h = harness(t, {visible: true, loader: async () => {throw Error('synthetic scene failure');}});
  assert.equal((await h.api.ready).mode, 'fallback');
  assert.equal(h.api.snapshot().fallback.format, 'animated_svg_2d');
  h.api.setState('thinking');
  h.hide(true);
  h.hide(false);
  h.intersect(false);
  h.intersect(true);
  await tick();
  assert.equal(h.calls.loads, 1);
  assert.equal(h.api.retry(), true);
  assert.equal(h.api.retry(), false);
  await tick();
  assert.equal(h.calls.loads, 2);
  assert.equal(h.api.snapshot().mode, 'fallback');
});
