'use strict';
// Exact-photo validation and actual CPU scene texture lifecycle with synthetic
// image events. No rendered jewellery/try-on or GPU acceptance is asserted.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm'), THREE = require('three');
const {JSDOM} = require('jsdom');
const avatar = require('../../brites-concierge-avatar.js');
const product = {id: 'gid://shopify/Product/123', handle: 'bunny-necklace', title: 'Bunny necklace', image: 'https://cdn.shopify.com/s/files/1/123/products/bunny.jpg?width=5000&height=9000'};
test('only bounded HTTPS Shopify product photographs are accepted and resized safely', () => {
  const clean = avatar.productPhoto(product); assert.equal(clean.id, product.id); assert.equal(new URL(clean.image).searchParams.get('width'), '768'); assert.equal(new URL(clean.image).searchParams.has('height'), false);
  for (const image of ['http://cdn.shopify.com/s/files/a.jpg', 'https://cdn.shopify.com.evil.test/s/files/a.jpg', 'https://name:password@cdn.shopify.com/s/files/a.jpg', 'https://cdn.shopify.com:8888/s/files/a.jpg', 'data:image/png,a', 'https://cdn.shopify.com/other/a.jpg', 'https://cdn.shopify.com/s/files/' + 'a'.repeat(2100)]) assert.equal(avatar.productPhoto({...product, image}), null);
  for (const bad of [{id: 'gid://shopify/Customer/123'}, {handle: '../unsafe'}]) assert.equal(avatar.productPhoto({...product, ...bad}), null);
});
async function controller(t) {
  const dom = new JSDOM('<div id="mount"></div>', {url: 'https://sandbox.example/', pretendToBeVisual: true});
  const win = dom.window; win.matchMedia = () => ({matches: false, addEventListener() {}, removeEventListener() {}});
  let hidden = false; Object.defineProperty(win.document, 'hidden', {get: () => hidden});
  const shows = [], clears = [], floats = []; const engine = {setMotion() {}, render() {}, destroy() {}, snapshot() {return {};}, clearProduct() {clears.push(true);}, setFloating(value) {floats.push(value);}, showProduct(value) {return new Promise(resolve => shows.push({value, resolve}));}};
  const guide = avatar.create({container: win.document.getElementById('mount'), visible: true, greetingOnOpen: false, loadScene: async () => ({createAvatarScene: () => engine})}); await guide.ready;
  t.after(() => {guide.destroy(); win.close();}); return {guide, shows, clears, floats, hide() {hidden = true; win.document.dispatchEvent(new win.Event('visibilitychange'));}};
}
test('explicit exact photo selection is labelled, deduplicated and retired by clearing', async t => {
  const f = await controller(t); assert.equal(f.guide.element.querySelector('img'), null);
  assert.equal(f.guide.showProduct(product), true); assert.equal(f.shows.length, 1); assert.equal(f.shows[0].value.id, product.id);
  assert.equal(f.guide.element.querySelector('figcaption').textContent, 'Product photo'); assert.match(f.guide.element.querySelector('img').alt, /not a virtual try-on/);
  f.guide.showProduct(product); assert.equal(f.shows.length, 1); assert.equal(f.guide.showProduct({...product, image: 'https://other.invalid/wrong-piece.jpg'}), false); assert.equal(f.guide.snapshot().shownProduct, null); f.guide.clearProduct(); assert.equal(f.guide.snapshot().shownProduct, null); assert.equal(f.guide.element.querySelector('figure').hidden, true); assert.equal(f.guide.element.querySelector('img').onerror, null);
});
test('old texture completions do not replace newer exact selection or restart a hidden showcase', async t => {
  const f = await controller(t); f.guide.showProduct(product); f.guide.showProduct({...product, id: 'gid://shopify/Product/124', handle: 'moon-necklace', image: product.image.replace('bunny.jpg', 'moon.jpg')});
  f.shows[0].resolve(false); await Promise.resolve(); assert.equal(f.guide.snapshot().shownProduct.id, 'gid://shopify/Product/124');
  f.hide(); f.shows[1].resolve(true); await Promise.resolve(); assert.equal(f.guide.snapshot().shownProduct, null); assert.equal(f.guide.showProduct(product), false);
});
test('docked presentation reuses the existing scene and confirmation is a finite restrained gesture', async t => {
  const f = await controller(t); f.guide.setFloating(true); assert.equal(f.guide.snapshot().floating, true); assert.equal(f.floats.at(-1), true); f.guide.setFloating(false); assert.equal(f.floats.at(-1), false);
  const confirm = avatar.poseFor({mannerism: 'confirm', mannerismElapsed: .5});
  assert.equal(confirm.bodyYaw, 0); assert.ok(confirm.offer > 0); assert.ok(confirm.nod > 0);
  assert.equal(avatar.poseFor({mannerism: 'confirm', mannerismElapsed: 2}).offer, 0);
  assert.equal(avatar.poseFor({mannerism: 'confirm', mannerismElapsed: 2}).bodyYaw, 0);
  assert.equal(avatar.poseFor({mannerism: 'confirm', mannerismElapsed: .5, emotion: 'calm'}).bodyYaw, 0);
});
function scene(t) {
  const dom = new JSDOM('<div id="mount"></div>', {pretendToBeVisual: true}), win = dom.window, pending = [], jobs = new Map(); let clock = 1000, renderer, serial = 0;
  win.setTimeout = (callback, delay) => {jobs.set(++serial, {callback, due: clock + delay}); return serial;}; win.clearTimeout = key => jobs.delete(key);
  Object.defineProperty(win.performance, 'now', {value: () => clock}); win.HTMLCanvasElement.prototype.getContext = () => ({createImageData(w,h) {return {data: new Uint8ClampedArray(w*h*4)};}, putImageData() {}});
  const mount = win.document.getElementById('mount'); mount.getBoundingClientRect = () => ({width: 500, height: 440});
  class Renderer {constructor() {renderer = this; this.domElement = win.document.createElement('canvas'); this.shadowMap = {}; this.capabilities = {getMaxAnisotropy: () => 2}; this.info = {reset() {}, render: {calls: 0, triangles: 0}};} setPixelRatio(x) {this.ratio=x;} getPixelRatio() {return this.ratio;} setSize() {} setClearColor(color, alpha) {this.alpha=alpha;} setAnimationLoop(callback) {this.callback=callback;} render(scene) {this.scene=scene;} dispose() {} forceContextLoss() {}}
  class PMREM {fromEquirectangular(texture) {return {texture, dispose() {}};} dispose() {}}
  class Loader {setCrossOrigin(value) {this.crossOrigin = value;} load(url, success, progress, fail) {const texture = new THREE.Texture(); texture.image = {width: 100, height: 200}; const request = {url, success, fail, texture, disposed: 0}; texture.addEventListener('dispose', () => request.disposed++); pending.push(request); return texture;}}
  const source = fs.readFileSync(require.resolve('../../brites-concierge-avatar-scene.mjs'), 'utf8').replace(/^import[^\n]+\n/gm, '').replace(/export (const|function) /g, '$1 ') + '\nmodule.exports={createAvatarScene};'; const module = {exports: {}};
  vm.runInNewContext(source, {module, URL, THREE: {...THREE, WebGLRenderer: Renderer, PMREMGenerator: PMREM, TextureLoader: Loader}});
  const engine = module.exports.createAvatarScene({container: mount, quality: {...avatar.qualityFor({width: 1440, bloom: false}), textureSize: 16}, onFrame: time => avatar.poseFor({time})}); engine.setMotion({active: true, reducedMotion: false}); renderer.callback(1000);
  t.after(() => {engine.destroy(); win.close();}); return {engine, renderer, pending, jobs, advance(ms) {clock += ms; for (const [key, job] of [...jobs]) if (job.due <= clock) {jobs.delete(key); job.callback();}}};
}
test('actual scene loads the exact photo only on request and presents bounded aspect without fake fitting', async t => {
  const f = scene(t); assert.equal(f.pending.length, 0); const promise = f.engine.showProduct(product); assert.equal(f.pending.length, 1); assert.equal(new URL(f.pending[0].url).searchParams.get('width'), '768');
  f.pending[0].success(f.pending[0].texture); assert.equal(await promise, true); const card = f.renderer.scene.getObjectByName('verified-product-photo-showcase'); assert.equal(card.visible, true); assert.equal(f.engine.snapshot().showcase.id, product.id);
  const image = card.children.find(object => object.material.map === f.pending[0].texture); assert.equal(image.material.map, f.pending[0].texture); assert.equal(image.scale.y, .74); assert.equal(image.scale.x, .37);
});
test('late obsolete texture is disposed once and cannot replace new photo', async t => {
  const f = scene(t); const first = f.engine.showProduct(product), second = f.engine.showProduct({...product, id: 'gid://shopify/Product/124', handle: 'moon', image: product.image.replace('bunny.jpg', 'moon.jpg')});
  assert.equal(await first, false); assert.equal(f.pending[0].disposed, 1); f.pending[0].success(f.pending[0].texture); assert.equal(f.pending[0].disposed, 1); assert.equal(f.engine.snapshot().showcase, null);
  f.pending[1].success(f.pending[1].texture); assert.equal(await second, true); assert.equal(f.engine.snapshot().showcase.id, 'gid://shopify/Product/124');
  f.engine.clearProduct(); assert.equal(f.pending[1].disposed, 1); assert.equal(f.engine.snapshot().showcase, null);
});
test('failed and timed-out texture requests dispose allocations and never silently show another piece', async t => {
  const f = scene(t); const failed = f.engine.showProduct(product); f.pending[0].fail(); assert.equal(await failed, false); assert.equal(f.pending[0].disposed, 1);
  const timeout = f.engine.showProduct(product); f.advance(6100); assert.equal(await timeout, false); assert.equal(f.pending[1].disposed, 1); assert.equal(f.engine.snapshot().showcase, null); f.pending[1].success(f.pending[1].texture); assert.equal(f.pending[1].disposed, 1);
});
test('floating scene is transparent and restores studio ground without rebuilding geometry', t => {
  const f = scene(t), before = f.engine.snapshot().geometry.model.triangles;
  f.engine.setFloating(true); assert.equal(f.renderer.alpha, 0); assert.equal(f.renderer.scene.background, null); assert.equal(f.renderer.scene.getObjectByName('continuous-studio-sweep').visible, false);
  f.engine.setFloating(false); assert.equal(f.renderer.alpha, 1); assert.ok(f.renderer.scene.background.isColor); assert.equal(f.renderer.scene.background.getHexString(),'faf8f2'); assert.equal(f.renderer.scene.getObjectByName('continuous-studio-sweep').visible, false); assert.equal(f.renderer.scene.getObjectByName('grounding-platform').visible, true); assert.equal(f.engine.snapshot().geometry.model.triangles, before);
});
