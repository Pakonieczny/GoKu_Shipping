'use strict';

// Synthetic DOM/scene integration checks. These do not assert actual GPU frame
// rate, visual quality, or live storefront behaviour: those need browser QA.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM, VirtualConsole} = require('jsdom');
const avatarFactory = require('../../brites-concierge-avatar.js');
const widgetSource = fs.readFileSync(path.join(__dirname, '../../brites-concierge.js'), 'utf8');
const avatarLoaderSelector = 'script[src$="/brites-concierge-avatar.js"]';
const tick = () => new Promise(resolve => setImmediate(resolve));
async function settle() { await tick(); await tick(); }
function deferred() { let resolve, reject; const promise = new Promise((a, b) => {resolve = a; reject = b;}); return {promise, resolve, reject}; }
const clone = value => JSON.parse(JSON.stringify(value));

function makeDom() {
  const errors = [], virtualConsole = new VirtualConsole();
  virtualConsole.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM('<!doctype html><html lang="en"><body><button id="shop-control">Shop</button></body></html>', {
    url: 'https://growth-sandbox.example/concierge-sandbox.html',
    runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole
  });
  return {dom, window: dom.window, document: dom.window.document, errors};
}

function mediaAndVisibility(window, {reducedMotion = false} = {}) {
  let hidden = false;
  Object.defineProperty(window.document, 'hidden', {configurable: true, get: () => hidden});
  const listeners = new Set(), observers = [];
  const media = {matches: reducedMotion, media: '(prefers-reduced-motion: reduce)',
    addEventListener(type, listener) {if (type === 'change') listeners.add(listener);},
    removeEventListener(type, listener) {if (type === 'change') listeners.delete(listener);}
  };
  window.matchMedia = () => media;
  window.IntersectionObserver = class {
    constructor(callback) {this.callback = callback; this.disconnected = false; observers.push(this);}
    observe(element) {this.element = element;}
    disconnect() {this.disconnected = true;}
  };
  return {
    observers, listeners,
    hide(value) {hidden = value; window.document.dispatchEvent(new window.Event('visibilitychange'));},
    motion(value) {media.matches = value; for (const listener of listeners) listener({matches: value});},
    intersect(value) {for (const observer of observers) if (!observer.disconnected) observer.callback([{target: observer.element, isIntersecting: value}]);}
  };
}

function makeAvatar(t, options = {}) {
  const env = makeDom(), visibility = mediaAndVisibility(env.window, options), calls = {loads: [], motion: [], render: [], invalidate: 0, destroy: 0};
  const engine = {
    setMotion(value) {if (calls.fail === 'motion') throw Error('Synthetic motion failure'); calls.motion.push({...value});},
    render(pose, force) {if (calls.fail === 'render') throw Error('Synthetic draw failure'); calls.render.push({pose: {...pose}, force});},
    invalidate() {if (calls.fail === 'invalidate') throw Error('Synthetic invalidate failure'); calls.invalidate++;},
    snapshot() {const m = calls.motion.at(-1) || {}; return {animated: m.active === true && m.reducedMotion !== true};},
    destroy() {calls.destroy++; if (calls.fail === 'destroy') throw Error('Synthetic cleanup failure');}
  };
  let sceneOptions;
  const module = {AVATAR_SCENE_DECLARATIONS:{schema:1,textures:[{name:'Porcelain micro-surface',kind:'porcelain'},{name:'Champagne brushed grain',kind:'brush'},{name:'Champagne roughness',kind:'roughness'},{name:'Aquamarine iris radial fibres',kind:'color'}]},createAvatarScene(value) {sceneOptions = value; if(options.createSceneThrows)throw Error('Synthetic WebGL renderer unavailable'); return engine;}};
  const container = env.document.createElement('div'); env.document.body.appendChild(container);
  const api = avatarFactory.create({container, assetBase: 'https://growth-sandbox.example/',
    visible: options.visible === true,
    loadScene: url => {calls.loads.push(url); return options.loader ? options.loader(module) : Promise.resolve(module);}
  });
  t.after(() => {api.destroy(); env.window.close();});
  return {...env, api, visibility, calls, get sceneOptions() {return sceneOptions;}};
}

const fixtureVariant = {id: 'gid://shopify/ProductVariant/101', numericId: '101', title: 'Sterling Silver', price: 54, available: true, options: [{name: 'Metal', value: 'Sterling Silver'}]};
const fixtureProduct = {id: 'gid://shopify/Product/1', handle: 'bunny-1', url: 'https://britesjewelry.com/products/bunny-1', title: 'Bunny Necklace', type: 'Necklace', currency: 'USD', variants: [fixtureVariant], variantsComplete: true, suggestedVariantId: fixtureVariant.id, why: 'A bunny design for the requested interest.', minPrice: 54};
const fixtureAnswer = {reply: 'This bunny necklace could be a thoughtful match.', preferences: {}, products: [fixtureProduct], question: 'Would you like to compare the metal options?'};
function response(data, status = 200) {return {ok: status >= 200 && status < 300, status, json: async () => clone(data)};}

function makeWidget(t, options = {}) {
  const env = makeDom(), {window, document} = env;
  const visibility = mediaAndVisibility(window), network = [], instances = [], utterances = [], devices = [], speech = {cancels: 0, pauses: 0}, avatarCalls = {create: 0, retry: 0};
  const script = document.createElement('script'); script.src = 'https://growth-sandbox.example/brites-concierge.js'; script.dataset.sandbox = 'true';
  Object.defineProperty(document, 'currentScript', {configurable: true, get: () => script});
  if (options.saved) window.sessionStorage.setItem('brites-concierge-v1', JSON.stringify(options.saved));
  const forbidden = name => {devices.push(name); throw Error('The concierge must not request microphone/camera access.');};
  Object.defineProperty(window.navigator, 'mediaDevices', {configurable: true, value: {getUserMedia: () => forbidden('getUserMedia'), enumerateDevices: () => forbidden('enumerateDevices')}});
  window.SpeechRecognition = function () {forbidden('SpeechRecognition');};
  window.webkitSpeechRecognition = function () {forbidden('webkitSpeechRecognition');};
  if (!options.noSpeech) {
    window.SpeechSynthesisUtterance = class {constructor(text) {if(options.utteranceThrows)throw Error('Synthetic voice constructor failure');this.text = text;}};
    window.speechSynthesis = {cancel() {speech.cancels++;if(options.cancelThrows)throw Error('Synthetic cancellation failure');},pause(){speech.pauses++;},speak(utterance) {utterances.push(utterance);if(options.speakStartsThenThrows)utterance.onstart();if(options.speakThrows||options.speakStartsThenThrows)throw Error('Synthetic voice synthesis failure');}};
  }
  if (!options.noAvatarGlobal) window.BritesConciergeAvatar = {
    create(config) {
      avatarCalls.create++;
      if (options.createThrows) throw Error('Synthetic unavailable renderer');
      const record = {config, states: [], visible: config.visible === true, level: 0, state: config.initialState, visibility: [], destroyed: false};
      record.api = {
        ready: options.ready || Promise.resolve({mode: options.fallback ? 'fallback' : 'webgl'}),
        setState(value) {if (options.stateThrows) throw Error('Synthetic renderer draw failure'); record.state = value; record.states.push(value);},
        setVisible(value) {record.visible = value; record.visibility.push(value);},
        retry() {avatarCalls.retry++;},
        setLevel(value) {record.level = value;}, lookAt() {},
        snapshot() {return {state: record.state, visible: record.visible, mode: options.fallback ? 'fallback' : 'webgl'};},
        destroy() {record.destroyed = true; record.visible = false;}
      };
      instances.push(record); return record.api;
    }
  };
  window.fetch = async (raw, init = {}) => {
    const url = new URL(raw, window.location.href), body = init.body ? JSON.parse(init.body) : null;
    network.push({url, body, init});
    if (url.pathname === '/api/concierge' && body?.event) return response({});
    if (url.pathname === '/api/concierge' && body?.message) return options.answer ? options.answer(body, init) : response(fixtureAnswer);
    if (url.pathname === '/api/growth/product') return options.product ? options.product(url, init) : response({product: fixtureProduct});
    throw Error('Unexpected synthetic request: ' + url.pathname);
  };
  window.HTMLElement.prototype.scrollIntoView = function () {};
  window.eval(widgetSource);
  const root = document.querySelector('brites-concierge').shadowRoot;
  const button = label => Array.from(root.querySelectorAll('button')).find(node => node.textContent.trim() === label || node.getAttribute('aria-label') === label);
  const input = root.querySelector('input[aria-label="Message the gift concierge"]');
  async function ask(text = 'Something with a bunny') {input.value = text; root.querySelector('form').dispatchEvent(new window.Event('submit', {bubbles: true, cancelable: true})); await settle();}
  async function add() {button('Choose options').click(); button('Review adding to bag').click(); button('Confirm add to bag').click(); await settle();}
  t.after(() => {try {window.BritesConcierge?.close();} catch {} window.close();});
  return {...env, visibility, network, instances, utterances, devices, speech, avatarCalls, root, input, button, ask, add,
    open() {root.querySelector('.launcher').click();},
    get avatar() {return instances.at(-1);}, get panel() {return root.querySelector('.panel');},
    get messages() {return root.querySelector('.messages');}
  };
}

test('avatar imports no scene until the shopper opens it', async t => {
  const h = makeAvatar(t);
  await settle(); assert.equal(h.calls.loads.length, 0); assert.equal(h.api.element.hidden, true);
  h.api.setState('thinking'); await settle(); assert.equal(h.calls.loads.length, 0);
  h.api.setVisible(true); const ready = await h.api.ready;
  assert.equal(h.calls.loads.length, 1); assert.equal(ready.mode, 'webgl'); assert.equal(ready.loading, false); assert.equal(h.api.element.hidden, false);
  assert.equal(new URL(h.calls.loads[0]).pathname, '/assets/brites-concierge-avatar-scene.mjs');
});

test('closing and reopening pause and resume the existing scene', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready;
  assert.equal(h.calls.motion.at(-1).active, true);
  h.api.setVisible(false); assert.equal(h.calls.motion.at(-1).active, false);
  const frames = h.calls.render.length; h.api.setState('speaking'); assert.equal(h.calls.render.length, frames);
  h.api.setVisible(true); assert.equal(h.calls.motion.at(-1).active, true); assert.equal(h.calls.loads.length, 1);
});

test('document visibility and intersection independently stop scene motion', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready;
  h.visibility.hide(true); assert.equal(h.calls.motion.at(-1).active, false);
  h.visibility.intersect(false); h.visibility.hide(false); assert.equal(h.calls.motion.at(-1).active, false);
  h.visibility.intersect(true); assert.equal(h.calls.motion.at(-1).active, true);
  h.api.setVisible(false); h.visibility.hide(true); h.visibility.hide(false); assert.equal(h.calls.motion.at(-1).active, false);
});

test('reduced motion keeps expression changes but removes timed motion', async t => {
  const h = makeAvatar(t, {visible: true, reducedMotion: true}); await h.api.ready;
  assert.deepEqual(h.calls.motion.at(-1), {active: true, reducedMotion: true});
  h.api.setState('speaking'); h.api.setLevel(.8);
  const first = h.sceneOptions.onFrame(1), later = h.sceneOptions.onFrame(9);
  assert.deepEqual(first, later); assert.equal(first.bob, 0); assert.equal(first.gesture, 0); assert.equal(first.bodyRoll, 0);
  h.visibility.motion(false); assert.equal(h.calls.motion.at(-1).reducedMotion, false);
  const resumed = h.sceneOptions.onFrame(1);
  assert.equal(resumed.bob, 0); assert.equal(resumed.gazeY, 0);
  h.api.setLevel(.25); const audible = h.sceneOptions.onFrame(9);
  assert.ok(audible.speechEnergy > resumed.speechEnergy, 'voice emission follows measured output after resuming');
  assert.equal(audible.mouthOpen, 0); assert.equal(resumed.mouthOpen, 0);
  assert.equal(audible.mouthCurve, resumed.mouthCurve, 'the semantic curve remains closed at every volume');
  assert.deepEqual(audible.speechBands, [0, 0, 0, 0, 0, 0], 'legacy amplitude cannot invent a spectrum');
});

test('scene loading rejection resolves a static fallback', async t => {
  const h = makeAvatar(t, {visible: true, loader: async () => {throw Error('Synthetic GPU unavailable');}});
  const ready = await h.api.ready;
  assert.equal(ready.mode, 'fallback'); assert.equal(ready.loading, false); assert.equal(h.api.element.dataset.rendering, 'fallback');
  h.api.setState('thinking'); h.api.setVisible(false); h.api.setVisible(true);
  await settle(); assert.equal(h.calls.loads.length, 1); assert.equal(h.calls.render.length, 0);
});

test('explicit retry recovers a transient scene import failure without an automatic loop', async t => {
  let attempts = 0;
  const h = makeAvatar(t, {visible: true, loader: async module => {if (++attempts === 1) throw Error('Synthetic transient import failure'); return module;}});
  assert.equal((await h.api.ready).mode, 'fallback');
  h.api.setState('thinking'); h.visibility.hide(true); h.visibility.hide(false); h.visibility.intersect(false); h.visibility.intersect(true);
  await settle(); assert.equal(attempts, 1);
  assert.equal(h.api.retry(), true); assert.equal(h.api.retry(), false);
  await settle(); assert.equal(attempts, 2); assert.equal(h.api.snapshot().mode, 'webgl');
  assert.equal(h.api.retry(), false); assert.equal(attempts, 2);
});

test('an explicit retry failure remains static until a later explicit request', async t => {
  const h = makeAvatar(t, {visible: true, loader: async () => {throw Error('Synthetic persistent rendering failure');}});
  await h.api.ready;
  assert.equal(h.api.retry(), true); await settle();
  assert.equal(h.api.snapshot().mode, 'fallback'); assert.equal(h.calls.loads.length, 2);
  h.api.setState('thinking'); h.api.setLevel(.8); h.visibility.hide(true); h.visibility.hide(false);
  await settle(); assert.equal(h.calls.loads.length, 2);
  h.api.setVisible(false); assert.equal(h.api.retry(), false);
  h.api.setVisible(true); h.visibility.hide(true); assert.equal(h.api.retry(), false);
  h.visibility.hide(false); h.visibility.intersect(false); assert.equal(h.api.retry(), false);
  h.api.destroy(); assert.equal(h.api.retry(), false); assert.equal(h.calls.loads.length, 2);
});

test('avatar layers stay exclusive while styles are missing and across context recovery', async t => {
  const loading = deferred(); let module;
  const h = makeAvatar(t, {visible: true, loader: value => {module = value; return loading.promise;}});
  const surface = h.api.element.querySelector('.brites-avatar__surface');
  const fallback = h.api.element.querySelector('.brites-avatar__fallback');
  const staleStyle = h.document.createElement('style');
  staleStyle.textContent = '.brites-avatar__fallback{display:grid}'; h.document.head.appendChild(staleStyle);
  assert.equal(surface.style.visibility, 'hidden');
  assert.equal(fallback.hidden, true);
  assert.equal(h.window.getComputedStyle(fallback).display, 'none');
  assert.equal(h.api.snapshot().fallback.active, false);
  assert.equal(h.api.element.querySelector('.brites-avatar__loading').hidden, false);
  loading.resolve(module); await h.api.ready;
  assert.equal(surface.style.visibility, 'visible');
  assert.equal(fallback.hidden, true);
  assert.equal(h.window.getComputedStyle(fallback).display, 'none');
  assert.equal(h.api.element.querySelector('.brites-avatar__loading').hidden, true);
  h.sceneOptions.onContext(true);
  assert.equal(surface.style.visibility, 'hidden');
  assert.equal(fallback.hidden, false);
  assert.notEqual(h.window.getComputedStyle(fallback).display, 'none');
  h.sceneOptions.onContext(false);
  assert.equal(surface.style.visibility, 'visible');
  assert.equal(fallback.hidden, true);
  assert.equal(h.window.getComputedStyle(fallback).display, 'none');
  assert.equal(fallback.querySelector('svg').getAttribute('aria-hidden'), 'true');
  assert.match(h.api.element.getAttribute('aria-label'), /Brites jewellery gift guide/);
});

test('a partially constructed scene cannot leave a second canvas on explicit retry', async t => {
  let attempts = 0;
  const h = makeAvatar(t, {visible: true, loader: module => ({...module, createAvatarScene(config) {
    config.container.appendChild(config.container.ownerDocument.createElement('canvas'));
    if (++attempts === 1) throw Error('Synthetic constructor failure after canvas attachment');
    return module.createAvatarScene(config);
  }})});
  assert.equal((await h.api.ready).mode, 'fallback');
  const surface = h.api.element.querySelector('.brites-avatar__surface');
  assert.equal(surface.querySelectorAll('canvas').length, 0);
  assert.equal(h.api.retry(), true); await settle();
  assert.equal(h.api.snapshot().mode, 'webgl');
  assert.equal(surface.querySelectorAll('canvas').length, 1);
});

test('failed engine cleanup cannot leave a stale canvas beneath a replacement scene', async t => {
  const h = makeAvatar(t, {visible: true, loader: module => ({...module, createAvatarScene(config) {
    config.container.appendChild(config.container.ownerDocument.createElement('canvas'));
    const engine = module.createAvatarScene(config);
    return {...engine, destroy() {engine.destroy(); throw Error('Synthetic disposal failure');}};
  }})});
  await h.api.ready;
  const surface = h.api.element.querySelector('.brites-avatar__surface');
  assert.equal(surface.querySelectorAll('canvas').length, 1);
  h.calls.fail = 'render'; h.api.setState('thinking');
  assert.equal(h.api.snapshot().mode, 'fallback');
  assert.equal(surface.querySelectorAll('canvas').length, 0);
  assert.equal(surface.style.visibility, 'hidden');
  delete h.calls.fail; assert.equal(h.api.retry(), true); await settle();
  assert.equal(h.api.snapshot().mode, 'webgl');
  assert.equal(surface.querySelectorAll('canvas').length, 1);
  assert.equal(h.api.element.querySelector('.brites-avatar__fallback').hidden, true);
});

test('a failed first draw resolves fallback without announcing 3-D readiness', async t => {
  const h = makeAvatar(t, {visible: true}); const events = [];
  h.api.element.addEventListener('brites-avatar:ready', () => events.push('ready'));
  h.calls.fail = 'render';
  assert.equal((await h.api.ready).mode, 'fallback');
  assert.deepEqual(events, []);
  assert.equal(h.api.element.querySelector('.brites-avatar__fallback').hidden, false);
});

test('pausing during first import never flashes a different character before the first draw', async t => {
  const loading = deferred(); let module, readyResolved = false;
  const h = makeAvatar(t, {visible: true, loader: value => {module = value; return loading.promise;}});
  const events = []; h.api.element.addEventListener('brites-avatar:ready', () => events.push('ready'));
  h.api.ready.then(() => {readyResolved = true;});
  h.api.setPaused(true); loading.resolve(module); await settle();
  const surface = h.api.element.querySelector('.brites-avatar__surface');
  const fallback = h.api.element.querySelector('.brites-avatar__fallback');
  assert.equal(h.calls.render.length, 0);
  assert.equal(h.api.snapshot().mode, 'pending');
  assert.equal(h.api.snapshot().fallback.active, false);
  assert.equal(surface.style.visibility, 'hidden');
  assert.equal(fallback.hidden, true);
  assert.match(h.api.element.querySelector('.brites-avatar__loading').textContent, /animation resumes/);
  assert.equal(readyResolved, false); assert.deepEqual(events, []);
  h.api.setPaused(false);
  assert.equal((await h.api.ready).mode, 'webgl');
  assert.equal(surface.style.visibility, 'visible');
  assert.equal(fallback.hidden, true); assert.deepEqual(events, ['ready']);
});

test('recovery keeps the failure companion until the next successful draw without a loading flash', async t => {
  const loading = deferred(); let attempts = 0, module;
  const h = makeAvatar(t, {visible: true, loader: value => {module = value; return ++attempts === 1 ? Promise.reject(Error('Synthetic import failure')) : loading.promise;}});
  await h.api.ready;
  const fallback = h.api.element.querySelector('.brites-avatar__fallback'), notice = h.api.element.querySelector('.brites-avatar__loading');
  assert.equal(fallback.hidden, false); assert.equal(notice.hidden, true);
  assert.equal(h.api.retry(), true); assert.equal(fallback.hidden, false); assert.equal(notice.hidden, true);
  loading.resolve(module); await settle();
  assert.equal(h.api.snapshot().mode, 'webgl'); assert.equal(fallback.hidden, true); assert.equal(notice.hidden, true);
});

test('failed controller setup removes its attached frame and listeners before an explicit retry', async t => {
  const env = makeDom(), visibility = mediaAndVisibility(env.window), container = env.document.createElement('div'); env.document.body.appendChild(container);
  const Observer = env.window.IntersectionObserver;
  env.window.IntersectionObserver = class extends Observer {observe(element) {super.observe(element); throw Error('Synthetic observer setup failure');}};
  assert.throws(() => avatarFactory.create({container, visible: true}), /observer setup failure/);
  assert.equal(container.querySelectorAll('.brites-avatar').length, 0);
  assert.equal(visibility.listeners.size, 0); assert.equal(visibility.observers[0].disconnected, true);
  env.window.IntersectionObserver = Observer;
  const api = avatarFactory.create({container, visible: false});
  assert.equal(container.querySelectorAll('.brites-avatar').length, 1);
  t.after(() => {api.destroy(); env.window.close();});
});

test('restoration while paused keeps fallback visible until a successful recovery draw', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready;
  const events = []; h.api.element.addEventListener('brites-avatar:restored', () => events.push('restored'));
  const surface = h.api.element.querySelector('.brites-avatar__surface');
  const fallback = h.api.element.querySelector('.brites-avatar__fallback');
  h.sceneOptions.onContext(true); h.api.setPaused(true);
  const draws = h.calls.render.length;
  h.sceneOptions.onContext(false);
  assert.equal(h.calls.render.length, draws);
  assert.equal(h.api.snapshot().mode, 'pending');
  assert.equal(h.api.snapshot().fallback.active, true);
  assert.equal(surface.style.visibility, 'hidden');
  assert.equal(fallback.hidden, false); assert.deepEqual(events, []);
  h.api.setPaused(false);
  assert.ok(h.calls.render.length > draws);
  assert.equal(h.api.snapshot().mode, 'webgl');
  assert.equal(surface.style.visibility, 'visible');
  assert.equal(fallback.hidden, true); assert.deepEqual(events, ['restored']);
});

test('retry preserves browser context restoration and recovers a discarded failed engine', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready;
  h.sceneOptions.onContext(true); assert.equal(h.api.retry(), false); assert.equal(h.calls.loads.length, 1);
  h.sceneOptions.onContext(false); h.calls.fail = 'render'; h.api.setState('thinking');
  assert.equal(h.api.snapshot().mode, 'fallback'); assert.equal(h.calls.destroy, 1);
  delete h.calls.fail; assert.equal(h.api.retry(), true); await settle();
  assert.equal(h.calls.loads.length, 2); assert.equal(h.api.snapshot().mode, 'webgl');
});

test('renderer failure preserves loaded texture declarations without claiming created maps', async t => {
  const h = makeAvatar(t, {visible: true, createSceneThrows: true});
  const ready = await h.api.ready;
  assert.equal(ready.mode, 'fallback'); assert.equal(ready.declarations.source, 'loaded_scene_module');
  assert.equal(ready.declarations.textures.length, 4); assert.ok(ready.declarations.textures.every(value => value.width === 2048 && value.height === 2048));
  assert.equal(ready.textures, undefined); assert.equal(h.api.element.dataset.rendering, 'fallback');
});

test('context loss pauses rendering and restoration respects current visibility', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready;
  h.sceneOptions.onContext(true); assert.equal(h.api.snapshot().mode, 'fallback'); assert.equal(h.calls.motion.at(-1).active, false);
  h.api.setVisible(false); h.sceneOptions.onContext(false);
  assert.equal(h.api.snapshot().mode, 'pending'); assert.equal(h.calls.motion.at(-1).active, false);
  h.api.setVisible(true); assert.equal(h.calls.motion.at(-1).active, true); assert.equal(h.api.snapshot().mode, 'webgl');
});

test('destroy during asynchronous import prevents scene creation', async t => {
  const loading = deferred(); const h = makeAvatar(t, {visible: true, loader: () => loading.promise});
  h.api.destroy(); assert.equal((await h.api.ready).destroyed, true);
  loading.resolve({createAvatarScene() {throw Error('Destroyed avatar must not build a scene.');}}); await settle();
  assert.equal(h.calls.render.length, 0); assert.equal(h.api.element.isConnected, false);
});

test('destroy disconnects motion observers and ignores late state/visibility calls', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready;
  h.api.destroy(); const motions = h.calls.motion.length, renders = h.calls.render.length;
  h.api.destroy(); h.api.setVisible(true); h.api.setState('speaking'); h.api.lookAt(1, 1); h.api.setLevel(1);
  h.visibility.hide(true); h.visibility.motion(true); h.visibility.intersect(true);
  assert.equal(h.calls.destroy, 1); assert.equal(h.calls.motion.length, motions); assert.equal(h.calls.render.length, renders);
  assert.equal(h.visibility.listeners.size, 0); assert.equal(h.visibility.observers[0].disconnected, true);
});

test('invalid avatar states and extreme pointer/speech values are bounded', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready;
  h.api.setState('untrusted arbitrary state'); assert.equal(h.api.snapshot().state, 'idle');
  h.api.setState('speaking'); h.api.lookAt(100, -100); h.api.setLevel(100);
  const pose = h.sceneOptions.onFrame(4);
  assert.ok(Math.abs(pose.headYaw) <= .1); assert.ok(pose.mouthOpen <= 1); assert.ok(Math.abs(pose.gazeY) <= .07);
});

for (const failure of ['invalidate', 'motion', 'render']) test(failure + ' engine failure becomes a safe fallback after initialization', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready; h.calls.fail = failure;
  assert.doesNotThrow(() => h.api.setState('thinking'));
  assert.equal(h.api.snapshot().mode, 'fallback');
  assert.doesNotThrow(() => {h.api.setVisible(false); h.api.setState('speaking'); h.api.setVisible(true);});
  await settle(); assert.equal(h.calls.loads.length, 1);
});

test('engine cleanup exceptions do not prevent DOM and listener cleanup', async t => {
  const h = makeAvatar(t, {visible: true}); await h.api.ready; h.calls.fail = 'destroy';
  assert.doesNotThrow(() => h.api.destroy());
  assert.equal(h.api.element.isConnected, false); assert.equal(h.visibility.listeners.size, 0);
  assert.equal(h.visibility.observers[0].disconnected, true);
});

test('closed widget stays unobtrusive and creates no avatar or speech', async t => {
  const h = makeWidget(t); await settle();
  assert.equal(h.panel.hidden, true); assert.equal(h.root.querySelector('.launcher').getAttribute('aria-expanded'), 'false');
  assert.equal(h.instances.length, 0); assert.equal(h.utterances.length, 0); assert.equal(h.devices.length, 0);
});

test('open/close expose accessible controls and return keyboard focus', async t => {
  const h = makeWidget(t); h.open(); await settle();
  const launcher = h.root.querySelector('.launcher');
  assert.equal(launcher.getAttribute('aria-controls'), h.panel.id); assert.equal(launcher.getAttribute('aria-expanded'), 'true');
  assert.equal(h.panel.getAttribute('role'), 'dialog'); assert.ok(h.panel.getAttribute('aria-label'));
  assert.equal(h.root.activeElement, h.button('Talk to me')); assert.equal(h.avatar.visible, true);
  assert.equal(h.root.querySelector('.concierge-avatar-stage').getAttribute('aria-hidden'), 'true');
  h.button('Close gift concierge').click();
  assert.equal(h.panel.hidden, true); assert.equal(h.avatar.visible, false); assert.equal(h.root.activeElement, launcher);
  assert.equal(launcher.getAttribute('aria-expanded'), 'false');
});

test('typing is a silent listening expression and clear input returns idle', async t => {
  const h = makeWidget(t); h.open(); h.input.value = 'A gift for my sister'; h.input.dispatchEvent(new h.window.Event('input'));
  assert.equal(h.avatar.state, 'listening'); assert.equal(h.utterances.length, 0); assert.equal(h.devices.length, 0);
  h.input.value = ''; h.input.dispatchEvent(new h.window.Event('input')); assert.equal(h.avatar.state, 'idle');
});

test('search moves from thinking to recommendation without automatic speech', async t => {
  const reply = deferred(); const h = makeWidget(t, {answer: () => reply.promise}); h.open();
  const pending = h.ask(); assert.equal(h.avatar.state, 'thinking'); assert.equal(h.input.disabled, true);
  reply.resolve(response(fixtureAnswer)); await pending;
  assert.equal(h.avatar.state, 'success'); assert.equal(h.input.disabled, false); assert.equal(h.root.querySelectorAll('.card').length, 1);
  assert.equal(h.utterances.length, 0);
});

// Native speech state, interruption and late callback coverage is exercised
// in concierge-voice-ui22.test.cjs. Browser synthesis is no longer a feature.

test('start fresh resets the guide and aborts a pending answer', async t => {
  const replies = []; const h = makeWidget(t, {answer: () => {const reply = deferred(); replies.push(reply); return reply.promise;}});
  h.open(); const first = h.ask(); replies[0].resolve(response(fixtureAnswer)); await first;
  const pending = h.ask('Second request'); const signal = h.network.findLast(r => r.body?.message).init.signal;
  h.button('Start fresh').click(); assert.equal(signal.aborted, true); assert.equal(h.avatar.state, 'idle');
  assert.equal(h.avatar.state, 'idle'); assert.equal(h.avatar.level, 0);
  replies[1].resolve(response(fixtureAnswer)); await pending;
  assert.equal(h.root.querySelectorAll('.card').length, 0); assert.match(h.messages.textContent, /Looking for something personal/);
  assert.equal(h.button('Send').disabled, false);
});

test('late search results after dismissal stay hidden without stealing focus or speaking', async t => {
  const reply = deferred(); const h = makeWidget(t, {answer: () => reply.promise}); h.open(); const pending = h.ask();
  h.window.BritesConcierge.close(); h.document.getElementById('shop-control').focus();
  reply.resolve(response(fixtureAnswer)); await pending;
  assert.equal(h.panel.hidden, true); assert.equal(h.avatar.visible, false); assert.equal(h.utterances.length, 0);
  assert.equal(h.document.activeElement.id, 'shop-control');
  h.window.BritesConcierge.open(); assert.equal(h.panel.hidden, false); assert.equal(h.root.querySelectorAll('.card').length, 0);
});

for(const completion of ['success','failure'])test('an open-widget '+completion+' preserves intentional outside-shop focus',async t=>{
  const reply=deferred(),h=makeWidget(t,{answer:()=>reply.promise});h.open();const pending=h.ask();assert.equal(h.input.disabled,true);
  const outside=h.document.getElementById('shop-control');outside.focus();assert.equal(h.document.activeElement,outside);
  reply.resolve(completion==='success'?response(fixtureAnswer):response({error:'Synthetic service recovery'},503));await pending;
  assert.equal(h.panel.hidden,false);assert.equal(h.document.activeElement,outside);assert.equal(h.root.activeElement,null);assert.equal(h.input.disabled,false);assert.equal(h.errors.length,0);assert.equal(h.utterances.length,0);
  assert.equal(h.root.querySelectorAll('.card').length,completion==='success'?1:0);
});
test('search completion preserves keyboard focus on another concierge control',async t=>{
  const reply=deferred(),h=makeWidget(t,{answer:()=>reply.promise});h.open();const pending=h.ask();const read=h.button('Pause animation');read.focus();
  reply.resolve(response(fixtureAnswer));await pending;assert.equal(h.root.activeElement,read);assert.equal(h.input.disabled,false);assert.equal(h.utterances.length,0);
});
test('search completion restores the composer when disabling it caused blur and no shopper focus move',async t=>{
  const reply=deferred(),h=makeWidget(t,{answer:()=>reply.promise});h.open();h.button('Type instead').click();const pending=h.ask();
  // jsdom cannot blur an already disabled element. Emulate the browser's
  // automatic blur without a new user-initiated focusin on another control.
  h.input.disabled=false;h.input.blur();h.input.disabled=true;assert.equal(h.document.activeElement,h.document.body);
  reply.resolve(response(fixtureAnswer));await pending;assert.equal(h.root.activeElement,h.input);assert.equal(h.input.disabled,false);
});
test('a submit without composer focus ownership does not take shop focus on completion',async t=>{
  const h=makeWidget(t);h.open();const outside=h.document.getElementById('shop-control');outside.focus();await h.ask();assert.equal(h.document.activeElement,outside);assert.equal(h.root.querySelectorAll('.card').length,1);
});
test('late failed search after dismissal keeps the panel hidden and ordinary shop focus intact',async t=>{
  const reply=deferred(),h=makeWidget(t,{answer:()=>reply.promise});h.open();const pending=h.ask();h.window.BritesConcierge.close();const outside=h.document.getElementById('shop-control');outside.focus();
  reply.resolve(response({error:'Synthetic service recovery'},503));await pending;assert.equal(h.document.activeElement,outside);assert.equal(h.panel.hidden,true);assert.equal(h.avatar.visible,false);assert.equal(h.utterances.length,0);
});

test('avatar still loading does not block search, variant review, or sandbox cart', async t => {
  const ready = deferred(); const h = makeWidget(t, {ready: ready.promise}); h.open(); await h.ask(); await h.add();
  assert.equal(h.root.querySelectorAll('.card').length, 1); assert.equal(h.avatar.state, 'success');
  assert.equal(JSON.parse(h.window.sessionStorage.getItem('brites-sandbox-cart'))[0].variantId, '101');
  ready.resolve({mode: 'webgl'});
});

for (const mode of ['constructor failure', 'static fallback', 'script failure']) test(mode + ' leaves search and confirmed sandbox cart functional', async t => {
  const h = makeWidget(t, {createThrows: mode === 'constructor failure', fallback: mode === 'static fallback', noAvatarGlobal: mode === 'script failure'});
  h.open();
  if (mode === 'script failure') h.root.querySelector(avatarLoaderSelector).dispatchEvent(new h.window.Event('error'));
  await h.ask(); await h.add();
  assert.equal(h.errors.length, 0); assert.equal(h.root.querySelectorAll('.card').length, 1);
  assert.equal(JSON.parse(h.window.sessionStorage.getItem('brites-sandbox-cart')).length, 1);
  assert.match(h.messages.textContent, /Added to the sandbox bag/);
});

test('delayed avatar script completion after dismissal initializes hidden', async t => {
  const h = makeWidget(t, {noAvatarGlobal: true}); h.open(); const loader = h.root.querySelector(avatarLoaderSelector); h.window.BritesConcierge.close();
  let config;
  h.window.BritesConciergeAvatar = {create(value) {config = value; return {setState() {}, setVisible() {}, setLevel() {}, ready: Promise.resolve({mode: 'fallback'})};}};
  loader.dispatchEvent(new h.window.Event('load')); await settle();
  assert.equal(config.visible, false); assert.equal(h.panel.hidden, true); assert.equal(h.utterances.length, 0);
});

test('failed avatar script is cleaned up and a fresh explicit reopen loads it once', async t => {
  const h = makeWidget(t, {noAvatarGlobal: true}); h.open();
  const failedLoader = h.root.querySelector(avatarLoaderSelector), expressionLoader = h.root.querySelector('script[src$="/brites-concierge-expression.js"]'); assert.ok(expressionLoader, 'independent expression asset is also loading'); failedLoader.dispatchEvent(new h.window.Event('error')); await settle();
  assert.equal(failedLoader.isConnected, false); assert.equal(failedLoader.onload, null); assert.equal(failedLoader.onerror, null);
  await h.ask(); assert.equal(h.root.querySelector(avatarLoaderSelector), null, 'conversation does not retry the failed avatar asset'); assert.equal(expressionLoader.isConnected, true, 'avatar failure does not remove the independent expression loader');
  h.window.BritesConcierge.close(); h.window.BritesConcierge.open();
  const nextLoader = h.root.querySelector(avatarLoaderSelector); assert.ok(nextLoader); assert.notEqual(nextLoader, failedLoader);
  h.window.BritesConcierge.open(); h.window.dispatchEvent(new h.window.PageTransitionEvent('pageshow', {persisted: true}));
  assert.equal(h.root.querySelectorAll(avatarLoaderSelector).length, 1, 'one in-flight avatar load serves concurrent reopen/restoration'); assert.equal(h.root.querySelectorAll('script[src$="/brites-concierge-expression.js"]').length, 1, 'expression loading is independently deduplicated');
  let creates = 0;
  h.window.BritesConciergeAvatar = {create(config) {creates++; return {setState() {}, setVisible() {}, setLevel() {}, destroy() {}};}};
  nextLoader.dispatchEvent(new h.window.Event('load')); await settle();
  assert.equal(creates, 1); assert.equal(nextLoader.isConnected, false); assert.equal(h.utterances.length, 0);
});

test('a transient avatar constructor failure recovers on explicit reopen', async t => {
  const options = {createThrows: true}, h = makeWidget(t, options); h.open(); await settle();
  assert.equal(h.avatarCalls.create, 1); assert.equal(h.instances.length, 0);
  await h.ask(); assert.equal(h.avatarCalls.create, 1);
  options.createThrows = false; h.window.BritesConcierge.close(); h.window.BritesConcierge.open(); await settle();
  assert.equal(h.avatarCalls.create, 2); assert.equal(h.instances.length, 1); assert.equal(h.avatar.visible, true);
  assert.equal(h.utterances.length, 0); await h.add(); assert.equal(JSON.parse(h.window.sessionStorage.getItem('brites-sandbox-cart')).length, 1);
});

test('destroyed avatar API is recreated only on explicit reopen or open pageshow', async t => {
  const options = {stateThrows: true}, h = makeWidget(t, options); h.open(); await settle();
  const previous = h.avatar; h.input.value = 'gift'; h.input.dispatchEvent(new h.window.Event('input'));
  assert.equal(previous.destroyed, true); options.stateThrows = false;
  await h.ask(); assert.equal(h.avatarCalls.create, 1, 'ordinary conversation does not retry');
  h.window.dispatchEvent(new h.window.PageTransitionEvent('pageshow', {persisted: true})); await settle();
  assert.equal(h.avatarCalls.create, 2); assert.notEqual(h.avatar, previous); assert.equal(h.avatar.visible, true);
  h.window.BritesConcierge.close(); const retries = h.avatarCalls.retry;
  h.window.dispatchEvent(new h.window.PageTransitionEvent('pageshow', {persisted: true})); assert.equal(h.avatarCalls.retry, retries);
  h.window.BritesConcierge.open(); assert.equal(h.avatarCalls.retry, retries + 1);
  assert.equal(h.utterances.length, 0);
});

test('no browser speech support is needed for typed product actions', async t => {
  const h = makeWidget(t, {noSpeech: true}); h.open(); await h.ask(); await h.add();
  assert.equal(h.button('Read aloud'), undefined); assert.equal(h.errors.length, 0);
  assert.equal(JSON.parse(h.window.sessionStorage.getItem('brites-sandbox-cart')).length, 1);
});
test('policy answers preserve visible product ordinals and recheck them on the next shopper request', async t => {
  const h=makeWidget(t,{answer:body=>response(body.message==='Can I return it?'?{schema:1,reply:'Check the current refund policy.',question:null,preferences:fixtureAnswer.preferences,products:[],meanings:[],actions:[],policyOnly:true,policyKnowledge:{status:'verified'}}:fixtureAnswer)});
  h.open();await h.ask();const handles=fixtureAnswer.products.map(p=>p.handle);
  await h.ask('Can I return it?');assert.equal(h.root.querySelector('.status').textContent,'Shop policy checked just now.');
  const saved=JSON.parse(h.window.sessionStorage.getItem('brites-concierge-v1'));assert(saved);assert.deepEqual(saved.productHandles,handles);
  await h.ask('Open the first piece');const requests=h.network.filter(n=>n.body?.message);assert.deepEqual(requests.at(-1).body.context.productHandles,handles);assert.equal(h.errors.length,0);
});

for(const boundary of ['checkoutBoundary','budgetClarification'])test(boundary+' informational replies preserve visible ordinals without claiming a new live selection',async t=>{
  const boundaryMessage=boundary==='checkoutBoundary'?'Pay now using my saved card':'My total budget for three necklaces is $150',h=makeWidget(t,{answer:body=>response(body.message===boundaryMessage?{schema:1,reply:boundary==='checkoutBoundary'?'Complete payment yourself through the secure checkout.':'The overall budget has not been applied as an item limit.',question:null,preferences:fixtureAnswer.preferences,products:[],meanings:[],actions:[],[boundary]:true,live:false}:fixtureAnswer)});
  h.open();await h.ask();const handles=fixtureAnswer.products.map(p=>p.handle),card=h.root.querySelector('.card');await h.ask(boundaryMessage);
  assert.deepEqual(JSON.parse(h.window.sessionStorage.getItem('brites-concierge-v1')).productHandles,handles);assert.equal(h.root.querySelector('.card'),card);assert.doesNotMatch(h.root.querySelector('.status').textContent,/checked just now|live selection/i);assert.equal(h.utterances.length,0);assert.equal(h.network.filter(n=>n.url.pathname==='/api/growth/product').length,0);
  await h.ask('Open the first piece');assert.deepEqual(h.network.filter(n=>n.body?.message).at(-1).body.context.productHandles,handles);assert.equal(h.errors.length,0);
});

test('request errors show a recoverable expression without breaking ordinary shop controls', async t => {
  const h = makeWidget(t, {answer: async () => response({error: 'Synthetic service recovery'}, 503)}); h.open(); await h.ask();
  assert.equal(h.avatar.state, 'error'); assert.match(h.messages.textContent, /Synthetic service recovery/);
  assert.equal(h.input.disabled, false); assert.equal(h.button('Send').disabled, false);
  h.document.getElementById('shop-control').focus(); assert.equal(h.document.activeElement.id, 'shop-control');
  h.window.BritesConcierge.close(); assert.equal(h.avatar.visible, false);
});

test('restored open session remains open, and restored closed session stays lazy', async t => {
  const saved = {history: [{role: 'assistant', content: 'Prior conversation'}], preferences: {}, productHandles: [], uncertainVariants: [], updatedAt: Date.now()};
  const open = makeWidget(t, {saved: {...saved, open: true}}), closed = makeWidget(t, {saved: {...saved, open: false}});
  assert.equal(open.panel.hidden, false); assert.equal(open.instances.length, 1); assert.equal(open.avatar.visible, true);
  assert.equal(closed.panel.hidden, true); assert.equal(closed.instances.length, 0);
  assert.equal(open.utterances.length, 0); assert.equal(closed.utterances.length, 0);
});

test('typed shopping and confirmed cart never request microphone or camera', async t => {
  const h = makeWidget(t); h.open(); await h.ask(); await h.add();
  h.visibility.hide(true); h.window.BritesConcierge.close(); assert.deepEqual(h.devices, []);
  assert.equal(h.root.querySelectorAll('video,audio,input[type="file"]').length, 0);
  assert.ok(h.network.every(request => request.url.hostname === 'growth-sandbox.example'));
});

// These are correctness requirements beyond the normal factory fallback. A
// browser lifecycle end must silence speech even if visibilitychange is skipped.
test('pagehide hides the avatar for navigation/bfcache', async t => {
  const h = makeWidget(t); h.open(); await h.ask();
  h.window.dispatchEvent(new h.window.PageTransitionEvent('pagehide', {persisted: true}));
  assert.equal(h.avatar.visible, false); assert.equal(h.avatar.level, 0);
});

test('pageshow restores avatar only for an open visible concierge', async t => {
  const h = makeWidget(t); h.open(); await h.ask();
  h.window.dispatchEvent(new h.window.PageTransitionEvent('pagehide', {persisted: true}));
  assert.equal(h.avatar.visible, false);
  h.window.dispatchEvent(new h.window.PageTransitionEvent('pageshow', {persisted: true}));
  assert.equal(h.avatar.visible, true);
  h.window.BritesConcierge.close();
  h.window.dispatchEvent(new h.window.PageTransitionEvent('pageshow', {persisted: true}));
  assert.equal(h.avatar.visible, false);
  h.window.BritesConcierge.open(); h.visibility.hide(true);
  h.window.dispatchEvent(new h.window.PageTransitionEvent('pagehide', {persisted: true}));
  h.window.dispatchEvent(new h.window.PageTransitionEvent('pageshow', {persisted: true}));
  assert.equal(h.avatar.visible, false);
});

test('a renderer draw exception cannot block text search or sandbox cart', async t => {
  const h = makeWidget(t, {stateThrows: true}); h.open();
  h.input.value = 'A bunny gift'; h.input.dispatchEvent(new h.window.Event('input'));
  assert.equal(h.errors.length, 0, 'renderer errors must stay inside the optional avatar boundary');
  await h.ask(); await h.add();
  assert.equal(h.errors.length, 0); assert.equal(h.root.querySelectorAll('.card').length, 1);
  assert.equal(JSON.parse(h.window.sessionStorage.getItem('brites-sandbox-cart')).length, 1);
});

test('parts-only and personalized pieces hand off to the product page without a cart write', async t => {
  for (const product of [
    {...fixtureProduct, type: 'Component', partsOnly: true, title: 'Bunny Necklace Component'},
    {...fixtureProduct, title: 'Handwriting Bunny Necklace'}
  ]) {
    const answer = {...fixtureAnswer, products: [product]}, h = makeWidget(t, {answer: () => response(answer)});
    h.open(); await h.ask(); h.button('Choose options').click();
    const handoff = h.button(product.partsOnly ? 'Review details on product page' : 'Open personalization on product page');
    assert.ok(handoff); handoff.click(); await settle();
    assert.equal(h.window.sessionStorage.getItem('brites-sandbox-cart'), null);
    assert.equal(h.network.filter(request => request.url.pathname === '/api/growth/product').length, 0);
    assert.ok(h.network.some(request => request.body?.event === 'product_opened' && request.body.productId === product.id));
  }
});

test('sandbox cart requires explicit review and confirmation, while cancel remains non-mutating', async t => {
  const h = makeWidget(t); h.open(); await h.ask(); h.button('Choose options').click();
  h.button('Review adding to bag').click();
  assert.equal(h.window.sessionStorage.getItem('brites-sandbox-cart'), null);
  h.button('Cancel').click();
  assert.equal(h.window.sessionStorage.getItem('brites-sandbox-cart'), null);
  h.button('Review adding to bag').click(); h.button('Confirm add to bag').click(); await settle();
  const cart = JSON.parse(h.window.sessionStorage.getItem('brites-sandbox-cart'));
  assert.equal(cart.length, 1); assert.equal(cart[0].variantId, fixtureVariant.numericId);
});

test('a failed concierge turn can be retried safely without duplicating product or cart actions', async t => {
  let attempts = 0;
  const h = makeWidget(t, {answer: () => ++attempts === 1 ? response({error: 'Synthetic temporary outage'}, 503) : response(fixtureAnswer)});
  h.open(); await h.ask('A bunny gift');
  assert.equal(h.root.querySelectorAll('.card').length, 0); assert.match(h.messages.textContent, /temporary outage/);
  assert.equal(h.button('Send').disabled, false); assert.equal(h.window.sessionStorage.getItem('brites-sandbox-cart'), null);
  await h.ask('A bunny gift');
  assert.equal(h.root.querySelectorAll('.card').length, 1); assert.equal(attempts, 2);
  assert.equal(h.network.filter(request => request.body?.event === 'cart_requested').length, 0);
  const turns = h.network.filter(request => request.body?.message);
  assert.equal(turns.length, 2); assert.deepEqual(turns[1].body.history, [{role: 'user', content: 'A bunny gift'}]);
});
