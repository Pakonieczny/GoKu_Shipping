'use strict';
// Actual widget, storefront bridge and checked catalogue core. Synthetic native
// callbacks do not certify microphones, paid provider sessions or GPU rendering.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {JSDOM, VirtualConsole} = require('jsdom');
const Core = require('../../netlify/functions/_britesGrowth.js');
const Bridge = require('../../brites-storefront-bridge.js');
const Expression = require('../../brites-concierge-expression.js');
const widget = fs.readFileSync(require.resolve('../../brites-concierge.js'), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const settle = async () => {await new Promise(setImmediate); await new Promise(setImmediate);};
const piece = (id, handle, title, type) => ({
  id: 'gid://shopify/Product/' + id, handle, title, type,
  url: 'https://britesjewelry.com/products/' + handle,
  currency: 'USD', minPrice: 54, variantsComplete: true,
  suggestedVariantId: 'gid://shopify/ProductVariant/' + id + '1',
  variants: [{id: 'gid://shopify/ProductVariant/' + id + '1', numericId: id + '1', title: 'Sterling Silver', price: 54, available: true, options: [{name: 'Metal', value: 'Sterling Silver'}]}]
});
const necklaces = [piece('601', 'compass-necklace', 'Compass Necklace', 'Necklace'), piece('602', 'star-necklace', 'Star Necklace', 'Necklace')];
const earrings = [piece('603', 'moon-earrings', 'Moon Earrings', 'Earrings'), piece('604', 'star-earrings', 'Star Earrings', 'Earrings')];
const identity = rows => rows.map(({id, handle, title}) => ({id, handle, title}));
const speechSignal = {amplitude: .6, bands: [.08, .2, .4, .3, .1, .04], brightness: .35, valid: true};

function fixture(t, {deferCore = false, initialPage, savedState} = {}) {
  const errors = [], console = new VirtualConsole();
  console.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM('<!doctype html><body></body>', {url: 'https://preview.example/concierge-sandbox.html', pretendToBeVisual: true, runScripts: 'outside-only', virtualConsole: console});
  const w = dom.window, d = w.document, calls = {core: [], byHandle: [], controls: [], presented: [], states: [], signals: [], starts: 0, stops: 0};
  let page = clone(initialPage || {pageKind: 'collection', contextRevision: 1, discoveryRevision: 1, filter: 'earrings', search: '', sort: 'featured', loading: false, currentHandle: '', focusedHandle: '', visiblePieces: identity(earrings)});
  let config, controller, releaseCore, native = null, avatarState = 'idle', currentSignal = null;
  const script = d.createElement('script'); script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true';
  Object.defineProperty(d, 'currentScript', {get: () => script});
  Object.defineProperty(d, 'hidden', {get: () => false});
  w.matchMedia = () => ({matches: false, addEventListener() {}});
  w.HTMLElement.prototype.scrollIntoView = function () {};
  w.sessionStorage.setItem('brites-concierge-v1', JSON.stringify(savedState || {updatedAt: Date.now(), open: false, history: [], preferences: {type: page.filter === 'necklaces' ? 'necklace' : 'earrings', query: page.search}, products: page.filter === 'necklaces' ? necklaces : earrings, meanings: [], policyLinks: [], productHandles: page.visiblePieces.map(p => p.handle), selectedVariants: {}, uncertainVariants: [], pendingTurn: null}));
  const publish = () => {page.contextRevision++; d.dispatchEvent(new w.CustomEvent('brites-storefront:context'));};
  w.BritesStorefrontBridge = Bridge;
  w.BritesSandboxStorefront = {
    snapshot: () => clone(page),
    async execute(action) {
      calls.controls.push(clone(action));
      page.activeSection = action.section || (action.type === 'zoom' ? 'image' : page.activeSection);
      if (action.handle) {page.pageKind = 'product'; page.currentHandle = action.handle; page.focusedHandle = action.handle;}
      publish();
      return {ok: true, snapshot: clone(page), message: 'The requested part of the test shop is in view.'};
    },
    presentProducts(rows) {
      calls.presented.push(clone(rows)); page.discoveryRevision++; page.pageKind = 'collection'; page.filter = 'all'; page.search = ''; page.currentHandle = page.focusedHandle = ''; page.visiblePieces = identity(rows); publish();
      return {ok: true};
    }
  };
  w.BritesConciergeExpression = {create(options) {controller = Expression.create(options); return controller;}};
  w.BritesConciergeAvatar = {create(options) {
    avatarState = options.initialState;
    return {
      setState(value) {avatarState = value; calls.states.push(value); if (value !== 'speaking') currentSignal = null;},
      setSpeechSignal(value) {currentSignal = value ? clone(value) : null; calls.signals.push(currentSignal);},
      setLevel() {}, setInputSignal() {}, setExpression() {}, setEmotion() {}, setVisible() {}, setPaused() {}, setFloating() {}, triggerGreeting() {}, cancelPerformance() {}, clearFocus() {}, clearProduct() {}, focusProduct() {}, showProduct() {return true;}, cue() {}, retry() {}, destroy() {}
    };
  }};
  const client = {state: 'listening', outputMeterState: 'ready', playbackBlocked: false, start: async () => {calls.starts++; return true;}, stop: async () => {calls.stops++; native = null;}, dispose: async () => {}, updateContext() {}, get currentOutput() {return native ? clone(native) : null;}, get currentInput() {return null;}};
  w.BritesConciergeVoice = {create(options) {config = options; return client;}};
  const service = {saveProducts: async () => {}, productIssues: async () => [], research: async () => [], storySupplements: async () => []};
  const shopify = {search: async () => {throw Error('Displayed meanings must use exact checked handles.');}, byHandle: async handle => {calls.byHandle.push(handle); const p = [...necklaces, ...earrings].find(p => p.handle === handle); return p ? {...clone(p), description: p.title, tags: [], options: [], images: [], checkedAt: Date.now()} : null;}};
  w.fetch = async (raw, init = {}) => {
    const body = init.body ? JSON.parse(init.body) : {};
    if (!body.message) return {ok: true, json: async () => ({enabled: true})};
    calls.core.push({body: clone(body), signal: init.signal});
    if (deferCore) return new Promise(resolve => {releaseCore = answer => resolve({ok: true, json: async () => clone(answer)});});
    const answer = await Core.concierge({service, shopify, ...body, now: () => Date.now()});
    return {ok: true, json: async () => clone(answer)};
  };
  w.eval(widget);
  const root = d.querySelector('brites-concierge').shadowRoot;
  const button = text => [...root.querySelectorAll('button')].find(n => n.textContent.trim() === text);
  publish();
  const h = {
    w, d, root, calls, errors, client, button,
    saved: () => JSON.parse(w.sessionStorage.getItem('brites-concierge-v1')),
    get avatarState() {return avatarState;}, get currentSignal() {return currentSignal;}, get page() {return clone(page);},
    async openTyping() {w.BritesConcierge.open(); await settle(); button('Type instead').click(); await settle();},
    submit(message) {root.querySelector('form input').value = message; root.querySelector('form').dispatchEvent(new w.Event('submit', {bubbles: true, cancelable: true}));},
    async openVoice() {w.BritesConcierge.open(); await settle(); button('Talk to me').click(); await settle(); root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load')); await settle();},
    manualEarrings() {page.pageKind = 'collection'; page.discoveryRevision++; page.filter = 'earrings'; page.search = ''; page.currentHandle = page.focusedHandle = ''; page.visiblePieces = identity(earrings); publish();},
    release(answer) {assert.ok(releaseCore, 'a checked core request is actually pending'); releaseCore(answer);},
    setSpeaking() {
      const turn = {inputItemId: 'input-1', turnVersion: 1, currentTurn: true};
      config.onSpeechStarted({itemId: turn.inputItemId, turnVersion: turn.turnVersion, reason: 'speech'});
      config.onTranscript({role: 'user', itemId: turn.inputItemId, turnVersion: turn.turnVersion, currentTurn: true, text: 'Thank you.', final: true});
      native = {...turn, responseId: 'response-1', itemId: 'output-1', playing: true};
      config.onPlaybackState(clone(native)); client.state = 'speaking'; config.onState('speaking'); config.onOutputMeterState('ready');
      config.onLevel({...turn, responseId: native.responseId, itemId: native.itemId, input: 0, output: speechSignal.amplitude, outputSignal: clone(speechSignal)});
    }
  };
  t.after(() => {try {w.BritesConcierge.close();} catch {} controller?.destroy(); w.close();});
  return h;
}

const necklacePage = {pageKind: 'collection', contextRevision: 1, discoveryRevision: 1, filter: 'necklaces', search: 'bunny', sort: 'featured', loading: false, currentHandle: '', focusedHandle: '', visiblePieces: identity(necklaces)};
const staleAnswer = {live: true, reply: 'An obsolete necklace reply.', preferences: {type: 'necklace', query: 'bunny'}, products: necklaces, meanings: []};

test('manual completed discovery clears the superseded request durably and releases the thinking UI', async t => {
  const h = fixture(t, {deferCore: true, initialPage: necklacePage}); await h.openTyping(); h.submit('Compare these pieces'); await settle();
  assert.equal(h.saved().pendingTurn.message, 'Compare these pieces'); assert.equal(h.avatarState, 'thinking'); assert.equal(h.root.querySelector('form input').disabled, true);
  h.manualEarrings(); await settle();
  assert.equal(h.calls.core[0].signal.aborted, true); assert.equal(h.saved().pendingTurn, null); assert.equal(h.saved().preferences.type, 'earrings'); assert.equal(h.avatarState, 'idle'); assert.equal(h.root.querySelector('form input').disabled, false);
  h.release(staleAnswer); for (let i = 0; i < 4; i++) await settle();
  assert.equal(h.calls.presented.length, 0); assert.equal(h.saved().pendingTurn, null); assert.equal(h.saved().preferences.type, 'earrings'); assert.doesNotMatch(h.saved().latestReply || '', /obsolete necklace/);
  const reloaded = fixture(t, {initialPage: h.page, savedState: h.saved()}); reloaded.w.BritesConcierge.open(); await settle();
  assert.doesNotMatch(reloaded.root.textContent, /last request didn.t finish|previous live check was interrupted/i); assert.equal(reloaded.saved().preferences.type, 'earrings'); assert.equal(h.errors.length + reloaded.errors.length, 0);
});

test('settling a superseded typed lookup preserves an independently playing current native output', async t => {
  const h = fixture(t, {deferCore: true, initialPage: necklacePage}); await h.openTyping(); h.submit('Compare these pieces'); await settle(); await h.openVoice(); h.setSpeaking();
  assert.equal(h.avatarState, 'speaking'); assert.deepEqual(h.currentSignal, speechSignal); const stops = h.calls.stops;
  h.manualEarrings(); await settle();
  assert.equal(h.saved().pendingTurn, null); assert.equal(h.avatarState, 'speaking'); assert.deepEqual(h.currentSignal, speechSignal); assert.equal(h.client.currentOutput.playing, true); assert.equal(h.calls.stops, stops);
  h.release(staleAnswer); await settle(); assert.equal(h.avatarState, 'speaking'); assert.deepEqual(h.currentSignal, speechSignal); assert.equal(h.calls.presented.length, 0);
});

for (const message of ['Tell me the meaning of both earrings.', 'Tell me the symbolism of each.', 'Tell me the history of them.', 'Tell me their meaning.']) {
  test('plural collection semantics use checked current handles: ' + message, async t => {
    const h = fixture(t); await h.openTyping(); h.submit(message); for (let i = 0; i < 5; i++) await settle();
    assert.equal(h.calls.controls.length, 0); assert.equal(h.calls.core.length, 1); assert.deepEqual(h.calls.core[0].body.context.productHandles, earrings.map(p => p.handle)); assert.deepEqual(h.calls.byHandle, earrings.map(p => p.handle));
    assert.deepEqual(h.saved().products.map(p => p.handle), earrings.map(p => p.handle)); assert.equal(h.saved().pendingTurn, null); assert.equal(h.errors.length, 0);
  });
}

for (const [message, type] of [['Open the meaning of Moon Earrings.', 'highlight'], ['Scroll to the meaning of Moon Earrings.', 'scroll'], ['Highlight the meaning of Moon Earrings.', 'highlight'], ['Zoom the image of Moon Earrings.', 'zoom']]) {
  test('explicit named page control stays on the bridge: ' + message, async t => {
    const h = fixture(t); await h.openTyping(); h.submit(message); for (let i = 0; i < 3; i++) await settle();
    assert.equal(h.calls.core.length, 0); assert.equal(h.calls.controls.length, 1); assert.equal(h.calls.controls[0].type, type); assert.equal(h.calls.controls[0].handle, earrings[0].handle); assert.equal(h.errors.length, 0);
  });
}

for (const message of ['Please open the meaning of both earrings.', 'Would you scroll to the symbolism of each?', 'Could you highlight their meaning?', 'Zoom the image of both earrings.']) {
  test('explicit plural page control is not silently changed into a catalogue answer: ' + message, async t => {
    const h = fixture(t); await h.openTyping(); h.submit(message); for (let i = 0; i < 4; i++) await settle();
    assert.equal(h.calls.core.length, 0, 'explicit page controls retain bridge validation and target ambiguity checks'); assert.equal(h.calls.byHandle.length, 0); assert.equal(h.errors.length, 0);
  });
}
