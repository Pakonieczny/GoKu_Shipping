'use strict';

// Independent completion checks. The host, bridge, widget, guide and realtime
// client are production code. Only transport, audio hardware, avatar pixels
// and scrolling are synthetic. These tests cannot certify physical speech,
// GPU appearance, mobile touch or live Shopify installation.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM, VirtualConsole } = require('jsdom');
const project = process.env.BRITES_ADVERSARIAL45_PROJECT || path.resolve(__dirname, '../..');
const Voice = require(path.join(project, 'brites-concierge-voice.js'));
const read = name => fs.readFileSync(path.join(project, name), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const settle = async () => { await new Promise(setImmediate); await new Promise(setImmediate); };
function deferred() { let resolve; return { promise: new Promise(done => { resolve = done; }), resolve }; }
async function eventually(predicate, reason) {
  for (let n = 0; n < 60; n++) { if (predicate()) return; await settle(); }
  assert.ok(predicate(), reason);
}

function product(id, title, motif, type = 'Earrings', materials = ['Sterling Silver', '14/20 Gold Filled']) {
  const handle = 'independent-completion45-' + id;
  const axis = type === 'Necklace' ? 'Chain Length' : 'Hoop Diameter';
  const values = type === 'Necklace' ? ['17 inch', '21 inch'] : ['9mm', '11mm'];
  return {
    id: 'gid://shopify/Product/' + id, handle, title, type,
    url: 'https://britesjewelry.com/products/' + handle,
    description: 'This published ' + motif + ' design has a charm measuring 13 mm wide and 8 mm high.',
    tags: ['motif:' + motif], currency: 'USD', image: 'https://cdn.shopify.com/' + handle + '.jpg',
    images: [{ url: 'https://cdn.shopify.com/' + handle + '.jpg', alt: title }],
    detailState: 'checked', checkedAt: Date.now(), variantsComplete: true,
    options: [{ name: 'Metal Choice', values: materials }, { name: axis, values }],
    variants: materials.flatMap((metal, m) => values.map((value, n) => ({
      id: 'gid://shopify/ProductVariant/' + (id * 10 + m * 2 + n + 1),
      numericId: String(id * 10 + m * 2 + n + 1),
      title: metal + ' / ' + value, price: 43 + m * 27 + n * 6, available: true,
      options: [{ name: 'Metal Choice', value: metal }, { name: axis, value }]
    })))
  };
}
function catalogue() {
  return [
    product(74401, 'Fox Portrait Stud Earrings', 'fox'),
    product(74402, 'Fox Outline Stud Earrings', 'fox'),
    product(74403, 'Rabbit Drop Earrings', 'rabbit'),
    product(74404, 'Fox Pendant Necklace', 'fox', 'Necklace'),
    product(74405, 'Butterfly Pendant Necklace', 'butterfly', 'Necklace'),
    product(74406, 'Leaf Drop Earrings', 'leaf'),
    product(74407, 'Moon Pendant Necklace', 'moon', 'Necklace')
  ];
}

async function fixture(t, { rows = catalogue(), nativeVoice = false, manualClock = false, confirmedStop = true } = {}) {
  const errors = [], console = new VirtualConsole();
  console.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM(read('concierge-sandbox.html'), { url: 'https://preview.example/concierge-sandbox.html', runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole: console });
  const w = dom.window, d = w.document, requests = [], packets = [], hostResults = [];
  let client, channel, config, turn = 0, conciergeRead = null;
  let clock = Date.now(), timerId = 0;
  const timers = new Map();
  if (manualClock) {
    w.Date.now = () => clock;
    w.setTimeout = (fn, ms = 0) => { const id = ++timerId; timers.set(id, { at: clock + Number(ms), fn }); return id; };
    w.clearTimeout = id => timers.delete(id);
  }
  delete d.body.dataset.catalogueSeed;
  w.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  w.HTMLElement.prototype.scrollIntoView = function () {};
  w.scrollTo = () => {};
  w.fetch = async (raw, init = {}) => {
    const url = new URL(String(raw), w.location.href);
    requests.push({ url, init });
    let body;
    if (url.pathname === '/api/growth/catalogue') body = { live: true, checkedAt: Date.now(), products: rows, pageInfo: { hasNextPage: false, endCursor: null } };
    else if (url.pathname === '/api/growth/inventory') body = { live: true, checkedAt: Date.now(), products: rows, inventory: { schema: 1, total: rows.length, offset: 0, limit: 24, loaded: rows.length, detailsLoaded: rows.length, ready: true, partial: false, expiresAt: Date.now() + 300000 }, pageInfo: { hasNextPage: false, nextOffset: null } };
    else if (url.pathname === '/api/growth/product') body = { live: true, checkedAt: Date.now(), product: rows.find(p => p.handle === url.searchParams.get('handle')) };
    else if (url.pathname === '/api/growth/storefront-services') body = { schema: 1, guidance: {}, conflicts: [], offers: { items: [] } };
    else if (url.pathname === '/api/growth/knowledge') body = { live: true, checkedAt: Date.now(), meanings: [] };
    else if (url.pathname === '/api/growth/events') body = { ok: true };
    else if (url.pathname === '/api/concierge') {
      const input = JSON.parse(init.body || '{}');
      body = conciergeRead && input.message ? await conciergeRead(input) : { live: true, reply: 'INDEPENDENT45_REMOTE_FALLBACK', products: [], meanings: [], preferences: {}, preserveSelection: true };
    } else if (url.pathname === '/api/concierge-voice') {
      const action = JSON.parse(init.body || '{}').action;
      body = action === 'start' ? { sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n', stopToken: 'independent45-synthetic', maxDurationMs: 120000 } : { ok: true, enabled: true, nativeAudio: true, publicDemo: true, stopped: action === 'stop' && confirmedStop };
    } else throw Error('Unexpected independent request ' + url.pathname);
    return { ok: true, json: async () => clone(body) };
  };
  for (const name of ['brites-catalogue-intents.js', 'brites-concierge-shopping-guide.js', 'brites-storefront-bridge.js', 'brites-concierge-voice-actions.js', 'concierge-sandbox.js']) w.eval(read(name));
  await settle();
  const store = w.BritesSandboxStorefront;
  await store.preloadInventory(); await settle();
  if (nativeVoice) {
    w.HTMLMediaElement.prototype.play = async function () {};
    w.HTMLMediaElement.prototype.pause = function () {};
    class Peer {
      constructor() { this.iceGatheringState = 'complete'; }
      addTrack() {} close() {}
      createDataChannel() { channel = { readyState: 'connecting', send: value => packets.push(JSON.parse(value)), close() {} }; return channel; }
      async createOffer() { return { type: 'offer', sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n' }; }
      async setLocalDescription(value) { this.localDescription = value; }
      async setRemoteDescription() { channel.readyState = 'open'; channel.onopen(); }
    }
    const runtime = {
      document: d, location: w.location,
      navigator: { mediaDevices: { async getUserMedia() { const track = { readyState: 'live', stop() { this.readyState = 'ended'; }, addEventListener() {}, removeEventListener() {} }; return { getTracks: () => [track], getAudioTracks: () => [track] }; } } },
      RTCPeerConnection: Peer, AbortController: w.AbortController, fetch: w.fetch,
      setTimeout: w.setTimeout.bind(w), clearTimeout: w.clearTimeout.bind(w), addEventListener: w.addEventListener.bind(w), removeEventListener: w.removeEventListener.bind(w)
    };
    w.BritesConciergeVoice = { publicContext: Voice.publicContext, create(options) { config = options; client = Voice.create({ ...options, runtime, greeting: false, onFinalizedTurn: async turn => { const result = await options.onFinalizedTurn(turn); hostResults.push({ turn, result }); return result; } }); return client; } };
  }
  w.BritesConciergeAvatar = { create() { return { setState() {}, setEmotion() {}, setVisible() {}, setPaused() {}, setLevel() {}, setFloating() {}, cancelPerformance() {}, triggerGreeting() {}, cue() {}, focusProduct() {}, clearFocus() {}, showProduct() {}, clearProduct() {}, destroy() {} }; } };
  const script = d.createElement('script'); script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true';
  Object.defineProperty(d, 'currentScript', { get: () => script });
  w.eval(read('brites-concierge.js')); await settle(); w.BritesConcierge.open({ focus: false }); await settle();
  const root = d.querySelector('brites-concierge').shadowRoot;
  t.after(async () => { w.BritesConcierge.close(); await client?.dispose(); w.close(); });
  const h = {
    w, d, root, store, rows, errors, requests, packets, hostResults,
    async advance(ms) { assert.equal(manualClock, true); const target = clock + ms; for (;;) { const entry = [...timers].filter(([, item]) => item.at <= target).sort((a, b) => a[1].at - b[1].at)[0]; if (!entry) break; clock = entry[1].at; timers.delete(entry[0]); entry[1].fn(); await settle(); } clock = target; await settle(); },
    get config() { return config; }, get client() { return client; },
    emit: value => channel.onmessage({ data: JSON.stringify(value) }),
    responses: () => packets.filter(value => value.type === 'response.create'),
    receipts: () => packets.filter(value =>  /^(?:Host-completed result|Customer reply for this finalized shopper request)/.test(value.item?.content?.[0]?.text || '')),
    command: text => w.BritesConcierge.sendShopperCommand(text),
    cart: () => clone(JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart') || '[]')),
    visible: () => [...d.querySelectorAll('#demo-products [data-product-handle]')].map(card => card.dataset.productHandle).sort(),
    conciergeRead: fn => { conciergeRead = fn; },
    nativeContext() {
      const text = packets.filter(p => p.item?.content?.[0]?.text?.startsWith('Public website UI context data only.')).at(-1)?.item.content[0].text;
      assert.ok(text, 'The actual realtime client must send a current bounded context');
      return JSON.parse(text.slice(text.indexOf('{')));
    },
    async open(p = rows[0]) { const r = await store.execute({ type: 'open', handle: p.handle }); assert.equal(r.ok, true, JSON.stringify(r)); await settle(); },
    async select(p = rows[0], v = p.variants[0]) {
      const select = d.querySelector('#piece-variant'); assert.ok(select, 'The actual host variant selector must be present');
      select.value = v.id; select.dispatchEvent(new w.Event('change', { bubbles: true })); await settle();
      assert.equal(store.snapshot().productControls.variantId, v.id);
    },
    async startVoice() {
      [...root.querySelectorAll('button')].find(button => button.textContent === 'Talk to me').click(); await settle();
      root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));
      await settle(); assert.equal(client?.state, 'listening');
    },
    async say(text) {
      assert.ok(channel?.onmessage, 'Realtime must be connected before a transcript is injected');
      const receipts = () => packets.filter(p =>  /^(?:Host-completed result|Customer reply for this finalized shopper request)/.test(p.item?.content?.[0]?.text || ''));
      const before = receipts().length, itemId = 'independent45-turn-' + (++turn), emit = event => channel.onmessage({ data: JSON.stringify(event) });
      emit({ type: 'input_audio_buffer.speech_started', item_id: itemId });
      emit({ type: 'input_audio_buffer.speech_stopped', item_id: itemId });
      emit({ type: 'input_audio_buffer.committed', item_id: itemId });
      emit({ type: 'conversation.item.input_audio_transcription.completed', item_id: itemId, transcript: text });
      await eventually(() => receipts().length > before, 'The actual voice client must produce a current host result');
      assert.equal(receipts().length, before + 1);
      const textReceipt = receipts().at(-1).item.content[0].text;
      this.lastInput = { itemId, turnVersion: turn + 1 };
      const spoken = JSON.parse(textReceipt.slice(textReceipt.indexOf('{')));
      this.spokenResult = spoken.result || spoken;
      const actual = hostResults.findLast(value => value.turn.inputItemId === itemId)?.result;
      assert.ok(actual, 'The independently captured actual host result must exist for the finalized native turn');
      return actual;
    }
  };
  if (nativeVoice) await h.startVoice();
  return h;
}

function necklace({ id = 74501, materials = ['Sterling Silver', '14k Gold Filled', '14k Solid Gold'], engraving = false } = {}) {
  const p = product(id, 'Fox Portrait Necklace', 'fox', 'Necklace', materials);
  p.options[1].values = ['16 inches', '18 inches'];
  if (engraving) p.options.push({ name: 'Engraving', values: ['No Engraving', 'Engraved'] });
  p.variants = materials.flatMap((metal, m) => ['16 inches', '18 inches'].flatMap((length, n) => (engraving ? ['No Engraving', 'Engraved'] : [null]).map((wording, e) => ({
    id: 'gid://shopify/ProductVariant/' + (id * 100 + m * 8 + n * 2 + e + 1),
    numericId: String(id * 100 + m * 8 + n * 2 + e + 1),
    title: [metal, length, wording].filter(Boolean).join(' / '),
    price: 45 + m * 70 + n * 6 + e * 8, available: true,
    options: [{ name: 'Metal Choice', value: metal }, { name: 'Chain Length', value: length }].concat(wording ? [{ name: 'Engraving', value: wording }] : [])
  }))));
  return p;
}

const INTERNAL_COPY = /\b(?:backend|postconditions?|control_storefront|completedActions|stepResults|inputItemId|turnVersion|no further action(?:s)? (?:was|were) completed|completed \d+ (?:of \d+ )?(?:requested )?actions|remaining website actions|motif (?:parser|resolver|metadata))\b/i;
function shopperCopy(result) {
  const reply = result.reply || result.error;
  assert.equal(typeof reply, 'string', 'Every handled website request needs a shopper-readable reply');
  assert.ok(reply.trim(), 'A handled request cannot silently complete');
  assert.doesNotMatch(reply, INTERNAL_COPY, 'Customer copy must not explain internal processing');
  return reply;
}
function currentChoices(h) { return clone(h.store.snapshot().productControls.selectedOptions); }
function expectedChoices(p, v) { return clone(v.options).sort((a, b) => a.name.localeCompare(b.name)); }
function sortedChoices(h) { return currentChoices(h).sort((a, b) => a.name.localeCompare(b.name)); }
async function chosen(h, p, { quantity = 1, engraved = false } = {}) {
  await h.open(p);
  for (const words of ['Select 14k Solid Gold', 'Select 16 inches'].concat(engraved ? ['Select Engraved'] : p.options.some(g => g.name === 'Engraving') ? ['Select No Engraving'] : [])) {
    const result = await h.say(words); assert.equal(result.ok, true, JSON.stringify(result)); shopperCopy(result);
  }
  if (quantity !== 1) { const result = await h.say('Set quantity to ' + quantity); assert.equal(result.ok, true, JSON.stringify(result)); }
  assert.equal(h.store.snapshot().productControls.quantity, quantity);
  const variant = p.variants.find(v => v.options.every(o => o.value === (o.name === 'Metal Choice' ? '14k Solid Gold' : o.name === 'Chain Length' ? '16 inches' : engraved ? 'Engraved' : 'No Engraving')));
  assert.equal(h.store.snapshot().productControls.variantId, variant.id);
  return variant;
}

for (const phrase of ['Take me to the homepage', 'Take me home', 'Go to the home page', 'Open the homepage', 'Please take me back to the homepage']) test('literal finalized native home request changes the real host route: ' + phrase, async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true });
  await h.open(p);
  const result = await h.say(phrase);
  assert.equal(result.ok, true, JSON.stringify(result)); shopperCopy(result);
  assert.notEqual(h.store.snapshot().pageKind, 'product', 'The current listing must actually close');
  assert.equal(h.store.snapshot().currentHandle, '', 'Home must not leave an active product');
  assert.equal(h.w.location.search.includes('product='), false, 'The actual URL must leave its product route');
  assert.ok(h.d.querySelector('#demo-products [data-product-handle]'), 'Home/catalogue must be rendered again');
  assert.deepEqual(h.cart(), []); assert.deepEqual(h.errors, []);
});

for (const phrase of [
  'Select a 16 inch solid gold necklace for me',
  'Can you select 16 inch solid gold for this necklace',
  'Choose this necklace in solid gold at 16 inches',
  'Make this necklace 16 inches in solid gold'
]) test('one natural native request selects two exact published options without searching: ' + phrase, async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true });
  await h.open(p);
  const input = h.d.querySelector('[aria-label="Quantity of this exact piece"]'); assert.ok(input);
  input.value = '3'; input.dispatchEvent(new h.w.Event('change', { bubbles: true })); await settle();
  const result = await h.say(phrase), exact = p.variants.find(v => v.title === '14k Solid Gold / 16 inches');
  assert.equal(result.ok, true, JSON.stringify(result)); shopperCopy(result);
  assert.equal(h.store.snapshot().pageKind, 'product'); assert.equal(h.store.snapshot().currentHandle, p.handle);
  assert.deepEqual(sortedChoices(h), expectedChoices(p, exact));
  assert.equal(h.d.querySelector('#piece-variant').value, exact.id, 'The real native selector must resolve the exact variant');
  assert.equal(h.store.snapshot().productControls.quantity, 3, 'An option request must preserve the shopper quantity');
  assert.equal(h.d.querySelector('[aria-label="Quantity of this exact piece"]').value, '3');
  assert.deepEqual(h.cart(), []);
  assert.equal(h.requests.filter(r => r.url.pathname === '/api/concierge' && JSON.parse(r.init.body || '{}').message).length, 0, 'A current-product setter must not fall through to motif discovery');
});

test('two published solid-gold karats require a shopper clarification, preserving current selections and cart', async t => {
  const p = necklace({ materials: ['Sterling Silver', '10k Solid Gold', '14k Solid Gold'] }), h = await fixture(t, { rows: [p], nativeVoice: true });
  await h.open(p); await h.select(p, p.variants[0]);
  const before = currentChoices(h), result = await h.say('Select a 16 inch solid gold necklace for me'), copy = shopperCopy(result);
  assert.equal(result.ok, false); assert.deepEqual(currentChoices(h), before);
  assert.match(copy, /10k|14k/i, 'The guide must ask about actual published karats');
  assert.match(copy, /(?:which|choose|prefer)/i, 'The ambiguity must become a clear shopper question');
  assert.deepEqual(h.cart(), []);
});

test('unpublished length cannot quietly select another exact option or add a substitute', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true });
  await h.open(p); await h.select(p, p.variants[0]);
  const result = await h.say('Select this necklace in 17 inch solid gold'), copy = shopperCopy(result);
  assert.equal(result.ok, false); assert.doesNotMatch(copy, /(?:selected|changed|set to) (?:14k )?solid gold.*17/i);
  assert.equal(h.store.snapshot().productControls.selectedOptions.some(o => o.name === 'Chain Length' && o.value === '17 inches'), false);
  assert.deepEqual(h.cart(), []);
});

test('literal native controls update real dropdown, quantity, exact add and cart, then target only the requested bag line', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true }), exact = await chosen(h, p, { quantity: 2 });
  const add = await h.say('Add this exact piece to my cart'); assert.equal(add.ok, true, JSON.stringify(add)); shopperCopy(add);
  assert.equal(h.cart().length, 1); assert.equal(h.cart()[0].variantId, exact.numericId); assert.equal(h.cart()[0].quantity, 2);
  assert.deepEqual(clone(h.cart()[0].variantOptions).sort((a, b) => a.name.localeCompare(b.name)), expectedChoices(p, exact));
  const bag = await h.say('Open my cart'); assert.equal(bag.ok, true, JSON.stringify(bag)); shopperCopy(bag);
  assert.equal(h.store.snapshot().pageKind, 'bag');
  assert.ok(h.d.querySelector('[data-bag-line]'), 'Cart opening must render its actual line controls');
  const quantity = await h.say('Change the quantity of the first item in my cart to 3'); assert.equal(quantity.ok, true, JSON.stringify(quantity)); shopperCopy(quantity);
  assert.equal(h.cart()[0].quantity, 3); assert.equal(h.d.querySelector('[data-bag-line] input[type="number"]').value, '3');
  const removal = await h.say('Remove the first item from my cart'); assert.equal(removal.ok, true, JSON.stringify(removal)); shopperCopy(removal);
  assert.equal(h.cart().length, 0); assert.equal(h.d.querySelectorAll('[data-bag-line]').length, 0);
});

test('voice add waits for every required option rather than guessing a length, metal or engraving choice', async t => {
  const p = necklace({ engraving: true }), h = await fixture(t, { rows: [p], nativeVoice: true });
  await h.open(p); await h.say('Select 14k Solid Gold');
  const result = await h.say('Add this to my cart'), copy = shopperCopy(result);
  assert.notEqual(result.ok, true); assert.deepEqual(h.cart(), []);
  assert.match(copy, /(?:choose|select|length|engraving)/i);
  assert.deepEqual(currentChoices(h), [{ name: 'Metal Choice', value: '14k Solid Gold' }]);
});

test('duplicate native ASR and model replay cannot add the chosen quantity twice', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true }); await chosen(h, p, { quantity: 2 });
  await h.say('Add this exact piece to my cart'); const before = h.cart(), input = h.lastInput;
  h.emit({ type: 'conversation.item.input_audio_transcription.completed', item_id: input.itemId, transcript: 'Add this exact piece to my cart' });
  const repeated = await h.config.onTool({ type: 'add', handle: p.handle, variantId: h.store.snapshot().productControls.variantId }, { name: 'control_storefront', inputItemId: input.itemId, turnVersion: input.turnVersion, currentTurn: true, responseId: 'adversarial45-replayed-add' });
  await settle(); assert.equal(repeated.alreadyCompleted, true); assert.deepEqual(h.cart(), before);
});

test('fresh exact variant price change refuses native add and preserves every pre-existing bag line', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true }); await chosen(h, p);
  await h.say('Add this exact piece to my cart'); const before = h.cart();
  const id = h.store.snapshot().productControls.variantId; p.variants.find(v => v.id === id).price += 7;
  const result = await h.say('Add this exact piece to my cart'); assert.notEqual(result.ok, true); shopperCopy(result);
  assert.deepEqual(h.cart(), before, 'The earlier purchased choice must survive a refused fresh add');
});

test('a finalized native add cannot start an owner generation before its actual fresh host verification finishes', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true }), variant = await chosen(h, p, { quantity: 2 });
  const gate = deferred(), fetch = h.w.fetch, itemId = 'independent45-held-native-add'; let heldReads = 0;
  h.w.fetch = async (raw, init) => {
    const url = new URL(String(raw), h.w.location.href);
    if (url.pathname === '/api/growth/product' && url.searchParams.get('handle') === p.handle) { heldReads++; await gate.promise; }
    return fetch(raw, init);
  };
  t.after(() => gate.resolve());
  const responses = h.responses().length, receipts = h.receipts().length, completed = h.hostResults.length;
  for (const type of ['input_audio_buffer.speech_started', 'input_audio_buffer.speech_stopped', 'input_audio_buffer.committed']) h.emit({ type, item_id: itemId });
  h.emit({ type: 'conversation.item.input_audio_transcription.completed', item_id: itemId, transcript: 'Add this exact piece to my cart' });
  await eventually(() => heldReads === 1, 'The actual host must reach its fresh exact-choice check');
  assert.equal(h.responses().length, responses, 'The configured manual response writer must not generate an explanation while the local host check is pending');
  assert.equal(h.receipts().length, receipts); assert.equal(h.hostResults.length, completed); assert.deepEqual(h.cart(), []);
  assert.equal(h.store.snapshot().productControls.variantId, variant.id); assert.equal(h.store.snapshot().productControls.quantity, 2);
  gate.resolve(); await eventually(() => h.receipts().length === receipts + 1, 'Only the completed actual host result may request the native reply');
  assert.equal(h.responses().length, responses + 1); assert.equal(h.hostResults.length, completed + 1);
  const actual = h.hostResults.at(-1); assert.equal(actual.turn.inputItemId, itemId); assert.equal(actual.result.ok, true); shopperCopy(actual.result);
  const response = h.responses().at(-1).response; assert.equal(response.tool_choice, 'none'); assert.ok(response.instructions.includes(JSON.stringify(actual.result.reply)));
  const text = h.receipts().at(-1).item.content[0].text, spoken = JSON.parse(text.slice(text.indexOf('{')));
  assert.deepEqual(Object.keys(spoken), ['reply']); assert.equal(spoken.reply, actual.result.reply); assert.doesNotMatch(text, INTERNAL_COPY);
  assert.equal(h.cart().length, 1); assert.equal(h.cart()[0].variantId, variant.numericId); assert.equal(h.cart()[0].quantity, 2);
  // Packet timing is observable here; provider audio words and physical sound
  // are synthetic, so this cannot establish audible censorship or playback.
});

test('a raw duplicated storefront identity cannot become verified facts through the finalized native cache path', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true }), actual = h.store;
  h.w.BritesSandboxStorefront = { ...actual, snapshot() {
    const page = actual.snapshot();
    const duplicate = page.visiblePieces.find(piece => piece.handle === p.handle);
    assert.ok(duplicate, 'The actual host must currently display the target identity');
    return { ...page, visiblePieces: [...page.visiblePieces, clone(duplicate)] };
  } };
  h.d.dispatchEvent(new h.w.CustomEvent('brites-storefront:context')); await settle();
  const result = await h.say('What metal is used for Fox Portrait Necklace?');
  assert.notEqual(result.verified, true, 'Sanitizer deduplication must not mint a unique checked identity from an ambiguous raw host cohort');
  assert.notEqual(result.productFacts?.status, 'verified'); assert.equal(result.ok, false); shopperCopy(result);
  assert.deepEqual(h.cart(), []); assert.equal(actual.snapshot().pageKind, 'collection');
  assert.equal(h.requests.some(request => request.url.pathname === '/api/concierge' && JSON.parse(request.init.body || '{}').message), false, 'An ambiguous current identity should ask for a unique piece without model discovery');
});

test('literal private engraving reaches the visible field and exact saved cart line without entering spoken receipts or history', async t => {
  const p = necklace({ engraving: true }), h = await fixture(t, { rows: [p], nativeVoice: true }); await chosen(h, p, { engraved: true });
  const literal = 'PRIVATE_45 Always & forever — Mia';
  const set = await h.say('Fill my engraving with ' + literal); assert.equal(set.ok, true, JSON.stringify(set)); shopperCopy(set);
  assert.equal(h.d.querySelector('textarea[name="test-engraving-preview"]').value, literal);
  const add = await h.say('Add this exact piece to my cart'); assert.equal(add.ok, true, JSON.stringify(add)); shopperCopy(add);
  assert.equal(h.cart().length, 1);
  assert.ok(JSON.stringify(h.cart()[0]).includes(literal), 'Chosen engraving must travel with the exact added cart line');
  assert.doesNotMatch(JSON.stringify(h.receipts()), /PRIVATE_45/);
  assert.doesNotMatch(h.w.sessionStorage.getItem('brites-concierge-v1') || '', /PRIVATE_45/);
  assert.doesNotMatch(JSON.stringify(h.nativeContext()), /PRIVATE_45/);
});

test('unsupported ordinary control language still receives shopper wording without internal action counts', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true });
  const result = await h.say('Open Fox Portrait Necklace then select Platinum');
  assert.equal(h.store.snapshot().currentHandle, p.handle);
  assert.equal(result.ok, false); shopperCopy(result);
  assert.match(result.reply || result.error, /(?:platinum|available|choose|option)/i);
});

test('the speech provider receives shopper wording and no raw host control plans, snapshots or action diagnostics', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true });
  await h.say('Open Fox Portrait Necklace then select Platinum');
  const data = JSON.stringify(h.spokenResult);
  assert.doesNotMatch(data, /completedActions|stepResults|contextRevision|productControls|controlVersion|\"snapshot\"|\"action\"|NO_ACTION_AUTHORITY/i, 'Raw host execution data must not be sent for speech generation');
  shopperCopy(h.spokenResult);
  const response = h.responses().at(-1).response;
  assert.equal(response.tool_choice, 'none');
  assert.match(response.instructions, /(?:shopper|customer)/i);
  assert.doesNotMatch(response.instructions, /mention only its actual completedActions/i, 'The provider must not be invited to explain internal action plans');
});

test('one spoken exact cart length edit changes its real dropdown and variant while preserving quantity and every other line', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true }); await chosen(h, p, { quantity: 2 });
  await h.say('Add this exact piece to my cart'); await h.say('Add this exact piece to my cart'); await h.say('Open my cart');
  const before = h.cart(), ids = clone(h.store.snapshot().bagControls.lines.map(line => line.lineId));
  const result = await h.say('Change the chain length of the first item in my cart to 18 inches'); assert.equal(result.ok, true, JSON.stringify(result)); shopperCopy(result);
  const exact = p.variants.find(v => v.title === '14k Solid Gold / 18 inches'), after = h.cart();
  assert.equal(after[0].variantId, exact.numericId); assert.equal(after[0].quantity, 2); assert.equal(after[0].price, exact.price);
  assert.deepEqual(clone(after[0].variantOptions).sort((a, b) => a.name.localeCompare(b.name)), expectedChoices(p, exact));
  assert.deepEqual(after[1], before[1]); assert.deepEqual(clone(h.store.snapshot().bagControls.lines.map(line => line.lineId)), ids);
  assert.equal(h.d.querySelector('[data-bag-line] select[data-bag-option="Chain Length"]').value, '18 inches');
});

test('spoken cart engraving addition and literal edit are saved only on that exact line, and removing the engraving choice clears its wording', async t => {
  const p = necklace({ engraving: true }), h = await fixture(t, { rows: [p], nativeVoice: true }); await chosen(h, p);
  await h.say('Add this exact piece to my cart'); await h.say('Add this exact piece to my cart'); await h.say('Open my cart');
  const before = h.cart(), literal = 'PRIVATE_45 Cart — Lily';
  const enable = await h.say('Select Engraved for the first item in my cart'); assert.equal(enable.ok, true, JSON.stringify(enable)); shopperCopy(enable);
  assert.equal(h.cart()[0].variantOptions.find(o => o.name === 'Engraving').value, 'Engraved');
  const text = await h.say('Set the engraving for the first item in my cart to ' + literal); assert.equal(text.ok, true, JSON.stringify(text)); shopperCopy(text);
  assert.equal(h.d.querySelector('[data-bag-line] [data-bag-engraving]').value, literal);
  assert.ok(JSON.stringify(h.cart()[0]).includes(literal)); assert.deepEqual(h.cart()[1], before[1]);
  assert.doesNotMatch(JSON.stringify(h.receipts()), /PRIVATE_45/); assert.doesNotMatch(JSON.stringify(h.nativeContext()), /PRIVATE_45/);
  assert.doesNotMatch(h.w.sessionStorage.getItem('brites-concierge-v1') || '', /PRIVATE_45/, 'Saved chat history must also omit literal cart engraving');
  const remove = await h.say('Select No Engraving for the first item in my cart'); assert.equal(remove.ok, true, JSON.stringify(remove)); shopperCopy(remove);
  assert.equal(h.cart()[0].variantOptions.find(o => o.name === 'Engraving').value, 'No Engraving');
  assert.equal(JSON.stringify(h.cart()[0]).includes(literal), false, 'Removing an engraving choice must remove its private words from that line');
  assert.deepEqual(h.cart()[1], before[1]);
});

test('clearing exact saved cart engraving retains the selected engraved variant and the second line', async t => {
  const p = necklace({ engraving: true }), h = await fixture(t, { rows: [p], nativeVoice: true }); await chosen(h, p, { engraved: true });
  await h.say('Fill my engraving with PRIVATE_45 Original'); await h.say('Add this exact piece to my cart'); await h.say('Add this exact piece to my cart'); await h.say('Open my cart');
  const before = h.cart(), result = await h.say('Clear the engraving for the first item in my cart'); assert.equal(result.ok, true, JSON.stringify(result)); shopperCopy(result);
  assert.equal(h.cart()[0].variantId, before[0].variantId); assert.equal(h.d.querySelector('[data-bag-line] [data-bag-engraving]').value, '');
  assert.equal(JSON.stringify(h.cart()[0]).includes('PRIVATE_45'), false); assert.deepEqual(h.cart()[1], before[1]);
});

test('an ambiguous same-title cart option edit asks which line and preserves both selected configurations', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true }); await chosen(h, p);
  await h.say('Add this exact piece to my cart'); await h.say('Add this exact piece to my cart'); await h.say('Open my cart');
  const before = h.cart(), result = await h.say('Change Fox Portrait Necklace in my cart to 18 inches'); assert.notEqual(result.ok, true); shopperCopy(result);
  assert.deepEqual(h.cart(), before, 'Same-title lines need an ordinal or options distinguishing their identities');
});

test('a refreshed target cart option whose price changed refuses the edit while preserving every existing line', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true }); await chosen(h, p);
  await h.say('Add this exact piece to my cart'); await h.say('Add this exact piece to my cart'); await h.say('Open my cart');
  const before = h.cart(); p.variants.find(v => v.title === '14k Solid Gold / 18 inches').price += 19;
  const result = await h.say('Change the chain length of the first item in my cart to 18 inches'); assert.notEqual(result.ok, true); shopperCopy(result);
  assert.deepEqual(h.cart(), before, 'A newly changed price cannot be silently accepted as the old current option');
});

test('typed home has the same real route result as the finalized native home command', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p] }); await h.open(p);
  const result = await h.command('Take me to the homepage'); assert.equal(result.ok, true, JSON.stringify(result)); shopperCopy(result);
  assert.notEqual(h.store.snapshot().pageKind, 'product'); assert.equal(h.store.snapshot().currentHandle, ''); assert.deepEqual(h.cart(), []);
});

test('typed exact cart engraving remains a cart control rather than becoming general customization guidance', async t => {
  const p = necklace({ engraving: true }), h = await fixture(t, { rows: [p] }); await h.open(p);
  await h.select(p, p.variants.find(v => v.title === '14k Solid Gold / 16 inches / No Engraving'));
  for (const text of ['Add this exact piece to my cart', 'Open my cart', 'Select Engraved for the first item in my cart', 'Set the engraving for the first item in my cart to PRIVATE_45 Typed cart']) {
    const result = await h.command(text); assert.equal(result.ok, true, JSON.stringify(result)); shopperCopy(result);
  }
  assert.equal(h.store.snapshot().pageKind, 'bag'); assert.equal(h.d.querySelector('[data-bag-line] [data-bag-engraving]').value, 'PRIVATE_45 Typed cart');
  assert.equal(h.cart()[0].variantOptions.find(o => o.name === 'Engraving').value, 'Engraved');
  assert.doesNotMatch(h.w.sessionStorage.getItem('brites-concierge-v1') || '', /PRIVATE_45/);
});

// Independent native lifecycle clock. It controls network/media events only;
// it does not manufacture actual playback, microphone recognition or timing.
function speechFixture(t, { handled = true } = {}) {
  let clock = 0, timerId = 0, peer, channel, audio, hostCalls = 0, toolCalls = 0;
  const timers = new Map(), packets = [], requests = [], warnings = [];
  const sdp = 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:45 1 UDP 123 192.0.2.45 50000 typ host\r\n';
  const track = { readyState: 'live', stop() { this.readyState = 'ended'; } };
  audio = { paused: true, ended: false, muted: false, volume: 1, currentTime: 0, srcObject: null, setAttribute() {}, play() { this.paused = false; return Promise.resolve(); }, pause() { this.paused = true; }, remove() {} };
  class Peer {
    constructor() { peer = this; this.iceGatheringState = 'complete'; this.connectionState = 'new'; }
    addTrack() {} close() { this.connectionState = 'closed'; }
    createDataChannel() { channel = { readyState: 'connecting', send: raw => packets.push(JSON.parse(raw)), close() { this.readyState = 'closed'; } }; return channel; }
    async createOffer() { return { type: 'offer', sdp }; }
    async setLocalDescription(value) { this.localDescription = value; }
    async setRemoteDescription() { this.connectionState = 'connected'; channel.readyState = 'open'; channel.onopen(); }
  }
  const runtime = {
    document: { hidden: false, body: { appendChild() {} }, createElement: () => audio, addEventListener() {}, removeEventListener() {} },
    navigator: { mediaDevices: { getUserMedia: async () => ({ getTracks: () => [track], getAudioTracks: () => [track] }) } },
    location: { origin: 'https://independent-voice45.test' }, RTCPeerConnection: Peer, AbortController,
    setTimeout(fn, ms) { const id = ++timerId; timers.set(id, { at: clock + ms, fn }); return id; }, clearTimeout(id) { timers.delete(id); }, addEventListener() {}, removeEventListener() {},
    fetch: async (_, init) => { const input = JSON.parse(init.body); requests.push(input.action); return Response.json(input.action === 'start' ? { sdp, stopToken: 'independent45-only', maxDurationMs: 120000 } : { enabled: true, stopped: true }); }
  };
  const voice = Voice.create({ runtime, greeting: false, onTurnWarning: value => warnings.push(value), onFinalizedTurn: async () => { hostCalls++; return handled ? { handled: true, ok: true, reply: 'Your selected necklace is in your cart.', cartChanged: true, completedActions: [{ type: 'add', handle: 'independent45-necklace' }] } : { handled: false }; }, onTool: async () => { toolCalls++; return { ok: true, reply: 'Your cart is open.', cartChanged: false }; } });
  t.after(async () => { await voice.dispose(); assert.equal(timers.size, 0, 'All independent fixture timers must retire'); });
  const microtasks = async () => { for (let n = 0; n < 30; n++) await Promise.resolve(); };
  const emit = value => channel.onmessage({ data: JSON.stringify(value) });
  async function advance(ms) { const limit = clock + ms; for (;;) { const entry = [...timers].filter(([, item]) => item.at <= limit).sort((a, b) => a[1].at - b[1].at)[0]; if (!entry) break; clock = entry[1].at; timers.delete(entry[0]); entry[1].fn(); await microtasks(); } clock = limit; await microtasks(); }
  const responses = () => packets.filter(p => p.type === 'response.create');
  async function speech(item = 'independent45-shopper') { emit({ type: 'input_audio_buffer.speech_started', item_id: item }); emit({ type: 'input_audio_buffer.speech_stopped', item_id: item }); emit({ type: 'input_audio_buffer.committed', item_id: item }); emit({ type: 'conversation.item.input_audio_transcription.completed', item_id: item, transcript: 'Add this exact necklace to my cart' }); await microtasks(); }
  function bind(id, request = responses().at(-1)) { assert.ok(request, 'A current response request must exist'); emit({ type: 'response.created', response: { id, metadata: request.response.metadata } }); }
  function startOutput(id) { emit({ type: 'output_audio_buffer.started', response_id: id }); }
  function drain(id) { emit({ type: 'output_audio_buffer.stopped', response_id: id }); }
  function done(id, status = 'completed', details) { emit({ type: 'response.done', response: { id, status, ...(details ? { status_details: details } : {}) } }); }
  function sameSession() { assert.equal(requests.filter(x => x === 'start').length, 1); assert.equal(requests.filter(x => x === 'stop').length, 0); assert.equal(peer.connectionState, 'connected'); }
  function speechOnly(request) { assert.equal(request.response.tool_choice, 'none', 'Speech recovery must not invite control/tool execution'); assert.match(request.response.instructions, /(?:unfinished|stopped|continue|finish)/i); assert.doesNotMatch(request.response.instructions, /backend|server_error|max_output_tokens|token limit/i, 'Recovery must not ask the customer to hear a technical reason'); }
  return { voice, emit, advance, speech, bind, startOutput, drain, done, responses, packets, warnings, sameSession, speechOnly, hostCalls: () => hostCalls, toolCalls: () => toolCalls, settle: microtasks };
}

test('a token-limited native answer waits for real drain and continues its unfinished speech in the same session without another cart action', async t => {
  const f = speechFixture(t); await f.voice.start(); await f.speech(); f.bind('cut-off'); f.startOutput('cut-off');
  f.done('cut-off', 'incomplete', { reason: 'max_output_tokens' }); assert.equal(f.responses().length, 1, 'The unfinished tail must not overlap buffered speech');
  f.drain('cut-off'); await f.settle(); assert.equal(f.responses().length, 2); f.speechOnly(f.responses()[1]);
  assert.equal(f.hostCalls(), 1); assert.equal(f.toolCalls(), 0); f.sameSession();
  f.bind('tail'); f.startOutput('tail'); f.done('tail'); f.drain('tail'); assert.equal(f.voice.state, 'listening');
});

test('a lost native drain cannot latch a truncated answer forever or stop the new recovery tail with late old events', async t => {
  const f = speechFixture(t); await f.voice.start(); await f.speech(); f.bind('lost-drain'); f.startOutput('lost-drain'); f.done('lost-drain', 'incomplete', { reason: 'max_output_tokens' });
  await f.advance(29999); assert.equal(f.responses().length, 1); await f.advance(1); assert.equal(f.responses().length, 2); f.speechOnly(f.responses()[1]);
  assert.ok(f.packets.some(p => p.type === 'output_audio_buffer.clear'), 'Only exhausted inactive output should be cleared');
  f.bind('recovered-tail'); f.startOutput('recovered-tail'); f.drain('lost-drain'); f.emit({ type: 'output_audio_buffer.cleared', response_id: 'lost-drain' });
  assert.equal(f.voice.state, 'speaking', 'Old output completion cannot finish current recovery speech');
  f.done('recovered-tail'); f.drain('recovered-tail'); assert.equal(f.voice.state, 'listening'); assert.equal(f.hostCalls(), 1); f.sameSession();
});

test('repeated native truncation has at most two speech-only tails and leaves listening ready for a new shopper turn', async t => {
  const f = speechFixture(t); await f.voice.start(); await f.speech();
  for (let n = 0; n < 3; n++) { const id = 'bounded-cut-' + n; f.bind(id); f.startOutput(id); f.done(id, 'incomplete', { reason: 'max_output_tokens' }); f.drain(id); await f.settle(); }
  assert.equal(f.responses().length, 3, 'Recovery cannot generate an unbounded chain'); f.responses().slice(1).forEach(f.speechOnly);
  assert.equal(f.voice.state, 'listening'); assert.equal(f.hostCalls(), 1); assert.equal(f.toolCalls(), 0); f.sameSession();
  await f.speech('independent45-next'); assert.equal(f.hostCalls(), 2); assert.equal(f.responses().length, 4);
});

test('an explicitly transient provider speech failure retries one explanation while never replaying the already completed add', async t => {
  const f = speechFixture(t); await f.voice.start(); await f.speech(); f.bind('failed-explanation');
  f.done('failed-explanation', 'failed', { error: { type: 'server_error', message: 'PRIVATE_PROVIDER_ERROR_45' } }); await f.settle();
  assert.equal(f.responses().length, 2); f.speechOnly(f.responses()[1]); assert.equal(f.hostCalls(), 1); assert.equal(f.toolCalls(), 0);
  f.bind('one-retry'); f.done('one-retry', 'failed', { error: { type: 'server_error', message: 'PRIVATE_PROVIDER_ERROR_45' } }); await f.settle();
  assert.equal(f.responses().length, 2); assert.equal(f.voice.state, 'listening'); assert.doesNotMatch(JSON.stringify(f.warnings), /PRIVATE_PROVIDER_ERROR_45/); f.sameSession();
});

for (const reason of ['content_filter', 'cancelled']) test('unverified native failure reason ' + reason + ' cannot automatically revive a shopper turn', async t => {
  const f = speechFixture(t); await f.voice.start(); await f.speech(); f.bind('unverified-failure');
  f.done('unverified-failure', 'failed', { reason, error: { type: reason } }); await f.settle();
  assert.equal(f.responses().length, 1); assert.equal(f.hostCalls(), 1); assert.equal(f.voice.state, 'listening'); f.sameSession();
});

test('genuine new shopper speech cancels every unfinished tail and late old response events cannot restart it', async t => {
  const f = speechFixture(t); await f.voice.start(); await f.speech(); const old = f.responses()[0]; f.bind('old-cut'); f.startOutput('old-cut'); f.done('old-cut', 'incomplete', { reason: 'max_output_tokens' });
  await f.speech('new-current-shopper'); assert.equal(f.hostCalls(), 2); assert.equal(f.responses().length, 2);
  f.drain('old-cut'); f.bind('old-late-binding', old); f.done('old-late-binding', 'incomplete', { reason: 'max_output_tokens' }); await f.settle();
  assert.equal(f.responses().length, 2); f.bind('fresh-answer'); f.done('fresh-answer'); assert.equal(f.voice.state, 'listening'); f.sameSession();
});

for (const failed of [false, true]) test('empty or failed native recognition after a false audio interruption resumes only the already-safe reply: ' + (failed ? 'failed ASR' : 'empty ASR'), async t => {
  const f = speechFixture(t); await f.voice.start(); await f.speech(); f.bind('speaking-before-noise'); f.startOutput('speaking-before-noise');
  const item = 'independent45-empty-noise'; f.emit({ type: 'input_audio_buffer.speech_started', item_id: item }); f.emit({ type: 'input_audio_buffer.speech_stopped', item_id: item }); f.emit({ type: 'input_audio_buffer.committed', item_id: item });
  f.emit(failed ? { type: 'conversation.item.input_audio_transcription.failed', item_id: item } : { type: 'conversation.item.input_audio_transcription.completed', item_id: item, transcript: '' }); await f.settle();
  assert.equal(f.responses().length, 2); assert.equal(f.hostCalls(), 1, 'No understood shopper words can rerun the cart add'); assert.equal(f.toolCalls(), 0);
  const replay = f.responses()[1].response; assert.equal(replay.tool_choice, 'none'); assert.match(replay.instructions, /Your selected necklace is in your cart/);
  assert.doesNotMatch(replay.instructions, INTERNAL_COPY); f.sameSession();
  f.emit({ type: 'conversation.item.input_audio_transcription.completed', item_id: item, transcript: 'Add this exact necklace to my cart' }); await f.settle();
  assert.equal(f.hostCalls(), 1, 'Late final text for the retired noisy input cannot mint cart authority'); assert.equal(f.responses().length, 2);
  f.drain('speaking-before-noise'); f.bind('safe-noise-recovery'); f.startOutput('safe-noise-recovery'); f.done('safe-noise-recovery'); f.drain('safe-noise-recovery'); assert.equal(f.voice.state, 'listening');
});

test('repeated false audio interruptions have a bounded speech resume and never mint new website action authority', async t => {
  const f = speechFixture(t); await f.voice.start(); await f.speech();
  for (let n = 0; n < 3; n++) {
    const id = 'noise-speaking-' + n; f.bind(id); f.startOutput(id);
    const item = 'independent45-noise-' + n; f.emit({ type: 'input_audio_buffer.speech_started', item_id: item }); f.emit({ type: 'input_audio_buffer.speech_stopped', item_id: item }); f.emit({ type: 'input_audio_buffer.committed', item_id: item }); f.emit({ type: 'conversation.item.input_audio_transcription.failed', item_id: item }); await f.settle();
  }
  assert.equal(f.responses().length, 3, 'At most two noise-driven speech resumes may use the original checked reply');
  assert.equal(f.hostCalls(), 1); assert.equal(f.toolCalls(), 0); assert.equal(f.voice.state, 'listening'); f.sameSession();
});

test('an early provider control result waits for generation completion and native output drain before one safe spoken acknowledgement', async t => {
  const f = speechFixture(t, { handled: false }); await f.voice.start(); await f.speech(); f.bind('owning-tool-response'); f.startOutput('owning-tool-response');
  f.emit({ type: 'response.function_call_arguments.done', response_id: 'owning-tool-response', call_id: 'one-current-control', name: 'control_storefront', arguments: JSON.stringify({ type: 'bag' }) }); await f.settle();
  assert.equal(f.toolCalls(), 1); assert.equal(f.responses().length, 1, 'A fast UI action must not collide with a still-active provider generation');
  f.done('owning-tool-response'); assert.equal(f.responses().length, 1, 'A new acknowledgement must not cut off buffered native audio');
  f.drain('owning-tool-response'); await f.settle(); assert.equal(f.responses().length, 2); assert.equal(f.responses()[1].response.tool_choice, 'none');
  f.emit({ type: 'response.function_call_arguments.done', response_id: 'owning-tool-response', call_id: 'one-current-control', name: 'control_storefront', arguments: JSON.stringify({ type: 'bag' }) }); await f.settle();
  assert.equal(f.toolCalls(), 1); assert.equal(f.responses().length, 2); f.sameSession();
});

test('real new speech expires a queued provider acknowledgement rather than playing it over the newer shopper request', async t => {
  const f = speechFixture(t, { handled: false }); await f.voice.start(); await f.speech(); f.bind('old-tool-owner'); f.startOutput('old-tool-owner');
  f.emit({ type: 'response.function_call_arguments.done', response_id: 'old-tool-owner', call_id: 'old-control-call', name: 'control_storefront', arguments: JSON.stringify({ type: 'bag' }) }); await f.settle(); assert.equal(f.toolCalls(), 1); assert.equal(f.responses().length, 1);
  await f.speech('independent45-real-new-request'); assert.equal(f.responses().length, 2); const current = f.responses()[1];
  f.done('old-tool-owner'); f.drain('old-tool-owner'); await f.settle(); assert.equal(f.responses().length, 2, 'A queued old result cannot revive after new shopper words');
  assert.equal(current.response.metadata.brites_input_item, 'independent45-real-new-request'); assert.equal(f.toolCalls(), 1); f.sameSession();
});

for (const [status, details] of [['incomplete', { reason: 'max_output_tokens' }], ['failed', { error: { type: 'server_error' } }]]) test('a queued completed control explanation supersedes its ' + status + ' owning reply, avoiding duplicate recovery generations', async t => {
  const f = speechFixture(t, { handled: false }); await f.voice.start(); await f.speech(); f.bind('cut-tool-owner'); f.startOutput('cut-tool-owner');
  f.emit({ type: 'response.function_call_arguments.done', response_id: 'cut-tool-owner', call_id: 'checked-control-once', name: 'control_storefront', arguments: JSON.stringify({ type: 'bag' }) }); await f.settle();
  f.done('cut-tool-owner', status, details); assert.equal(f.responses().length, 1);
  f.drain('cut-tool-owner'); await f.settle(); assert.equal(f.responses().length, 2); assert.equal(f.responses()[1].response.tool_choice, 'none');
  f.bind('single-completed-explanation'); f.startOutput('single-completed-explanation'); f.done('single-completed-explanation'); f.drain('single-completed-explanation'); await f.settle();
  assert.equal(f.responses().length, 2, 'The completed control acknowledgement must replace, rather than follow, another unfinished generation');
  assert.equal(f.toolCalls(), 1); assert.equal(f.voice.state, 'listening'); f.sameSession();
});

test('the actual opted-in widget renews only a server-confirmed expired session and speaks the safe unfinished reply without rerunning the cart', async t => {
  const p = necklace(), h = await fixture(t, { rows: [p], nativeVoice: true, manualClock: true }); await chosen(h, p); await h.say('Add this exact piece to my cart');
  const before = h.cart(), callCount = h.hostResults.length, request = h.responses().at(-1);
  h.emit({ type: 'response.created', response: { id: 'renewal-old-answer', metadata: request.response.metadata } }); h.emit({ type: 'output_audio_buffer.started', response_id: 'renewal-old-answer' }); h.emit({ type: 'response.done', response: { id: 'renewal-old-answer', status: 'completed' } });
  // Streamed progress is retained until the exact session deadline, as real
  // ongoing media would be; this is not a physical speech-duration assertion.
  for (let n = 0; n < 11; n++) { await h.advance(10000); h.emit({ type: 'response.output_audio_transcript.delta', response_id: 'renewal-old-answer', delta: 'Current native words' }); }
  await h.advance(10000);
  const start = h.requests.filter(r => r.url.pathname === '/api/concierge-voice' && JSON.parse(r.init.body).action === 'start');
  const stop = h.requests.filter(r => r.url.pathname === '/api/concierge-voice' && JSON.parse(r.init.body).action === 'stop');
  assert.equal(start.length, 2, 'The confirmed old call must close before the opted-in replacement starts'); assert.equal(stop.length, 1);
  assert.deepEqual(h.cart(), before); assert.equal(h.hostResults.length, callCount, 'A speech-only renewal must not mint another shopper action');
  const renewed = h.responses().at(-1).response; assert.equal(renewed.tool_choice, 'none'); assert.match(renewed.instructions, /(?:cart|test bag)/i); assert.doesNotMatch(renewed.instructions, INTERNAL_COPY);
  assert.ok(['listening', 'thinking'].includes(h.client.state)); assert.equal([...h.root.querySelectorAll('button')].find(b => b.textContent === 'End voice')?.getAttribute('aria-pressed'), 'true');
});

test('a remote unconfirmed expiry cannot automatically create a second provider call in the real widget', async t => {
  const h = await fixture(t, { rows: [necklace()], nativeVoice: true, manualClock: true, confirmedStop: false });
  await h.advance(120000);
  assert.equal(h.requests.filter(r => r.url.pathname === '/api/concierge-voice' && JSON.parse(r.init.body).action === 'start').length, 1);
  assert.equal(h.client.state, 'idle'); assert.match(h.root.textContent, /Voice paused|Talk to me to continue/i); assert.deepEqual(h.cart(), []);
});

test('actual End voice cancels expiry renewal and a delayed session deadline cannot reopen the microphone or change the bag', async t => {
  const h = await fixture(t, { rows: [necklace()], nativeVoice: true, manualClock: true });
  await h.advance(110000); const button = [...h.root.querySelectorAll('button')].find(b => b.textContent === 'End voice'); assert.ok(button); button.click(); await settle(); await h.advance(120000);
  assert.equal(h.requests.filter(r => r.url.pathname === '/api/concierge-voice' && JSON.parse(r.init.body).action === 'start').length, 1);
  assert.equal(h.client.state, 'idle'); assert.deepEqual(h.cart(), []);
});
