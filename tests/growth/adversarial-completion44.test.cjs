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
const project = process.env.BRITES_ADVERSARIAL44_PROJECT || path.resolve(__dirname, '../..');
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
  const handle = 'independent-completion44-' + id;
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

async function fixture(t, { rows = catalogue(), nativeVoice = false } = {}) {
  const errors = [], console = new VirtualConsole();
  console.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM(read('concierge-sandbox.html'), { url: 'https://preview.example/concierge-sandbox.html', runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole: console });
  const w = dom.window, d = w.document, requests = [], packets = [];
  let client, channel, turn = 0, conciergeRead = null;
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
      body = conciergeRead && input.message ? await conciergeRead(input) : { live: true, reply: 'INDEPENDENT44_REMOTE_FALLBACK', products: [], meanings: [], preferences: {}, preserveSelection: true };
    } else if (url.pathname === '/api/concierge-voice') {
      const action = JSON.parse(init.body || '{}').action;
      body = action === 'start' ? { sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n', stopToken: 'independent44-synthetic', maxDurationMs: 120000 } : { ok: true, enabled: true, nativeAudio: true, publicDemo: true, stopped: action === 'stop' };
    } else throw Error('Unexpected independent request ' + url.pathname);
    return { ok: true, json: async () => clone(body) };
  };
  for (const name of ['brites-catalogue-intents.js', 'brites-concierge-shopping-guide.js', 'brites-storefront-bridge.js', 'concierge-sandbox.js']) w.eval(read(name));
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
    w.BritesConciergeVoice = { publicContext: Voice.publicContext, create(options) { client = Voice.create({ ...options, runtime, greeting: false }); return client; } };
  }
  w.BritesConciergeAvatar = { create() { return { setState() {}, setEmotion() {}, setVisible() {}, setPaused() {}, setLevel() {}, setFloating() {}, cancelPerformance() {}, triggerGreeting() {}, cue() {}, focusProduct() {}, clearFocus() {}, showProduct() {}, clearProduct() {}, destroy() {} }; } };
  const script = d.createElement('script'); script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true';
  Object.defineProperty(d, 'currentScript', { get: () => script });
  w.eval(read('brites-concierge.js')); await settle(); w.BritesConcierge.open({ focus: false }); await settle();
  const root = d.querySelector('brites-concierge').shadowRoot;
  t.after(async () => { w.BritesConcierge.close(); await client?.dispose(); w.close(); });
  const h = {
    w, d, root, store, rows, errors, requests, packets,
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
      const receipts = () => packets.filter(p => p.item?.content?.[0]?.text?.startsWith('Host-completed result'));
      const before = receipts().length, itemId = 'independent44-turn-' + (++turn), emit = event => channel.onmessage({ data: JSON.stringify(event) });
      emit({ type: 'input_audio_buffer.speech_started', item_id: itemId });
      emit({ type: 'input_audio_buffer.speech_stopped', item_id: itemId });
      emit({ type: 'input_audio_buffer.committed', item_id: itemId });
      emit({ type: 'conversation.item.input_audio_transcription.completed', item_id: itemId, transcript: text });
      await eventually(() => receipts().length > before, 'The actual voice client must produce a current host result');
      assert.equal(receipts().length, before + 1);
      const textReceipt = receipts().at(-1).item.content[0].text;
      return JSON.parse(textReceipt.slice(textReceipt.indexOf('{'))).result;
    }
  };
  if (nativeVoice) await h.startVoice();
  return h;
}

for (const nativeVoice of [false, true]) for (const [phrase, indices] of [
  ['Show animal jewelry without necklaces', [0, 1, 2]],
  ['Show animal necklaces and earrings', [0, 1, 2, 3, 4]],
  ['Show animal jewelry without earrings', [3, 4]]
]) test('visible discovery agrees with checked positive and negative categories: ' + phrase + ' / ' + (nativeVoice ? 'synthetic realtime' : 'typed'), async t => {
  const h = await fixture(t, { nativeVoice }), reads = h.requests.length;
  const result = await (nativeVoice ? h.say(phrase) : h.command(phrase));
  assert.equal(result.ok, true, JSON.stringify(result));
  const expected = indices.map(i => h.rows[i].handle).sort();
  assert.deepEqual(h.visible(), expected, 'The actual host grid, not only the guide receipt, must contain the requested categories');
  assert.deepEqual(clone(result.products || []).map(p => p.handle).sort(), expected, 'The public result and actual rendered cards must refer to the same identities');
  assert.equal(h.requests.length, reads, 'Warm typed and realtime category discovery must avoid extra model/product requests');
  assert.deepEqual(h.cart(), []); assert.deepEqual(h.errors, []);
});

for (const nativeVoice of [false, true]) test('solid-gold discovery requires explicit construction on one available exact variant / ' + (nativeVoice ? 'synthetic realtime' : 'typed'), async t => {
  const rows = [
    product(74411, 'Fox Outline Stud Earrings', 'fox', 'Earrings', ['Sterling Silver', '14k Gold']),
    product(74412, 'Fox Portrait Stud Earrings', 'fox', 'Earrings', ['14k Solid Gold']),
    product(74413, 'Fox Drop Earrings', 'fox', 'Earrings', ['14k Gold Filled']),
    product(74414, 'Fox Huggie Earrings', 'fox', 'Earrings', ['14k Gold Plated'])
  ];
  const h = await fixture(t, { nativeVoice, rows }), reads = h.requests.length;
  const result = await (nativeVoice ? h.say('Show solid gold fox earrings') : h.command('Show solid gold fox earrings'));
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.deepEqual(h.visible(), [rows[1].handle], 'Karat purity without a published construction cannot prove solid gold; filled and plated are also excluded');
  assert.deepEqual(clone(result.products || []).map(p => p.handle), [rows[1].handle]);
  assert.equal(h.requests.length, reads); assert.deepEqual(h.cart(), []);
});

for (const nativeVoice of [false, true]) for (const [phrase, firstIndices, budgetIndices] of [
  ['Show animal jewelry without necklaces', [0, 1, 2], [0, 2]],
  ['Show animal necklaces and earrings', [0, 1, 2, 3, 4], [0, 2, 3]]
]) test('material and price follow-ups retain the completed positive/negative category scope: ' + phrase + ' / ' + (nativeVoice ? 'synthetic realtime' : 'typed'), async t => {
  const rows = catalogue();
  for (const [index, price] of [[0, 32], [1, 74], [2, 46], [3, 35], [4, 80], [5, 33]]) {
    rows[index].variants.filter(v => v.options[0].value === '14/20 Gold Filled').forEach((v, n) => { v.price = price + n * 6; });
  }
  const h = await fixture(t, { rows, nativeVoice }), ask = text => nativeVoice ? h.say(text) : h.command(text), reads = h.requests.length;
  for (const text of [phrase, 'What about gold filled?']) {
    const result = await ask(text); assert.equal(result.ok, true, JSON.stringify(result));
    assert.deepEqual(h.visible(), firstIndices.map(i => rows[i].handle).sort(), 'A material follow-up must not replace category exclusions or introduce unrelated motifs');
  }
  const capped = await ask('Under USD 50'); assert.equal(capped.ok, true, JSON.stringify(capped));
  assert.deepEqual(h.visible(), budgetIndices.map(i => rows[i].handle).sort());
  assert.deepEqual(clone(capped.products || []).map(p => p.handle).sort(), h.visible());
  assert.equal(h.requests.length, reads); assert.deepEqual(h.cart(), []); assert.deepEqual(h.errors, []);
});

for (const nativeVoice of [false, true]) for (const phrase of ['What width is the charm?', 'What diameter is the charm?', 'What size is the charm?']) test('chain length cannot stand in for unknown requested charm measurements: ' + phrase + ' / ' + (nativeVoice ? 'synthetic realtime' : 'typed'), async t => {
  const p = product(74421, 'Fox Pendant Necklace', 'fox', 'Necklace');
  p.description = 'The necklace includes a 21 inch chain. The charm has a polished surface.';
  const h = await fixture(t, { nativeVoice, rows: [p] }); await h.open(p);
  const before = clone(h.store.snapshot()), reads = h.requests.length;
  const result = await (nativeVoice ? h.say(phrase) : h.command(phrase));
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.match(result.reply, /(?:charm[^.!?]{0,140}(?:not confirm|not published|unknown|not provided|not specified|not available|missing))|(?:(?:not confirm|unknown|not provided|not specified|not available|missing)[^.!?]{0,140}charm)/i, 'The guide must state that the requested component measurement is unknown');
  assert.deepEqual(clone(h.store.snapshot()), before); assert.equal(h.requests.length, reads);
});

for (const hold of ['cartHold', 'recommendationHold']) test('a held current listing keeps descriptive identity under review and sends no purchasable native fact authority: ' + hold, async t => {
  const p = product(74422, 'Fox Portrait Stud Earrings', 'fox');
  const h = await fixture(t, { rows: [p], nativeVoice: true }); await h.open(p); await h.select(p, p.variants[2]);
  p[hold] = true; p.checkedAt = Date.now();
  await h.store.preloadInventory({ retry: true }); await settle();
  assert.equal(h.store.snapshot().productControls.variantId, p.variants[2].id, 'The hold is applied to a real previously selected literal variant, not an unselected fixture');
  const before = clone(h.store.snapshot()), reads = h.requests.length;
  const identity = await h.say('What am I looking at?');
  assert.equal(identity.ok, true, JSON.stringify(identity));
  assert.equal(identity.verified, false);
  assert.equal(identity.productFacts?.status, 'unconfirmed');
  assert.match(identity.reply, /Fox Portrait Stud Earrings/); assert.match(identity.reply, /under review/i);
  const price = await h.say('How much is this?');
  assert.notEqual(price.ok, true, 'A review hold cannot become a verified price/stock answer');
  assert.equal(price.verified, false);
  assert.doesNotMatch(price.reply || price.error || '', /\b(?:USD|CAD)\s*\d|\$\s*\d/);
  const context = h.nativeContext();
  assert.equal(context.currentHandle, p.handle, 'Safe current-page identity remains useful');
  assert.equal(context.productControls?.selectedVariant, undefined);
  assert.equal(context.productControls?.itemTotalPrice, undefined);
  if (context.currentProduct) {
    assert.notEqual(context.currentProduct.status, 'verified');
    assert.equal(context.currentProduct.minPrice, undefined);
    assert.equal(context.currentProduct.selectedVariant, undefined);
    assert.deepEqual(context.currentProduct.variants || [], []);
  }
  assert.deepEqual(clone(h.store.snapshot()), before); assert.equal(h.requests.length, reads); assert.deepEqual(h.cart(), []);
});

for (const nativeVoice of [false, true]) test('checked unavailable stock remains a truthful published fact without add authority / ' + (nativeVoice ? 'synthetic realtime' : 'typed'), async t => {
  const p = product(74423, 'Rabbit Drop Earrings', 'rabbit'); p.variants.forEach(v => { v.available = false; });
  const h = await fixture(t, { rows: [p], nativeVoice }); await h.open(p);
  const before = clone(h.store.snapshot()), stock = await (nativeVoice ? h.say('Is this in stock?') : h.command('Is this in stock?'));
  assert.equal(stock.ok, true, JSON.stringify(stock)); assert.equal(stock.productFacts?.status, 'verified');
  assert.equal(stock.productFacts.availability.status, 'unavailable');
  assert.match(stock.reply, /unavailable|no[^.!?]{0,60}available|none[^.!?]{0,60}available/i);
  assert.deepEqual(clone(h.store.snapshot()), before, 'A truthful stock read leaves current page and controls intact');
  const add = await (nativeVoice ? h.say('Add this exact piece to my cart') : h.command('Add this exact piece to my cart'));
  assert.notEqual(add.ok, true); assert.deepEqual(h.cart(), []);
});

for (const change of ['manual navigation', 'manual exact variant']) test('a delayed old product answer cannot commit after ' + change, async t => {
  const h = await fixture(t); await h.open();
  const facts = (await h.command('How much is this?')).productFacts;
  assert.equal(facts.handle, h.rows[0].handle);
  // Exercise the real remote fallback while keeping all host execution,
  // snapshots and events intact. Only the optional cached-read dependency is
  // unavailable; navigation remains the original production host.
  h.w.BritesSandboxStorefront = { ...h.store, readProduct: async () => null };
  const start = deferred(), release = deferred();
  h.conciergeRead(async () => { start.resolve(); await release.promise; return { live: true, reply: 'OLD_PRODUCT_ANSWER44', products: [], meanings: [], preferences: { interests: ['retired-response44'] }, preserveSelection: true, productFacts: facts }; });
  const pending = h.command('Tell me about this piece');
  await start.promise;
  if (change === 'manual navigation') await h.open(h.rows[2]); else await h.select(h.rows[0], h.rows[0].variants[2]);
  const afterManual = clone(h.store.snapshot());
  release.resolve(); const result = await pending; await settle();
  assert.notEqual(result.ok, true, 'A response for retired product/choice context must not become a successful current answer');
  assert.doesNotMatch(h.root.querySelector('.caption-text')?.textContent || '', /OLD_PRODUCT_ANSWER44/);
  assert.doesNotMatch(h.w.sessionStorage.getItem('brites-concierge-v1') || '', /OLD_PRODUCT_ANSWER44|retired-response44/);
  assert.deepEqual(clone(h.store.snapshot()), afterManual, 'The old answer cannot undo current manual controls or navigation');
  assert.equal(h.root.querySelector('.composer input').disabled, false);
});

for (const hold of ['cartHold', 'recommendationHold']) test('a delayed verified price cannot commit after a real same-product review hold refresh: ' + hold, async t => {
  const h = await fixture(t); await h.open(); await h.select(h.rows[0], h.rows[0].variants[2]);
  const facts = (await h.command('How much is this?')).productFacts;
  assert.equal(facts.status, 'verified'); assert.equal(facts.selectedVariant.price, 70);
  h.w.BritesSandboxStorefront = { ...h.store, readProduct: async () => null };
  const start = deferred(), release = deferred();
  h.conciergeRead(async () => { start.resolve(); await release.promise; return { live: true, reply: 'RETIRED_PRICE_BEFORE_HOLD44 USD 70.00', products: [], meanings: [], preferences: { interests: ['retired-hold-response44'] }, preserveSelection: true, productFacts: facts }; });
  const pending = h.command('Tell me about this piece'); await start.promise;
  h.rows[0][hold] = true; h.rows[0].checkedAt = Date.now();
  await h.store.preloadInventory({ retry: true }); await settle();
  const afterHold = clone(h.store.snapshot());
  assert.equal(afterHold.currentHandle, h.rows[0].handle);
  assert.equal(afterHold.productControls.variantId, h.rows[0].variants[2].id, 'This is a hold-only change with the same actual selected identity');
  release.resolve(); const result = await pending; await settle();
  assert.notEqual(result.ok, true, 'A newly held exact product cannot accept earlier verified price authority');
  assert.doesNotMatch(h.root.querySelector('.caption-text')?.textContent || '', /RETIRED_PRICE_BEFORE_HOLD44/);
  assert.doesNotMatch(h.w.sessionStorage.getItem('brites-concierge-v1') || '', /RETIRED_PRICE_BEFORE_HOLD44|retired-hold-response44/);
  assert.deepEqual(clone(h.store.snapshot()), afterHold); assert.deepEqual(h.cart(), []);
  assert.equal(h.root.querySelector('.composer input').disabled, false);
});

test('an unchanged public context event does not cancel a legitimate delayed current-product answer', async t => {
  const h = await fixture(t); await h.open(); await h.select();
  const facts = (await h.command('How much is this?')).productFacts;
  h.w.BritesSandboxStorefront = { ...h.store, readProduct: async () => null };
  const start = deferred(), release = deferred();
  h.conciergeRead(async () => { start.resolve(); await release.promise; return { live: true, reply: 'CURRENT_UNCHANGED_ANSWER44', products: [], meanings: [], preferences: {}, preserveSelection: true, productFacts: facts }; });
  const pending = h.command('Tell me about this piece'); await start.promise;
  const before = clone(h.store.snapshot());
  h.d.dispatchEvent(new h.w.CustomEvent('brites-storefront:context', { detail: before })); await settle();
  assert.equal(h.root.querySelector('.composer input').disabled, true, 'A duplicate awareness publication is not a new shopper choice');
  release.resolve(); const result = await pending; await settle();
  assert.equal(result.ok, true, JSON.stringify(result)); assert.match(result.reply, /CURRENT_UNCHANGED_ANSWER44/);
  assert.equal(result.productFacts?.handle, h.rows[0].handle);
  assert.deepEqual(clone(h.store.snapshot()), before); assert.deepEqual(h.cart(), []);
  assert.equal(h.root.querySelector('.composer input').disabled, false);
});

test('an unchanged storefront context publication preserves the focused current recommendation button', async t => {
  const h = await fixture(t); await h.open();
  const shown = await h.command('More like this'); assert.equal(shown.ok, true, JSON.stringify(shown));
  const button = h.root.querySelector('.shopping-suggestion button'); assert.ok(button);
  button.focus(); assert.equal(h.root.activeElement, button);
  const before = clone(h.store.snapshot());
  h.d.dispatchEvent(new h.w.CustomEvent('brites-storefront:context', { detail: clone(before) }));
  await settle();
  assert.equal(button.isConnected, true, 'A duplicate context event must not replace actionable help under keyboard focus');
  assert.equal(h.root.activeElement, button);
  assert.deepEqual(clone(h.store.snapshot()), before); assert.deepEqual(h.cart(), []);
});

test('expired inventory withdraws visible old recommendation price and action without changing current choices or bag', async t => {
  const h = await fixture(t); await h.open(); await h.select();
  const shown = await h.command('More like this'); assert.equal(shown.ok, true, JSON.stringify(shown));
  const old = h.root.querySelector('.shopping-suggestion'); assert.ok(old);
  const title = old.querySelector('.shopping-suggestion-title').textContent;
  const before = clone(h.store.snapshot().productControls), cart = h.cart();
  const now = h.w.Date.now(); h.w.Date.now = () => now + 301000;
  assert.deepEqual(clone(h.store.getInventory()), [], 'The production cache must genuinely expire');
  h.d.dispatchEvent(new h.w.CustomEvent('brites-storefront:inventory', { detail: h.store.inventoryStatus() }));
  await settle();
  assert.equal(h.root.querySelector('.shopping-suggestion'), null, 'A stored presentation cannot keep an expired actionable recommendation alive');
  assert.doesNotMatch(h.root.querySelector('.shopping-help').textContent, new RegExp(title));
  assert.deepEqual(clone(h.store.snapshot().productControls), before); assert.deepEqual(h.cart(), cart);
});

test('a connected discovery-card chooser uses the actual host choices and cannot overwrite a full bag', async t => {
  const h = await fixture(t);
  const result = await h.command('Show fox earrings'); assert.equal(result.ok, true, JSON.stringify(result));
  const card = h.root.querySelector('.card[data-product-id="' + h.rows[0].id + '"]'); assert.ok(card);
  const choose = [...card.querySelectorAll('button')].find(b => b.textContent === 'Choose options'); assert.ok(choose); choose.click();
  await eventually(() => h.store.snapshot().currentHandle === h.rows[0].handle && h.store.snapshot().productControls?.optionsOpen === true, 'The connected card must reveal the actual exact host menu');
  assert.deepEqual(clone(h.store.snapshot().productControls.selectedOptions), [], 'Opening choices cannot make a silent material/size decision');
  await h.select();
  const saved = Array.from({ length: 50 }, (_, n) => ({ lineId: 'independent44-line-' + n, productId: h.rows[0].id, handle: h.rows[0].handle, title: h.rows[0].title, variantId: h.rows[0].variants[0].numericId, variant: h.rows[0].variants[0].title, variantOptions: clone(h.rows[0].variants[0].options), price: h.rows[0].variants[0].price, currency: 'USD', quantity: n ? 1 : 20 }));
  h.w.sessionStorage.setItem('brites-sandbox-cart', JSON.stringify(saved));
  const before = clone(h.cart()), response = await h.command('Add this exact piece to my cart');
  assert.notEqual(response.ok, true, 'A full bounded bag must reject another line');
  assert.deepEqual(h.cart(), before, 'The chooser path cannot drop the earlier quantity-20 line or rewrite the other 49');
  assert.equal(h.cart().reduce((sum, row) => sum + row.quantity, 0), 69);
  assert.equal(card.querySelector('.review'), null, 'Connected-host cards must not expose the legacy separate cart writer');
});

test('review confirmation refuses an exact variant whose published options change under the same ID and price', async t => {
  const h = await fixture(t); await h.open(); await h.select();
  const reviewed = await h.command('Review adding this piece to my cart'); assert.equal(reviewed.ok, true, JSON.stringify(reviewed));
  const confirm = h.d.querySelector('.product-review .primary'); assert.ok(confirm, 'The actual host confirmation must be visible');
  const changed = h.rows[0].variants[0];
  changed.options[0].value = '14/20 Gold Filled'; changed.title = '14/20 Gold Filled / 9mm';
  confirm.click();
  await eventually(() => !h.store.snapshot().productControls.busy, 'Fresh review comparison must settle'); await settle();
  assert.deepEqual(h.cart(), [], 'Identity and price alone cannot authorize obsolete option wording');
  assert.doesNotMatch(h.root.querySelector('.caption-text')?.textContent || '', /Added to|in your bag/i);
});
