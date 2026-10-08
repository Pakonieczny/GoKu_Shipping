'use strict';

// Actual mounted host, portable bridge, widget and native voice client. Only
// public catalogue/network responses, microphone and WebRTC transport are
// synthetic. These checks do not establish live audio or browser appearance.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM, VirtualConsole } = require('jsdom');
const Voice = require('../../brites-concierge-voice.js');
const project = path.resolve(__dirname, '../..');
const read = name => fs.readFileSync(path.join(project, name), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const settle = async () => { await new Promise(setImmediate); await new Promise(setImmediate); };
async function eventually(check, label) {
  for (let n = 0; n < 40; n++) { if (check()) return; await settle(); }
  assert.ok(check(), label);
}
function product(id, title, motif, type = 'Earrings') {
  const handle = 'deictic43-' + id, choices = ['Sterling Silver', '14/20 Gold Filled'];
  return {
    id: 'gid://shopify/Product/' + id, handle, title, type,
    url: 'https://britesjewelry.com/products/' + handle, currency: 'USD',
    tags: ['motif:' + motif], description: 'The ' + motif + ' charm is 12 mm wide and 9 mm high.',
    image: 'https://cdn.shopify.com/' + handle + '.jpg',
    checkedAt: Date.now(), detailState: 'checked', variantsComplete: true,
    options: [{ name: 'Metal', values: choices }],
    variants: choices.map((value, i) => ({
      id: 'gid://shopify/ProductVariant/' + (id * 10 + i + 1),
      numericId: String(id * 10 + i + 1), title: value,
      price: 40 + i * 20, available: true, options: [{ name: 'Metal', value }]
    }))
  };
}
async function fixture(t, nativeVoice, suppliedRows) {
  const rows = suppliedRows || [product(43801, 'Butterfly Cutout Stud Earrings', 'butterfly'), product(43802, 'Butterfly Outline Stud Earrings', 'butterfly'), product(43803, 'Fox Stud Earrings', 'fox'), product(43804, 'Moon Necklace', 'moon', 'Necklace')];
  const errors = [], console = new VirtualConsole();
  console.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM(read('concierge-sandbox.html'), { url: 'https://preview.example/concierge-sandbox.html', runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole: console });
  const w = dom.window, d = w.document, requests = [], packets = [];
  let client, channel, turn = 0;
  delete d.body.dataset.catalogueSeed;
  w.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  w.HTMLElement.prototype.scrollIntoView = function () {};
  w.scrollTo = () => {};
  w.fetch = async (raw, init = {}) => {
    const url = new URL(raw, w.location.href); requests.push({ url, init });
    let body;
    if (url.pathname === '/api/growth/catalogue') body = { live: true, checkedAt: Date.now(), products: rows, pageInfo: { hasNextPage: false, endCursor: null } };
    else if (url.pathname === '/api/growth/inventory') body = { live: true, checkedAt: Date.now(), products: rows.map(p => ({ ...p, checkedAt: Date.now() })), inventory: { schema: 1, total: rows.length, offset: 0, limit: 24, loaded: rows.length, detailsLoaded: rows.length, ready: true, partial: false, expiresAt: Date.now() + 300000 }, pageInfo: { hasNextPage: false, nextOffset: null } };
    else if (url.pathname === '/api/growth/product') body = { live: true, checkedAt: Date.now(), product: rows.find(p => p.handle === url.searchParams.get('handle')) };
    else if (url.pathname === '/api/growth/storefront-services') body = { schema: 1, guidance: {}, conflicts: [], offers: { items: [] } };
    else if (url.pathname === '/api/growth/knowledge') body = { live: true, checkedAt: Date.now(), meanings: [] };
    else if (url.pathname === '/api/growth/events') body = { ok: true };
    else if (url.pathname === '/api/concierge-voice') {
      const action = init.body ? JSON.parse(init.body).action : '';
      body = action === 'start' ? { sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n', stopToken: 'synthetic-deictic43', maxDurationMs: 120000 } : { ok: true, enabled: true, nativeAudio: true, publicDemo: true, stopped: action === 'stop' };
    } else if (url.pathname === '/api/concierge') body = { live: true, reply: 'DEICTIC_REMOTE_FALLBACK43', products: [rows[0]], meanings: [], preferences: {}, preserveSelection: true };
    else throw Error('Unexpected synthetic deictic route ' + url.pathname);
    return { ok: true, json: async () => clone(body) };
  };
  for (const name of ['brites-catalogue-intents.js', 'brites-concierge-shopping-guide.js', 'brites-storefront-bridge.js', 'concierge-sandbox.js']) w.eval(read(name));
  await settle(); await w.BritesSandboxStorefront.preloadInventory(); await settle();
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
      document: d, location: w.location, navigator: { mediaDevices: { async getUserMedia() { const track = { readyState: 'live', stop() { this.readyState = 'ended'; }, addEventListener() {}, removeEventListener() {} }; return { getTracks: () => [track], getAudioTracks: () => [track] }; } } },
      RTCPeerConnection: Peer, AbortController: w.AbortController, fetch: w.fetch,
      setTimeout: w.setTimeout.bind(w), clearTimeout: w.clearTimeout.bind(w), addEventListener: w.addEventListener.bind(w), removeEventListener: w.removeEventListener.bind(w)
    };
    w.BritesConciergeVoice = { publicContext: Voice.publicContext, create(options) { client = Voice.create({ ...options, runtime, greeting: false }); return client; } };
  }
  w.BritesConciergeAvatar = { create() { return { setState() {}, setEmotion() {}, setVisible() {}, setPaused() {}, retry() {}, triggerGreeting() {}, clearFocus() {}, focusProduct() {}, setLevel() {}, setSpeechSignal() {}, clearProduct() {}, showProduct() {}, cancelPerformance() {}, setFloating() {}, cue() {}, destroy() {} }; } };
  const script = d.createElement('script'); script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true';
  Object.defineProperty(d, 'currentScript', { get: () => script });
  w.eval(read('brites-concierge.js')); await settle(); w.BritesConcierge.open({ focus: false }); await settle();
  t.after(async () => { w.BritesConcierge.close(); await client?.dispose(); w.close(); });
  const root = d.querySelector('brites-concierge').shadowRoot;
  return {
    w, d, root, rows, requests, errors, store: w.BritesSandboxStorefront,
    command: text => w.BritesConcierge.sendShopperCommand(text),
    cart: () => clone(JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart') || '[]')),
    async startVoice() {
      [...root.querySelectorAll('button')].find(button => button.textContent === 'Talk to me').click(); await settle();
      root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));
      await settle(); assert.equal(client?.state, 'listening');
    },
    async say(text) {
      assert.ok(channel?.onmessage, 'Synthetic realtime must be connected');
      const receipts = () => packets.filter(packet => packet.item?.content?.[0]?.text?.startsWith('Host-completed result'));
      const before = receipts().length, itemId = 'deictic43-native-' + (++turn), emit = event => channel.onmessage({ data: JSON.stringify(event) });
      emit({ type: 'input_audio_buffer.speech_started', item_id: itemId });
      emit({ type: 'input_audio_buffer.speech_stopped', item_id: itemId });
      emit({ type: 'input_audio_buffer.committed', item_id: itemId });
      emit({ type: 'conversation.item.input_audio_transcription.completed', item_id: itemId, transcript: text });
      for (let n = 0; n < 5; n++) await settle();
      assert.equal(receipts().length, before + 1, text + ' must produce one current-turn host receipt');
      const receipt = receipts().at(-1).item.content[0].text;
      return JSON.parse(receipt.slice(receipt.indexOf('{'))).result;
    }
  };
}
function checked(result, phrase) {
  assert.equal(result.ok, true, phrase + ': ' + (result.reply || result.error));
  assert.doesNotMatch(result.reply || '', /DEICTIC_REMOTE_FALLBACK43/);
}

for (const nativeVoice of [false, true]) test('More like this uses the current bag-opened product and exact alternatives through ' + (nativeVoice ? 'synthetic native realtime' : 'typed widget'), async t => {
  const h = await fixture(t, nativeVoice), ask = text => nativeVoice ? h.say(text) : h.command(text);
  checked(await h.command('What animal earrings do you have?'), 'old collection query');
  if (nativeVoice) await h.startVoice();
  let before = clone(h.store.snapshot()), reads = h.requests.length;
  const ambiguous = await ask('More like this');
  checked(ambiguous, 'collection clarification');
  assert.match(ambiguous.reply, /Which.*pieces|Which piece/);
  assert.deepEqual(clone(ambiguous.recommendations), []);
  assert.deepEqual(clone(h.store.snapshot()), before, 'A collection deictic must not guess a named product');
  assert.equal(h.requests.length, reads);
  for (const text of ['Open Butterfly Cutout Stud Earrings', 'Select 14/20 Gold Filled', 'Add this to my cart', 'Take me to my cart']) checked(await h.command(text), text);
  const bag = h.cart();
  assert.equal(bag.length, 1);
  [...h.d.querySelectorAll('.bag-item button')].find(button => button.textContent === 'View its live details').click();
  await eventually(() => h.store.snapshot().pageKind === 'product' && h.store.snapshot().currentHandle === h.rows[0].handle, 'Native bag link opens exact listing');
  checked(await h.command('Select 14/20 Gold Filled'), 'restore explicit current selection');
  if (nativeVoice) await h.startVoice();
  await settle(); before = clone(h.store.snapshot()); reads = h.requests.length;
  const missingPair = await ask('Show me a matching necklace');
  checked(missingPair, 'missing matching necklace');
  assert.deepEqual(clone(missingPair.recommendations), []);
  assert.match(missingPair.reply, /haven’t found|no.*matching/i);
  assert.deepEqual(clone(h.store.snapshot()), before);
  const similar = await ask('More like this');
  checked(similar, 'current deictic');
  assert.deepEqual(clone(similar.recommendations).map(row => row.handle), [h.rows[1].handle]);
  const recommendation = similar.recommendations[0], exact = h.rows[1].variants[1];
  assert.equal(recommendation.variantId, exact.id);
  assert.equal(recommendation.price, exact.price);
  assert.equal(recommendation.currency, 'USD');
  assert.ok(Number.isFinite(recommendation.checkedAt));
  assert.match(recommendation.why, /Shares the butterfly motif/);
  assert.deepEqual(clone(h.store.snapshot()), before);
  assert.deepEqual(h.cart(), bag);
  const rejected = await ask('Not those');
  checked(rejected, 'literal rejection');
  assert.deepEqual(clone(rejected.recommendations), []);
  assert.match(rejected.reply, /excluding those pieces.*haven’t found another checked alternative/i);
  const exhausted = await ask('More like this');
  checked(exhausted, 'exhausted deictic');
  assert.deepEqual(clone(exhausted.recommendations), []);
  assert.match(exhausted.reply, /haven’t found another checked alternative/i);
  assert.deepEqual(clone(h.store.snapshot()), before);
  assert.deepEqual(h.cart(), bag);
  assert.equal(h.requests.length, reads, 'Warmed matching, typed/native deictics and rejection must cause zero HTTP');
  assert.deepEqual(h.errors, []);
});

for (const nativeVoice of [false, true]) test('ordinary Sea Otter charm revives exact matching and literal next options after dismissal through ' + (nativeVoice ? 'synthetic native realtime' : 'typed widget'), async t => {
  const current = product(43811, 'Sea Otter Necklace Charm', 'otter', 'Charms'), metals = ['Sterling Silver', '14/20 Gold Filled'], styles = ['Necklace Charm', 'Bracelet Charm'], engraving = ['No', 'Yes'];
  current.partsOnly = true;
  current.options = [{ name: 'Metal Choice', values: metals }, { name: 'Charm Type', values: styles }, { name: 'Engraving', values: engraving }];
  let id = 4381101;
  current.variants = metals.flatMap((metal, m) => styles.flatMap((style, s) => engraving.map((value, e) => { const numericId = String(id++); return { id: 'gid://shopify/ProductVariant/' + numericId, numericId, title: metal + ' / ' + style + ' / ' + value, price: 30 + m * 10 + s * 5 + e * 12, available: true, options: [{ name: 'Metal Choice', value: metal }, { name: 'Charm Type', value: style }, { name: 'Engraving', value }] }; })));
  const stud = product(43812, 'Sea Otter Charm Stud Earrings', 'otter'), eating = product(43813, 'Eating Otter Stud Earrings', 'otter'), butterfly = product(43814, 'Butterfly Stud Earrings', 'butterfly');
  const h = await fixture(t, nativeVoice, [current, stud, eating, butterfly]), ask = text => nativeVoice ? h.say(text) : h.command(text);
  checked(await h.command('What animal earrings do you have?'), 'earlier discovery');
  checked(await h.command('No thanks'), 'passive dismissal');
  assert.equal(h.root.querySelector('.shopping-help').hidden, true);
  const opened = await h.store.execute({ type: 'open', handle: current.handle });
  assert.equal(opened.ok, true);
  await settle(); if (nativeVoice) await h.startVoice();
  let before = clone(h.store.snapshot()), reads = h.requests.length;
  const matching = await ask('Show me matching earrings');
  checked(matching, 'matching after mute');
  assert.deepEqual(clone(matching.recommendations).map(row => row.handle).sort(), [stud.handle, eating.handle].sort());
  assert.deepEqual(clone(h.store.snapshot()), before);
  assert.equal(h.root.querySelector('.shopping-help').hidden, false);
  const help = await ask('Help me choose this piece');
  checked(help, 'ordinary charm walkthrough');
  assert.equal(h.store.snapshot().productControls.openedOption, 'Metal Choice');
  assert.deepEqual(clone(h.store.snapshot().productControls.selectedOptions), []);
  assert.equal(h.requests.length, reads, 'Explicit matching and opening the next literal charm menu are local');
  const quantity = h.d.querySelector('.product-quantity input'); quantity.value = '3'; quantity.dispatchEvent(new h.w.Event('change', { bubbles: true }));
  for (const variant of current.variants) {
    const select = h.d.querySelector('#piece-variant'); select.value = variant.id; select.dispatchEvent(new h.w.Event('change', { bubbles: true }));
    await settle(); before = clone(h.store.snapshot()); reads = h.requests.length;
    const answer = await ask('Show me matching earrings');
    checked(answer, variant.title);
    assert.deepEqual(clone(answer.recommendations).map(row => row.handle).sort(), [stud.handle, eating.handle].sort());
    for (const row of answer.recommendations) {
      const p = [stud, eating].find(p => p.handle === row.handle), expected = p.variants.find(v => v.options[0].value === variant.options[0].value);
      assert.equal(row.variantId, expected.id);
      assert.equal(row.price, expected.price);
      assert.equal(row.currency, 'USD');
      assert.ok(Number.isFinite(row.checkedAt));
      assert.match(row.why, /Shares the otter motif/);
      assert.match(row.why, /sold separately/);
    }
    assert.deepEqual(clone(h.store.snapshot()), before, 'Matching must preserve each literal current style, engraving and quantity');
    assert.equal(h.store.snapshot().productControls.quantity, 3);
    assert.deepEqual(h.cart(), []);
    assert.equal(h.requests.length, reads);
  }
  assert.deepEqual(h.errors, []);
});
