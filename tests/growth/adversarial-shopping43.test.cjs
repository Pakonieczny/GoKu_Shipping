'use strict';

// Independent adversarial acceptance: real host + bridge + widget, intentionally
// different option labels, variant prices, themes and identities from the old
// guided fixtures. Network, provider and avatar hardware are synthetic only.
// These tests do not establish browser menu rendering, native mic/audio, WebGL,
// subjective friendliness, production Shopify cart, or live payment behavior.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM, VirtualConsole } = require('jsdom');
const Bridge = require('../../brites-storefront-bridge.js');
const Voice = require('../../brites-concierge-voice.js');
const Growth = require('../../netlify/functions/_britesGrowth.js');
const rootPath = path.resolve(__dirname, '../..');
const source = name => fs.readFileSync(path.join(rootPath, name), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const settle = async () => { await new Promise(setImmediate); await new Promise(setImmediate); };
function deferred() { let resolve; return { promise: new Promise(done => { resolve = done; }), resolve }; }

function product(id, title, { type = 'Earrings', motif = 'butterfly', silver = 42, gold = 72, currency = 'USD', unavailableGold = false, description } = {}) {
  const handle = 'review43-' + id;
  const axis = type === 'Necklace' ? 'Necklace Length' : 'Hoop Size';
  const values = type === 'Necklace' ? ['16 inch', '18 inch'] : ['8.5mm', '10mm'];
  return {
    id: 'gid://shopify/Product/' + id, handle, title, type,
    url: 'https://britesjewelry.com/products/' + handle, currency,
    description: description || 'The ' + motif + ' charm measures 11 mm wide and 7 mm high. This exact published listing offers the displayed material and ' + axis.toLowerCase() + ' choices.',
    tags: ['motif:' + motif], image: 'https://cdn.shopify.com/' + handle + '-1.jpg',
    images: [1, 2, 3].map(index => ({ url: 'https://cdn.shopify.com/' + handle + '-' + index + '.jpg', alt: title + ' view ' + index })),
    options: [{ name: 'Metal Choice', values: ['Sterling Silver', '14k Gold Filled'] }, { name: axis, values }],
    variantsComplete: true, detailState: 'checked', checkedAt: Date.now(),
    variants: ['Sterling Silver', '14k Gold Filled'].flatMap((material, m) => values.map((value, i) => ({
      id: 'gid://shopify/ProductVariant/' + (id * 10 + m * 2 + i + 1),
      numericId: String(id * 10 + m * 2 + i + 1),
      title: material + ' / ' + value, price: (m ? gold : silver) + i * 4,
      available: !m || !unavailableGold,
      options: [{ name: 'Metal Choice', value: material }, { name: axis, value }]
    })))
  };
}

function catalogue() {
  return [
    product(43101, 'Butterfly Huggie Earrings'),
    product(43102, 'Rabbit Huggie Earrings', { motif: 'rabbit', silver: 48, gold: 56 }),
    product(43103, 'Elephant Hoop Earrings', { motif: 'elephant', silver: 47, gold: 79 }),
    product(43104, 'Leaf Huggie Earrings', { motif: 'leaf', silver: 39, gold: 54 }),
    product(43105, 'Butterfly Chain Necklace', { type: 'Necklace', silver: 35, gold: 55 }),
    product(43106, 'Bee Huggie Earrings', { motif: 'bee', silver: 46, gold: 57 }),
    product(43107, 'Butterfly Stud Earrings', { silver: 49, gold: 58 }),
    product(43108, 'Whale Hoop Earrings', { motif: 'whale', silver: 45, gold: 51, unavailableGold: true }),
    product(43109, 'Moon Hoop Earrings', { motif: 'moon', silver: 40, gold: 59 }),
    product(43110, 'Maple Charm', { type: 'Charm', motif: 'maple', silver: 25, gold: 40 })
  ];
}

async function fixture(t, { rows = catalogue(), mountWidget = true, partial = false, nativeVoice = false, savedPreferences = null } = {}) {
  const errors = [], vc = new VirtualConsole();
  vc.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM(source('concierge-sandbox.html'), { url: 'https://preview.example/concierge-sandbox.html', runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole: vc });
  const w = dom.window, d = w.document, requests = [];
  let productRead = null, client = null, channel = null, turn = 0;
  const packets = [], hostResults = [], customerReplies = [];
  delete d.body.dataset.catalogueSeed;
  w.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  w.HTMLElement.prototype.scrollIntoView = function () {};
  w.scrollTo = () => {};
  w.fetch = async (raw, init = {}) => {
    const url = new URL(raw, w.location.href); requests.push({ url, init });
    let body;
    if (url.pathname === '/api/growth/catalogue') body = { live: true, checkedAt: Date.now(), products: rows, pageInfo: { hasNextPage: false, endCursor: null } };
    else if (url.pathname === '/api/growth/inventory') body = { live: true, checkedAt: Date.now(), products: rows.map(p => ({ ...p, checkedAt: Date.now() })), inventory: { schema: 1, total: rows.length, offset: 0, limit: 24, loaded: rows.length, detailsLoaded: rows.length, ready: !partial, partial, expiresAt: Date.now() + 300000 }, pageInfo: { hasNextPage: false, nextOffset: null } };
    else if (url.pathname === '/api/growth/product') {
      if (productRead) await productRead(url, init);
      body = { live: true, checkedAt: Date.now(), product: rows.find(p => p.handle === url.searchParams.get('handle')) };
    } else if (url.pathname === '/api/growth/storefront-services') body = { schema: 1, guidance: {}, conflicts: [], offers: { items: [] } };
    else if (url.pathname === '/api/growth/knowledge') body = { meanings: [], live: true, checkedAt: Date.now() };
    else if (url.pathname === '/api/growth/events') body = { ok: true };
    else if (url.pathname === '/api/concierge-voice') {
      const action = init.body ? JSON.parse(init.body).action : '';
      body = action === 'start' ? { sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host\r\n', stopToken: 'synthetic-adversarial43', maxDurationMs: 120000 } : { ok: true, enabled: true, nativeAudio: true, publicDemo: true, stopped: action === 'stop' };
    }
    else if (url.pathname === '/api/concierge') body = { live: true, reply: 'Synthetic server fallback has no product authority.', products: [], meanings: [], preferences: {}, preserveSelection: true };
    else throw Error('Unexpected adversarial read: ' + url.pathname);
    return { ok: true, json: async () => clone(body) };
  };
  // New shared production modules can load before the actual bridge/host.
  for (const name of ['brites-catalogue-intents.js', 'brites-concierge-shopping-guide.js']) {
    if (fs.existsSync(path.join(rootPath, name))) w.eval(source(name));
  }
  w.eval(source('brites-storefront-bridge.js'));
  w.eval(source('concierge-sandbox.js'));
  await settle(); await w.BritesSandboxStorefront.preloadInventory(); await settle();
  const bridge = Bridge.create({ storefront: () => w.BritesSandboxStorefront });
  if (mountWidget) {
    if (savedPreferences) w.sessionStorage.setItem('brites-concierge-v1', JSON.stringify({ preferences: savedPreferences, updatedAt: Date.now() }));
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
      w.BritesConciergeVoice = { publicContext: Voice.publicContext, create(options) {
        client = Voice.create({ ...options, runtime, greeting: false, onFinalizedTurn: async value => {
          // Observe the actual widget callback before the voice client reduces
          // it to customer speech. Facts and authority assertions use this
          // return value; provider packets must carry only the customer reply.
          const result = await options.onFinalizedTurn(value);
          hostResults.push({ turn: value.turnVersion, inputItemId: value.inputItemId, text: value.text, result: clone(result) });
          return result;
        } }); return client;
      } };
    }
    w.BritesConciergeAvatar = { create() { return { setState() {}, setEmotion() {}, setVisible() {}, setPaused() {}, retry() {}, triggerGreeting() {}, clearFocus() {}, focusProduct() {}, setLevel() {}, setSpeechSignal() {}, clearProduct() {}, showProduct() {}, cancelPerformance() {}, setFloating() {}, cue() {}, destroy() {} }; } };
    const script = d.createElement('script'); script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true';
    Object.defineProperty(d, 'currentScript', { get: () => script });
    w.eval(source('brites-concierge.js')); await settle(); w.BritesConcierge.open({ focus: false }); await settle();
  }
  t.after(async () => { bridge.destroy(); w.BritesConcierge?.close(); await client?.dispose(); w.close(); });
  return {
    w, d, rows, errors, requests, bridge, store: w.BritesSandboxStorefront,
    get root() { return d.querySelector('brites-concierge')?.shadowRoot; },
    command: text => w.BritesConcierge.sendShopperCommand(text),
    lookup: fn => { productRead = fn; },
    cart: () => JSON.parse(w.sessionStorage.getItem('brites-sandbox-cart') || '[]'),
    handles: () => [...d.querySelectorAll('#demo-products [data-product-handle]')].map(card => card.dataset.productHandle),
    async startVoice() {
      [...this.root.querySelectorAll('button')].find(button => button.textContent === 'Talk to me').click(); await settle();
      this.root.querySelector('script[src$="brites-concierge-voice.js"]')?.dispatchEvent(new w.Event('load'));
      await settle(); assert.equal(client?.state, 'listening');
    },
    async say(text) {
      assert(channel?.onmessage, 'Synthetic realtime must actually be connected before a transcript is injected');
      const before = packets.filter(packet => packet.item?.content?.[0]?.text?.startsWith('Customer reply for this finalized shopper request.')).length, hostBefore = hostResults.length, packetBefore = packets.length;
      const itemId = 'adversarial43-native-' + (++turn), emit = event => channel.onmessage({ data: JSON.stringify(event) });
      emit({ type: 'input_audio_buffer.speech_started', item_id: itemId });
      emit({ type: 'input_audio_buffer.speech_stopped', item_id: itemId });
      emit({ type: 'input_audio_buffer.committed', item_id: itemId });
      emit({ type: 'conversation.item.input_audio_transcription.completed', item_id: itemId, transcript: text });
      for (let n = 0; n < 5; n++) await settle();
      const receipts = packets.filter(packet => packet.item?.content?.[0]?.text?.startsWith('Customer reply for this finalized shopper request.'));
      assert.equal(receipts.length, before + 1, text + ' must add exactly one receipt for this finalized current turn');
      const receipt = receipts.at(-1)?.item.content[0].text;
      assert(receipt, text + ' must produce an actual customer-only voice receipt');
      const value = JSON.parse(receipt.slice(receipt.indexOf('{')));
      assert.deepEqual(Object.keys(value), ['reply'], 'The speaker receives only customer copy, without facts, plans, diagnostics or control authority');
      assert.equal(typeof value.reply, 'string'); assert(value.reply.trim()); assert(value.reply.length <= 4000);
      assert.doesNotMatch(value.reply, /PRIVATE_|variantId|completedActions|matchingVariantIds|postcondition|tool_choice|function_call|turnVersion|inputItemId|storefrontBinding|schema\s*[:=]|no further action|no website action was confirmed|that preparation was interrupted|backend/i);
      assert.equal(hostResults.length, hostBefore + 1, 'Exactly one real finalized widget callback supplies the internal result');
      const completed = hostResults.at(-1); assert.equal(completed.inputItemId, itemId); assert.equal(completed.text, text);
      const responses = packets.slice(packetBefore).filter(packet => packet.type === 'response.create');
      assert.equal(responses.length, 1, 'Each host-handled current turn requests one spoken answer');
      assert.equal(responses[0].response.tool_choice, 'none', 'The spoken receipt cannot authorize another model tool action');
      assert(responses[0].response.instructions.includes(JSON.stringify(value.reply)), 'The exact customer sentence is the requested utterance');
      assert.doesNotMatch(responses[0].response.instructions, /"(?:productFacts|products|actions|completedActions|snapshot|publicContext|variantId|matchingVariantIds|engravingText|giftNote)"\s*:|PRIVATE_/i);
      customerReplies.push({ inputItemId: itemId, payload: value });
      return completed.result;
    },
    packets, hostResults, customerReplies,
    async bridgeSay(text) { const r = bridge.resolve(text); assert.equal(r.ok, true, text + ': ' + r.reason); return bridge.execute(r.action); }
  };
}

for (const page of ['collection', 'product']) test('D01: broad inventory question never becomes only current-product variants on ' + page, async t => {
  const h = await fixture(t);
  if (page === 'product') await h.command('Open Leaf Huggie Earrings');
  const before = h.requests.length, result = await h.command('What do you have?');
  assert.equal(result.ok, true, result.reply || result.error);
  assert.equal(result.productFacts?.handle, undefined, 'A broad shop question cannot borrow the open product as its subject');
  assert.match(result.reply, /earrings?/i);
  assert.match(result.reply, /necklaces?/i);
  assert.match(result.reply, /charms?/i);
  assert.doesNotMatch(result.reply, /\b(?:bracelets?|rings?)\b/i, 'Broad discovery cannot invent a ring category from the substring in earrings');
  assert.equal(h.requests.slice(before).some(r => r.url.pathname === '/api/concierge'), false, 'Ready test inventory should not need slow server inference for broad discovery');
  assert.deepEqual(h.errors, []);
});

test('D02/A01: butterfly availability question from an unrelated detailed listing discovers earrings locally', async t => {
  const h = await fixture(t); await h.command('Open Leaf Huggie Earrings');
  const before = h.requests.length, result = await h.command('Do you have butterfly earrings?');
  assert.equal(result.ok, true, result.reply || result.error);
  assert.deepEqual(h.handles().sort(), [h.rows[0].handle, h.rows[6].handle].sort());
  assert.equal(result.productFacts?.handle, undefined);
  assert.equal(h.requests.length, before, 'Complete checked inventory must satisfy this without a fallback product/concierge read');
});

test('D03: broad animal taxonomy selects animal earrings and excludes other types/motifs', async t => {
  const h = await fixture(t); await h.command('Open Leaf Huggie Earrings');
  const before = h.requests.length, result = await h.command('What animal earrings do you have?');
  assert.equal(result.ok, true, result.reply || result.error);
  assert.deepEqual(h.handles().sort(), [0, 1, 2, 5, 6, 7].map(i => h.rows[i].handle).sort());
  assert.equal(h.requests.length, before);
});

test('D04: corrective No changes motif, while negative discovery leaves the actual selection untouched', async t => {
  const h = await fixture(t); await h.command('Show leaf earrings');
  const corrected = await h.command('No, I meant butterfly earrings instead');
  assert.equal(corrected.ok, true, corrected.reply || corrected.error);
  assert.deepEqual(h.handles().sort(), [h.rows[0].handle, h.rows[6].handle].sort());
  const before = clone(h.store.snapshot()), count = h.requests.length;
  const declined = await h.command("Don't show animal earrings");
  assert.equal(declined.ok, false, 'A negated request must not execute its positive search');
  assert.deepEqual(clone(h.store.snapshot()), before);
  assert.equal(h.requests.length, count);
});

test('D08: gold filled budget is one available matching variant, with necklace/type/stock exclusions', async t => {
  const h = await fixture(t, { mountWidget: false });
  const r = await h.store.execute({ type: 'search', query: 'gold filled earrings under USD 60', filter: 'earrings' });
  assert.equal(r.ok, true, r.message);
  const handles = h.handles();
  assert.equal(handles.includes(h.rows[0].handle), false, 'Cheap silver does not make expensive gold filled affordable');
  assert.equal(handles.includes(h.rows[4].handle), false, 'A matching necklace cannot satisfy earrings');
  assert.equal(handles.includes(h.rows[7].handle), false, 'An unavailable gold option cannot satisfy its cheap price');
  assert.deepEqual(handles.sort(), [1, 3, 5, 6, 8].map(i => h.rows[i].handle).sort());
});

test('P05: cross-product, ambiguous and unpublished option requests cannot mutate current selection', async t => {
  const h = await fixture(t); await h.command('Open Leaf Huggie Earrings');
  const before = clone(h.store.snapshot().productControls);
  for (const text of ['Choose 14k Gold Filled for Butterfly Stud Earrings', 'Select 10mm for Rabbit Huggie Earrings', 'Use Sterling Silver for the other earrings', 'Select Platinum', 'Select Sterling Silver for an imaginary piece', 'Select Sterling Silver for Metal Choice on Butterfly Stud Earrings', 'Select Platinum for Metal Choice on this piece', 'No, choose 14k Gold Filled for Butterfly Stud Earrings instead', 'Open the options for Butterfly Stud Earrings']) {
    const r = await h.command(text); assert.equal(r.ok, false, text);
    assert.deepEqual(clone(h.store.snapshot().productControls), before, text);
    assert.deepEqual(h.cart(), []);
  }
});

test('A01/A02: manual product entry supports exact current dimensions without a fresh model request', async t => {
  const h = await fixture(t); await h.store.execute({ type: 'open', handle: h.rows[0].handle });
  const count = h.requests.length, r = await h.command('What size is the charm?');
  assert.equal(r.ok, true, r.reply || r.error);
  assert.equal(r.productFacts?.handle, h.rows[0].handle);
  assert.match(r.reply, /11\s*mm/i); assert.match(r.reply, /7\s*mm/i);
  assert.equal(h.store.snapshot().currentHandle, h.rows[0].handle);
  assert.equal(h.requests.length, count);
});

test('P08: incomplete add reveals the next actual option rather than dead-end instruction or false addition', async t => {
  const h = await fixture(t); await h.command('Open Butterfly Huggie Earrings');
  await h.command('Select Sterling Silver');
  const r = await h.command('Add it to my cart');
  assert.equal(r.handled, true);
  assert.deepEqual(h.cart(), []);
  const pc = h.store.snapshot().productControls;
  assert.equal(pc.optionsOpen, true, r.reply);
  assert.equal(pc.openedOption, 'Hoop Size');
  assert.match(r.reply, /8\.5mm|10mm|Hoop Size/i);
  assert.deepEqual(clone(pc.selectedOptions), [{ name: 'Metal Choice', value: 'Sterling Silver' }]);
});

test('P03/P04/P09/P14: real menu, exact selections, explicit addition, bag and mock checkout complete one flow', async t => {
  const h = await fixture(t); await h.command('Open Butterfly Huggie Earrings');
  assert.equal((await h.command('Open the metal menu')).ok, true);
  assert.equal(h.store.snapshot().productControls.optionsOpen, true);
  assert.equal(h.d.querySelector('.option-menu').hidden, false);
  assert.equal((await h.command('Select Sterling Silver')).ok, true);
  assert.equal((await h.command('Select 10mm')).ok, true);
  assert.equal((await h.command('Set quantity to 2')).ok, true);
  const selected = h.store.snapshot().productControls;
  assert.equal(selected.variantId, h.rows[0].variants[1].id);
  assert.equal(selected.quantity, 2);
  const add = await h.command('Add it to my cart');
  assert.equal(add.ok, true, add.reply || add.error);
  await settle();
  assert.equal(add.cartChanged, true, 'An explicit complete Add request must report actual host addition, not only a prepared review');
  assert.equal(h.cart().length, 1); assert.equal(h.cart()[0].variantId, h.rows[0].variants[1].numericId); assert.equal(h.cart()[0].quantity, 2);
  const bag = await h.command('Take me to my cart'); assert.equal(bag.ok, true, bag.reply); assert.equal(h.store.snapshot().pageKind, 'bag');
  assert.equal(h.d.querySelectorAll('[data-bag-line]').length, 1);
  assert.match(h.d.querySelector('.bag-page').textContent, /Sterling Silver \/ 10mm/);
  const checkout = await h.command('Open checkout'); assert.equal(checkout.ok, true, checkout.reply); assert.equal(h.store.snapshot().pageKind, 'checkout');
  assert.equal(h.store.snapshot().currentHandle, '');
  assert.match(h.d.querySelector('.checkout-page').textContent, /simulation|test/i);
  assert.equal(h.d.querySelector('input[type=email],input[type=password],[name=address],[name=card_number]'), null);
  const shipping = await h.command('Show the shipping step'); assert.equal(shipping.ok, true, shipping.reply);
  const review = await h.command('Complete test checkout'); assert.equal(review.ok, true, review.reply);
  assert.equal(h.store.snapshot().checkoutControls.complete, false);
  const check = h.d.querySelector('#confirm-test-checkout'); check.checked = true; check.dispatchEvent(new h.w.Event('change', { bubbles: true }));
  const button = [...h.d.querySelectorAll('button')].find(b => b.textContent === 'Complete test checkout'); assert(button); button.click(); await settle();
  assert.equal(h.store.snapshot().checkoutControls.complete, true);
  assert.equal(h.cart().length, 1);
  assert.equal(h.requests.some(r => r.init.method === 'POST' && /cart|checkout|payment|order/.test(r.url.pathname)), false);
  assert.deepEqual(h.errors, []);
});

test('P10: separate review route exposes exact shopper confirmation and a double click adds only once', async t => {
  const h = await fixture(t); await h.command('Open Butterfly Huggie Earrings');
  await h.command('Select Sterling Silver'); await h.command('Select 10mm');
  const result = await h.command('Review adding this piece to my cart');
  assert.equal(result.ok, true, result.reply || result.error);
  assert.deepEqual(h.cart(), []);
  const confirm = h.d.querySelector('.product-review .primary');
  assert(confirm, 'The explicit Review request must use the actual visible confirmation');
  confirm.click(); confirm.click(); await settle();
  assert.equal(h.cart().length, 1);
  assert.equal(h.cart()[0].variantId, h.rows[0].variants[1].numericId);
});

test('P12: identical title bag lines require exact current options and preserve the other stable line', async t => {
  const h = await fixture(t, { mountWidget: false });
  await h.store.execute({ type: 'open', handle: h.rows[0].handle });
  for (const v of [h.rows[0].variants[0], h.rows[0].variants[1]]) {
    await h.store.execute({ type: 'select-option', handle: h.rows[0].handle, variantId: v.id });
    await h.store.execute({ type: 'review-add', handle: h.rows[0].handle });
    h.d.querySelector('.product-review .primary').click(); await settle();
  }
  await h.store.execute({ type: 'bag' });
  const before = clone(h.store.snapshot().bagControls.lines);
  assert.equal(before.length, 2);
  assert.equal(h.bridge.resolve('Remove Butterfly Huggie Earrings from my cart').ok, false);
  assert.deepEqual(clone(h.store.snapshot().bagControls.lines), before);
  const removed = await h.bridgeSay('Remove Butterfly Huggie Earrings Sterling Silver / 10mm from my cart');
  assert.equal(removed.ok, true, removed.reason);
  assert.deepEqual(clone(h.store.snapshot().bagControls.lines).map(line => line.lineId), [before[0].lineId]);
});

test('A08/P17: an expired delayed product read cannot replace a newer actual bag page', async t => {
  const h = await fixture(t, { mountWidget: false });
  const now = h.w.Date.now; h.w.Date.now = () => now() + 300001;
  const gate = deferred(); let entered = false;
  h.lookup(async () => { entered = true; await gate.promise; });
  const pending = h.store.execute({ type: 'open', handle: h.rows[0].handle });
  await settle(); assert.equal(entered, true, 'The test must actually exercise a deferred exact read, not its warm cache');
  await h.store.execute({ type: 'bag' }); const before = clone(h.store.snapshot());
  gate.resolve(); const r = await pending;
  assert.equal(r.ok, false);
  assert.deepEqual(clone(h.store.snapshot()), before);
  assert.equal(h.d.querySelector('.product-layout'), null);
  assert.equal(h.store.snapshot().pageKind, 'bag');
});

test('R01/R02/R04: quietly prepared related options obey same-variant budget, material, stock and currency', () => {
  const Guide = require('../../brites-concierge-shopping-guide.js');
  const rows = catalogue(), current = rows[0];
  const foreign = product(43201, 'Butterfly Huggie Earrings in Canada', { silver: 10, gold: 20, currency: 'CAD' });
  const held = product(43202, 'Butterfly Mini Huggie Earrings', { silver: 10, gold: 20 }); held.recommendationHold = true;
  const soldGold = product(43203, 'Butterfly Outline Huggie Earrings', { silver: 10, gold: 20, unavailableGold: true });
  const copy = clone([...rows, foreign, held, soldGold]);
  const guide = Guide.create({ products: copy, preferences: { material: 'gold filled', interests: ['butterfly'], budget: { max: 60, currency: 'USD' } } });
  const context = { pageKind: 'product', currentHandle: current.handle, contextRevision: 9, productControls: { handle: current.handle, productId: current.id, variantId: current.variants[2].id, selectedOptions: [{ name: 'Metal Choice', value: '14k Gold Filled' }, { name: 'Hoop Size', value: '8.5mm' }], quantity: 1 } };
  const before = clone(context), pack = guide.prepare(context);
  assert.deepEqual(context, before, 'Background preparation cannot alter actual shopper controls');
  assert.deepEqual(copy, [...rows, foreign, held, soldGold], 'Preparation cannot mutate source inventory');
  assert.deepEqual(pack.alternatives.map(p => p.handle), [rows[6].handle]);
  assert.deepEqual(pack.matching.map(p => p.handle), [rows[4].handle]);
  for (const recommendation of [...pack.alternatives, ...pack.matching]) {
    assert.equal(recommendation.currency, 'USD'); assert.ok(recommendation.price <= 60);
    assert.match(recommendation.variantTitle, /Gold Filled/); assert.match(recommendation.why, /butterfly/i);
    assert.equal(recommendation.action.type, 'open'); assert.equal(recommendation.requiresFreshCheck, true);
  }
});

test('R06: uncertainty help is quiet, finite and dismissible, while explicit later help stays available', () => {
  const Guide = require('../../brites-concierge-shopping-guide.js'), rows = catalogue();
  let clock = Date.now(); const guide = Guide.create({ products: rows, now: () => clock });
  const context = { pageKind: 'product', currentHandle: rows[0].handle, contextRevision: 9, productControls: { handle: rows[0].handle, productId: rows[0].id, variantId: null, selectedOptions: [], quantity: 1 } };
  const first = guide.suggest({ message: "I'm not sure", trigger: 'uncertain', context });
  assert(first.suggestion); assert.equal(first.suggestion.kind, 'alternatives');
  assert.equal(context.productControls.variantId, null);
  guide.markShown(first.suggestion);
  assert.equal(guide.suggest({ trigger: 'uncertain', context }).suggestion, null, 'Repeated uncertainty cannot repeatedly interrupt during cooldown');
  guide.dismiss({ kind: 'alternatives', handle: rows[0].handle }); clock += 50000;
  assert.equal(guide.suggest({ trigger: 'uncertain', context }).suggestion, null, 'Dismissal remains effective after the timer');
  assert(guide.suggest({ message: 'Please suggest similar alternatives', context }).suggestion, 'A later explicit request can revive the requested help');
});

test('P02/R03: current product help is actually wired to guide copy and the next required visible menu', async t => {
  const h = await fixture(t); await h.command('Open Butterfly Huggie Earrings');
  const before = h.requests.length, result = await h.command('Help me choose this');
  assert.equal(result.ok, true, result.reply || result.error);
  assert.match(result.reply, /Metal Choice|Sterling Silver|Gold Filled/i);
  assert.equal(h.store.snapshot().currentHandle, h.rows[0].handle);
  assert.equal(h.store.snapshot().productControls.optionsOpen, true);
  assert.deepEqual(clone(h.store.snapshot().productControls.selectedOptions), []);
  assert.equal(h.requests.length, before, 'Prepared product help must use checked inventory immediately');
  assert.deepEqual(h.cart(), []);
});

test('P04/P05: actual guide option buttons execute the bound current value while ambiguous material aliases preserve the menu', async t => {
  const rows = catalogue(), p = rows[0];
  // Both literal gold-filled values exist, so a generic "gold filled" answer
  // is genuinely ambiguous rather than a fixture with only one possible match.
  p.options[0].values.push('18k Gold Filled');
  p.variants.push(...p.variants.filter(v => v.title.startsWith('14k')).map((v, index) => ({ ...clone(v), id: 'gid://shopify/ProductVariant/' + (431019 + index), numericId: String(431019 + index), title: v.title.replace('14k', '18k'), options: v.options.map(option => option.name === 'Metal Choice' ? { ...option, value: '18k Gold Filled' } : clone(option)) })));
  const h = await fixture(t, { rows }); await h.command('Open Butterfly Huggie Earrings'); await h.command('Help me choose this');
  const before = clone(h.store.snapshot().productControls), count = h.requests.length;
  const ambiguous = await h.command('Gold filled please'); assert.equal(ambiguous.ok, false, ambiguous.reply || ambiguous.error);
  assert.deepEqual(clone(h.store.snapshot().productControls), before);
  const help = h.root.querySelector('.shopping-help'), button = [...help.querySelectorAll('button')].find(node => node.textContent === 'Sterling Silver');
  assert(button, 'Helpful option chips must expose a real priced material choice'); button.click();
  for (let n = 0; n < 8; n++) await settle();
  const pc = h.store.snapshot().productControls;
  assert.deepEqual(clone(pc.selectedOptions), [{ name: 'Metal Choice', value: 'Sterling Silver' }]);
  assert.equal(pc.openedOption, 'Hoop Size'); assert.equal(pc.optionsOpen, true);
  assert.equal(h.requests.length, count); assert.deepEqual(h.cart(), []); assert.deepEqual(h.errors, []);
});

test('D01: ordinary shop-type questions enumerate the checked catalogue instead of literal query words', async t => {
  const h = await fixture(t); await h.command('Open Leaf Huggie Earrings');
  for (const text of ['What kinds of jewelry do you sell?', 'What types of jewellery do you have?', 'Can you show me what you have?']) {
    const before = h.requests.length, result = await h.command(text);
    assert.equal(result.ok, true, result.reply || result.error);
    assert.match(result.reply, /earrings?/i, text);
    assert.match(result.reply, /necklaces?/i, text);
    assert.match(result.reply, /charms?/i, text);
    assert.doesNotMatch(result.reply, /\b(?:bracelets?|rings?)\b/i, text + ' must name only actual inventory categories');
    assert.equal(h.requests.length, before, text + ' should use the checked catalogue');
  }
});

test('D05: a short just-earrings follow-up replaces necklace type and retains the animal theme', async t => {
  const h = await fixture(t);
  const first = await h.command('Show animal necklaces under USD 60');
  assert.equal(first.ok, true, first.reply || first.error);
  assert.deepEqual(h.handles(), [h.rows[4].handle]);
  const count = h.requests.length, next = await h.command('Just earrings');
  assert.equal(next.ok, true, next.reply || next.error);
  assert.deepEqual(h.handles().sort(), [0, 1, 2, 5, 6, 7].map(i => h.rows[i].handle).sort(), 'Just is conversational narrowing; the animal theme remains and the old necklace type is replaced');
  const finish = await h.command('What about gold filled?');
  assert.equal(finish.ok, true, finish.reply || finish.error);
  assert.deepEqual(h.handles().sort(), [1, 5, 6].map(i => h.rows[i].handle).sort(), 'The short material follow-up retains animal earrings and the USD 60 cap on one exact available variant; receipt: ' + JSON.stringify(finish));
  const fresh = await h.command('Show earrings');
  assert.equal(fresh.ok, true, fresh.reply || fresh.error);
  assert.deepEqual(h.handles().sort(), [1, 3, 5, 6, 8].map(i => h.rows[i].handle).sort(), 'A fresh category request resets obsolete animal narrowing while retaining the shopper\'s recent gold-filled/USD 60 preferences');
  const replacement = await h.command('Show silver leaf earrings under USD 40');
  assert.equal(replacement.ok, true, replacement.reply || replacement.error);
  assert.deepEqual(h.handles(), [h.rows[3].handle], 'A new complete theme/material/price request replaces the obsolete animal/gold-filled/USD 60 restrictions');
  assert.equal(h.requests.length, count, 'Warm short refinements must use the checked catalogue');
  assert.deepEqual(h.cart(), []);
});

for (const phrase of ['More like that', 'Anything cheaper?', 'Not those']) test('D05: checked collection short follow-up retains scope and gives honest local recovery: ' + phrase, async t => {
  const h = await fixture(t);
  await h.command('Show animal earrings gold filled under USD 60');
  const eligible = [1, 5, 6].map(i => h.rows[i].handle).sort();
  assert.deepEqual(h.handles().sort(), eligible);
  const count = h.requests.length, result = await h.command(phrase);
  assert.equal(result.ok, true, result.reply || result.error);
  assert.doesNotMatch(result.reply, /Synthetic server fallback/i, 'A short checked-collection question needs useful grounded recovery');
  assert.match(result.reply, /checked|loaded|collection|which|choose|name|prefer|different|instead|other|price|cost|compare|lowest|budget|earrings?/i);
  assert.equal(h.store.snapshot().pageKind, 'collection');
  assert.deepEqual(h.handles().sort(), eligible, 'A vague comparison/rejection cannot widen or clear exact theme/material/USD-cap criteria');
  for (const row of (result.recommendations || [])) {
    assert(eligible.includes(row.handle)); assert.equal(row.currency, 'USD'); assert(row.price <= 60); assert.match(row.variantTitle, /Gold Filled/i);
  }
  assert.equal(h.requests.length, count, 'Loaded reference choices and honest clarification need no new model/product read');
  assert.deepEqual(h.cart(), []);
});

for (const phrase of ['More like that', 'Anything cheaper?', 'Not those', 'Just earrings', 'What about gold filled?']) test('D05: actual current-product short follow-up retains literal choices and relevant preferences: ' + phrase, async t => {
  const rows = catalogue(); rows.push(product(43112, 'Butterfly Teardrop Earrings', { silver: 31, gold: 61 }));
  rows.push(product(43113, 'Butterfly Earrings in Canada', { silver: 9, gold: 19, currency: 'CAD' }));
  const h = await fixture(t, { rows });
  await h.command('Show silver butterfly earrings under USD 60');
  await h.command('Open Butterfly Huggie Earrings'); await h.command('Select Sterling Silver'); await h.command('Select 10mm');
  let rejected = [];
  if (phrase === 'Not those') {
    const shown = await h.command('Show me similar pieces');
    assert.equal(shown.ok, true, shown.reply || shown.error);
    rejected = (shown.recommendations || []).map(row => row.handle);
    assert(rejected.length > 0, 'Not those must refer to actual suggestions that were just shown');
  }
  const before = clone(h.store.snapshot()), count = h.requests.length, result = await h.command(phrase);
  assert.equal(result.ok, true, result.reply || result.error);
  assert.doesNotMatch(result.reply, /Synthetic server fallback/i);
  if (phrase === 'Just earrings') {
    assert.equal(h.store.snapshot().pageKind, 'collection');
    assert.deepEqual(h.handles().sort(), [rows[0].handle, rows[6].handle, rows[10].handle].sort(), 'Short type narrowing preserves butterfly, silver and USD 60 rather than resetting to unrelated motifs/markets');
  } else {
    assert.deepEqual(clone(h.store.snapshot()), before, 'A question or rejection never changes the actual product/options/quantity');
    if (phrase === 'What about gold filled?') {
      assert.match(result.reply, /Gold Filled/i);
      assert.deepEqual(clone(result.requestedFacts?.priceRange), { min: 72, max: 76, currency: 'USD' }, 'The material answer must be underpinned by exact checked matching variant prices; the question does not require unsolicited spoken pricing');
      assert.equal(result.requestedFacts?.availableVariants, 2);
      assert.deepEqual(clone(result.requestedFacts?.matchingVariantIds).sort(), h.rows[0].variants.filter(v => v.title.startsWith('14k')).map(v => v.id).sort());
      assert.deepEqual(clone(h.store.snapshot().productControls.selectedOptions), before.productControls.selectedOptions);
    } else if (phrase === 'Not those') {
      assert.match(result.reply, /different|prefer|instead|other|choose|which|what|browse|show|compare|checked|take your time/i);
      assert((result.recommendations || []).every(row => !rejected.includes(row.handle)), 'Rejected suggested identities cannot immediately be offered again');
    } else {
      assert((result.recommendations || []).length > 0, 'Current exact product must underpin grounded alternatives');
      for (const row of result.recommendations) {
        assert.equal(row.currency, 'USD'); assert.match(row.title, /Butterfly.*Earrings/i); assert.match(row.variantTitle, /Sterling Silver/i);
        assert(row.price <= 60); assert.notEqual(row.handle, rows[0].handle);
        if (phrase === 'Anything cheaper?') assert(row.price < 46, 'Cheaper is strictly below the actual selected unit price');
      }
    }
  }
  assert.equal(h.requests.length, count, 'Current checked facts, refinements and suggestions should already be available');
  assert.deepEqual(h.cart(), []);
});

test('D04/D05: not-those correction remains positive discovery while an explicit negative is non-mutating', async t => {
  const h = await fixture(t); await h.command('Show butterfly earrings gold filled under USD 60');
  assert.deepEqual(h.handles(), [h.rows[6].handle]);
  const count = h.requests.length, correction = await h.command('Not those, show animal earrings');
  assert.equal(correction.ok, true, correction.reply || correction.error);
  assert.deepEqual(h.handles().sort(), [1, 5, 6].map(i => h.rows[i].handle).sort());
  const before = clone(h.store.snapshot()), negative = await h.command("Don't show animal earrings");
  assert.equal(negative.ok, false, negative.reply || negative.error);
  assert.deepEqual(clone(h.store.snapshot()), before);
  assert.equal(h.requests.length, count); assert.deepEqual(h.cart(), []);
});

test('P06: signed or imprecise length strings cannot normalize into a published choice', async t => {
  const h = await fixture(t); await h.command('Open Butterfly Chain Necklace');
  await h.command('Select Sterling Silver'); await h.command('Select 18 inch');
  const before = clone(h.store.snapshot().productControls), count = h.requests.length;
  assert.equal(before.variantId, h.rows[4].variants[1].id);
  for (const text of ['Select -18 inch', 'Select -18"', 'Select −18 inches', 'Select −18"', 'Select +18 inch', 'Select 18.0 inch', 'Select 18.5 inch', 'Select 18 inches or 16 inches']) {
    assert.equal(h.bridge.resolve(text).ok, false, text + ' must be refused before a host mutation');
    assert.deepEqual(clone(h.store.snapshot().productControls), before);
  }
  assert.equal(h.requests.length, count); assert.deepEqual(h.cart(), []);
});

test('A01/D05: current-product pronouns and material facts do not become unrelated global discovery', async t => {
  const h = await fixture(t); await h.store.execute({ type: 'open', handle: h.rows[0].handle });
  for (const text of ['Do they come in silver?', 'What metals can I choose?', 'Show me their sizes']) {
    const before = h.requests.length, result = await h.command(text);
    assert.equal(result.ok, true, result.reply || result.error);
    assert.equal(h.store.snapshot().pageKind, 'product', text);
    assert.equal(h.store.snapshot().currentHandle, h.rows[0].handle, text);
    assert.match(result.reply, text.includes('sizes') ? /8\.5mm|10mm|11\s*mm/ : /Sterling Silver|14k Gold Filled/i);
    assert.equal(h.requests.length, before, text + ' should use exact current facts');
    assert.deepEqual(h.cart(), []);
  }
});

for (const nativeVoice of [false, true]) for (const phrase of ['What am I looking at?', 'Which piece is this?', 'What is the current piece?', 'What size is it?', 'How much is this?', 'What metals can I choose?']) test('A01/A03/R08: literal current question follows manual listing entry after older animal cards: ' + phrase + ' / ' + (nativeVoice ? 'synthetic realtime' : 'typed'), async t => {
  const h = await fixture(t, { nativeVoice }), p = h.rows[6], selected = p.variants[1];
  const old = await h.command('What animal earrings do you have?');
  assert.equal(old.ok, true, old.reply || old.error); assert.equal(h.handles().length, 6);
  const link = h.d.querySelector('#demo-products [data-product-handle="' + p.handle + '"] a');
  assert(link, 'The shopper must enter a real displayed listing link rather than an assistant open command'); link.click();
  for (let n = 0; n < 5; n++) await settle();
  assert.equal(h.store.snapshot().pageKind, 'product'); assert.equal(h.store.snapshot().currentHandle, p.handle);
  assert.equal(h.d.querySelector('.product-copy h1').textContent, p.title);
  const nativeSelect = h.d.querySelector('#piece-variant'), quantity = h.d.querySelector('input[aria-label="Quantity of this exact piece"]');
  nativeSelect.value = selected.id; nativeSelect.dispatchEvent(new h.w.Event('change', { bubbles: true }));
  quantity.value = '2'; quantity.dispatchEvent(new h.w.Event('change', { bubbles: true }));
  await settle();
  if (nativeVoice) await h.startVoice();
  const before = clone(h.store.snapshot()), count = h.requests.length;
  assert.equal(before.productControls.variantId, selected.id); assert.equal(before.productControls.quantity, 2);
  const result = await (nativeVoice ? h.say(phrase) : h.command(phrase));
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.equal(result.productFacts?.handle, p.handle, 'The exact manually opened product must provide the answer');
  assert.equal(result.productFacts.productId, p.id); assert.equal(result.productFacts.title, p.title);
  assert.equal(result.verified, true); assert.equal(result.live, true); assert.equal(result.cached, true);
  assert(Number.isFinite(result.checkedAt) && Date.now() - result.checkedAt < 300000);
  assert.deepEqual(clone(result.productFacts.sources), [{ title: 'Published Brites product details', url: p.url, checkedAt: result.checkedAt }]);
  assert.equal(result.productFacts.selectedVariant.id, selected.id); assert.equal(result.productFacts.quantity, 2);
  assert.match(result.reply, /Butterfly Stud Earrings/);
  assert.doesNotMatch(result.reply, /Synthetic server fallback|choose a listing|select a piece|connect with animal|Rabbit Huggie|Elephant Hoop/i, 'Old discovery or another card cannot answer the current-product question');
  if (/size/i.test(phrase)) { assert.match(result.reply, /11\s*mm/); assert.match(result.reply, /7\s*mm/); }
  else if (/much/i.test(phrase)) {
    assert.match(result.reply, /USD 53\.00.*each.*quantity 2.*USD 106\.00/i, 'Quote the actual manual selected unit price and two-item subtotal');
    assert.equal(result.productFacts.selectedVariant.price, 53); assert.equal(result.productFacts.itemTotalPrice, 106);
  } else if (/metals/i.test(phrase)) { assert.match(result.reply, /Sterling Silver/); assert.match(result.reply, /14k Gold Filled/); }
  else { assert.match(result.reply, /charm measures 11 mm wide and 7 mm high/i, 'An identity answer includes this listing’s checked details'); }
  assert.deepEqual(clone(h.store.snapshot()), before, 'Factual answers preserve the manually chosen controls, current page, quantity and navigation');
  assert.equal(h.requests.length, count, 'The checked current facts must use zero new model/product HTTP after the manual view is warm');
  assert.deepEqual(h.cart(), []); assert.deepEqual(h.errors, []);
});

test('A02: hidden template and noscript measurements cannot become visible facts after public product normalization', async t => {
  const rows = catalogue(), p = rows[0];
  const normalized = Growth.normalizeProduct({
    id: p.id, handle: p.handle, title: p.title, status: 'ACTIVE', onlineStoreUrl: p.url, productType: p.type,
    descriptionHtml: '<template>The charm measures 1 mm wide and 2 mm high.</template><noscript>The charm measures 3 mm wide and 4 mm high.</noscript><!-- The charm measures 5 mm wide and 6 mm high. --><p>The charm measures 11 mm wide and 7 mm high.</p>',
    options: p.options, variants: { nodes: p.variants.map(v => ({ ...v, availableForSale: v.available, selectedOptions: v.options })), pageInfo: { hasNextPage: false } }
  }, 'USD', Date.now());
  p.description = normalized.description;
  const h = await fixture(t, { rows }); await h.store.execute({ type: 'open', handle: p.handle });
  const count = h.requests.length, result = await h.command('What size is the charm?');
  assert.equal(result.ok, true, result.reply || result.error);
  assert.match(result.reply, /11\s*mm/); assert.match(result.reply, /7\s*mm/);
  assert.doesNotMatch(result.reply, /(?<![\d.])[1-6]\s*mm\b/, 'Only actually published visible measurements survive the upstream normalization and current-fact reply; the actual 8.5mm hoop option is legitimate');
  assert((result.productFacts?.dimensions || []).every(sentence => !/(?<![\d.])[1-6]\s*mm\b/.test(sentence)));
  assert.equal(h.requests.length, count); assert.deepEqual(h.cart(), []);
});

for (const nativeVoice of [false, true]) for (const kind of ['metal-only', 'hoop-size', 'chain-length']) test('A02/R08: generic size question distinguishes missing physical measurements from published option axes: ' + kind + ' / ' + (nativeVoice ? 'synthetic realtime' : 'typed'), async t => {
  const p = kind === 'chain-length'
    ? product(43122, 'Butterfly Chain Necklace', { type: 'Necklace', description: 'This butterfly necklace offers published metal and chain length choices.' })
    : product(43121, kind === 'metal-only' ? 'Butterfly Cutout Stud Earrings' : 'Butterfly Huggie Earrings', { description: kind === 'metal-only' ? 'These butterfly cutout stud earrings are offered in the published metal options.' : 'These butterfly huggie earrings offer published metal and hoop size choices.' });
  if (kind === 'metal-only') {
    p.options = [p.options[0]];
    p.variants = [p.variants[0], p.variants[2]].map(v => ({ ...v, title: v.options[0].value, options: [v.options[0]] }));
  }
  const h = await fixture(t, { rows: [p], nativeVoice }); await h.store.execute({ type: 'open', handle: p.handle });
  if (nativeVoice) await h.startVoice();
  const before = clone(h.store.snapshot()), count = h.requests.length, ask = text => nativeVoice ? h.say(text) : h.command(text);
  const result = await ask('What size is this piece?');
  assert.equal(result.ok, true, JSON.stringify(result)); assert.equal(result.productFacts?.handle, p.handle);
  assert.deepEqual(clone(result.productFacts.dimensions), [], 'Option length/diameter values are not physical charm or pendant measurements');
  if (kind === 'metal-only') {
    assert.match(result.reply, /size|dimensions?|measurements?/i);
    assert.match(result.reply, /(?:do(?:es)? not|doesn['’]t).*?(?:confirm|give|publish|provide|list|state)|not (?:published|provided|listed|available|stated|verified)|no (?:physical|published|verified|size)|unknown/i, 'An exact listing without measurements must say so honestly');
    assert.doesNotMatch(result.reply, /Metal Choice|Sterling Silver|Gold Filled/i, 'Size must not be answered with unrelated metal choices');
  } else {
    const group = kind === 'hoop-size' ? 'Hoop Size' : 'Necklace Length';
    assert.match(result.reply, new RegExp(group, 'i'), 'Published option sizes must keep their actual axis label');
    assert.match(result.reply, kind === 'hoop-size' ? /8\.5\s*mm/ : /16\s*inch/i);
    assert.match(result.reply, kind === 'hoop-size' ? /10\s*mm/ : /18\s*inch/i);
    assert.doesNotMatch(result.reply, /(?:charm|pendant)\s+(?:is|measures|diameter)\s+(?:8\.5|10|16|18)\s*(?:mm|inch)/i);
    const specific = await ask(kind === 'hoop-size' ? 'What width is the charm?' : 'What width is the pendant?');
    assert.equal(specific.ok, true, JSON.stringify(specific));
    assert.match(specific.reply, /(?:do(?:es)? not|doesn['’]t).*?(?:confirm|give|publish|provide|list|state)|not (?:published|provided|listed|available|stated|verified)|no (?:physical|published|verified|size)|unknown/i, 'A particular charm/pendant dimension cannot be inferred from hoop or chain choices');
    assert.deepEqual(clone(specific.productFacts?.dimensions), []);
  }
  assert.deepEqual(clone(h.store.snapshot()), before, 'Size questions preserve the actual current product, literal choices and quantity');
  assert.equal(h.requests.length, count); assert.deepEqual(h.cart(), []);
});

for (const [page, phrase] of [['collection', 'More like that'], ['collection', 'Anything cheaper?'], ['collection', 'Not those'], ['product', 'More like that'], ['product', 'Not those']]) test('R08/D05: actual synthetic realtime uses the same grounded short-follow-up behavior: ' + page + ' / ' + phrase, async t => {
  const rows = catalogue(); rows.push(product(43112, 'Butterfly Teardrop Earrings', { silver: 31, gold: 61 }));
  rows.push(product(43113, 'Butterfly Earrings in Canada', { silver: 9, gold: 19, currency: 'CAD' }));
  const h = await fixture(t, { rows, nativeVoice: true });
  let rejected = [];
  if (page === 'collection') await h.command('Show animal earrings gold filled under USD 60');
  else {
    await h.command('Show silver butterfly earrings under USD 60');
    await h.command('Open Butterfly Huggie Earrings'); await h.command('Select Sterling Silver'); await h.command('Select 10mm');
    if (phrase === 'Not those') { const shown = await h.command('Show me similar pieces'); rejected = (shown.recommendations || []).map(row => row.handle); assert(rejected.length > 0); }
  }
  await h.startVoice(); const before = clone(h.store.snapshot()), count = h.requests.length;
  const result = await h.say(phrase); assert.equal(result.ok, true, JSON.stringify(result));
  assert.doesNotMatch(result.reply || '', /Synthetic server fallback/i);
  if (page === 'collection') {
    assert.equal(h.store.snapshot().pageKind, 'collection');
    assert.deepEqual(h.handles().sort(), [1, 5, 6].map(i => h.rows[i].handle).sort());
    assert.match(result.reply, /checked|loaded|collection|which|choose|name|prefer|different|instead|other|price|cost|compare|lowest|budget|earrings?/i);
  } else {
    assert.deepEqual(clone(h.store.snapshot()), before);
    if (phrase === 'More like that') {
      assert((result.recommendations || []).length > 0);
      for (const row of result.recommendations) { assert.equal(row.currency, 'USD'); assert.match(row.title, /Butterfly.*Earrings/i); assert.match(row.variantTitle, /Sterling Silver/i); assert(row.price <= 60); assert.notEqual(row.handle, rows[0].handle); }
    } else { assert((result.recommendations || []).every(row => !rejected.includes(row.handle))); assert.match(result.reply, /different|prefer|instead|other|choose|which|what|browse|show|compare|checked|take your time/i); }
  }
  assert.equal(h.requests.length, count, 'Synthetic realtime must use the same cached local recovery/alternatives without remote inference');
  assert.deepEqual(h.cart(), []);
});

test('R01/R03/R04: actual matching request respects necklace type, shows native option price and leaves current selection intact', async t => {
  const rows = catalogue(); rows.push(product(43111, 'Butterfly Outline Charm', { type: 'Charm', silver: 19, gold: 29 }));
  const h = await fixture(t, { rows }); await h.command('Open Butterfly Huggie Earrings');
  await h.command('Select Sterling Silver'); await h.command('Select 10mm');
  const before = clone(h.store.snapshot()), count = h.requests.length;
  const result = await h.command('A matching necklace?');
  assert.equal(result.ok, true, result.reply || result.error);
  assert.deepEqual(clone(h.store.snapshot()), before, 'Comparison preparation must preserve actual product controls and route');
  assert.equal(h.requests.length, count, 'Matching suggestions must already be ready');
  assert.deepEqual(clone(result.recommendations || []).map(row => row.handle), [rows[4].handle]);
  assert.match(result.reply, /\b35(?:\.00)?\b/); assert.match(result.reply, /\bUSD\b/);
  assert.match(result.reply, /Sterling Silver/i);
  const help = h.root.querySelector('.shopping-help'); assert.equal(help.hidden, false);
  assert.match(help.textContent, /Butterfly Chain Necklace/);
  assert.match(help.textContent, /\b35(?:\.00)?\b/); assert.match(help.textContent, /\bUSD\b/);
  assert.doesNotMatch(help.textContent, /Butterfly Outline Charm/);
  assert.deepEqual(h.cart(), []);
});

for (const nativeVoice of [false, true]) test('R02/R08: inherited positive motif follows the actual manually opened charm while exact variant and explicit negative preferences remain strict / ' + (nativeVoice ? 'synthetic realtime' : 'typed'), async t => {
  for (const [scenario, extra, expectMatch] of [['current-motif', {}, true], ['explicit-otter-exclusion', { excludedInterests: ['otter'] }, false], ['explicit-material-exclusion', { excludedMetals: ['gold filled'] }, false], ['native-budget-cap', { budget: 50 }, false]]) {
    const current = product(43251, 'Sea Otter Necklace Charm', { type: 'Charm', motif: 'otter', silver: 24, gold: 44 }); current.partsOnly = true;
    const groups = ['Necklace Charm', 'Bracelet Charm']; current.options[1] = { name: 'Charm Type', values: groups };
    current.variants.forEach((variant, index) => { variant.options[1] = { name: 'Charm Type', value: groups[index % 2] }; variant.title = variant.options.map(option => option.value).join(' / '); });
    const good = product(43252, 'Eating Otter Stud Earrings', { motif: 'otter', silver: 42, gold: 56 });
    const deceptive = product(43253, 'Sea Otter Hoop Earrings', { motif: 'otter', silver: 12, gold: 89 });
    const unavailable = product(43254, 'Sea Otter Huggie Earrings', { motif: 'otter', silver: 18, gold: 45, unavailableGold: true });
    const foreign = product(43255, 'Sea Otter Stud Earrings Canada', { motif: 'otter', silver: 19, gold: 39, currency: 'CAD' });
    const preferences = { interests: ['butterfly'], materialQuery: 'gold filled', metal: 'gold', budget: 60, budgetCurrency: 'USD', ...extra };
    const h = await fixture(t, { rows: [current, good, deceptive, unavailable, foreign], nativeVoice, savedPreferences: preferences });
    const restored = JSON.parse(h.w.sessionStorage.getItem('brites-concierge-v1')).preferences;
    assert.deepEqual(restored.interests, ['butterfly'], 'The real widget must load a prior positive theme rather than a convenient neutral fixture');
    const link = h.d.querySelector('#demo-products [data-product-handle="' + current.handle + '"] a'); assert(link); link.click();
    for (let n = 0; n < 5; n++) await settle();
    assert.equal(h.store.snapshot().currentHandle, current.handle);
    const select = h.d.querySelector('#piece-variant'), quantity = h.d.querySelector('input[aria-label="Quantity of this exact piece"]');
    select.value = current.variants[1].id; select.dispatchEvent(new h.w.Event('change', { bubbles: true }));
    quantity.value = '2'; quantity.dispatchEvent(new h.w.Event('change', { bubbles: true }));
    const added = await h.command('Add this exact piece to my cart'); assert.equal(added.ok, true, JSON.stringify(added)); assert.equal(added.cartChanged, true);
    const cart = clone(h.cart()); assert.equal(cart.length, 1); assert.equal(cart[0].quantity, 2);
    if (nativeVoice) await h.startVoice();
    const before = clone(h.store.snapshot()), count = h.requests.length;
    const result = await (nativeVoice ? h.say('Show me matching earrings') : h.command('Show me matching earrings'));
    assert.equal(result.ok, true, scenario + ': ' + JSON.stringify(result));
    const rows = clone(result.recommendations || []);
    if (expectMatch) {
      assert.deepEqual(rows.map(row => row.handle), [good.handle], 'Exact current otter motif wins over a positive historical butterfly theme, while one available native variant must meet every actual preference');
      assert.equal(rows[0].variantId, good.variants[2].id); assert.equal(rows[0].price, 56); assert.equal(rows[0].currency, 'USD');
      assert.match(rows[0].variantTitle, /14k Gold Filled/); assert.match(rows[0].why, /Shares the otter motif/); assert.match(rows[0].why, /sold separately/);
    } else { assert.deepEqual(rows, [], scenario + ' must remain an actual restriction'); assert.match(result.reply, /haven.t found.*matching earrings/i); }
    assert.deepEqual(clone(h.store.snapshot()), before, scenario + ' preserves manually chosen material/style, quantity and exact current route');
    assert.deepEqual(h.cart(), cart, scenario + ' preserves the nonempty exact cart');
    assert.equal(h.requests.length, count, scenario + ' is checked local matching with zero new HTTP');
    assert.deepEqual(h.errors, []);
  }
});

test('R06: actual uncertainty support accepts dismissal without mutation and explicit suggestions can revive it', async t => {
  const h = await fixture(t); await h.command('Open Butterfly Huggie Earrings');
  const before = clone(h.store.snapshot()), count = h.requests.length;
  const uncertain = await h.command("I'm not sure");
  assert.equal(uncertain.ok, true, uncertain.reply || uncertain.error);
  assert((uncertain.recommendations || []).length > 0, 'Uncertainty must offer grounded alternatives');
  assert.deepEqual(clone(h.store.snapshot()), before);
  const decline = await h.command('No thanks'); assert.equal(decline.ok, true, decline.reply);
  assert.equal(h.root.querySelector('.shopping-help').hidden, true);
  assert.deepEqual(clone(h.store.snapshot()), before);
  const revive = await h.command('Please show me similar pieces');
  assert.equal(revive.ok, true, revive.reply || revive.error);
  assert((revive.recommendations || []).length > 0);
  assert.equal(h.root.querySelector('.shopping-help').hidden, false);
  assert.deepEqual(clone(h.store.snapshot()), before);
  assert.equal(h.requests.length, count);
});

test('R03: cheaper alternatives use one lower-priced available material in the same native currency', async t => {
  const rows = catalogue(); rows.push(product(43112, 'Butterfly Teardrop Earrings', { silver: 31, gold: 61 }));
  rows.push(product(43113, 'Butterfly Earrings in Canada', { silver: 9, gold: 19, currency: 'CAD' }));
  const h = await fixture(t, { rows }); await h.command('Open Butterfly Huggie Earrings');
  await h.command('Select Sterling Silver'); await h.command('Select 10mm');
  const before = clone(h.store.snapshot()), count = h.requests.length;
  const result = await h.command('Anything cheaper?');
  assert.equal(result.ok, true, result.reply || result.error);
  assert.deepEqual(clone(h.store.snapshot()), before);
  const recs = clone(result.recommendations || []);
  assert(recs.some(row => row.handle === rows[10].handle), 'A genuine lower-priced motif alternative should be offered');
  assert.equal(recs.some(row => row.handle === rows[11].handle), false, 'Cheaper CAD cannot be compared to the selected USD price');
  for (const row of recs) { assert.equal(row.currency, 'USD'); assert(row.price < 46); assert.match(row.variantTitle, /Sterling Silver/i); }
  assert.equal(h.requests.length, count);
});

test('D02/R07: meaningful recipient and occasion words defer to the existing gift core rather than inventing a local no-match', async t => {
  const h = await fixture(t); await h.command('Open Leaf Huggie Earrings');
  const before = clone(h.store.snapshot()), count = h.requests.length;
  const result = await h.command("I want silver butterfly earrings for my sister's birthday under USD 60");
  assert.equal(h.requests.slice(count).some(request => request.url.pathname === '/api/concierge'), true, 'Recipient and birthday must not be literal motif tokens used to declare no matching butterfly designs');
  assert.match(result.reply, /Synthetic server fallback/);
  assert.deepEqual(clone(h.store.snapshot()), before, 'This synthetic core has no product authority; it must preserve the real page');
  assert.deepEqual(h.cart(), []);
});

test('P16: recipient, birthday and command-looking words inside a private gift note stay one literal field with no model/history leakage', async t => {
  const h = await fixture(t); await h.command('Open Leaf Huggie Earrings');
  for (const literal of ['Neutral private fixture note PRIVATE_ADVERSARIAL43', "For my sister's birthday then open my cart PRIVATE_ADVERSARIAL43"]) {
    const before = h.requests.length, result = await h.command('Set gift note to ' + literal);
    assert.equal(result.ok, true, result.reply || result.error);
    const stored = JSON.parse(h.w.sessionStorage.getItem('brites-sandbox-gift-preferences') || '{}');
    assert.equal(stored.giftNote, literal, 'Only the exact visible local field receives the literal');
    assert.equal(h.d.querySelector('[data-store-section=gifts] [name=note]').value, literal);
    assert.equal(h.store.snapshot().pageKind, 'product'); assert.equal(h.store.snapshot().currentHandle, h.rows[3].handle);
    assert.equal(JSON.stringify(h.store.snapshot()).includes(literal), false, 'Public awareness must omit the private value');
    assert.equal(JSON.stringify(result).includes(literal), false, 'Public action receipt must omit the private value');
    assert.equal((h.w.sessionStorage.getItem('brites-concierge-v1') || '').includes(literal), false, 'Conversation/history storage must redact private text');
    assert.equal(h.root.querySelector('.messages').textContent.includes(literal), false);
    assert.equal(h.requests.slice(before).some(request => request.url.pathname === '/api/concierge'), false, 'A gifting-context heuristic cannot send the opaque setter to the model');
    assert.equal(h.requests.slice(before).some(request => (request.init.body || '').includes(literal)), false, 'Telemetry/request bodies must omit the private value');
    assert.deepEqual(h.cart(), []);
  }
});

test('R08: actual synthetic realtime transcript path completes discovery, current details, guidance, exact add, bag and mock checkout', async t => {
  const h = await fixture(t, { nativeVoice: true });
  await h.store.execute({ type: 'open', handle: h.rows[3].handle }); await h.startVoice();
  const reads = h.requests.length;
  const butterfly = await h.say('Do you have butterfly earrings?');
  assert.equal(butterfly.ok, true, JSON.stringify(butterfly));
  assert.deepEqual(h.handles().sort(), [h.rows[0].handle, h.rows[6].handle].sort());
  const animals = await h.say('What animal earrings do you have?');
  assert.equal(animals.ok, true, JSON.stringify(animals));
  assert.deepEqual(h.handles().sort(), [0, 1, 2, 5, 6, 7].map(i => h.rows[i].handle).sort());
  assert.equal(h.requests.length, reads, 'Warm native discovery must share the same zero-read path as typing');
  const open = await h.say('Open Butterfly Huggie Earrings'); assert.equal(open.ok, true, JSON.stringify(open));
  const facts = await h.say('What size is the charm?'); assert.equal(facts.ok, true, JSON.stringify(facts));
  assert.equal(facts.productFacts.handle, h.rows[0].handle); assert.match(facts.reply, /11\s*mm/); assert.match(facts.reply, /7\s*mm/);
  const guidance = await h.say('Help me choose this'); assert.equal(guidance.ok, true, JSON.stringify(guidance));
  assert.equal(h.store.snapshot().productControls.openedOption, 'Metal Choice');
  assert.deepEqual(clone(h.store.snapshot().productControls.selectedOptions), []);
  const silver = await h.say('Select Sterling Silver'); assert.equal(silver.ok, true, JSON.stringify(silver));
  assert.equal(h.store.snapshot().productControls.openedOption, 'Hoop Size');
  const size = await h.say('Select 10mm'); assert.equal(size.ok, true, JSON.stringify(size));
  assert.equal(h.store.snapshot().productControls.variantId, h.rows[0].variants[1].id);
  const addition = await h.say('Add this piece to my cart'); assert.equal(addition.ok, true, JSON.stringify(addition));
  assert.equal(addition.cartChanged, true); assert.equal(h.cart().length, 1); assert.equal(h.cart()[0].variantId, h.rows[0].variants[1].numericId);
  const bag = await h.say('Take me to my cart'); assert.equal(bag.ok, true, JSON.stringify(bag)); assert.equal(h.store.snapshot().pageKind, 'bag');
  const checkout = await h.say('Open checkout'); assert.equal(checkout.ok, true, JSON.stringify(checkout)); assert.equal(h.store.snapshot().pageKind, 'checkout');
  const shipping = await h.say('Show the shipping step'); assert.equal(shipping.ok, true, JSON.stringify(shipping)); assert.equal(h.store.snapshot().checkoutControls.step, 'shipping');
  const prepare = await h.say('Complete test checkout'); assert.equal(prepare.ok, true, JSON.stringify(prepare)); assert.equal(h.store.snapshot().checkoutControls.complete, false);
  assert.match(h.d.querySelector('.checkout-page').textContent, /test|simulation/i);
  const acknowledgement = h.d.querySelector('#confirm-test-checkout'); acknowledgement.checked = true; acknowledgement.dispatchEvent(new h.w.Event('change', { bubbles: true }));
  [...h.d.querySelectorAll('button')].find(button => button.textContent === 'Complete test checkout').click(); await settle();
  assert.equal(h.store.snapshot().checkoutControls.complete, true);
  assert.equal(h.requests.slice(reads).some(request => request.url.pathname === '/api/concierge'), false, 'Warm voice shopping flow must not infer remotely: ' + JSON.stringify(h.requests.slice(reads).filter(request => request.url.pathname === '/api/concierge').map(request => JSON.parse(request.init.body))));
  assert.equal(h.requests.some(request => /cart\/add|payment|order/.test(request.url.pathname)), false);
  assert.deepEqual(h.errors, []);
});
