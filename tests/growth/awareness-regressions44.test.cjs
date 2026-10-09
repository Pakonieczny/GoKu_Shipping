'use strict';
// Public-fact and delayed-response regressions. Transport and avatar hardware
// are synthetic; host, bridge, widget and shared facts code are the real code.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM, VirtualConsole } = require('jsdom');
const Growth = require('../../netlify/functions/_britesGrowth.js');
const Guide = require('../../brites-concierge-shopping-guide.js');
const Widget = require('../../brites-concierge.js');
const root = path.resolve(__dirname, '../..');
const read = name => fs.readFileSync(path.join(root, name), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const settle = async () => { await new Promise(setImmediate); await new Promise(setImmediate); };
const deferred = () => { let resolve; return { promise: new Promise(done => { resolve = done; }), resolve }; };

function product(description) {
  return { id: 'gid://shopify/Product/84401', handle: 'fox-medallion-necklace', title: 'Fox Medallion Necklace', type: 'Necklace', currency: 'USD', url: 'https://britesjewelry.com/products/fox-medallion-necklace', description, image: 'https://cdn.shopify.com/fox-medallion.jpg', tags: ['motif:fox'], checkedAt: Date.now(), detailState: 'checked', variantsComplete: true,
    options: [{ name: 'Metal', values: ['Sterling Silver'] }, { name: 'Chain Length', values: ['18 inch'] }],
    variants: [{ id: 'gid://shopify/ProductVariant/844011', numericId: '844011', title: 'Sterling Silver / 18 inch', price: 57, available: true, options: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Chain Length', value: '18 inch' }] }] };
}
async function backend(piece, message) {
  return Growth.concierge({ message, preferences: {}, context: { pageKind: 'product', currentHandle: piece.handle, currency: 'USD', productControls: { handle: piece.handle, productId: piece.id, variantId: piece.variants[0].id, quantity: 2 } },
    shopify: { readProduct: async () => ({ product: clone(piece), fromCache: true }) }, service: { productIssues: async () => piece.cartHold || piece.recommendationHold ? [{ productId: piece.id, issues: [{ kind: 'identity', status: 'open', blocks: [piece.cartHold ? 'cart' : 'recommendation'] }] }] : [] }, ai: async () => { throw Error('No inference may invent physical dimensions.'); }, now: Date.now });
}

for (const question of ['What width is the charm?', 'What diameter is the charm?', 'What size is the charm?']) test('backend dimension request cannot borrow chain measurements: ' + question, async () => {
  const answer = await backend(product('The necklace includes an 18 inch chain. The charm has a brushed surface.'), question);
  assert.equal(answer.productFacts.status, 'verified');
  assert.match(answer.reply, /do not confirm the charm[’']s (?:width|diameter|measurements)/i);
  assert.doesNotMatch(answer.reply, /Published measurements:.*18 inch|Published size choices:.*Chain Length/i);
  assert.deepEqual(answer.actions, []);
});

test('one shared measurement helper keeps literal width, height and diameter without borrowing companion lengths', () => {
  const facts = { title: 'Fox Medallion Necklace', dimensions: ['The charm measures 10mm wide and 14 mm tall, on your selected chain.', 'The chain measures 18 inches long.', 'The pendant has a 10 mm diameter.'], options: [{ name: 'Chain Length', values: ['18 inch'] }] };
  const width = Guide.productMeasurements(facts, 'What width is the charm?');
  assert.match(width.dimensions.join(' '), /10mm wide/); assert.doesNotMatch(width.dimensions.join(' '), /chain|18 inch/i); assert.equal(width.unknown, '');
  const height = Guide.productMeasurements(facts, 'What height is the charm?');
  assert.match(height.dimensions.join(' '), /14 mm tall/); assert.equal(height.unknown, '');
  const diameter = Guide.productMeasurements(facts, 'What diameter is the charm?');
  assert.deepEqual(diameter.dimensions, ['The pendant has a 10 mm diameter.']); assert.equal(diameter.unknown, '');
});

test('mixed measured clauses stay with their component and missing diameter is admitted', () => {
  const facts = { dimensions: ['The charm measures 12 mm wide and the chain measures 18 inches long.'], options: [] };
  const width = Guide.productMeasurements(facts, 'What width is the charm?');
  assert.deepEqual(width.dimensions, ['The charm measures 12 mm wide']);
  const chain = Guide.productMeasurements(facts, 'What length is the chain?');
  assert.deepEqual(chain.dimensions, ['the chain measures 18 inches long.']);
  const diameter = Guide.productMeasurements(facts, 'What diameter is the charm?');
  assert.deepEqual(diameter.dimensions, []); assert.match(diameter.unknown, /charm[’']s diameter/);
  const ambiguous = Guide.productMeasurements({ dimensions: ['The charm measures 10 × 12mm.'], options: [] }, 'What width is the charm?');
  assert.deepEqual(ambiguous.dimensions, []); assert.match(ambiguous.unknown, /width/);
  const unmeasured = Guide.productMeasurements({ dimensions: [], options: [{ name: 'Chain Length', values: ['18 inch'] }] }, 'What are its dimensions?');
  assert.equal(unmeasured.optionGroups[0].name, 'Chain Length'); assert.match(unmeasured.unknown, /do not confirm the piece[’']s measurements/);
});

test('backend keeps known physical dimensions while admitting an absent requested weight', async () => {
  const answer = await backend(product('The pendant measures 12mm wide and 18mm high.'), 'What are its dimensions and weight?');
  assert.match(answer.reply, /12mm wide and 18mm high/);
  assert.match(answer.reply, /do not confirm the piece[’']s weight/);
  assert.doesNotMatch(answer.reply, /\d+ grams?/);
});

test('held public fact envelopes retire commercial values, while checked unavailability stays factual', async () => {
  for (const hold of ['cartHold', 'recommendationHold']) {
    const piece = product('The pendant measures 12mm wide.'); piece[hold] = true;
    const answer = await backend(piece, 'How much is it?');
    assert.equal(answer.productFacts.status, 'unconfirmed'); assert.equal(answer.live, false); assert.doesNotMatch(answer.reply, /USD 57|USD 114/);
    assert.deepEqual(Widget.publicProductFacts({ status: 'verified', [hold]: true, selectedVariant: { price: 57 }, priceRange: { min: 57, max: 57 } }), { status: 'unconfirmed' });
  }
  const unavailable = product('The pendant measures 12mm wide.'); unavailable.variants[0].available = false;
  const answer = await backend(unavailable, 'How much is it and is it available?');
  assert.equal(answer.productFacts.status, 'verified'); assert.equal(answer.productFacts.selectedVariant.available, false); assert.match(answer.reply, /unavailable at the published check time/); assert.deepEqual(answer.actions, []);
});

async function widgetFixture(t) {
  const errors = [], console = new VirtualConsole(); console.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM(read('concierge-sandbox.html'), { url: 'https://preview.example/concierge-sandbox.html', runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole: console });
  const w = dom.window, d = w.document, piece = product('The charm measures 12mm wide and 18mm high.'), requests = [];
  delete d.body.dataset.catalogueSeed;
  w.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} }); w.HTMLElement.prototype.scrollIntoView = function () {}; w.scrollTo = () => {};
  let remote;
  w.fetch = async (raw, init = {}) => {
    const url = new URL(raw, w.location.href); requests.push({ url, init }); let body;
    if (url.pathname === '/api/growth/catalogue') body = { live: true, checkedAt: Date.now(), products: [piece], pageInfo: { hasNextPage: false, endCursor: null } };
    else if (url.pathname === '/api/growth/inventory') body = { live: true, checkedAt: Date.now(), products: [piece], inventory: { schema: 1, total: 1, offset: 0, limit: 24, loaded: 1, detailsLoaded: 1, ready: true, partial: false, expiresAt: Date.now() + 300000 }, pageInfo: { hasNextPage: false, nextOffset: null } };
    else if (url.pathname === '/api/growth/product') body = { live: true, checkedAt: Date.now(), product: piece };
    else if (url.pathname === '/api/growth/storefront-services') body = { schema: 1, guidance: {}, conflicts: [], offers: { items: [] } };
    else if (url.pathname === '/api/growth/knowledge') body = { live: true, checkedAt: Date.now(), meanings: [] };
    else if (url.pathname === '/api/concierge') { const request = init.body ? JSON.parse(init.body) : {}; body = request.message ? await remote(request, init) : { ok: true }; }
    else if (url.pathname === '/api/growth/events') body = { ok: true };
    else throw Error('Unexpected public request ' + url.pathname);
    return { ok: true, json: async () => clone(body) };
  };
  for (const name of ['brites-catalogue-intents.js', 'brites-concierge-shopping-guide.js', 'brites-storefront-bridge.js', 'concierge-sandbox.js']) w.eval(read(name));
  await settle(); await w.BritesSandboxStorefront.preloadInventory(); await settle();
  w.BritesConciergeAvatar = { create: () => ({ setState() {}, setEmotion() {}, setVisible() {}, setPaused() {}, clearFocus() {}, focusProduct() {}, clearProduct() {}, showProduct() {}, cancelPerformance() {}, setFloating() {}, cue() {} }) };
  const script = d.createElement('script'); script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true'; Object.defineProperty(d, 'currentScript', { get: () => script });
  w.eval(read('brites-concierge.js')); w.BritesConcierge.open({ focus: false }); await settle();
  const store = w.BritesSandboxStorefront;
  t.after(() => { w.BritesConcierge.close(); w.close(); });
  return { w, d, store, piece, requests, errors, command: text => w.BritesConcierge.sendShopperCommand(text), get root() { return d.querySelector('brites-concierge').shadowRoot; }, onRemote: fn => { remote = fn; } };
}

test('pointer-only context revisions do not cancel a pending remote factual response', async t => {
  const h = await widgetFixture(t); await h.store.execute({ type: 'open', handle: h.piece.handle });
  const old = await h.command('How much is this?'), started = deferred(), release = deferred();
  h.w.BritesSandboxStorefront = Object.freeze({ ...h.store, readProduct: async () => null });
  h.onRemote(async (request, init) => { started.resolve(init); await release.promise; return { live: true, reply: 'Current Fox facts checked.', preferences: {}, products: [], meanings: [], preserveSelection: true, productFacts: old.productFacts }; });
  const pending = h.command('Tell me about this piece'), init = await started.promise, before = h.store.snapshot();
  const visible = h.d.querySelector('.product-copy'); visible.dispatchEvent(new h.w.Event('pointerover', { bubbles: true }));
  assert(h.store.snapshot().contextRevision > before.contextRevision); assert.equal(init.signal.aborted, false, 'Gaze movement must not reset a real pending response');
  release.resolve(); const result = await pending;
  assert.equal(result.ok, true, JSON.stringify(result)); assert.match(result.reply, /Current Fox facts/); assert.equal(h.store.snapshot().currentHandle, h.piece.handle); assert.deepEqual(h.errors, []);
});

for (const drift of ['cart hold', 'recommendation hold', 'price', 'stock']) test('fresh inventory ' + drift + ' cancels an old remote fact answer without changing the shopper controls', async t => {
  const h = await widgetFixture(t); await h.store.execute({ type: 'open', handle: h.piece.handle });
  const old = await h.command('How much is this?'), started = deferred(), release = deferred();
  h.w.BritesSandboxStorefront = Object.freeze({ ...h.store, readProduct: async () => null });
  h.onRemote(async (request, init) => { started.resolve(init); await release.promise; return { live: true, reply: 'OLD QUOTE USD 57.00', preferences: {}, products: [], meanings: [], preserveSelection: true, productFacts: old.productFacts }; });
  const pending = h.command('Tell me about this piece'), init = await started.promise;
  const selected = h.d.querySelector('#piece-variant').value, quantity = h.d.querySelector('.product-quantity input').value;
  if (drift === 'cart hold') h.piece.cartHold = true;
  else if (drift === 'recommendation hold') h.piece.recommendationHold = true;
  else if (drift === 'price') h.piece.variants[0].price = 69;
  else h.piece.variants[0].available = false;
  h.piece.checkedAt = Date.now();
  await h.store.preloadInventory({ retry: true }); await settle();
  assert.equal(init.signal.aborted, true, 'Fresh reviewed facts must retire old answer authority');
  assert.equal(h.root.querySelector('.composer input').disabled, false);
  release.resolve(); const result = await pending;
  assert.equal(result.ok, false); assert.match(result.error, /cancelled/i); assert.doesNotMatch(h.root.querySelector('.caption-text').textContent, /OLD QUOTE/);
  assert.doesNotMatch(h.w.sessionStorage.getItem('brites-concierge-v1'), /OLD QUOTE/);
  assert.equal(h.d.querySelector('#piece-variant').value, selected); assert.equal(h.d.querySelector('.product-quantity input').value, quantity);
  assert.equal(h.store.snapshot().currentHandle, h.piece.handle); assert.deepEqual(h.errors, []);
});
