'use strict';

// Exercise the mounted host, bridge and widget. Catalogue responses and
// rendering hardware are synthetic; focus and control identity are real DOM.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM, VirtualConsole } = require('jsdom');
const project = path.resolve(__dirname, '../..');
const read = name => fs.readFileSync(path.join(project, name), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const settle = async () => { await new Promise(setImmediate); await new Promise(setImmediate); };

function piece(id, title, motif, type, now, price = 40) {
  const handle = 'recommendation44-' + id, choices = ['Sterling Silver', '14/20 Gold Filled'];
  return {
    id: 'gid://shopify/Product/' + id, handle, title, type, currency: 'USD',
    url: 'https://britesjewelry.com/products/' + handle,
    tags: ['motif:' + motif], description: 'The ' + motif + ' charm is 12 mm wide and 9 mm high.',
    image: 'https://cdn.shopify.com/' + handle + '.jpg',
    checkedAt: now, detailState: 'checked', variantsComplete: true,
    options: [{ name: 'Metal', values: choices }],
    variants: choices.map((value, i) => ({
      id: 'gid://shopify/ProductVariant/' + (id * 10 + i + 1),
      numericId: String(id * 10 + i + 1), title: value, available: true,
      price: price + i * 20, options: [{ name: 'Metal', value }]
    }))
  };
}

async function fixture(t) {
  const clock = { now: Date.now() }, errors = [], console = new VirtualConsole();
  console.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM(read('concierge-sandbox.html'), {
    url: 'https://preview.example/concierge-sandbox.html',
    runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole: console
  });
  const w = dom.window, d = w.document, requests = [];
  const rows = [
    piece(44001, 'Butterfly Cutout Stud Earrings', 'butterfly', 'Earrings', clock.now),
    piece(44002, 'Butterfly Outline Stud Earrings', 'butterfly', 'Earrings', clock.now, 30),
    piece(44003, 'Butterfly Necklace', 'butterfly', 'Necklace', clock.now, 35),
    piece(44004, 'Fox Stud Earrings', 'fox', 'Earrings', clock.now)
  ];
  delete d.body.dataset.catalogueSeed;
  w.Date.now = () => clock.now;
  w.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  w.HTMLElement.prototype.scrollIntoView = function () {};
  w.scrollTo = () => {};
  w.fetch = async (raw, init = {}) => {
    const url = new URL(raw, w.location.href); requests.push({ url, init });
    let body;
    if (url.pathname === '/api/growth/catalogue') body = { live: true, checkedAt: clock.now, products: rows, pageInfo: { hasNextPage: false, endCursor: null } };
    else if (url.pathname === '/api/growth/inventory') body = {
      live: true, checkedAt: clock.now, products: rows.map(p => ({ ...p, checkedAt: clock.now })),
      inventory: { schema: 1, total: rows.length, offset: 0, limit: 24, loaded: rows.length, detailsLoaded: rows.length, ready: true, partial: false, expiresAt: clock.now + 300000 },
      pageInfo: { hasNextPage: false, nextOffset: null }
    };
    else if (url.pathname === '/api/growth/product') body = { live: true, checkedAt: clock.now, product: rows.find(p => p.handle === url.searchParams.get('handle')) };
    else if (url.pathname === '/api/growth/storefront-services') body = { schema: 1, guidance: {}, conflicts: [], offers: { items: [] } };
    else if (url.pathname === '/api/growth/knowledge') body = { live: true, checkedAt: clock.now, meanings: [] };
    else if (url.pathname === '/api/growth/events') body = { ok: true };
    else if (url.pathname === '/api/concierge-voice') body = { ok: true, enabled: false, nativeAudio: false };
    else throw Error('Unexpected recommendation44 route ' + url.pathname);
    return { ok: true, json: async () => clone(body) };
  };
  for (const name of ['brites-catalogue-intents.js', 'brites-concierge-shopping-guide.js', 'brites-storefront-bridge.js', 'concierge-sandbox.js']) w.eval(read(name));
  await settle(); await w.BritesSandboxStorefront.preloadInventory(); await settle();
  w.BritesConciergeAvatar = { create() { return { setState() {}, setEmotion() {}, setVisible() {}, setPaused() {}, triggerGreeting() {}, clearFocus() {}, focusProduct() {}, clearProduct() {}, cancelPerformance() {}, setFloating() {}, cue() {} }; } };
  const script = d.createElement('script'); script.src = '/brites-concierge.js'; script.dataset.sandbox = 'true';
  Object.defineProperty(d, 'currentScript', { get: () => script });
  w.eval(read('brites-concierge.js')); await settle(); w.BritesConcierge.open({ focus: false }); await settle();
  t.after(() => { w.BritesConcierge.close(); w.close(); });
  const root = d.querySelector('brites-concierge').shadowRoot, store = w.BritesSandboxStorefront;
  assert.equal((await store.execute({ type: 'open', handle: rows[0].handle })).ok, true);
  await settle();
  return {
    w, d, root, store, rows, clock, requests, errors,
    command: text => w.BritesConcierge.sendShopperCommand(text),
    help: () => root.querySelector('.shopping-help'),
    cards: () => [...root.querySelectorAll('.shopping-suggestion')],
    async refresh() { await store.preloadInventory({ retry: true }); await settle(); },
    context() { d.dispatchEvent(new w.CustomEvent('brites-storefront:context', { detail: store.snapshot() })); }
  };
}

test('unchanged public context preserves the actual focused recommendation control without reads or mutations', async t => {
  const h = await fixture(t), answer = await h.command('More like this');
  assert.equal(answer.ok, true); assert.equal(answer.recommendations.length, 1);
  const button = h.cards()[0].querySelector('button'), before = clone(h.store.snapshot()), reads = h.requests.length;
  button.focus(); assert.equal(h.root.activeElement, button);
  h.context(); h.context();
  assert.equal(button.isConnected, true); assert.equal(h.root.activeElement, button);
  assert.equal(h.cards()[0].querySelector('button'), button);
  assert.equal(h.requests.length, reads); assert.deepEqual(clone(h.store.snapshot()), before);
  assert.deepEqual(h.errors, []);
});

test('expired inventory retires visible recommendation price, variant and View piece controls', async t => {
  const h = await fixture(t); assert.equal((await h.command('More like this')).recommendations.length, 1);
  const before = clone(h.store.snapshot()), reads = h.requests.length;
  h.clock.now += 301000;
  h.d.dispatchEvent(new h.w.CustomEvent('brites-storefront:inventory'));
  assert.equal(h.store.getInventory().length, 0);
  assert.equal(h.cards().length, 0);
  assert.doesNotMatch(h.help().textContent, /From USD 30|Butterfly Outline/);
  assert.match(h.help().textContent, /fresh check/);
  assert.equal(h.requests.length, reads);
  assert.equal(h.store.snapshot().currentHandle, before.currentHandle);
  assert.deepEqual(clone(h.store.snapshot().productControls), before.productControls);
  assert.deepEqual(clone(h.store.snapshot().bagControls), before.bagControls);
});

test('a same-timestamp stock refresh invalidates a stored comparison and does not show an unrequested replacement', async t => {
  const h = await fixture(t); await h.command('More like this');
  const old = h.cards()[0], before = clone(h.store.snapshot()), reads = h.requests.length;
  h.rows[1].variants.forEach(variant => { variant.available = false; });
  await h.refresh();
  assert.equal(old.isConnected, false); assert.equal(h.cards().length, 0);
  assert.deepEqual(clone(h.store.snapshot().productControls), before.productControls);
  assert.deepEqual(clone(h.store.snapshot().bagControls), before.bagControls);
  assert.ok(h.requests.slice(reads).every(request => request.url.pathname !== '/api/concierge'));
});

test('refreshed price and selected material rebuild exact checked recommendation facts', async t => {
  const h = await fixture(t); await h.command('More like this');
  const old = h.cards()[0]; h.rows[1].variants[0].price = 33; await h.refresh();
  assert.equal(old.isConnected, false); assert.match(h.cards()[0].textContent, /From USD 33\.00.*Sterling Silver/);
  const silver = h.cards()[0], select = h.d.querySelector('#piece-variant');
  select.value = h.rows[0].variants[1].id; select.dispatchEvent(new h.w.Event('change', { bubbles: true }));
  assert.equal(silver.isConnected, false); assert.match(h.cards()[0].textContent, /From USD 50\.00.*14\/20 Gold Filled/);
  assert.equal(h.store.snapshot().productControls.selectedOptions[0].value, '14/20 Gold Filled');
  assert.equal(h.store.snapshot().bagControls.itemCount, 0);
});

test('a price change beyond the original cheaper comparison retires its old candidate', async t => {
  const h = await fixture(t); assert.equal((await h.command('Anything cheaper?')).recommendations[0].price, 30);
  h.rows[1].variants[0].price = 45; h.rows[1].variants[1].price = 70; await h.refresh();
  assert.equal(h.cards().length, 0);
  assert.doesNotMatch(h.help().textContent, /lower-priced alternative|From USD 30/);
  assert.equal(h.store.snapshot().bagControls.itemCount, 0);
});

test('a refresh retains only previously requested identities and reports the actual remaining count', async t => {
  const h = await fixture(t);
  h.rows.push(piece(44005, 'Butterfly Petite Stud Earrings', 'butterfly', 'Earrings', h.clock.now, 32));
  await h.refresh(); assert.equal((await h.command('More like this')).recommendations.length, 2);
  h.rows[1].variants.forEach(variant => { variant.available = false; });
  h.rows.push(piece(44006, 'Butterfly New Stud Earrings', 'butterfly', 'Earrings', h.clock.now, 31));
  await h.refresh();
  assert.equal(h.cards().length, 1);
  assert.match(h.cards()[0].textContent, /Butterfly Petite/);
  assert.doesNotMatch(h.help().textContent, /Butterfly New|I have 2/);
  assert.match(h.help().textContent, /1 previously suggested alternative/);
  assert.equal(h.store.snapshot().bagControls.itemCount, 0);
});

test('No thanks remains muted through refresh and explicit matching revives only checked separate pieces', async t => {
  const h = await fixture(t); await h.command('More like this');
  assert.equal((await h.command('No thanks')).ok, true); assert.equal(h.help().hidden, true);
  await h.refresh(); h.context(); assert.equal(h.help().hidden, true);
  const before = clone(h.store.snapshot()), answer = await h.command('Show me matching necklaces');
  assert.equal(answer.ok, true); assert.equal(answer.recommendations.length, 1);
  assert.match(answer.recommendations[0].why, /sold separately/);
  assert.equal(h.help().hidden, false); assert.equal(h.cards().length, 1);
  assert.deepEqual(clone(h.store.snapshot()), before);
});
