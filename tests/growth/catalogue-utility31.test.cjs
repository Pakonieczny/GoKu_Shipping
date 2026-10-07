'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const core = require('../../netlify/functions/_britesGrowth');
const NOW = Date.parse('2026-10-07T14:00:00Z');
function published(id, extras = {}) {
  return {id, handle: 'test-charm-' + id, title: 'Test Heart Necklace', product_type: 'Necklace',
    body_html: '<p>Sterling silver heart necklace.</p>', options: [{name: 'Metal'}],
    images: [{src: 'https://cdn.shopify.com/heart.jpg'}],
    variants: [{id: id + 1000, title: 'Sterling Silver', price: '50', available: true, option1: 'Sterling Silver', sku: 'TEST-' + id}], ...extras};
}
const hidden = published(2, {handle: 'test-studio-heart', title: 'Test Heart Component', product_type: 'Custom Charm Studio', images: [],
  body_html: '<p>Added by Custom Charm Studio. Hidden from all collections and search.</p>',
  variants: [{id: 1002, title: 'Default Title', price: '8', available: true, sku: 'STUDIO-EXT-TEST'}]});
function fixture(rows) {
  const calls = [], shop = core.createShopify({env: {}, now: () => NOW, fetch: async (url, options) => {
    calls.push({url, method: options?.method || 'GET'});
    return {ok: true, json: async () => url.endsWith('/cart.js') ? {currency: 'USD'} : url.includes('/products/test-studio-heart.js') ? {...hidden, variants: hidden.variants.map(v => ({...v, price: 800}))} : {products: rows}};
  }});return {shop, calls};
}
test('published hidden utility is removed from discovery while browse continuation follows the upstream page', async () => {
  const f = fixture(Array.from({length: 60}, (_, i) => i ? published(i + 3) : hidden)), result = await f.shop.browse();
  assert.equal(result.products.length, 59);assert.ok(result.products.every(p => p.handle !== hidden.handle));
  assert.deepEqual(result.pageInfo, {hasNextPage: true, endCursor: 'storefront:2'});
  assert.ok(f.calls.every(c => c.method === 'GET'));assert.equal(result.products[0].variants[0].price, 50);
});
test('exact component reads remain available for existing studio integrations', async () => {
  const f = fixture([hidden]), p = await f.shop.byHandle(hidden.handle);
  assert.equal(p.id, 'gid://shopify/Product/2');assert.equal(p.variants[0].sku, 'STUDIO-EXT-TEST');
  assert.equal(p.variants[0].price, 8);assert.ok(f.calls.every(c => c.method === 'GET'));
});
test('hidden studio component cannot become a ranked shopper recommendation', () => {
  const f = core.normalizeProduct({id: 'gid://shopify/Product/2', handle: hidden.handle, title: hidden.title, productType: hidden.product_type,
    status: 'ACTIVE', onlineStoreUrl: 'https://britesjewelry.com/products/' + hidden.handle, descriptionHtml: hidden.body_html,
    variants: {nodes: [{id: 'gid://shopify/ProductVariant/1002', title: 'Default Title', sku: 'STUDIO-EXT-TEST', price: '8', availableForSale: true, selectedOptions: []}], pageInfo: {hasNextPage: false}}}, 'USD', NOW);
  assert.deepEqual(core.rankProducts([f], {}, NOW), []);
});
test('physical custom jewellery is retained unless both published visibility and every studio SKU corroborate exclusion', () => {
  for (const p of [published(1), {...hidden, body_html: 'Added by Custom Charm Studio. A physical custom necklace.'},
    {...hidden, body_html: 'Hidden from all collections and search.'}, {...hidden, variants: [{sku: ''}]},
    {...hidden, variants: [{sku: 'STUDIO-EXT-TEST'}, {sku: 'PHYSICAL-TEST'}]}, {...hidden, variants: []}]) {
    const before = JSON.stringify(p);assert.equal(core.isStorefrontDiscoveryProduct(p), true);assert.equal(JSON.stringify(p), before);
  }
  assert.equal(core.isStorefrontDiscoveryProduct(hidden), false);
});
test('gift-service words do not replace an exact selection with new gift-discovery questions', () => {
  for (const service of ['gift wrapping', 'gift notes', 'gift packages', 'gift packaging', 'gift box']) {
    const p = core.intentFrom('Find a silver bunny necklace under $50 CAD and explain ' + service);
    assert.equal(p.giftDiscovery, false);assert.equal(p.gifting, false);
    assert.deepEqual(p.interests, ['bunny']);assert.equal(p.type, 'necklace');assert.equal(p.metal, 'silver');
    assert.equal(p.budget, 50);assert.equal(p.budgetCurrency, 'CAD');
    const actualGift = core.intentFrom('A bunny necklace gift for my sister with ' + service);
    assert.equal(actualGift.giftDiscovery, true);assert.equal(actualGift.recipient, 'sister');
  }
  const prior = core.intentFrom('A silver bunny gift for my sister');
  assert.equal(core.intentFrom('Can I get gift wrapping?', [], prior).giftDiscovery, true);
});
