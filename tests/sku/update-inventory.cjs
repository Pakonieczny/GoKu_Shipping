// Tests for etsyUpdateListingInventoryProxy's legacy SKU-write path — the one
// the SKU console uses. Run: node tests/sku/update-inventory.cjs
const assert = require('node:assert');
const Module = require('node:module');

/* The function's only external dependency at require time is node-fetch (via
   etsyRateLimiter). There is no node_modules here and the test must not reach
   the network, so the require is intercepted. Firestore is never reached:
   etsyRateLimiter only loads firebase-admin when FIREBASE_* env is present. */
let etsyCalls = [];
let respondWith = null;
const origLoad = Module._load;
Module._load = function (request, parent, isMain) {
  if (request === 'node-fetch') {
    return async (url, init = {}) => {
      etsyCalls.push({ url: String(url), method: (init.method || 'GET').toUpperCase(), body: init.body ? JSON.parse(init.body) : null });
      const r = respondWith(etsyCalls.length, etsyCalls.at(-1));
      return {
        ok: r.status >= 200 && r.status < 300,
        status: r.status,
        headers: { get: () => null },
        text: async () => JSON.stringify(r.body),
        json: async () => r.body,
      };
    };
  }
  return origLoad.call(this, request, parent, isMain);
};

process.env.CLIENT_ID = 'cid';
process.env.CLIENT_SECRET = 'secret';
const proxy = require('../../netlify/functions/etsyUpdateListingInventoryProxy.js');

/* A two-variation listing: property 200 (colour) x property 513 (length). */
function twoPropInventory(over = {}) {
  const product = (pid, colour, length, price, qty, sku) => ({
    product_id: pid,
    sku,
    property_values: [
      { property_id: 200, property_name: 'Primary color', scale_id: null, value_ids: [colour], values: ['c' + colour] },
      { property_id: 513, property_name: 'Length', scale_id: 342, value_ids: [length], values: ['l' + length] },
    ],
    offerings: [{ offering_id: pid * 2, quantity: qty, is_enabled: true, price: { amount: price, divisor: 100, currency_code: 'USD' } }],
  });
  return {
    products: [
      product(1, 11, 21, 1999, 5, 'OLD_A'),
      product(2, 11, 22, 2499, 3, 'OLD_B'),
      product(3, 12, 21, 2199, 7, 'OLD_C'),
    ],
    price_on_property: [200, 513],
    quantity_on_property: [200, 513],
    sku_on_property: [],
    ...over,
  };
}

const call = body => proxy.handler({
  httpMethod: 'POST',
  headers: { 'access-token': 'AT' },
  body: JSON.stringify(body),
});

let passed = 0;
const failures = [];
const cases = [];
const test = (name, fn) => cases.push({ name, fn });

/* Answers the GET with `inv`, accepts the PUT. */
function serve(inv, putStatus = 200) {
  etsyCalls = [];
  respondWith = (n, c) => {
    if (c.method === 'GET') return { status: 200, body: inv };
    return putStatus === 200
      ? { status: 200, body: { ...inv, products: c.body.products } }
      : { status: putStatus, body: { error: 'rejected' } };
  };
}
const putBody = () => etsyCalls.find(c => c.method === 'PUT').body;

/* ── the reported failure ─────────────────────────────────────────────── */

test('REGRESSION: a partial sku_on_property no longer trips price_on_property', async () => {
  // Etsy's GET returns this shape for listings edited in its own UI. Echoing it
  // back produced: "price_on_property: unsupported number of property IDs.
  // Supports only zero or all 2 variation properties, as at least one
  // `*_on_property` field is linked to all 2 properties."
  const inv = twoPropInventory({ price_on_property: [200, 513], quantity_on_property: [200, 513], sku_on_property: [200] });
  serve(inv);
  const res = await call({ listing_id: 555, items: [{ product_id: 1, sku: 'New_0555' }] });

  assert.equal(res.statusCode, 200, res.body);
  const put = putBody();
  assert.deepEqual(put.sku_on_property, [200, 513], 'widened to every variation property');
  assert.deepEqual(put.price_on_property, [200, 513]);
  assert.deepEqual(put.quantity_on_property, [200, 513]);
});

test('Widening preserves every product\'s own price and quantity', async () => {
  const inv = twoPropInventory({ sku_on_property: [200] });
  serve(inv);
  await call({ listing_id: 555, items: [{ product_id: 1, sku: 'New_0555' }] });
  const put = putBody();
  assert.deepEqual(put.products.map(p => p.offerings[0].price), [19.99, 24.99, 21.99],
    'prices unchanged — widening only changes the "varies by" declaration');
  assert.deepEqual(put.products.map(p => p.offerings[0].quantity), [5, 3, 7]);
});

test('The SKU is applied to every variant', async () => {
  serve(twoPropInventory());
  const res = await call({ listing_id: 555, items: [{ product_id: 1, sku: '  Spaced_1  ' }] });
  assert.equal(res.statusCode, 200);
  assert.deepEqual(putBody().products.map(p => p.sku), ['Spaced_1', 'Spaced_1', 'Spaced_1']);
});

/* ── shapes that must NOT be rewritten ────────────────────────────────── */

test('All-zero on_property fields are left at zero', async () => {
  const inv = twoPropInventory({ price_on_property: [], quantity_on_property: [], sku_on_property: [] });
  serve(inv);
  await call({ listing_id: 555, items: [{ product_id: 1, sku: 'X' }] });
  const put = putBody();
  assert.deepEqual([put.price_on_property, put.quantity_on_property, put.sku_on_property], [[], [], []]);
});

test('A zero field stays zero even when another is full — Etsy allows "zero or all"', async () => {
  const inv = twoPropInventory({ price_on_property: [200, 513], quantity_on_property: [200, 513], sku_on_property: [] });
  serve(inv);
  await call({ listing_id: 555, items: [{ product_id: 1, sku: 'X' }] });
  assert.deepEqual(putBody().sku_on_property, [], 'not widened — it was already valid');
});

test('An all-partial listing is left untouched — Etsy only enforces this once one field is full', async () => {
  const inv = twoPropInventory({ price_on_property: [200], quantity_on_property: [200], sku_on_property: [200] });
  serve(inv);
  await call({ listing_id: 555, items: [{ product_id: 1, sku: 'X' }] });
  const put = putBody();
  assert.deepEqual([put.price_on_property, put.quantity_on_property, put.sku_on_property], [[200], [200], [200]]);
});

test('A single-property listing is unaffected', async () => {
  const inv = {
    products: [
      { product_id: 1, sku: 'A', property_values: [{ property_id: 200, value_ids: [11], values: ['red'] }], offerings: [{ quantity: 2, is_enabled: true, price: { amount: 1000, divisor: 100, currency_code: 'USD' } }] },
      { product_id: 2, sku: 'B', property_values: [{ property_id: 200, value_ids: [12], values: ['blue'] }], offerings: [{ quantity: 4, is_enabled: true, price: { amount: 1200, divisor: 100, currency_code: 'USD' } }] },
    ],
    price_on_property: [200], quantity_on_property: [200], sku_on_property: [],
  };
  serve(inv);
  const res = await call({ listing_id: 777, items: [{ product_id: 1, sku: 'Solo_0777' }] });
  assert.equal(res.statusCode, 200);
  const put = putBody();
  assert.deepEqual(put.price_on_property, [200]);
  assert.deepEqual(put.sku_on_property, []);
  assert.deepEqual(put.products.map(p => p.sku), ['Solo_0777', 'Solo_0777']);
});

test('A listing with no variations at all still writes its SKU', async () => {
  const inv = {
    products: [{ product_id: 9, sku: '', property_values: [], offerings: [{ quantity: 1, is_enabled: true, price: { amount: 500, divisor: 100, currency_code: 'USD' } }] }],
    price_on_property: [], quantity_on_property: [], sku_on_property: [],
  };
  serve(inv);
  const res = await call({ listing_id: 888, items: [{ product_id: 9, sku: 'Flat_0888' }] });
  assert.equal(res.statusCode, 200);
  assert.deepEqual(putBody().products.map(p => p.sku), ['Flat_0888']);
});

/* ── error paths ──────────────────────────────────────────────────────── */

test('Etsy\'s own rejection is passed through with its status', async () => {
  serve(twoPropInventory(), 400);
  const res = await call({ listing_id: 555, items: [{ product_id: 1, sku: 'X' }] });
  assert.equal(res.statusCode, 400);
  assert.ok(JSON.parse(res.body).error);
});

test('A wrong Variant 1 product_id is refused before any write', async () => {
  serve(twoPropInventory());
  const res = await call({ listing_id: 555, items: [{ product_id: 99, sku: 'X' }] });
  assert.equal(res.statusCode, 400);
  assert.match(JSON.parse(res.body).error, /Missing Variant 1/);
  assert.ok(!etsyCalls.some(c => c.method === 'PUT'), 'nothing written');
});

test('Every response carries an exact Etsy call count', async () => {
  serve(twoPropInventory());
  const res = await call({ listing_id: 555, items: [{ product_id: 1, sku: 'X' }] });
  assert.equal(JSON.parse(res.body).etsy_call_count, 2, 'one GET + one PUT');
});

(async () => {
  console.log('\netsyUpdateListingInventoryProxy · legacy SKU write');
  for (const c of cases) {
    try { await c.fn(); passed++; console.log('  ✓ ' + c.name); }
    catch (err) { failures.push(c.name); console.log('  ✗ ' + c.name + '\n      ' + ((err && err.stack) || err)); }
  }
  console.log(`\n${passed} passed, ${failures.length} failed`);
  if (failures.length) process.exitCode = 1;
})();
