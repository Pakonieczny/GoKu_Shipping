// Tests for etsyShopListingsProxy's catalog projection — the reader the SKU
// console's local index is built from. Run: node tests/sku/shop-listings.cjs
const assert = require('node:assert');
const Module = require('node:module');

let etsyCalls = [];
let respondWith = null;
const origLoad = Module._load;
Module._load = function (request, parent, isMain) {
  if (request === 'node-fetch') {
    return async (url, init = {}) => {
      etsyCalls.push({ url: String(url), method: (init.method || 'GET').toUpperCase() });
      const r = respondWith();
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
process.env.SHOP_ID = '12345';
const proxy = require('../../netlify/functions/etsyShopListingsProxy.js');

/* A listing as Etsy actually returns it with includes=Images,Inventory: a long
   description, every image in every size, and one product per variant
   combination. This is what overflowed the 6 MB function response cap. */
function fatListing(id, variants = 24) {
  const image = n => ({
    listing_image_id: id * 100 + n,
    rank: n,
    url_75x75: `https://i.etsystatic.com/${id}/${n}/il_75x75.jpg`,
    url_170x135: `https://i.etsystatic.com/${id}/${n}/il_170x135.jpg`,
    url_570xN: `https://i.etsystatic.com/${id}/${n}/il_570xN.jpg`,
    url_fullxfull: `https://i.etsystatic.com/${id}/${n}/il_fullxfull.jpg`,
    full_height: 3000, full_width: 3000, alt_text: 'A piece of jewellery, photographed',
  });
  const product = n => ({
    product_id: id * 1000 + n,
    sku: n === 0 ? `Gold_${String(id).slice(-4)}` : `Gold_${String(id).slice(-4)}`,
    is_deleted: false,
    property_values: [
      { property_id: 200, property_name: 'Primary color', scale_id: null, scale_name: null, value_ids: [n], values: ['colour ' + n] },
      { property_id: 513, property_name: 'Length', scale_id: 342, scale_name: 'Inches', value_ids: [n], values: [(16 + n) + '"'] },
    ],
    offerings: [{ offering_id: id * 2000 + n, quantity: 10, is_enabled: true, is_deleted: false, price: { amount: 1999, divisor: 100, currency_code: 'USD' } }],
  });
  return {
    listing_id: id,
    title: `14k Gold Charm Necklace, Personalised Gift, Handmade ${id}`,
    description: 'A very long shop description. '.repeat(120),
    price: { amount: 1999, divisor: 100, currency_code: 'USD' },
    shop_section_id: 11,
    last_modified_timestamp: 1700000000,
    tags: Array.from({ length: 13 }, (_, i) => 'tag-number-' + i),
    materials: Array.from({ length: 8 }, (_, i) => 'material-' + i),
    images: Array.from({ length: 10 }, (_, i) => image(i)),
    inventory: { products: Array.from({ length: variants }, (_, i) => product(i)), price_on_property: [200, 513], quantity_on_property: [], sku_on_property: [] },
  };
}

const page = (n, variants) => Array.from({ length: n }, (_, i) => fatListing(700000 + i, variants));

/* How many variants per listing it takes for an unprojected 100-listing page to
   breach the cap. A two-property listing (say 12 colours x 10 lengths) reaches
   this easily, which is why only some shops ever hit the error. */
function variantsNeededToOverflow() {
  for (const v of [24, 48, 96, 144, 200, 300]) {
    const body = JSON.stringify({ count: 5627, results: page(100, v) });
    if (Buffer.byteLength(body, 'utf8') > LAMBDA_CAP) return v;
  }
  return null;
}

const call = qs => proxy.handler({
  httpMethod: 'GET',
  headers: { 'access-token': 'AT' },
  queryStringParameters: Object.fromEntries(new URLSearchParams(qs)),
});

function serve(results) {
  etsyCalls = [];
  respondWith = () => ({ status: 200, body: { count: 5627, results } });
}

let passed = 0;
const failures = [];
const cases = [];
const test = (name, fn) => cases.push({ name, fn });

const LAMBDA_CAP = 6291556;

/* ── the reported failure ─────────────────────────────────────────────── */

test('REGRESSION: a page that used to breach the 6 MB cap now fits easily', async () => {
  // Size the fixture to the failure that was actually reported:
  //   Function.ResponseSizeTooLarge — Response payload size exceeded maximum
  //   allowed payload size (6291556 bytes).
  const variants = variantsNeededToOverflow();
  assert.ok(variants, 'could not build an overflowing page — fixture is unrealistic');

  serve(page(100, variants));
  const unprojected = await call('limit=100&offset=0&state=active&includes=Images,Inventory');
  const before = Buffer.byteLength(unprojected.body, 'utf8');
  assert.ok(before > LAMBDA_CAP, `expected the old shape to overflow; got ${before}`);

  serve(page(100, variants));
  const projected = await call('limit=100&offset=0&state=active&includes=Images,Inventory&projection=catalog');
  assert.equal(projected.statusCode, 200);
  const after = Buffer.byteLength(projected.body, 'utf8');
  assert.ok(after < LAMBDA_CAP, `projected page is ${after} bytes, cap is ${LAMBDA_CAP}`);
  assert.ok(after < 200_000, `expected well under 200 KB, got ${after}`);
  assert.ok(after / before < 0.05, `expected >95% smaller; ${before} -> ${after}`);
});

test('Even a shop of 300-variant listings stays inside the cap', async () => {
  serve(page(100, 300));
  const res = await call('limit=100&projection=catalog&includes=Images,Inventory');
  const bytes = Buffer.byteLength(res.body, 'utf8');
  assert.ok(bytes < 200_000,
    `projection cost is independent of variant count; got ${bytes}`);
});

test('A projected row carries exactly the fields the catalog needs', async () => {
  serve([fatListing(881234)]);
  const res = await call('limit=100&projection=catalog&includes=Images,Inventory');
  const row = JSON.parse(res.body).results[0];
  assert.deepEqual(Object.keys(row).sort(),
    ['image', 'last_modified_timestamp', 'listing_id', 'price', 'shop_section_id', 'sku', 'title']);
  assert.equal(row.listing_id, 881234);
  assert.equal(row.image, 'https://i.etsystatic.com/881234/0/il_570xN.jpg', 'rank 0 wins, display size');
  assert.equal(row.sku, 'Gold_1234', 'first non-empty variant SKU');
  assert.equal(row.shop_section_id, 11);
  assert.equal(row.last_modified_timestamp, 1700000000);
  assert.deepEqual(row.price, { amount: 1999, divisor: 100, currency_code: 'USD' });
  assert.ok(!('description' in row) && !('images' in row) && !('inventory' in row) && !('tags' in row));
});

/* ── the projection must not change anything else ─────────────────────── */

test('Without the parameter the payload is still Etsy\'s, verbatim', async () => {
  serve([fatListing(1)]);
  const res = await call('limit=100&includes=Images,Inventory');
  const body = JSON.parse(res.body);
  assert.equal(body.results[0].images.length, 10, 'the Pricing Console still gets everything');
  assert.equal(body.results[0].inventory.products.length, 24);
  assert.ok(body.results[0].description);
  assert.equal(body._meta.projection, 'full');
});

test('An unknown projection value is treated as no projection', async () => {
  serve([fatListing(1)]);
  const res = await call('limit=100&projection=something-else&includes=Images,Inventory');
  const body = JSON.parse(res.body);
  assert.ok(Array.isArray(body.results[0].images), 'not projected');
  assert.equal(body._meta.projection, 'full');
});

test('Paging, sorting and the call count are unaffected', async () => {
  serve(page(3));
  const res = await call('limit=100&offset=200&state=active&sort_on=updated&sort_order=desc&projection=catalog&includes=Images,Inventory');
  const body = JSON.parse(res.body);
  assert.equal(body.count, 5627, 'Etsy\'s own total still passes through');
  assert.equal(body.etsy_call_count, 1, 'projection is free — one Etsy call either way');
  assert.equal(body._meta.offset, 200);
  assert.equal(body._meta.projection, 'catalog');
  const sent = new URL(etsyCalls[0].url).searchParams;
  assert.equal(sent.get('sort_on'), 'updated');
  assert.equal(sent.get('sort_order'), 'desc');
  assert.equal(sent.get('limit'), '100');
  assert.equal(sent.get('offset'), '200');
});

/* ── projection edge cases ────────────────────────────────────────────── */

test('A listing with no images, no SKU and no section projects cleanly', async () => {
  serve([{ listing_id: 5, title: 'Bare', price: null, images: [], inventory: { products: [{ product_id: 1, sku: '' }] } }]);
  const res = await call('limit=100&projection=catalog&includes=Images,Inventory');
  const row = JSON.parse(res.body).results[0];
  assert.equal(row.image, '');
  assert.equal(row.sku, '');
  assert.equal(row.shop_section_id, null);
  assert.equal(row.last_modified_timestamp, null);
});

test('is_primary beats rank, and a later variant supplies the SKU', async () => {
  serve([{
    listing_id: 6, title: 'T',
    images: [{ rank: 0, url_570xN: 'first' }, { is_primary: true, rank: 4, url_570xN: 'primary' }],
    inventory: { products: [{ sku: '   ' }, { sku: ' Later_SKU ' }] },
  }]);
  const row = JSON.parse((await call('limit=100&projection=catalog&includes=Images,Inventory')).body).results[0];
  assert.equal(row.image, 'primary');
  assert.equal(row.sku, 'Later_SKU', 'blank SKUs are skipped and the value is trimmed');
});

test('An Etsy error is passed through untouched', async () => {
  etsyCalls = [];
  respondWith = () => ({ status: 403, body: { error: 'insufficient scope' } });
  const res = await call('limit=100&projection=catalog');
  assert.equal(res.statusCode, 403);
  assert.match(JSON.parse(res.body).error, /insufficient scope/);
});

(async () => {
  console.log('\netsyShopListingsProxy · catalog projection');
  for (const c of cases) {
    try { await c.fn(); passed++; console.log('  ✓ ' + c.name); }
    catch (err) { failures.push(c.name); console.log('  ✗ ' + c.name + '\n      ' + ((err && err.stack) || err)); }
  }
  console.log(`\n${passed} passed, ${failures.length} failed`);
  if (failures.length) process.exitCode = 1;
})();
