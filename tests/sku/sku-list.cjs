// Functional tests for SKU_List_V1.html (the sku.goldenspike.app console).
// Run: node tests/sku/sku-list.cjs
const assert = require('node:assert');
const crypto = require('node:crypto');
const { createApp } = require('./dom-harness.cjs');

let passed = 0;
const failures = [];
const only = process.argv[2];
const QUEUE = [];

function test(name, fn) {
  if (only && !name.includes(only)) return;
  QUEUE.push({ kind: 'test', name, fn });
}
const section = n => QUEUE.push({ kind: 'section', name: n });

async function runOne({ name, fn }) {
  const reg = [];
  try {
    await fn(reg);
    passed++;
    console.log('  ✓ ' + name);
  } catch (err) {
    failures.push([name, err]);
    console.log('  ✗ ' + name + '\n      ' + ((err && err.stack) || err));
  } finally {
    reg.forEach(a => a.cleanup && a.cleanup());
  }
}

const settle = (ms = 20) => new Promise(r => setTimeout(r, ms));

/* ── fixtures ─────────────────────────────────────────────────────────── */

const SECTIONS = [
  { shop_section_id: 11, title: 'NECKLACES', active_listing_count: 2377 },
  { shop_section_id: 22, title: 'EARRINGS', active_listing_count: 1462 },
  { shop_section_id: 33, title: 'CHARMS', active_listing_count: 1739 },
  { shop_section_id: 44, title: 'BRACELETS', active_listing_count: 29 },
  { shop_section_id: 55, title: 'RINGS', active_listing_count: 20 },
];

const listing = (id, over = {}) => ({
  listing_id: id,
  title: `Gold Charm Necklace ${id}`,
  price: { amount: 1999, divisor: 100, currency_code: 'USD' },
  shop_section_id: 11,
  last_modified_timestamp: 1_700_000_000,
  images: [
    { rank: 2, url_570xN: `https://img/${id}-b.jpg` },
    { rank: 1, url_570xN: `https://img/${id}-a.jpg` },
  ],
  inventory: { products: [{ product_id: id * 10, sku: `Gold_${String(id).slice(-4)}` }] },
  ...over,
});

const freshTokens = () => ({
  access_token: 'AT', refresh_token: 'RT', expires_in: '3600',
  issued_at: String(Math.floor(Date.now() / 1000)),
});
const expiredTokens = () => ({
  access_token: 'OLD', refresh_token: 'RT', expires_in: '3600',
  issued_at: String(Math.floor(Date.now() / 1000) - 7200),
});

const challengeFor = v => crypto.createHash('sha256').update(v).digest('base64')
  .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const paramsOf = url => new URL(url).searchParams;

/**
 * A fake shop of `n` listings that answers the console's real query shape.
 * Records every page request so tests can assert exact API spend.
 */
function fakeShop(rows, opts = {}) {
  const calls = { pages: 0, sections: 0, other: 0, offsets: [] };
  // Sorted per request, not once at construction: tests mutate `rows` between
  // syncs to stand in for an edit landing at Etsy.
  const snapshot = () => [...rows].sort((a, b) => (b.last_modified_timestamp || 0) - (a.last_modified_timestamp || 0));
  const impl = async (url, init) => {
    if (url.includes('etsyApiUsage')) {
      calls.other++;
      return { body: { ok: true, verified: true, etsy_remaining_today: opts.remaining ?? 3120 } };
    }
    if (url.includes('mode=sections')) {
      calls.sections++;
      return { body: { results: opts.sections || SECTIONS, etsy_call_count: 1 } };
    }
    if (url.includes('etsyShopListingsProxy')) {
      const q = paramsOf(url);
      calls.pages++;
      const offset = Number(q.get('offset'));
      calls.offsets.push(offset);
      const limit = Number(q.get('limit'));
      const all = snapshot();
      let rows = all.slice(offset, offset + limit);
      // Mirror etsyShopListingsProxy's catalog projection, including the
      // _meta.projection the client uses to tell whether it was applied.
      let projection = 'full';
      if (q.get('projection') === 'catalog' && !opts.serveFullShape) {
        projection = 'catalog';
        rows = rows.map(x => ({
          listing_id: x.listing_id,
          title: x.title,
          price: x.price,
          shop_section_id: x.shop_section_id ?? null,
          last_modified_timestamp: x.last_modified_timestamp ?? null,
          image: (x.images || []).slice().sort((a, b) => a.rank - b.rank)[0]?.url_570xN || '',
          sku: (x.inventory?.products || []).map(p => (p.sku || '').trim()).find(Boolean) || '',
        }));
      }
      return { body: { count: all.length, results: rows, _meta: { projection }, etsy_call_count: 1 } };
    }
    if (opts.rest) return opts.rest(url, init);
    throw new Error('unexpected fetch: ' + url);
  };
  return { impl, calls };
}

const shopOf = n => Array.from({ length: n }, (_, i) =>
  listing(700000 + i, { last_modified_timestamp: 1_700_000_000 - i }));

/* ── 1 · OAuth / PKCE ─────────────────────────────────────────────────── */

section('OAuth · PKCE verifier lifecycle');

test('Connect sends a challenge that matches the stored verifier', async reg => {
  const app = createApp(); reg.push(app);
  await app.domReady();
  await app.els.connectEtsyBtn.dispatch('click');
  const p = paramsOf(app.navigations.at(-1));
  assert.equal(p.get('redirect_uri'), 'https://sku.goldenspike.app');
  assert.equal(challengeFor(app.api.takeVerifier(p.get('state'))), p.get('code_challenge'));
});

test('REGRESSION: two authorize runs — the older callback still gets ITS verifier', async reg => {
  const a = createApp(); reg.push(a);
  await a.domReady();
  await a.els.connectEtsyBtn.dispatch('click');
  const first = paramsOf(a.navigations.at(-1));

  const b = createApp({ storage: a.localStorage._dump() }); reg.push(b);
  await b.domReady();
  await b.els.connectEtsyBtn.dispatch('click');
  assert.notEqual(first.get('state'), paramsOf(b.navigations.at(-1)).get('state'));

  const back = createApp({
    storage: b.localStorage._dump(),
    search: `?code=CODE1&state=${encodeURIComponent(first.get('state'))}`,
  });
  reg.push(back);
  assert.equal(back.api.bootOAuth(), 'exchange');
  const sent = paramsOf(back.navigations.at(-1));
  assert.equal(challengeFor(sent.get('code_verifier')), first.get('code_challenge'));
  assert.equal(sent.get('redirect_domain'), 'sku');
});

test('Legacy single-slot verifier from an older build still works', async reg => {
  const v = 'a'.repeat(64);
  const app = createApp({ storage: { etsy_code_verifier: v }, search: '?code=C&state=s1' }); reg.push(app);
  assert.equal(app.api.bootOAuth(), 'exchange');
  assert.equal(paramsOf(app.navigations.at(-1)).get('code_verifier'), v);
});

test('Replaying a used authorization code is refused', async reg => {
  const app = createApp({ storage: { etsy_code_verifier: 'v'.repeat(64) }, search: '?code=SAME&state=s' }); reg.push(app);
  app.sessionStorage.setItem('etsy_last_code', 'SAME');
  assert.equal(app.api.bootOAuth(), 'replay');
  assert.equal(app.navigations.length, 0);
});

test('Etsy error return is surfaced, not treated as a code', async reg => {
  const app = createApp({ search: '?error=access_denied' }); reg.push(app);
  assert.equal(app.api.bootOAuth(), 'error');
  assert.match(app.status(), /failed/i);
});

test('Token-bearing redirect stores tokens and cleans the URL', async reg => {
  const now = Math.floor(Date.now() / 1000);
  const app = createApp({ search: `?access_token=AT&refresh_token=RT&expires_in=3600&issued_at=${now}` });
  reg.push(app);
  assert.equal(app.api.bootOAuth(), 'tokens');
  assert.equal(app.api.getAccessToken(), 'AT');
  assert.ok(app.api.tokenIsFresh());
  assert.equal(app.location.search, '');
});

test('Off-origin Connect bounces to the bound origin first', async reg => {
  const app = createApp({ origin: 'https://goldenspike.app' }); reg.push(app);
  await app.domReady();
  await app.els.connectEtsyBtn.dispatch('click');
  assert.equal(app.navigations.at(-1), 'https://sku.goldenspike.app/?connect=1');
});

/* ── 2 · Token refresh ────────────────────────────────────────────────── */

section('Tokens · refresh lifecycle');

test('An expired access token is refreshed before the first catalog read', async reg => {
  const seen = [];
  const shop = fakeShop(shopOf(3));
  const app = createApp({
    storage: expiredTokens(),
    fetchImpl: async (url, init) => {
      seen.push(url);
      if (url.includes('refreshEtsyToken')) {
        return { body: { access_token: 'NEW', refresh_token: 'RT2', expires_in: 3600, issued_at: Math.floor(Date.now() / 1000) } };
      }
      return shop.impl(url, init);
    },
  });
  reg.push(app);
  await app.domReady();
  assert.equal(app.api.getAccessToken(), 'NEW');
  assert.ok(seen[0].includes('refreshEtsyToken'), 'refresh ran first');
});

test('A 401 mid-session triggers one refresh and one retry', async reg => {
  let listingCalls = 0, refreshes = 0;
  const shop = fakeShop(shopOf(2));
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url, init) => {
      if (url.includes('refreshEtsyToken')) {
        refreshes++;
        return { body: { access_token: 'NEW', refresh_token: 'RT2', expires_in: 3600, issued_at: Math.floor(Date.now() / 1000) } };
      }
      if (url.includes('etsyShopListingsProxy') && !url.includes('sections')) {
        listingCalls++;
        if (listingCalls === 1) return { status: 401, body: { error: 'expired' } };
      }
      return shop.impl(url, init);
    },
  });
  reg.push(app);
  await app.domReady();
  assert.equal(refreshes, 1);
  assert.equal(app.cards().length, 2);
});

test('A rejected refresh token is discarded so the UI asks for a reconnect', async reg => {
  const app = createApp({
    storage: expiredTokens(),
    fetchImpl: async () => ({ status: 400, body: { error: 'invalid_grant' } }),
  });
  reg.push(app);
  assert.equal(await app.api.refreshAccessToken(), false);
  assert.equal(app.api.getAccessToken(), '');
  assert.equal(app.status(), 'Not connected');
});

test('No tokens at all → prompt, and zero network traffic', async reg => {
  const app = createApp({ fetchImpl: async () => { throw new Error('must not fetch'); } }); reg.push(app);
  await app.domReady();
  assert.match(app.notice(), /Connect Etsy/i);
  assert.equal(app.fetchCalls.length, 0);
});

/* ── 3 · Catalog sync + API budget ────────────────────────────────────── */

section('Catalog · sync cost');

test('A cold start walks the whole shop at 100 listings per call', async reg => {
  const shop = fakeShop(shopOf(550));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();

  assert.equal(shop.calls.pages, 6, '550 listings = ceil(550/100) = 6 calls');
  assert.deepEqual(shop.calls.offsets, [0, 100, 200, 300, 400, 500]);
  assert.equal(app.api.Catalog.rows.length, 550);
  const q = paramsOf(app.fetchCalls.find(c => c.url.includes('etsyShopListingsProxy') && !c.url.includes('sections')).url);
  assert.equal(q.get('limit'), '100', 'always the maximum page size');
  assert.equal(q.get('includes'), 'Images,Inventory', 'images and SKUs inline — no per-listing calls');
  assert.equal(q.get('sort_on'), 'updated');
});

test('The projected cost for the real ~5,600-listing shop is ~57 calls', async reg => {
  const shop = fakeShop(shopOf(5627));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  assert.equal(app.api.Catalog.rows.length, 5627);
  assert.equal(shop.calls.pages, 57, 'one-off 57 calls, ~1.6% of a 3,500/day budget');
});

test('The catalog read asks for the server-side projection', async reg => {
  const shop = fakeShop(shopOf(10));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const q = paramsOf(app.fetchCalls.find(c => c.url.includes('etsyShopListingsProxy') && !c.url.includes('sections')).url);
  assert.equal(q.get('projection'), 'catalog',
    'without this the function response exceeds Netlify\'s 6 MB cap');
  assert.equal(q.get('includes'), 'Images,Inventory', 'the server still needs both to project from');
});

test('REGRESSION: an oversized page shrinks the page size instead of failing the sync', async reg => {
  // Function.ResponseSizeTooLarge — Response payload size exceeded maximum
  // allowed payload size (6291556 bytes).
  const rows = shopOf(120);
  const base = fakeShop(rows);
  let refusedLimits = [];
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url, init) => {
      if (url.includes('etsyShopListingsProxy') && !url.includes('sections')) {
        const limit = Number(paramsOf(url).get('limit'));
        if (limit > 50) {
          refusedLimits.push(limit);
          return { status: 502, body: '{"errorType":"Function.ResponseSizeTooLarge","errorMessage":"Response payload size exceeded maximum allowed payload size (6291556 bytes)."}' };
        }
      }
      return base.impl(url, init);
    },
  });
  reg.push(app);
  await app.domReady();

  assert.deepEqual(refusedLimits, [100], 'the 100-listing page was refused once');
  assert.equal(app.api.Catalog.rows.length, 120, 'the sync completed at the smaller page size');
  assert.ok(!app.status().includes('Sync failed'), 'and did not surface as a failure: ' + app.status());
});

test('A deploy that does not project is detected and the page size drops', async reg => {
  // An older function build answers without projecting. Full listings are
  // orders of magnitude larger, so marching on at 100 a page walks straight
  // into the 6 MB cap; the console drops to a size that still fits.
  const shop = fakeShop(shopOf(40), { serveFullShape: true });
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const limits = app.fetchCalls
    .filter(c => c.url.includes('etsyShopListingsProxy') && !c.url.includes('sections'))
    .map(c => Number(paramsOf(c.url).get('limit')));
  assert.equal(limits[0], 100, 'the first call still tries the cheap page size');
  assert.ok(limits.slice(1).every(l => l === 10), 'every call after the detection is small: ' + limits);
  assert.equal(app.api.Catalog.rows.length, 40, 'and the catalog still completes');
});

test('A page that is too large even at the floor fails loudly', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async () => ({ status: 502, body: '{"errorType":"Function.ResponseSizeTooLarge","errorMessage":"Response payload size exceeded maximum allowed payload size (6291556 bytes)."}' }),
  });
  reg.push(app);
  await app.domReady();
  assert.match(app.status(), /Sync failed/, 'not silently swallowed');
  assert.match(app.status(), /build \d{4}-\d{2}-\d{2}/, 'names the console build for diagnosis');
});

test('Searching the built catalog costs ZERO further API calls', async reg => {
  const shop = fakeShop(shopOf(300));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const afterSync = shop.calls.pages;

  for (const q of ['gold', 'necklace 7000', '700123', 'charm', 'zzz', '']) await app.type(q);
  await app.els.chipRow.children[1].dispatch('click');   // a section chip
  await app.scrollToEnd(3);

  assert.equal(shop.calls.pages, afterSync, 'no page reads');
  assert.equal(shop.calls.other, 1, 'only the free usage poll, which never touches Etsy');
});

test('A warm start only asks for what changed — 1 call', async reg => {
  const rows = shopOf(250);
  const shop1 = fakeShop(rows);
  const a = createApp({ storage: freshTokens(), fetchImpl: shop1.impl }); reg.push(a);
  await a.domReady();
  assert.equal(shop1.calls.pages, 3);

  // Same browser profile, same catalog: the store is in-memory per sandbox, so
  // hand the second app the first one's rows to stand in for IndexedDB.
  const shop2 = fakeShop(rows);
  const b = createApp({ storage: a.localStorage._dump(), fetchImpl: shop2.impl }); reg.push(b);
  await b.domReady();
  await b.api.Catalog.upsert(a.api.Catalog.rows.map(r => ({ ...r })));
  b.api.Catalog.lastSync = Math.floor(Date.now() / 1000);
  shop2.calls.pages = 0;
  const res = await b.api.syncCatalog('incremental');
  assert.equal(res.pages, 1, 'stops at the first page already older than the last sync');
  assert.equal(res.stopped, 'caught-up');
});

test('Incremental sync ingests listings modified since the last pass', async reg => {
  const rows = shopOf(120);
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const before = app.api.Catalog.rows.length;

  // An edit lands: newer timestamp, new SKU.
  rows[5].last_modified_timestamp = 1_800_000_000;
  rows[5].inventory = { products: [{ product_id: 1, sku: 'EDITED_SKU' }] };
  rows.push(listing(999111, { last_modified_timestamp: 1_800_000_001 }));

  app.api.Catalog.lastSync = 1_700_000_000;
  const res = await app.api.syncCatalog('incremental');
  assert.ok(res.pages <= 2, 'cheap: ' + res.pages + ' calls');
  assert.equal(app.api.Catalog.rows.length, before + 1, 'the new listing was added');
  assert.equal(app.api.Catalog.byId.get(rows[5].listing_id).sku, 'EDITED_SKU', 'the edit was applied');
});

test('Every proxy response\'s exact call count is accumulated into a daily total', async reg => {
  const shop = fakeShop(shopOf(250));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  // 3 listing pages + 1 sections call, each reporting etsy_call_count: 1
  assert.equal(app.api.Budget.read(), 4);
  assert.match(app.meters().api, /api 4/);
});

test('The daily spend counter resets on a new day', async reg => {
  const stale = JSON.stringify({ day: '2001-01-01', n: 999 });
  const app = createApp({ storage: { sku_api_spend_v1: stale } }); reg.push(app);
  assert.equal(app.api.Budget.read(), 0, 'yesterday\'s total is not carried forward');
});

test('Remaining key quota comes from the free usage endpoint', async reg => {
  const shop = fakeShop(shopOf(10), { remaining: 812 });
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  await app.api.refreshQuota();
  assert.match(app.meters().api, /812 left on key/);
});

test('Sync failure is reported and leaves the catalog intact', async reg => {
  const rows = shopOf(120);
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const kept = app.api.Catalog.rows.length;

  app.api.Catalog.lastSync = 1;
  const broken = createApp({ storage: freshTokens(), fetchImpl: async () => ({ status: 500, body: 'Missing SHOP_ID' }) });
  reg.push(broken);
  await broken.domReady();
  assert.match(broken.status(), /Sync failed/);
  assert.equal(app.api.Catalog.rows.length, kept);
});

/* ── 4 · Search ───────────────────────────────────────────────────────── */

section('Search · local, shop-wide');

function mixedShop() {
  return [
    listing(100001, { title: 'Gold Charm Necklace', shop_section_id: 11 }),
    listing(100002, { title: 'Silver Hoop Earrings', shop_section_id: 22, inventory: { products: [{ product_id: 2, sku: '' }] } }),
    listing(100003, { title: '14k Gold-Filled Bracelet', shop_section_id: 44 }),
    listing(100004, { title: 'Rose Gold Ring', shop_section_id: 55 }),
    listing(100005, { title: 'Enamel Charm', shop_section_id: 33, inventory: { products: [{ product_id: 5, sku: '' }] } }),
    listing(100006, { title: 'Unsorted Pendant', shop_section_id: null }),
  ];
}

async function bootMixed(reg, over = {}) {
  const shop = fakeShop(mixedShop(), over);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl, ...over });
  reg.push(app);
  await app.domReady();
  return { app, shop };
}

test('Search spans the whole catalog, not just what is on screen', async reg => {
  const shop = fakeShop(shopOf(500));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  assert.ok(app.cards().length < 500, 'only the first chunk is rendered');
  await app.type('700499');
  assert.deepEqual(app.cardIds(), [700499], 'found a listing that was never rendered');
});

test('Matching is case- and punctuation-insensitive', async reg => {
  const { app } = await bootMixed(reg);
  await app.type('gold filled');
  assert.deepEqual(app.cardIds(), [100003], '"14k Gold-Filled" matches "gold filled"');
});

test('All terms must match, in any order', async reg => {
  const { app } = await bootMixed(reg);
  await app.type('necklace gold');
  assert.deepEqual(app.cardIds(), [100001]);
  await app.type('necklace silver');
  assert.deepEqual(app.cardIds(), []);
});

test('SKU and listing id are searchable alongside the title', async reg => {
  const { app } = await bootMixed(reg);
  await app.type('Gold_0001');
  assert.deepEqual(app.cardIds(), [100001]);
  await app.type('100004');
  assert.deepEqual(app.cardIds(), [100004]);
});

test('A SKU search shows WHY the row matched, by highlighting the SKU', async reg => {
  const rows = [listing(111, { title: 'Player Sports Pendant', inventory: { products: [{ product_id: 1, sku: 'BASKETBALL' }] } })];
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  await app.type('basketball');
  const value = app.cards()[0].querySelector('[data-role="sku-value"]');
  assert.equal(value.textContent, 'BASKETBALL', 'the SKU reads unchanged');
  assert.equal(value.querySelectorAll('mark').length, 1, 'and the match is marked');
});

test('"sku:" narrows the search to SKUs alone', async reg => {
  const rows = [
    listing(1, { title: 'Basketball Charm Necklace', inventory: { products: [{ product_id: 1, sku: 'BB_0001' }] } }),
    listing(2, { title: 'Player Sports Pendant', inventory: { products: [{ product_id: 2, sku: 'BASKETBALL' }] } }),
  ];
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();

  await app.type('basketball');
  assert.deepEqual(app.cardIds().sort(), [1, 2], 'a plain search matches title AND sku');

  await app.type('sku:basketball');
  assert.deepEqual(app.cardIds(), [2], 'the prefix drops the title-only match');

  await app.type('SKU: basketball');
  assert.deepEqual(app.cardIds(), [2], 'case and spacing around the prefix do not matter');
});

test('"sku:" alone lists every listing that has one', async reg => {
  const rows = [
    listing(1, { inventory: { products: [{ product_id: 1, sku: 'HAS_ONE' }] } }),
    listing(2, { inventory: { products: [{ product_id: 2, sku: '' }] } }),
  ];
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  await app.type('sku:');
  assert.deepEqual(app.cardIds(), [1]);
});

test('Word-start matches rank above mid-word ones', async reg => {
  const rows = [
    listing(1, { title: 'Reengraved Charm' }),       // "gra" mid-word
    listing(2, { title: 'Gravel Pendant' }),          // starts the record
  ];
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  await app.type('gra');
  assert.deepEqual(app.cardIds(), [2, 1]);
});

test('Matched terms are highlighted as text, never as parsed HTML', async reg => {
  const rows = [listing(7, { title: 'Gold <script> Necklace' })];
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  await app.type('gold');
  const title = app.els.listContainer.querySelector('.title');
  assert.equal(title.textContent, 'Gold <script> Necklace', 'the raw title survives verbatim');
  assert.equal(title.querySelectorAll('mark').length, 1);
  assert.equal(title.querySelector('mark').textContent, 'Gold');
});

test('Every occurrence of every term is highlighted, with overlaps merged', async reg => {
  const rows = [listing(8, { title: 'Gold on gold GOLDEN charm' })];
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  await app.type('gold golden');
  const title = app.els.listContainer.querySelector('.title');
  assert.equal(title.textContent, 'Gold on gold GOLDEN charm', 'text is preserved exactly');
  assert.deepEqual(title.querySelectorAll('mark').map(m => m.textContent), ['Gold', 'gold', 'GOLDEN'],
    'the gold/golden overlap became one mark, not two nested ones');
});

test('The search box has focus as soon as the page is usable', async reg => {
  const shop = fakeShop(shopOf(5));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  assert.equal(app.document.activeElement, app.els.searchInput);
});

test('An empty query shows everything, newest-modified first', async reg => {
  const rows = [
    listing(1, { last_modified_timestamp: 100 }),
    listing(2, { last_modified_timestamp: 300 }),
    listing(3, { last_modified_timestamp: 200 }),
  ];
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  assert.deepEqual(app.cardIds(), [2, 3, 1]);
});

test('Escape and the clear button reset the query', async reg => {
  const { app } = await bootMixed(reg);
  await app.type('gold');
  assert.ok(app.cards().length < 6);
  await app.els.searchClear.dispatch('click');
  assert.equal(app.els.searchInput.value, '');
  assert.equal(app.cards().length, 6);

  await app.type('ring');
  await app.document.dispatch('keydown', { key: 'Escape' });
  assert.equal(app.els.searchInput.value, '');
});

test('Search stays correct after a SKU is written', async reg => {
  const rest = async (url, init) => {
    if (url.includes('InventoryDetailProxy')) return { body: { products: [{ product_id: 5, sku: '' }], etsy_call_count: 1 } };
    if (url.includes('UpdateListingInventoryProxy')) return { body: { ok: true, etsy_call_count: 2 } };
    throw new Error('unexpected ' + url);
  };
  const shop = fakeShop(mixedShop(), { rest });
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();

  await app.type('100005');
  await app.cards()[0].querySelector('[data-role="gen-btn"]').dispatch('click');

  await app.type('Enamel_0005');
  assert.deepEqual(app.cardIds(), [100005], 'the new SKU is immediately searchable');
});

/* ── 5 · Section chips ────────────────────────────────────────────────── */

section('Section quick-filters');

test('Exactly the five named section shortcuts are shown, in order', async reg => {
  const { app } = await bootMixed(reg);
  const labels = app.chips().map(c => c.label).filter(Boolean);
  assert.deepEqual(labels, [
    'All6', 'NECKLACES1', 'EARRINGS1', 'CHARMS1', 'BRACELETS1', 'RINGS1',
    'Missing SKU', 'Hide ✓ done',
  ]);
});

test('REGRESSION: the shop\'s other sections get no chip', async reg => {
  // Etsy returns dozens of sections. Rendering them all buried the five that
  // were asked for and pushed the row off the screen.
  const extras = [
    ...SECTIONS,
    { shop_section_id: 66, title: 'Zodiac / Birth Flower', active_listing_count: 14 },
    { shop_section_id: 77, title: 'Handwriting Jewelry', active_listing_count: 11 },
    { shop_section_id: 88, title: 'Evil Eye Jewelry', active_listing_count: 9 },
  ];
  const rows = [
    ...mixedShop(),
    listing(200001, { title: 'Aries Birth Flower Necklace', shop_section_id: 66 }),
    listing(200002, { title: 'Evil Eye Charm', shop_section_id: 88 }),
  ];
  const shop = fakeShop(rows, { sections: extras });
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();

  const labels = app.chips().map(c => c.label).filter(Boolean);
  for (const unwanted of ['Zodiac', 'Handwriting', 'Evil Eye', 'No section', 'Section ']) {
    assert.ok(!labels.some(l => l.includes(unwanted)),
      'unexpected chip containing "' + unwanted + '": ' + labels.join(' | '));
  }
  assert.deepEqual(labels, [
    'All8', 'NECKLACES1', 'EARRINGS1', 'CHARMS1', 'BRACELETS1', 'RINGS1',
    'Missing SKU', 'Hide ✓ done',
  ]);

  // Those listings are still in the catalog and still findable.
  assert.equal(app.api.Catalog.rows.length, 8);
  await app.type('evil eye');
  assert.deepEqual(app.cardIds(), [200002], 'searchable, just not chipped');
});

test('A named section the shop does not have is skipped, not shown dead', async reg => {
  const shop = fakeShop(mixedShop(), { sections: SECTIONS.slice(0, 2) });  // only NECKLACES, EARRINGS
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const labels = app.chips().map(c => c.label).filter(Boolean);
  assert.deepEqual(labels, ['All6', 'NECKLACES1', 'EARRINGS1', 'Missing SKU', 'Hide ✓ done']);
});

test('Section titles are matched case-insensitively', async reg => {
  const lower = SECTIONS.map(s => ({ ...s, title: s.title.toLowerCase() }));
  const shop = fakeShop(mixedShop(), { sections: lower });
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const labels = app.chips().map(c => c.label).filter(Boolean);
  assert.deepEqual(labels.slice(1, 6),
    ['necklaces1', 'earrings1', 'charms1', 'bracelets1', 'rings1'],
    'shown with Etsy\'s own capitalisation, matched regardless of it');
});

test('REGRESSION: section chips appear even when the catalog sync fails', async reg => {
  // A sync that died partway used to leave the operator with no section
  // shortcuts at all, because sections were fetched as a tail of a SUCCESSFUL
  // walk. They are the console's primary navigation and load on their own now.
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (url.includes('etsyApiUsage')) return { body: { ok: true, verified: true, etsy_remaining_today: 100 } };
      if (url.includes('mode=sections')) return { body: { results: SECTIONS, etsy_call_count: 1 } };
      return { status: 502, body: '{"errorType":"Function.ResponseSizeTooLarge"}' };
    },
  });
  reg.push(app);
  await app.domReady();

  assert.match(app.status(), /Sync failed/, 'the sync really did fail');
  const labels = app.chips().map(c => c.label).filter(Boolean);
  for (const name of ['NECKLACES', 'EARRINGS', 'CHARMS', 'BRACELETS', 'RINGS']) {
    assert.ok(labels.some(l => l.startsWith(name)), 'missing chip: ' + name + ' — got ' + labels.join(' | '));
  }
});

test('Without the sections call there are no section chips, and All still works', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url, init) => {
      if (url.includes('mode=sections')) return { status: 500, body: { error: 'nope' } };
      return fakeShop(mixedShop()).impl(url, init);
    },
  });
  reg.push(app);
  await app.domReady();
  const labels = app.chips().map(c => c.label).filter(Boolean);
  assert.deepEqual(labels, ['All6', 'Missing SKU', 'Hide ✓ done'],
    'no id-labelled placeholders — a chip is only ever one of the five names');
  assert.equal(app.cards().length, 6, 'the catalog is unaffected');
});

test('Every chip reports its own pressed state for assistive tech', async reg => {
  const { app } = await bootMixed(reg);
  const earrings = app.chips().find(c => c.label.startsWith('EARRINGS'));
  assert.equal(earrings.el.getAttribute('aria-pressed'), 'false');
  await earrings.el.dispatch('click');
  const after = app.chips().find(c => c.label.startsWith('EARRINGS'));
  assert.equal(after.el.getAttribute('aria-pressed'), 'true');
  assert.ok(after.on, 'and is visibly selected');
  assert.ok(!app.chips()[0].on, 'while All is no longer selected');
});

test('Exactly one section chip is selected at a time', async reg => {
  const { app } = await bootMixed(reg);
  await app.chips().find(c => c.label.startsWith('CHARMS')).el.dispatch('click');
  const on = app.chips().filter(c => c.on && !c.label.includes('Missing') && !c.label.includes('Hide'));
  assert.equal(on.length, 1, 'selected: ' + on.map(c => c.label).join(', '));
  assert.ok(on[0].label.startsWith('CHARMS'));
});

test('The state filters are independent of the section selection', async reg => {
  const { app } = await bootMixed(reg);
  await app.chips().find(c => c.label.startsWith('EARRINGS')).el.dispatch('click');
  await app.chips().find(c => c.label === 'Missing SKU').el.dispatch('click');
  assert.ok(app.chips().find(c => c.label.startsWith('EARRINGS')).on, 'section stays selected');
  assert.ok(app.chips().find(c => c.label === 'Missing SKU').on);
  assert.deepEqual(app.cardIds(), [100002], 'earrings AND missing a SKU');
});

test('Selecting a section filters locally and costs no API calls', async reg => {
  const { app, shop } = await bootMixed(reg);
  const before = shop.calls.pages;
  const earrings = app.chips().find(c => c.label.startsWith('EARRINGS'));
  await earrings.el.dispatch('click');
  assert.deepEqual(app.cardIds(), [100002]);
  assert.equal(shop.calls.pages, before, 'zero extra Etsy reads');
  assert.ok(app.chips().find(c => c.label.startsWith('EARRINGS')).on);
});

test('A section narrows the search rather than replacing it', async reg => {
  const { app } = await bootMixed(reg);
  await app.type('gold');
  assert.deepEqual(app.cardIds().sort(), [100001, 100003, 100004, 100006],
    'three gold titles plus one gold SKU');
  await app.chips().find(c => c.label.startsWith('RINGS')).el.dispatch('click');
  assert.deepEqual(app.cardIds(), [100004], 'gold AND rings');
});

test('"All" returns to the whole catalog', async reg => {
  const { app } = await bootMixed(reg);
  await app.chips().find(c => c.label.startsWith('CHARMS')).el.dispatch('click');
  assert.equal(app.cards().length, 1);
  await app.chips()[0].el.dispatch('click');
  assert.equal(app.cards().length, 6);
});

test('"Missing SKU" finds exactly the listings that need work', async reg => {
  const { app } = await bootMixed(reg);
  await app.chips().find(c => c.label === 'Missing SKU').el.dispatch('click');
  assert.deepEqual(app.cardIds().sort(), [100002, 100005]);
});

test('"Hide ✓ done" respects the persisted checkmarks', async reg => {
  const shop = fakeShop(mixedShop());
  const app = createApp({
    storage: { ...freshTokens(), sku_selected_ids_v1: JSON.stringify(['100001', '100003']) },
    fetchImpl: shop.impl,
  });
  reg.push(app);
  await app.domReady();
  await app.chips().find(c => c.label === 'Hide ✓ done').el.dispatch('click');
  assert.ok(!app.cardIds().includes(100001));
  assert.ok(!app.cardIds().includes(100003));
  assert.equal(app.cards().length, 4);
});

test('REGRESSION: checkmarks saved as numbers still register', async reg => {
  const shop = fakeShop(mixedShop());
  const app = createApp({
    storage: { ...freshTokens(), sku_selected_ids_v1: JSON.stringify([100001]) },
    fetchImpl: shop.impl,
  });
  reg.push(app);
  await app.domReady();
  const card = app.els.listContainer.querySelector('.card[data-listing-id="100001"]');
  assert.equal(card.querySelector('.selectBox').checked, true);
});

/* ── 6 · Infinite scroll ──────────────────────────────────────────────── */

section('Infinite scroll');

test('Only the first chunk renders; scrolling appends more', async reg => {
  const shop = fakeShop(shopOf(300));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const first = app.cards().length;
  assert.ok(first > 0 && first <= 120, 'first screen only, got ' + first);

  await app.scrollToEnd();
  assert.equal(app.cards().length, first + 60);
  await app.scrollToEnd(10);
  assert.equal(app.cards().length, 300, 'reaches the end of the result set');
});

test('Scrolling past the end is a no-op and the endcap says so', async reg => {
  const shop = fakeShop(shopOf(80));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  await app.scrollToEnd(5);
  assert.equal(app.cards().length, 80);
  assert.match(app.els.endcap.textContent, /end of 80 listings/);
});

test('A new search resets the scroll window to the top', async reg => {
  const shop = fakeShop(shopOf(300));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  await app.scrollToEnd(3);
  assert.ok(app.cards().length > 120);
  await app.type('gold');
  assert.ok(app.cards().length <= 120, 'restarted at the first chunk');
});

test('There are no pagination controls left', async reg => {
  const fs = require('node:fs');
  const html = fs.readFileSync(require('node:path').join(__dirname, '..', '..', 'SKU_List_V1.html'), 'utf8');
  for (const gone of ['id="prevBtn"', 'id="nextBtn"', 'id="pageJump"', 'id="pageSize"', 'id="pageNum"', '<footer']) {
    assert.ok(!html.includes(gone), 'still present: ' + gone);
  }
});

test('The pills and Generate sit on one row', async reg => {
  const shop = fakeShop([listing(100001, { title: 'Gold Charm', shop_section_id: 11 })], { sections: SECTIONS });
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const card = app.cards()[0];
  const rows = card.querySelectorAll('.row');
  assert.equal(rows.length, 1, 'one row, not a stack of them');
  const kinds = rows[0].children.map(c => c.className.split(' ')[0]);
  assert.deepEqual(kinds, ['selectBox', 'pill', 'pill', 'pill', 'btn'],
    'checkbox, id, price, section, Generate — all on the one line');
  assert.ok(rows[0].querySelector('.grow'), 'Generate is pushed to the end');
});

test('The SKU line is its own row, above the pills', async reg => {
  const shop = fakeShop([listing(100001)]);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const meta = app.cards()[0].querySelector('.meta');
  assert.deepEqual(meta.children.map(c => c.className.split(' ')[0]), ['title', 'skuline', 'row']);
});

test('The result meter tracks what is shown against what matched', async reg => {
  const shop = fakeShop(shopOf(300));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  assert.match(app.meters().result, /showing \d+ of 300/);
  await app.scrollToEnd(10);
  assert.match(app.meters().result, /showing 300 of 300/);
});

/* ── 6b · Image zoom ──────────────────────────────────────────────────── */

section('Image zoom (click-to-zoom, ported)');

async function oneCard(reg) {
  const shop = fakeShop([listing(424242)]);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl });
  reg.push(app);
  await app.domReady();
  const card = app.cards()[0];
  const { box, img } = app.layout(card, 300);
  await img.dispatch('load');
  return { app, card, box, img };
}

/* A click at (x,y) inside a 300x300 box whose top-left is the origin. */
const clickAt = (box, x, y) => box.dispatch('click', { clientX: x, clientY: y });

test('REGRESSION: nothing listens for wheel, so the page scrolls over images', async reg => {
  // The old pinch/scroll zoom called preventDefault on wheel, which froze the
  // page whenever the pointer sat over a picture — and in an infinitely
  // scrolling grid of pictures that is most of the window.
  const { app, box, img } = await oneCard(reg);
  assert.equal(app.hasListener(box, 'wheel'), false, 'no wheel handler on the image box');
  assert.equal(app.hasListener(img, 'wheel'), false, 'nor on the image');
  const html = require('node:fs').readFileSync(
    require('node:path').join(__dirname, '..', '..', 'SKU_List_V1.html'), 'utf8');
  assert.ok(!/addEventListener\(\s*["']wheel["']/.test(html), 'and none anywhere in the page');
});

test('An untouched image sits at scale 1 with no offset', async reg => {
  const { app, card } = await oneCard(reg);
  assert.deepEqual(app.zoomState(card), { scale: 1, x: 0, y: 0 });
});

test('A click zooms in and centres the clicked point', async reg => {
  const { app, card, box } = await oneCard(reg);
  // Click at (225, 150): 75px right of centre, vertically centred.
  await clickAt(box, 225, 150);
  const z = app.zoomState(card);
  assert.ok(z.scale > 1, 'zoomed in, got ' + z.scale);
  assert.ok(z.x < 0, 'panned left to bring the clicked point to centre, got ' + z.x);
  assert.equal(z.y, 0, 'no vertical movement for a vertically centred click');
  assert.equal(app.transform(card), `scale(${z.scale}) translate(${z.x}px, ${z.y}px)`);
});

test('Clicking dead centre zooms without panning', async reg => {
  const { app, card, box } = await oneCard(reg);
  await clickAt(box, 150, 150);
  const z = app.zoomState(card);
  // s1 = max(DESIRED 1.33, s0 * 1.5, needX, needY). From rest the 1.5x step
  // wins, so DESIRED is a floor rather than the first stop.
  assert.equal(z.scale, 1.5);
  assert.deepEqual([z.x, z.y], [0, 0]);
});

test('Successive clicks keep zooming, then snap back to 1', async reg => {
  const { app, card, box } = await oneCard(reg);
  await clickAt(box, 150, 150);
  const first = app.zoomState(card).scale;
  await clickAt(box, 150, 150);
  const second = app.zoomState(card).scale;
  assert.ok(second > first, `${first} -> ${second}`);

  // Clicking the same spot once the step would barely move returns to 1.
  let guard = 0, z = app.zoomState(card);
  while (z.scale !== 1 && guard++ < 12){ await clickAt(box, 150, 150); z = app.zoomState(card); }
  assert.equal(z.scale, 1, 'came back to unzoomed within ' + guard + ' clicks');
  assert.deepEqual([z.x, z.y], [0, 0], 'and the framing resets with it');
});

test('Zoom never exceeds the source\'s MAX_SCALE of 8', async reg => {
  const { app, card, box } = await oneCard(reg);
  for (let i = 0; i < 30; i++) await clickAt(box, 299, 299);   // hard into the corner
  assert.ok(app.zoomState(card).scale <= 8, 'got ' + app.zoomState(card).scale);
});

test('Dragging pans only once zoomed in', async reg => {
  const { app, card, box } = await oneCard(reg);

  await box.dispatch('mousedown', { clientX: 150, clientY: 150 });
  await box.dispatch('mousemove', { clientX: 120, clientY: 150 });
  assert.deepEqual(app.zoomState(card), { scale: 1, x: 0, y: 0 }, 'no pan at scale 1');
  await box.dispatch('mouseup', {});

  await clickAt(box, 150, 150);
  const before = app.zoomState(card);
  await box.dispatch('mousedown', { clientX: 150, clientY: 150 });
  await box.dispatch('mousemove', { clientX: 130, clientY: 140 });
  const after = app.zoomState(card);
  assert.notDeepEqual([after.x, after.y], [before.x, before.y], 'panned while zoomed');
  assert.equal(after.scale, before.scale, 'panning does not change the zoom');
  await box.dispatch('mouseup', {});
});

test('Pan movement is damped by half, as in the source', async reg => {
  const { app, card, box } = await oneCard(reg);
  await clickAt(box, 150, 150);              // scale 1.33, offsets 0
  await box.dispatch('mousedown', { clientX: 150, clientY: 150 });
  await box.dispatch('mousemove', { clientX: 170, clientY: 150 });   // +20px
  const z = app.zoomState(card);
  assert.ok(Math.abs(z.x - 10) < 0.001, 'a 20px drag moves 10 units, got ' + z.x);
});

test('A drag is not mistaken for a click', async reg => {
  const { app, card, box } = await oneCard(reg);
  await clickAt(box, 150, 150);
  const scaleAfterZoom = app.zoomState(card).scale;

  await box.dispatch('mousedown', { clientX: 150, clientY: 150 });
  await box.dispatch('mousemove', { clientX: 190, clientY: 150 });  // past dragThreshold
  await box.dispatch('mouseup', {});
  await clickAt(box, 190, 150);   // the click the browser fires after a drag
  assert.equal(app.zoomState(card).scale, scaleAfterZoom, 'the drag-click did not re-zoom');
});

test('Panning is clamped so the image cannot be dragged out of its box', async reg => {
  const { app, card, box } = await oneCard(reg);
  await clickAt(box, 150, 150);                    // scale 1.33
  await box.dispatch('mousedown', { clientX: 150, clientY: 150 });
  for (let i = 0; i < 40; i++) await box.dispatch('mousemove', { clientX: 150 + i * 40, clientY: 150 });
  await box.dispatch('mouseup', {});
  const z = app.zoomState(card);
  // maxX = (300*1.33 - 300) / 2 / 1.33
  const maxX = (300 * z.scale - 300) / 2 / z.scale;
  assert.ok(z.x <= maxX + 0.001, `${z.x} exceeds the clamp ${maxX}`);
});

test('A relayout re-clamps without changing the zoom', async reg => {
  const { app, card, box } = await oneCard(reg);
  await clickAt(box, 220, 150);
  const before = app.zoomState(card);
  await app.resize();
  const after = app.zoomState(card);
  assert.equal(after.scale, before.scale, 'ResizeObserver must not alter the zoom level');
});

test('Zoom applies before layout without destroying the framing', async reg => {
  // The first click can land before the browser has laid the image out.
  const shop = fakeShop([listing(9)]);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const card = app.cards()[0];
  const box = card.querySelector('.thumb-wrap');    // deliberately NOT laid out
  await clickAt(box, 80, 80);
  const z = app.zoomState(card);
  assert.ok(Number.isFinite(z.scale) && Number.isFinite(z.x) && Number.isFinite(z.y),
    'no NaN transform: ' + JSON.stringify(z));
});

test('Every image carries the rule-of-thirds grid', async reg => {
  const html = require('node:fs').readFileSync(
    require('node:path').join(__dirname, '..', '..', 'SKU_List_V1.html'), 'utf8');
  const rule = html.slice(html.indexOf('.thumb-wrap::after'), html.indexOf('.selectBox'));
  assert.ok(rule.includes('pointer-events:none'), 'the grid must not swallow clicks');
  assert.equal((rule.match(/linear-gradient/g) || []).length, 4, 'four hairlines');
  assert.ok(rule.includes('33.333% 0, 66.666% 0, 0 33.333%, 0 66.666%'), 'on the thirds');
  assert.ok(rule.includes('rgba(255,255,255,.18)'), '18% white, as in the source');
});

/* ── 7 · SKU write paths ──────────────────────────────────────────────── */

section('SKU line — inline edit, generate, save');

function skuShop(reg, opts = {}) {
  let put = null, detailCalls = 0;
  const inventory = opts.inventory || [{ product_id: 5, sku: opts.startingSku ?? '' }];
  const rest = async (url, init) => {
    if (url.includes('InventoryDetailProxy')) {
      detailCalls++;
      if (opts.detailFails) return { status: 500, body: { error: 'inventory unavailable' } };
      if (opts.detailDelay) await new Promise(r => setTimeout(r, opts.detailDelay));
      return { body: { products: inventory, etsy_call_count: 1 } };
    }
    if (url.includes('UpdateListingInventoryProxy')) {
      if (opts.writeFails) return { status: 409, body: { code: 'STALE_INVENTORY' } };
      put = JSON.parse(init.body);
      return { body: { ok: true, etsy_call_count: 2 } };
    }
    throw new Error('unexpected ' + url);
  };
  const rows = [listing(447788, {
    title: 'Ruby drop earrings',
    inventory: { products: [{ product_id: 5, sku: opts.startingSku ?? '' }] },
  })];
  const shop = fakeShop(rows, { rest });
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl });
  reg.push(app);
  return { app, shop, put: () => put, detailCalls: () => detailCalls };
}

const skuValue  = card => card.querySelector('[data-role="sku-value"]');
const skuInput  = card => card.querySelector('.skuinput');
const skuSave   = card => card.querySelector('[data-role="sku-save"]');
const skuCancel = card => card.querySelector('[data-role="sku-cancel"]');
const unsaved   = card => card.querySelector('[data-role="unsaved"]');
const genBtn    = card => card.querySelector('[data-role="gen-btn"]');

test('The SKU reads as one bold value, with no "(Variant 1)" noise', async reg => {
  const { app } = skuShop(reg, { startingSku: 'BASKETBALL' });
  await app.domReady();
  const card = app.cards()[0];
  assert.equal(skuValue(card).textContent, 'BASKETBALL');
  assert.equal(card.querySelector('.skulabel').textContent, 'SKU');
  const html = require('node:fs').readFileSync(
    require('node:path').join(__dirname, '..', '..', 'SKU_List_V1.html'), 'utf8');
  assert.ok(!html.includes('(Variant 1)'), 'the phrase is gone from the page entirely');
});

test('A listing with no SKU says so, and Generate is flagged', async reg => {
  const { app } = skuShop(reg);
  await app.domReady();
  const card = app.cards()[0];
  assert.equal(skuValue(card).textContent, '— none —');
  assert.ok(skuValue(card).classList.contains('none'));
  assert.equal(genBtn(card).style.background, 'OrangeRed');
});

test('REGRESSION: clicking the SKU opens the editor with no Etsy round trip', async reg => {
  // The old Edit SKU button fetched inventory before it could show anything,
  // which is the multi-second delay. The cached value opens instantly and the
  // product_id needed to WRITE is fetched in the background.
  const { app, detailCalls } = skuShop(reg, { startingSku: 'OLD_SKU', detailDelay: 5000 });
  await app.domReady();
  const card = app.cards()[0];

  await skuValue(card).dispatch('click');          // resolves without awaiting the fetch
  assert.ok(skuInput(card), 'the input is already there');
  assert.equal(skuInput(card).value, 'OLD_SKU', 'prefilled from the local catalog');
  assert.equal(app.document.activeElement, skuInput(card), 'and focused, ready to type');
});

test('There is no separate Edit SKU button any more', async reg => {
  const { app } = skuShop(reg, { startingSku: 'A' });
  await app.domReady();
  const labels = app.cards()[0].querySelectorAll('button').map(b => b.textContent);
  assert.ok(!labels.includes('Edit SKU'), 'got ' + labels.join(' | '));
  assert.ok(labels.includes('Generate'));
});

test('Editing and saving writes to Etsy and updates the catalog', async reg => {
  const { app, put } = skuShop(reg, { startingSku: 'OLD', inventory: [{ product_id: 21, sku: 'OLD' }] });
  await app.domReady();
  const card = app.cards()[0];

  await skuValue(card).dispatch('click');
  skuInput(card).value = '  Custom_ABC  ';
  await skuInput(card).dispatch('input');
  await skuSave(card).dispatch('click');

  assert.deepEqual(put(), { listing_id: 447788, items: [{ product_id: 21, sku: 'Custom_ABC' }] });
  assert.equal(skuValue(card).textContent, 'Custom_ABC', 'back to reading, showing the new value');
  assert.equal(app.api.Catalog.byId.get(447788).sku, 'Custom_ABC', 'and the catalog agrees');
});

test('Cancel discards the edit', async reg => {
  const { app, put } = skuShop(reg, { startingSku: 'KEEP' });
  await app.domReady();
  const card = app.cards()[0];
  await skuValue(card).dispatch('click');
  skuInput(card).value = 'THROWN_AWAY';
  await skuInput(card).dispatch('input');
  await skuCancel(card).dispatch('click');
  assert.equal(put(), null, 'nothing written');
  assert.equal(skuValue(card).textContent, 'KEEP');
});

test('Enter saves and Escape cancels', async reg => {
  const { app, put } = skuShop(reg, { startingSku: 'A' });
  await app.domReady();
  let card = app.cards()[0];

  await skuValue(card).dispatch('click');
  skuInput(card).value = 'VIA_ENTER';
  await skuInput(card).dispatch('input');
  await skuInput(card).dispatch('keydown', { key: 'Enter' });
  assert.equal(put().items[0].sku, 'VIA_ENTER');

  await skuValue(card).dispatch('click');
  skuInput(card).value = 'NOPE';
  await skuInput(card).dispatch('input');
  await skuInput(card).dispatch('keydown', { key: 'Escape' });
  assert.equal(skuValue(card).textContent, 'VIA_ENTER', 'unchanged');
});

test('A save in flight shows a spinner and locks the buttons', async reg => {
  let release;
  const gate = new Promise(r => { release = r; });
  const rows = [listing(447788, { inventory: { products: [{ product_id: 5, sku: '' }] } })];
  const shop = fakeShop(rows, {
    rest: async (url) => {
      if (url.includes('InventoryDetailProxy')) return { body: { products: [{ product_id: 5, sku: '' }] } };
      await gate;
      return { body: { ok: true, etsy_call_count: 2 } };
    },
  });
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const card = app.cards()[0];

  await skuValue(card).dispatch('click');
  skuInput(card).value = 'X';
  await skuInput(card).dispatch('input');
  const saving = skuSave(card).dispatch('click');

  await new Promise(r => setTimeout(r, 5));
  assert.ok(card.querySelector('.spin'), 'a spinner is visible while the write is in flight');
  assert.equal(skuSave(card).disabled, true);
  assert.equal(skuCancel(card).disabled, true);

  release();
  await saving;
  assert.ok(!card.querySelector('.spin'), 'and gone once it lands');
});

test('Generate proposes a SKU but does NOT write it', async reg => {
  const { app, put } = skuShop(reg);
  await app.domReady();
  const card = app.cards()[0];

  await genBtn(card).dispatch('click');
  assert.equal(put(), null, 'Generate never writes on its own');
  assert.equal(skuInput(card).value, 'Ruby_7788', 'derived from the title');
  assert.ok(unsaved(card), 'and is marked unsaved');
  assert.equal(unsaved(card).textContent, 'Unsaved');
  assert.ok(card.classList.contains('pending'), 'the whole card is flagged');
});

test('A generated SKU only goes live once saved', async reg => {
  const { app, put } = skuShop(reg);
  await app.domReady();
  const card = app.cards()[0];

  await genBtn(card).dispatch('click');
  await skuSave(card).dispatch('click');

  assert.deepEqual(put(), { listing_id: 447788, items: [{ product_id: 5, sku: 'Ruby_7788' }] });
  assert.ok(!card.classList.contains('pending'), 'no longer pending');
  assert.equal(unsaved(card), null, 'the marker is gone');
  assert.equal(skuValue(card).textContent, 'Ruby_7788');
  assert.equal(genBtn(card).style.background, '', 'and the warning colour clears');
});

test('REGRESSION: an unsaved generated SKU is lost on refresh, as the marker warns', async reg => {
  const { app } = skuShop(reg);
  await app.domReady();
  const card = app.cards()[0];
  await genBtn(card).dispatch('click');
  assert.equal(skuInput(card).value, 'Ruby_7788');

  // Nothing was written, so nothing reached the catalog...
  assert.equal(app.api.Catalog.byId.get(447788).sku, '', 'the catalog is untouched');
  // ...and a re-render (what a refresh or a filter change does) drops it.
  await app.type('ruby');
  assert.equal(skuValue(app.cards()[0]).textContent, '— none —');
});

test('A generated SKU can be edited before saving', async reg => {
  const { app, put } = skuShop(reg);
  await app.domReady();
  const card = app.cards()[0];
  await genBtn(card).dispatch('click');
  skuInput(card).value = 'Ruby_7788_B';
  await skuInput(card).dispatch('input');
  await skuSave(card).dispatch('click');
  assert.equal(put().items[0].sku, 'Ruby_7788_B');
});

test('Cancelling a generated SKU restores the previous value', async reg => {
  const { app, put } = skuShop(reg, { startingSku: 'ORIGINAL' });
  await app.domReady();
  const card = app.cards()[0];
  await genBtn(card).dispatch('click');
  assert.ok(card.classList.contains('pending'));
  await skuCancel(card).dispatch('click');
  assert.equal(put(), null);
  assert.equal(skuValue(card).textContent, 'ORIGINAL');
  assert.ok(!card.classList.contains('pending'));
});

test('A failed write is reported and the buttons come back', async reg => {
  const { app } = skuShop(reg, { writeFails: true });
  await app.domReady();
  const card = app.cards()[0];
  await genBtn(card).dispatch('click');
  await skuSave(card).dispatch('click');
  assert.match(app.alerts[0], /Could not save the SKU.*409/s);
  assert.equal(skuSave(card).disabled, false, 'retryable');
  assert.ok(card.classList.contains('pending'), 'still unsaved, still flagged');
});

test('A listing with no Variant 1 fails at save time, not at click time', async reg => {
  const { app } = skuShop(reg, { inventory: [] });
  await app.domReady();
  const card = app.cards()[0];
  await skuValue(card).dispatch('click');
  assert.ok(skuInput(card), 'the editor still opened instantly');
  await skuSave(card).dispatch('click');
  assert.match(app.alerts[0], /no Variant 1/i);
});

test('An inventory read failure surfaces only when a save needs it', async reg => {
  const { app } = skuShop(reg, { detailFails: true, startingSku: 'A' });
  await app.domReady();
  const card = app.cards()[0];
  await skuValue(card).dispatch('click');
  assert.equal(app.alerts.length, 0, 'opening the editor does not alert');
  await skuSave(card).dispatch('click');
  assert.match(app.alerts[0], /Could not save the SKU/);
});

test('SKU writes are counted against the daily API total', async reg => {
  const { app } = skuShop(reg);
  await app.domReady();
  const before = app.api.Budget.read();
  await genBtn(card_of(app)).dispatch('click');
  await skuSave(card_of(app)).dispatch('click');
  assert.equal(app.api.Budget.read(), before + 3, '1 inventory read + 2 for the write');
});
function card_of(app) { return app.cards()[0]; }

/* ── 8 · Catalog rebuild + drift ──────────────────────────────────────── */

section('Catalog integrity');

test('A drift between the local count and Etsy\'s live count is flagged', async reg => {
  const rows = shopOf(120);
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  assert.ok(!app.els.catalogMeter.className.includes('warn'));

  // A listing is deleted at Etsy — an incremental walk can never see this.
  app.api.Catalog.shopCount = 119;
  app.api.renderMeters();
  assert.ok(app.els.catalogMeter.className.includes('warn'), 'drift is surfaced');
  assert.match(app.els.catalogMeter.textContent, /120 \/ 119 live/);
});

test('A rebuild clears the catalog and re-reads the shop', async reg => {
  const rows = shopOf(150);
  const shop = fakeShop(rows);
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const firstPass = shop.calls.pages;

  await app.api.rebuildCatalog();
  assert.match(app.confirms[0], /Rebuild the whole local catalog/);
  assert.equal(shop.calls.pages, firstPass * 2, 'every page re-read');
  assert.equal(app.api.Catalog.rows.length, 150, 'and no duplicates');
});

test('Shift-clicking Sync rebuilds instead of doing an incremental pass', async reg => {
  const shop = fakeShop(shopOf(150));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const firstPass = shop.calls.pages;
  await app.els.syncBtn.dispatch('click', { shiftKey: true });
  assert.match(app.confirms[0] || '', /Rebuild the whole local catalog/);
  assert.equal(shop.calls.pages, firstPass * 2);
});

test('A plain Sync click stays incremental', async reg => {
  const shop = fakeShop(shopOf(150));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl }); reg.push(app);
  await app.domReady();
  const firstPass = shop.calls.pages;
  await app.els.syncBtn.dispatch('click', { shiftKey: false });
  assert.equal(app.confirms.length, 0, 'no rebuild prompt');
  assert.equal(shop.calls.pages, firstPass + 1, 'one catch-up call');
});

test('A declined rebuild changes nothing', async reg => {
  const shop = fakeShop(shopOf(150));
  const app = createApp({ storage: freshTokens(), fetchImpl: shop.impl, confirm: false }); reg.push(app);
  await app.domReady();
  const before = shop.calls.pages;
  await app.api.rebuildCatalog();
  assert.equal(shop.calls.pages, before);
  assert.equal(app.api.Catalog.rows.length, 150);
});

test('Catalog persistence degrades to memory without IndexedDB', async reg => {
  const app = createApp(); reg.push(app);
  const store = await app.api.openCatalogStore();
  assert.equal(store.kind, 'memory');
  await store.putMany([{ listing_id: 1, title: 'x' }]);
  assert.equal((await store.getAll()).length, 1);
  await store.setMeta('lastSync', 42);
  assert.equal(await store.getMeta('lastSync'), 42);
  await store.clear();
  assert.equal((await store.getAll()).length, 0);
});

/* ── 9 · Record shape + helpers ───────────────────────────────────────── */

section('Helpers');

test('toRecord keeps only what search and the card need', async reg => {
  const app = createApp(); reg.push(app);
  const r = app.api.toRecord(listing(881234, { title: 'Gold Charm' }));
  assert.deepEqual(Object.keys(r).sort(),
    ['image', 'listing_id', 'price', 'q', 'qsku', 'section_id', 'sku', 'title', 'updated']);
  assert.equal(r.image, 'https://img/881234-a.jpg', 'rank 1 wins');
  assert.equal(r.sku, 'Gold_1234');
  assert.equal(r.section_id, 11);
  assert.equal(r.q, 'gold charm gold 1234 881234', 'search key is precomputed once');
  assert.equal(r.qsku, 'gold 1234', 'and a SKU-only key alongside it');
});

test('toRecord reads the projected row and a full Etsy listing alike', async reg => {
  const app = createApp(); reg.push(app);
  const projected = app.api.toRecord({
    listing_id: 881234, title: 'Gold Charm',
    price: { amount: 1999, divisor: 100, currency_code: 'USD' },
    shop_section_id: 11, last_modified_timestamp: 1700000000,
    image: 'https://img/p.jpg', sku: 'Gold_1234',
  });
  const full = app.api.toRecord(listing(881234, { title: 'Gold Charm' }));
  assert.equal(projected.sku, 'Gold_1234');
  assert.equal(projected.image, 'https://img/p.jpg');
  assert.equal(projected.q, full.q, 'the same search key either way');
  assert.equal(projected.section_id, full.section_id);
  assert.equal(projected.updated, full.updated);
});

test('An empty projected SKU is honoured, not re-derived', async reg => {
  const app = createApp(); reg.push(app);
  const r = app.api.toRecord({ listing_id: 1, title: 'T', sku: '', image: '' });
  assert.equal(r.sku, '');
  assert.equal(r.image, '');
});

test('listingUpdatedAt accepts whichever timestamp Etsy sent', async reg => {
  const app = createApp(); reg.push(app);
  const { listingUpdatedAt } = app.api;
  assert.equal(listingUpdatedAt({ last_modified_timestamp: 5 }), 5);
  assert.equal(listingUpdatedAt({ updated_timestamp: 7 }), 7);
  assert.equal(listingUpdatedAt({ state_timestamp: 9 }), 9);
  assert.equal(listingUpdatedAt({}), 0);
});

test('normalizeText folds case, accents and punctuation', async reg => {
  const app = createApp(); reg.push(app);
  assert.equal(app.api.normalizeText('14K Gold-Filled — Rosé!'), '14k gold filled rose');
});

test('Price honours the Money divisor', async reg => {
  const app = createApp(); reg.push(app);
  assert.equal(app.api.formatPrice({ amount: 12345, divisor: 1000, currency_code: 'EUR' }), '12.35 EUR');
  assert.equal(app.api.formatPrice({ amount: 1999, divisor: 100, currency_code: 'USD' }), '19.99 USD');
  assert.equal(app.api.formatPrice(null), '');
});

test('pickPrimaryImage prefers is_primary, then rank', async reg => {
  const app = createApp(); reg.push(app);
  const { pickPrimaryImage } = app.api;
  assert.equal(pickPrimaryImage([{ rank: 1, url_570xN: 'a' }, { is_primary: true, url_570xN: 'b' }]), 'b');
  assert.equal(pickPrimaryImage([{ rank: 3, url_570xN: 'a' }, { rank: 1, url_570xN: 'b' }]), 'b');
  assert.equal(pickPrimaryImage([]), '');
});

(async function main() {
  for (const item of QUEUE) {
    if (item.kind === 'section') console.log('\n' + item.name);
    else await runOne(item);
  }
  console.log(`\n${passed} passed, ${failures.length} failed`);
  if (failures.length) {
    process.exitCode = 1;
    console.log('\nFailures:');
    failures.forEach(([n]) => console.log('  - ' + n));
  }
})();
