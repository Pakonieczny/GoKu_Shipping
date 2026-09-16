// Functional tests for SKU_List_V1.html (the sku.goldenspike.app console).
// Run: node tests/sku/sku-list.cjs
const assert = require('node:assert');
const crypto = require('node:crypto');
const { createApp } = require('./dom-harness.cjs');

let passed = 0;
const failures = [];
const only = process.argv[2];
const QUEUE = [];

// Registered synchronously, run sequentially by main() at the bottom: each
// case boots its own sandboxed copy of the page and must not interleave.
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
    console.log('  \u2713 ' + name);
  } catch (err) {
    failures.push([name, err]);
    console.log('  \u2717 ' + name + '\n      ' + ((err && err.stack) || err));
  } finally {
    reg.forEach(a => a.cleanup && a.cleanup());
  }
}

const settle = (ms = 20) => new Promise(r => setTimeout(r, ms));

/* ── fixtures ─────────────────────────────────────────────────────────── */

const listing = (id, over = {}) => ({
  listing_id: id,
  title: `Gold Charm Necklace ${id}`,
  price: { amount: 1999, divisor: 100, currency_code: 'USD' },
  images: [
    { rank: 2, url_fullxfull: `https://img/${id}-b.jpg` },
    { rank: 1, url_fullxfull: `https://img/${id}-a.jpg` },
  ],
  inventory: { products: [{ product_id: id * 10, sku: `Gold_${String(id).slice(-4)}` }] },
  ...over,
});

const tokenRedirect = t =>
  `?access_token=${t.a}&refresh_token=${t.r}&expires_in=${t.e}&issued_at=${t.i}`;

function freshTokens() {
  return { access_token: 'AT', refresh_token: 'RT', expires_in: '3600', issued_at: String(Math.floor(Date.now() / 1000)) };
}
function expiredTokens() {
  return { access_token: 'OLD', refresh_token: 'RT', expires_in: '3600', issued_at: String(Math.floor(Date.now() / 1000) - 7200) };
}

/* Verifies a PKCE pair exactly the way Etsy does. */
function challengeFor(verifier) {
  return crypto.createHash('sha256').update(verifier).digest('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
function paramsOf(url) { return new URL(url).searchParams; }

/* ── 1 · OAuth / PKCE — the reported invalid_grant bug ────────────────── */

section('OAuth · PKCE verifier lifecycle');

test('Connect sends a challenge that matches the stored verifier', async reg => {
  const app = createApp(); reg.push(app);
  await app.domReady();
  await app.els.connectEtsyBtn.dispatch('click');

  const url = app.navigations.at(-1);
  const p = paramsOf(url);
  assert.ok(url.startsWith('https://www.etsy.com/oauth/connect?'), 'goes to Etsy');
  assert.equal(p.get('code_challenge_method'), 'S256');
  assert.equal(p.get('redirect_uri'), 'https://sku.goldenspike.app');

  const state = p.get('state');
  const verifier = app.api.takeVerifier(state);
  assert.ok(verifier && verifier.length === 64, 'verifier stored under state');
  assert.equal(challengeFor(verifier), p.get('code_challenge'), 'challenge is SHA-256(verifier)');
});

test('REGRESSION: two authorize runs — the older callback still gets ITS verifier', async reg => {
  // This is the exact shape that produced `invalid_grant: code_verifier is
  // invalid`: a second Connect (stale tab / double click) overwrote the single
  // verifier slot, so the first code came back and was checked against the
  // second verifier.
  const a = createApp(); reg.push(a);
  await a.domReady();
  await a.els.connectEtsyBtn.dispatch('click');
  const first = paramsOf(a.navigations.at(-1));

  // second authorize in the same browser profile
  const b = createApp({ storage: a.localStorage._dump() }); reg.push(b);
  await b.domReady();
  await b.els.connectEtsyBtn.dispatch('click');
  const second = paramsOf(b.navigations.at(-1));
  assert.notEqual(first.get('state'), second.get('state'));

  // now the FIRST authorization returns
  const back = createApp({ storage: b.localStorage._dump(), search: `?code=CODE1&state=${encodeURIComponent(first.get('state'))}` });
  reg.push(back);
  const outcome = back.api.bootOAuth();
  assert.equal(outcome, 'exchange');
  const sent = paramsOf(back.navigations.at(-1));
  assert.equal(challengeFor(sent.get('code_verifier')), first.get('code_challenge'),
    'exchange carries the verifier that generated the FIRST challenge');
  assert.equal(sent.get('redirect_domain'), 'sku');
  assert.equal(sent.get('code'), 'CODE1');
});

test('Callback with an unknown state falls back to the newest verifier', async reg => {
  const a = createApp(); reg.push(a);
  await a.domReady();
  await a.els.connectEtsyBtn.dispatch('click');
  const issued = paramsOf(a.navigations.at(-1));

  const back = createApp({ storage: a.localStorage._dump(), search: '?code=C&state=mangled' }); reg.push(back);
  assert.equal(back.api.bootOAuth(), 'exchange');
  const sent = paramsOf(back.navigations.at(-1));
  assert.equal(challengeFor(sent.get('code_verifier')), issued.get('code_challenge'));
});

test('Legacy single-slot verifier from an older build still works', async reg => {
  const v = 'a'.repeat(64);
  const app = createApp({ storage: { etsy_code_verifier: v }, search: '?code=C&state=s1' }); reg.push(app);
  assert.equal(app.api.bootOAuth(), 'exchange');
  assert.equal(paramsOf(app.navigations.at(-1)).get('code_verifier'), v);
});

test('Verifier entries older than 15 minutes are swept', async reg => {
  const stale = JSON.stringify({ old: { v: 'x'.repeat(64), at: Date.now() - 16 * 60 * 1000 } });
  const app = createApp({ storage: { etsy_pkce_v2: stale } }); reg.push(app);
  await app.domReady();
  await app.els.connectEtsyBtn.dispatch('click');
  const map = JSON.parse(app.localStorage.getItem('etsy_pkce_v2'));
  assert.ok(!('old' in map), 'stale entry dropped');
  assert.equal(Object.keys(map).length, 1);
});

test('Replaying a used authorization code is refused, not re-exchanged', async reg => {
  const app = createApp({ storage: { etsy_code_verifier: 'v'.repeat(64) }, search: '?code=SAME&state=s' }); reg.push(app);
  assert.equal(app.api.bootOAuth(), 'exchange');

  const replay = createApp({ storage: app.localStorage._dump(), search: '?code=SAME&state=s' }); reg.push(replay);
  replay.sessionStorage.setItem('etsy_last_code', 'SAME');
  assert.equal(replay.api.bootOAuth(), 'replay');
  assert.equal(replay.navigations.length, 0, 'no second exchange');
  assert.match(replay.status(), /already used/i);
});

test('Callback with no verifier at all reports it instead of exchanging', async reg => {
  const app = createApp({ search: '?code=C&state=s' }); reg.push(app);
  assert.equal(app.api.bootOAuth(), 'no-verifier');
  assert.equal(app.navigations.length, 0);
  assert.match(app.status(), /Connect Etsy/);
  assert.equal(app.location.search, '', 'URL cleaned');
});

test('Etsy error return is surfaced, not treated as a code', async reg => {
  const app = createApp({ search: '?error=access_denied&error_description=User+denied' }); reg.push(app);
  assert.equal(app.api.bootOAuth(), 'error');
  assert.match(app.status(), /failed/i);
  assert.equal(app.navigations.length, 0);
});

test('Token-bearing redirect stores tokens and cleans the URL', async reg => {
  const t = { a: 'AT', r: 'RT', e: '3600', i: String(Math.floor(Date.now() / 1000)) };
  const app = createApp({ search: tokenRedirect(t) }); reg.push(app);
  assert.equal(app.api.bootOAuth(), 'tokens');
  assert.equal(app.api.getAccessToken(), 'AT');
  assert.equal(app.api.getRefreshToken(), 'RT');
  assert.ok(app.api.tokenIsFresh());
  assert.equal(app.location.search, '');
  assert.equal(app.status(), 'Connected');
});

test('Off-origin Connect bounces to the bound origin first', async reg => {
  // The verifier and the tokens live in ORIGIN's localStorage; starting the
  // handshake anywhere else strands them.
  const app = createApp({ origin: 'https://goldenspike.app' }); reg.push(app);
  await app.domReady();
  assert.match(app.status(), /sku\.goldenspike\.app/);
  await app.els.connectEtsyBtn.dispatch('click');
  assert.equal(app.navigations.at(-1), 'https://sku.goldenspike.app/?connect=1');
});

test('Landing back on the origin with ?connect=1 auto-starts the handshake', async reg => {
  const app = createApp({ search: '?connect=1' }); reg.push(app);
  await app.domReady();
  assert.ok(app.navigations.at(-1).startsWith('https://www.etsy.com/oauth/connect?'));
  assert.equal(app.location.search, '');
});

/* ── 2 · Token refresh ────────────────────────────────────────────────── */

section('Tokens · refresh lifecycle');

test('An expired access token is refreshed at boot before any listing read', async reg => {
  const seen = [];
  const app = createApp({
    storage: expiredTokens(),
    fetchImpl: async (url) => {
      seen.push(url);
      if (url.includes('refreshEtsyToken')) {
        return { body: { access_token: 'NEW', refresh_token: 'RT2', expires_in: 3600, issued_at: Math.floor(Date.now() / 1000) } };
      }
      return { body: { results: [listing(1111)] } };
    },
  });
  reg.push(app);
  await app.domReady();
  assert.equal(app.api.getAccessToken(), 'NEW');
  assert.ok(seen[0].includes('refreshEtsyToken'), 'refresh happened first');
  assert.ok(seen.some(u => u.includes('etsyShopListingsProxy')));
});

test('A 401 mid-session triggers one refresh and one retry', async reg => {
  let listingCalls = 0, refreshes = 0;
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url, init) => {
      if (url.includes('refreshEtsyToken')) {
        refreshes++;
        return { body: { access_token: 'NEW', refresh_token: 'RT2', expires_in: 3600, issued_at: Math.floor(Date.now() / 1000) } };
      }
      listingCalls++;
      if (listingCalls === 1) return { status: 401, body: { error: 'expired' } };
      assert.equal(init.headers.get('Access-Token'), 'NEW', 'retry carries the NEW token');
      return { body: { results: [listing(2222)] } };
    },
  });
  reg.push(app);
  await app.domReady();
  assert.equal(refreshes, 1);
  assert.equal(listingCalls, 2);
  assert.equal(app.cards().length, 1);
});

test('Concurrent refreshes collapse into one request', async reg => {
  let refreshes = 0;
  const app = createApp({
    storage: expiredTokens(),
    fetchImpl: async (url) => {
      if (url.includes('refreshEtsyToken')) {
        refreshes++;
        await new Promise(r => setTimeout(r, 10));
        return { body: { access_token: 'NEW', refresh_token: 'RT2', expires_in: 3600, issued_at: Math.floor(Date.now() / 1000) } };
      }
      return { body: { results: [] } };
    },
  });
  reg.push(app);
  await Promise.all([app.api.refreshAccessToken(), app.api.refreshAccessToken(), app.api.refreshAccessToken()]);
  assert.equal(refreshes, 1, 'Etsy rotates the refresh token — only one exchange may run');
});

test('A rejected refresh token is discarded so the UI asks for a reconnect', async reg => {
  const app = createApp({
    storage: expiredTokens(),
    fetchImpl: async () => ({ status: 400, body: { error: 'invalid_grant', error_description: 'refresh token is invalid' } }),
  });
  reg.push(app);
  const ok = await app.api.refreshAccessToken();
  assert.equal(ok, false);
  assert.equal(app.api.getAccessToken(), '');
  assert.equal(app.api.getRefreshToken(), '');
  assert.equal(app.status(), 'Not connected');
});

test('No tokens at all → prompt, and zero network traffic', async reg => {
  const app = createApp({ fetchImpl: async () => { throw new Error('must not fetch'); } }); reg.push(app);
  await app.domReady();
  assert.match(app.notice(), /connect Etsy first/i);
  assert.equal(app.fetchCalls.length, 0);
});

/* ── 3 · Listing load ─────────────────────────────────────────────────── */

section('Listings · load and render');

test('One bulk call supplies images and SKUs — no per-listing fan-out', async reg => {
  const rows = Array.from({ length: 25 }, (_, i) => listing(700000 + i));
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (!url.includes('etsyShopListingsProxy')) throw new Error('unexpected extra Etsy call: ' + url);
      return { body: { results: rows } };
    },
  });
  reg.push(app);
  await app.domReady();
  assert.equal(app.fetchCalls.length, 1, '1 call, not 1 + 2·N');
  const q = paramsOf(app.fetchCalls[0].url);
  assert.equal(q.get('includes'), 'Images,Inventory');
  assert.equal(q.get('limit'), '100');
  assert.equal(q.get('offset'), '0');
  assert.equal(q.get('state'), 'active');
  assert.equal(app.cards().length, 25);
});

test('Cards show title, lowest-rank image, divisor-correct price and SKU', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async () => ({ body: { results: [listing(881234)] } }),
  });
  reg.push(app);
  await app.domReady();
  const card = app.cards()[0];
  assert.equal(card.querySelector('.title').textContent, 'Gold Charm Necklace 881234');
  assert.equal(card.querySelector('.thumb').src, 'https://img/881234-a.jpg', 'rank 1 wins');
  assert.equal(card.querySelector('.sku').textContent, 'SKU: Gold_1234 (Variant 1)');
  const pills = card.querySelectorAll('.pill').map(p => p.textContent);
  assert.deepEqual(pills, ['#881234', '19.99 USD']);
});

test('Price honours a non-100 divisor and a missing price is omitted', async reg => {
  const app = createApp({ storage: freshTokens(), fetchImpl: async () => ({ body: { results: [] } }) });
  reg.push(app);
  assert.equal(app.api.formatPrice({ amount: 12345, divisor: 1000, currency_code: 'EUR' }), '12.35 EUR');
  assert.equal(app.api.formatPrice({ amount: 500, currency_code: 'USD' }), '5.00 USD');
  assert.equal(app.api.formatPrice(null), '');
  assert.equal(app.api.formatPrice({ amount: 'x' }), '');
});

test('Listings missing inline data fall back to the per-listing proxies', async reg => {
  const bare = listing(555, { images: [], inventory: { products: [] } });
  const calls = [];
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      calls.push(url);
      if (url.includes('etsyShopListingsProxy')) return { body: { results: [listing(111), bare] } };
      if (url.includes('etsyListingImagesProxy')) return { body: { results: [{ rank: 1, url_570xN: 'https://img/fallback.jpg' }] } };
      if (url.includes('etsyListingInventoryProxy')) return { body: { skus: ['Fallback_0555'] } };
      throw new Error('unexpected ' + url);
    },
  });
  reg.push(app);
  await app.domReady();
  assert.equal(calls.filter(u => u.includes('listingId=555')).length, 2, 'only the bare listing is refetched');
  assert.equal(calls.filter(u => u.includes('listingId=111')).length, 0);
  const cards = app.cards();
  assert.equal(cards[1].querySelector('.thumb').src, 'https://img/fallback.jpg');
  assert.equal(cards[1].querySelector('.sku').textContent, 'SKU: Fallback_0555 (Variant 1)');
});

test('One failing enrichment does not take the page down', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (url.includes('etsyShopListingsProxy')) {
        return { body: { results: [listing(1, { images: [], inventory: { products: [] } }), listing(2)] } };
      }
      throw new Error('network down');
    },
  });
  reg.push(app);
  await app.domReady();
  assert.equal(app.cards().length, 2, 'both cards still render');
  assert.equal(app.cards()[0].querySelector('.sku').textContent, 'SKU: —');
});

test('A proxy error renders the status and body instead of a blank grid', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async () => ({ status: 500, body: 'Missing SHOP_ID' }),
  });
  reg.push(app);
  await app.domReady();
  assert.match(app.els.listContainer.querySelector('.title').textContent, /Error loading listings: 500/);
  assert.match(app.els.listContainer.querySelector('.sku').textContent, /Missing SHOP_ID/);
});

test('Empty shop page says so', async reg => {
  const app = createApp({ storage: freshTokens(), fetchImpl: async () => ({ body: { results: [] } }) });
  reg.push(app);
  await app.domReady();
  assert.match(app.notice(), /No listings found/i);
});

test('A card with no SKU flags its Generate button', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (url.includes('etsyShopListingsProxy')) return { body: { results: [listing(9, { inventory: { products: [{ product_id: 90, sku: '' }] } })] } };
      if (url.includes('etsyListingImagesProxy')) return { body: { results: [] } };
      if (url.includes('etsyListingInventoryProxy')) return { body: { skus: [] } };
      throw new Error('unexpected ' + url);
    },
  });
  reg.push(app);
  await app.domReady();
  const gen = app.cards()[0].querySelector('[data-role="gen-btn"]');
  assert.equal(gen.style.background, 'OrangeRed');
});

/* ── 4 · Paging ───────────────────────────────────────────────────────── */

section('Paging');

test('Next / Prev / Go walk offsets and clamp at page 1', async reg => {
  const page = n => Array.from({ length: 100 }, (_, i) => listing(n * 1000 + i));
  const offsets = [];
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      offsets.push(paramsOf(url).get('offset'));
      return { body: { results: page(offsets.length) } };
    },
  });
  reg.push(app);
  await app.domReady();
  await app.els.nextBtn.dispatch('click');
  await app.els.nextBtn.dispatch('click');
  assert.deepEqual(offsets, ['0', '100', '200']);
  assert.equal(app.els.pageNum.textContent, '3');

  await app.els.prevBtn.dispatch('click');
  assert.equal(offsets.at(-1), '100');

  app.els.pageJump.value = '7';
  await app.els.goBtn.dispatch('click');
  assert.equal(offsets.at(-1), '600');
  assert.equal(app.els.pageNum.textContent, '7');

  app.els.pageJump.value = '0';
  await app.els.pageJump.dispatch('keydown', { key: 'Enter' });
  assert.equal(offsets.at(-1), '0', 'page 0 clamps to 1');
  assert.equal(app.els.prevBtn.disabled, true);
});

test('A short final page disables Next', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async () => ({ body: { results: [listing(1), listing(2)] } }),
  });
  reg.push(app);
  await app.domReady();
  assert.equal(app.els.nextBtn.disabled, true);
});

test('Page size change reloads from page 1 with the new limit', async reg => {
  const seen = [];
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => { seen.push(paramsOf(url)); return { body: { results: [] } }; },
  });
  reg.push(app);
  await app.domReady();
  app.els.pageSize.value = '25';
  await app.els.pageSize.dispatch('change');
  assert.equal(seen.at(-1).get('limit'), '25');
  assert.equal(seen.at(-1).get('offset'), '0');
});

test('Page size is clamped into Etsy\'s 1–100 window', async reg => {
  const seen = [];
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => { seen.push(paramsOf(url)); return { body: { results: [] } }; },
  });
  reg.push(app);
  await app.domReady();
  app.els.pageSize.value = '500';
  await app.els.pageSize.dispatch('change');
  assert.equal(seen.at(-1).get('limit'), '100');
  app.els.pageSize.value = '0';
  await app.els.pageSize.dispatch('change');
  assert.equal(seen.at(-1).get('limit'), '1');
});

test('Overlapping loads are ignored while one is in flight', async reg => {
  let n = 0;
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async () => { n++; await new Promise(r => setTimeout(r, 15)); return { body: { results: [] } }; },
  });
  reg.push(app);
  await app.domReady();
  await settle();
  await Promise.all([app.api.loadPage(2), app.api.loadPage(3), app.api.loadPage(4)]);
  assert.equal(n, 2, 'boot load + exactly one of three concurrent clicks');
});

/* ── 5 · Filters and selection ───────────────────────────────────────── */

section('Filters and selection');

const threeListings = () => ({
  body: {
    results: [
      listing(101, { title: 'Silver Hoop Earrings' }),
      listing(102, { title: 'Gold Charm Necklace' }),
      listing(103, { title: 'Gold Bar Pendant' }),
    ],
  },
});

test('Title search filters client-side', async reg => {
  const app = createApp({ storage: freshTokens(), fetchImpl: async () => threeListings() }); reg.push(app);
  await app.domReady();
  assert.equal(app.cards().length, 3);
  app.els.searchInput.value = 'gold';
  await app.els.searchInput.dispatch('input');
  assert.equal(app.cards().length, 2, 'case-insensitive substring match');
  app.els.searchInput.value = 'zzz';
  await app.els.searchInput.dispatch('input');
  assert.equal(app.cards().length, 0);
  assert.match(app.notice(), /match the current filters/);
});

test('Checkmarks persist and Filter Complete hides checked cards', async reg => {
  const app = createApp({ storage: freshTokens(), fetchImpl: async () => threeListings() }); reg.push(app);
  await app.domReady();
  const box = app.cards()[1].querySelector('.selectBox');
  box.checked = true;
  await box.dispatch('change');
  assert.ok(app.cards()[1].classList.contains('selected'));
  assert.deepEqual(JSON.parse(app.localStorage.getItem('sku_selected_ids_v1')), ['102']);

  await app.els.filterCompleteBtn.dispatch('click');
  assert.ok(app.els.filterCompleteBtn.classList.contains('active'));
  const ids = app.cards().map(c => c.querySelector('.pill').textContent);
  assert.deepEqual(ids, ['#101', '#103']);

  await app.els.filterCompleteBtn.dispatch('click');
  assert.equal(app.cards().length, 3);
});

test('REGRESSION: selections saved as numbers still show up as checked', async reg => {
  // Older builds stored raw numeric listing_ids; a Set of numbers never
  // matches the string ids a rebuilt card compares against.
  const app = createApp({
    storage: { ...freshTokens(), sku_selected_ids_v1: JSON.stringify([102]) },
    fetchImpl: async () => threeListings(),
  });
  reg.push(app);
  await app.domReady();
  assert.equal(app.cards()[1].querySelector('.selectBox').checked, true);
  assert.ok(app.cards()[1].classList.contains('selected'));
});

test('Unchecking removes the id from storage', async reg => {
  const app = createApp({
    storage: { ...freshTokens(), sku_selected_ids_v1: JSON.stringify(['102']) },
    fetchImpl: async () => threeListings(),
  });
  reg.push(app);
  await app.domReady();
  const box = app.cards()[1].querySelector('.selectBox');
  box.checked = false;
  await box.dispatch('change');
  assert.deepEqual(JSON.parse(app.localStorage.getItem('sku_selected_ids_v1')), []);
  assert.ok(!app.cards()[1].classList.contains('selected'));
});

test('Corrupt selection storage degrades to an empty set', async reg => {
  const app = createApp({ storage: { sku_selected_ids_v1: '{not json' } }); reg.push(app);
  assert.equal(app.api.loadSelectedSet().size, 0);
});

/* ── 6 · SKU generate + edit ─────────────────────────────────────────── */

section('SKU write paths');

test('Generate derives the SKU from the title and writes Variant 1', async reg => {
  let put = null;
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url, init) => {
      if (url.includes('etsyShopListingsProxy')) {
        return { body: { results: [listing(447788, { title: 'Ruby drop earrings', inventory: { products: [{ product_id: 5, sku: '' }] } })] } };
      }
      if (url.includes('etsyListingImagesProxy')) return { body: { results: [] } };
      if (url.includes('etsyListingInventoryProxy?')) return { body: { skus: [] } };
      if (url.includes('etsyListingInventoryDetailProxy')) return { body: { products: [{ product_id: 5, sku: '' }] } };
      if (url.includes('etsyUpdateListingInventoryProxy')) { put = JSON.parse(init.body); return { body: { ok: true } }; }
      throw new Error('unexpected ' + url);
    },
  });
  reg.push(app);
  await app.domReady();
  const card = app.cards()[0];
  const gen = card.querySelector('[data-role="gen-btn"]');
  await gen.dispatch('click');

  assert.deepEqual(put, { listing_id: 447788, items: [{ product_id: 5, sku: 'Ruby_7788' }] });
  assert.equal(card.querySelector('.sku').textContent, 'SKU: Ruby_7788 (Variant 1)');
  assert.equal(gen.style.background, '', 'warning colour cleared');
  assert.equal(gen.disabled, false);
  assert.equal(app.alerts.length, 0);
});

test('makeSkuFrom normalizes the first word and the id tail', async reg => {
  const app = createApp(); reg.push(app);
  const { makeSkuFrom } = app.api;
  assert.equal(makeSkuFrom('gold NECKLACE', 1234567890), 'Gold_7890');
  assert.equal(makeSkuFrom('  14k solid chain', 42), '14k_42');   // leading spaces skipped
  assert.equal(makeSkuFrom('14k solid chain', 987654), '14k_7654');
  assert.equal(makeSkuFrom('', 1111), 'Sku_1111');        // no word → literal fallback
  assert.equal(makeSkuFrom('!!! ???', 2222), 'Sku_2222');
});

test('Generate surfaces a write failure and re-enables the button', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (url.includes('etsyShopListingsProxy')) return { body: { results: [listing(600)] } };
      if (url.includes('etsyListingInventoryDetailProxy')) return { body: { products: [{ product_id: 7, sku: '' }] } };
      if (url.includes('etsyUpdateListingInventoryProxy')) return { status: 409, body: { code: 'STALE_INVENTORY' } };
      throw new Error('unexpected ' + url);
    },
  });
  reg.push(app);
  await app.domReady();
  const gen = app.cards()[0].querySelector('[data-role="gen-btn"]');
  await gen.dispatch('click');
  assert.match(app.alerts[0], /Generate failed.*409.*STALE_INVENTORY/s);
  assert.equal(gen.disabled, false);
});

test('Generate refuses a listing with no Variant 1', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (url.includes('etsyShopListingsProxy')) return { body: { results: [listing(601)] } };
      if (url.includes('etsyListingInventoryDetailProxy')) return { body: { products: [] } };
      throw new Error('must not write');
    },
  });
  reg.push(app);
  await app.domReady();
  await app.cards()[0].querySelector('[data-role="gen-btn"]').dispatch('click');
  assert.match(app.alerts[0], /Missing Variant 1/);
});

test('Edit SKUs opens, saves and updates the card', async reg => {
  let put = null;
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url, init) => {
      if (url.includes('etsyShopListingsProxy')) return { body: { results: [listing(330099)] } };
      if (url.includes('etsyListingInventoryDetailProxy')) return { body: { products: [{ product_id: 21, sku: 'Gold_0099' }] } };
      if (url.includes('etsyUpdateListingInventoryProxy')) { put = JSON.parse(init.body); return { body: { ok: true } }; }
      throw new Error('unexpected ' + url);
    },
  });
  reg.push(app);
  await app.domReady();
  const card = app.cards()[0];
  await card.querySelector('.btn.secondary').dispatch('click');   // Edit SKUs

  const editor = card.querySelector('[data-role="sku-editor"]');
  assert.equal(editor.style.display, 'block');
  const input = editor.querySelector('.skuInput');
  assert.equal(input.value, 'Gold_0099', 'prefilled from live inventory');

  input.value = '  Custom_ABC  ';
  await editor.querySelectorAll('button').find(b => b.textContent === 'Save').dispatch('click');

  assert.deepEqual(put, { listing_id: 330099, items: [{ product_id: 21, sku: 'Custom_ABC' }] });
  assert.equal(card.querySelector('.sku').textContent, 'SKU: Custom_ABC (Variant 1)');
  assert.equal(editor.style.display, 'none');
});

test('Editor Cancel closes without writing', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (url.includes('etsyShopListingsProxy')) return { body: { results: [listing(331)] } };
      if (url.includes('etsyListingInventoryDetailProxy')) return { body: { products: [{ product_id: 3, sku: 'A' }] } };
      throw new Error('must not write');
    },
  });
  reg.push(app);
  await app.domReady();
  const card = app.cards()[0];
  await card.querySelector('.btn.secondary').dispatch('click');
  const editor = card.querySelector('[data-role="sku-editor"]');
  await editor.querySelectorAll('button').find(b => b.textContent === 'Cancel').dispatch('click');
  assert.equal(editor.style.display, 'none');
});

test('Saving a SKU for a listing with no product_id is refused client-side', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (url.includes('etsyShopListingsProxy')) return { body: { results: [listing(332)] } };
      if (url.includes('etsyListingInventoryDetailProxy')) return { body: { products: [{ sku: 'A' }] } };
      throw new Error('must not write');
    },
  });
  reg.push(app);
  await app.domReady();
  const card = app.cards()[0];
  await card.querySelector('.btn.secondary').dispatch('click');
  const editor = card.querySelector('[data-role="sku-editor"]');
  await editor.querySelectorAll('button').find(b => b.textContent === 'Save').dispatch('click');
  assert.match(app.alerts[0], /no Variant 1/i);
});

test('Editor open failure is reported, not swallowed', async reg => {
  const app = createApp({
    storage: freshTokens(),
    fetchImpl: async (url) => {
      if (url.includes('etsyShopListingsProxy')) return { body: { results: [listing(333)] } };
      return { status: 404, body: { error: 'gone' } };
    },
  });
  reg.push(app);
  await app.domReady();
  await app.cards()[0].querySelector('.btn.secondary').dispatch('click');
  assert.match(app.alerts[0], /Could not open SKU editor.*404/s);
});

/* ── 7 · Helpers ─────────────────────────────────────────────────────── */

section('Helpers');

test('pickPrimaryImage prefers is_primary, then rank, then first', async reg => {
  const app = createApp(); reg.push(app);
  const { pickPrimaryImage } = app.api;
  assert.equal(pickPrimaryImage([{ rank: 1, url_fullxfull: 'a' }, { is_primary: true, url_fullxfull: 'b' }]), 'b');
  assert.equal(pickPrimaryImage([{ rank: 3, url_fullxfull: 'a' }, { rank: 1, url_fullxfull: 'b' }]), 'b');
  assert.equal(pickPrimaryImage([{ url_570xN: 'small' }]), 'small');
  assert.equal(pickPrimaryImage([]), '');
  assert.equal(pickPrimaryImage(null), '');
});

test('skusFromInventory drops blanks and whitespace', async reg => {
  const app = createApp(); reg.push(app);
  assert.deepEqual(app.api.skusFromInventory({ products: [{ sku: '  A  ' }, { sku: '' }, {}, { sku: 'B' }] }), ['A', 'B']);
  assert.deepEqual(app.api.skusFromInventory(null), []);
});

test('pLimit respects concurrency and never rejects', async reg => {
  const app = createApp(); reg.push(app);
  let live = 0, peak = 0;
  const out = await app.api.pLimit([1, 2, 3, 4, 5, 6, 7, 8], 3, async n => {
    live++; peak = Math.max(peak, live);
    await new Promise(r => setTimeout(r, 5));
    live--;
    if (n === 4) throw new Error('boom');
    return n;
  });
  assert.ok(peak <= 3, 'peak concurrency ' + peak);
  assert.equal(out.length, 8);
  assert.equal(out.filter(x => x === null).length, 1, 'the thrower resolved to null');
});

test('secondsUntilExpiry reads the stored issue time', async reg => {
  const now = Math.floor(Date.now() / 1000);
  const app = createApp({ storage: { access_token: 'A', expires_in: '3600', issued_at: String(now - 600) } });
  reg.push(app);
  const left = app.api.secondsUntilExpiry();
  assert.ok(left > 2990 && left <= 3000, 'left=' + left);
  assert.equal(app.api.tokenIsFresh(), true);
});

/* ── report ──────────────────────────────────────────────────────────── */

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
