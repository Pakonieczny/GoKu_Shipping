'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const storefront = require('../../netlify/functions/_britesStorefront');
const NOW = Date.parse('2026-10-07T05:00:00Z');
const HOME = '<div class="bj-promo-bar__inner"><span>Free shipping over $75</span><span>SHINE BRITE — code BRITES10 for 10% off</span></div>' +
  '<section class="bj-pop"><div data-bj-step="1"><p>Get 20% off your first order</p><form id="bjPopForm">Newsletter signup</form></div>' +
  '<div data-bj-step="2" hidden><p>Your code: <strong data-bj-code>BRITES20</strong></p></div></section>' +
  '<p>Materials sourced from suppliers in the US and Italy.</p>';
const SHIPPING = '<div class="shopify-policy__body"><p>Standard production takes 3–5 business days. Expedited production (2–3 business days) is available for a separate charge.</p>' +
  '<p>Free shipping: Free standard shipping on orders over $75.</p><p>Rates and delivery speed are calculated at checkout.</p></div>';
function reader({home = HOME, shipping = SHIPPING, failHome = false, failShipping = false} = {}) {
  const calls = [];let clock = NOW;
  const services = storefront.createStorefrontServices({now: () => clock, fetch: async (url, options) => {
    calls.push({url, options});const failed = url === storefront.HOME ? failHome : failShipping;
    return {ok: !failed, status: failed ? 503 : 200, headers: {get: name => name === 'content-type' ? 'text/html; charset=utf-8' : null}, text: async () => failed ? 'PRIVATE_SOURCE_ERROR' : url === storefront.HOME ? home : shipping};
  }});return {services, calls, advance: ms => clock += ms};
}
test('merchant production and source guidance remain attributed and do not imply arrival or independent certification', () => {
  const guidance = storefront.merchantGuidance();assert.equal(guidance.source.kind, 'merchant_statement');assert.equal(guidance.production.min, 2);assert.equal(guidance.production.max, 3);
  assert.equal(guidance.production.unit, 'days');assert.equal(guidance.production.shippingIncluded, false);assert.equal(guidance.production.needsConfirmation, true);
  assert.equal(guidance.sourcing.independentlyVerified, false);assert.equal(guidance.sourcing.needsConfirmation, true);assert.match(guidance.sourcing.summary, /studio says/);
  assert.equal(guidance.gifts.notes, true);assert.equal(guidance.gifts.wrapping, true);assert.equal(guidance.customization.requiresStudioReview, true);
  assert.match(guidance.customization.summary, /Compatibility.*cost and timing need studio review/);assert.equal(guidance.shipping.ratesAtCheckout, true);
});
test('current percent, disclosed code and newsletter requirements remain precisely bound', () => {
  const offers = storefront.parsePublishedOffers(HOME, NOW), main = offers.items.find(item => item.code === 'BRITES10'), newsletter = offers.items.find(item => item.code === 'BRITES20');
  assert.equal(main.percent, 10);assert.equal(main.firstOrderRequired, null);assert.equal(newsletter.percent, 20);assert.equal(newsletter.firstOrderRequired, true);
  assert.equal(newsletter.newsletterSignupRequired, true);assert.match(newsletter.summary, /shown after signup/);
  for (const item of offers.items) {assert.equal(item.checkoutValidated, false);assert.equal(item.expiresAt, null);assert.equal(item.stacking, null);assert.equal(item.currency, null);assert.equal(item.eligibility, 'confirm_at_checkout');assert.equal(item.source.checkedAt, NOW);}
});
test('a free shipping threshold is a published condition without an invented currency or destination', () => {
  const item = storefront.parsePublishedOffers(HOME, NOW).items.find(item => item.kind === 'shipping');
  assert.equal(item.thresholdDisplay, '$75');assert.equal(item.comparison, 'over');assert.equal(item.currency, null);assert.equal(item.minimumSpend, null);
  assert.match(item.summary, /Checkout confirms currency, service and destination/);
});
test('scripts, templates, comments and author instructions cannot provide shopper coupon knowledge', () => {
  const html = '<script>code PRIVATECODE for 99% off</script><template>code TEMPLATECODE for 90% off</template><!--code HIDDENCODE for 90% off-->' +
    '<p>Internal instructions: code BADCODE for 90% off</p><p>code PUBLIC10 for 10% off</p>';
  const offers = storefront.parsePublishedOffers(html, NOW);assert.deepEqual(offers.items.map(item => item.code), ['PUBLIC10']);
  assert.doesNotMatch(JSON.stringify(offers), /PRIVATECODE|TEMPLATECODE|HIDDENCODE|BADCODE|Internal instructions/);
});
test('a code cannot borrow a percent from a different distant offer or an expired claim', () => {
  const html = '<p>Get 20% off your first order</p>' + Array.from({length: 8}, () => '<div>Another unrelated section</div>').join('') + '<p>Your code: UNBOUND20</p>' +
    '<p>Expired code OLD10 for 10% off</p><p>Code JUSTCODE</p>';
  const offers = storefront.parsePublishedOffers(html, NOW);assert.deepEqual(offers.items, []);
});
test('a following expiry status cannot turn an old code into a published current offer', () => {
  const offers = storefront.parsePublishedOffers('<div>code OLD20 for 20% off</div><p>This offer has expired.</p>', NOW);
  assert.deepEqual(offers.items, []);
});
test('unrelated money amounts and zero/oversized percentage statements are not offers', () => {
  const offers = storefront.parsePublishedOffers('<p>Gift $75</p><p>code ZERO for 0% off</p><p>code EXCESS for 101% off</p>', NOW);
  assert.deepEqual(offers.items, []);
});
test('live services disclose conflicting production and sourcing without silently reconciling them', async () => {
  const f = reader(), result = await f.services.read();assert.equal(result.offers.status, 'published_not_checkout_validated');assert.equal(result.offers.items.length, 3);
  assert.equal(result.publishedProduction.standard.min, 3);assert.equal(result.publishedProduction.standard.max, 5);assert.equal(result.publishedProduction.standard.unit, 'business');
  assert.equal(result.publishedProduction.expedited.min, 2);assert.equal(result.publishedProduction.expedited.max, 3);
  assert.deepEqual(result.conflicts.map(item => item.topic), ['production', 'sourcing']);assert.ok(result.conflicts.every(item => item.needsConfirmation));
  assert.match(result.conflicts[0].merchantSummary, /2–3 days/);assert.match(result.conflicts[0].publishedSummary, /3–5 business days/);
  assert.match(result.conflicts[1].publishedSummary, /United States and Italy/);assert.match(result.conflicts[1].publishedSummary, /selected piece/);
  assert.equal(f.calls.length, 2);assert.ok(f.calls.every(item => item.url.startsWith('https://britesjewelry.com/') && item.options.redirect === 'error'));
});
test('an unavailable offers source preserves merchant help and emits no guessed code', async () => {
  const f = reader({failHome: true, failShipping: true}), result = await f.services.read();assert.equal(result.guidance.gifts.wrapping, true);
  assert.equal(result.offers.status, 'unavailable');assert.deepEqual(result.offers.items, []);assert.equal(result.offers.checkedAt, null);assert.deepEqual(result.conflicts, []);
  assert.doesNotMatch(JSON.stringify(result), /PRIVATE_SOURCE_ERROR|BRITES10|BRITES20/);
});
test('shipping evidence can supply a sourced threshold when homepage is unavailable but supplies no code', async () => {
  const result = await reader({failHome: true}).services.read();assert.equal(result.offers.items.length, 1);
  assert.equal(result.offers.items[0].kind, 'shipping');assert.equal(result.offers.items[0].source.url, 'https://britesjewelry.com/policies/shipping-policy');
  assert.equal(result.offers.items.some(item => item.code), false);
});
test('no observed offer is distinct from failed verification, and oversized sources fail closed', async () => {
  const noOffer = await reader({home: '<p>Welcome to Brites</p>', shipping: '<div class="shopify-policy__body"><p>Shipping options vary by destination.</p></div>'}).services.read();
  assert.equal(noOffer.offers.status, 'none_observed');assert.deepEqual(noOffer.offers.items, []);
  const large = await reader({home: 'a'.repeat(storefront.MAX_HTML_BYTES + 1), failShipping: true}).services.read();assert.equal(large.offers.status, 'unavailable');assert.deepEqual(large.offers.items, []);
});
test('concurrent readers share a bounded live check and reread rather than replaying stale offers', async () => {
  const f = reader(), [a, b] = await Promise.all([f.services.read(), f.services.read()]);assert.equal(a, b);assert.equal(f.calls.length, 2);
  await f.services.read();assert.equal(f.calls.length, 2);f.advance(60001);const fresh = await f.services.read();assert.equal(f.calls.length, 4);assert.equal(fresh.checkedAt, NOW + 60001);
});
test('a failed refresh after the TTL cannot replay earlier codes or stale source conflicts', async () => {
  let clock = NOW, fails = false;
  const services = storefront.createStorefrontServices({now: () => clock, fetch: async url => ({ok: !fails, status: fails ? 503 : 200,
    headers: {get: name => name === 'content-type' ? 'text/html' : null}, text: async () => url === storefront.HOME ? HOME : SHIPPING})});
  const first = await services.read();assert.equal(first.offers.items.length, 3);assert.equal(first.conflicts.length, 2);
  clock += 60001;fails = true;const next = await services.read();assert.equal(next.offers.status, 'unavailable');assert.deepEqual(next.offers.items, []);assert.deepEqual(next.conflicts, []);
  assert.equal(next.guidance.production.needsConfirmation, true);assert.equal(next.guidance.sourcing.needsConfirmation, true);
});
test('public services endpoint is anonymous read-only and rate limited', async () => {
  const core = require('../../netlify/functions/_britesGrowth'), source = fs.readFileSync(path.join(__dirname, '../../netlify/functions/britesGrowthApi.js'), 'utf8')
    .replace(/^import\s+\w+\s+from\s+['"][^'"]+['"];\s*$/gm, '')
    .replace('export default async (req,context) => {', 'return async (req,context) => {').replace(/export const config = [\s\S]*$/, '');
  let reads = 0, writes = 0, allowed = true;
  const injected = {...core, makeDb: () => ({}), createShopify: () => ({}), createGrowthService: () => ({rateLimit: async () => allowed}),
    readStorefrontServices: async () => {reads++;return {schema: 1, guidance: storefront.merchantGuidance(), offers: {status: 'unavailable', items: []}};}};
  const handler = new Function('core', 'demandStore', 'controllerStore', 'receiptSandboxCheck', 'etsyCacheReadOnly', 'historicalLookup', 'conciergeDiagnostics', 'Netlify', source)
    (injected, {}, {}, {}, {}, {}, {}, {env: {get: () => undefined}});
  const request = () => handler(new Request('https://brites-growth-sandbox.netlify.app/api/growth/storefront-services'), {params: {op: 'storefront-services'}, ip: 'synthetic'});
  assert.equal((await request()).status, 200);assert.equal(reads, 1);
  assert.equal((await handler(new Request('https://brites-growth-sandbox.netlify.app/api/growth/storefront-services', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({code: 'UNVERIFIED90'})}), {params: {op: 'storefront-services'}, ip: 'synthetic'})).status, 405);
  assert.equal(reads, 1);allowed = false;assert.equal((await request()).status, 429);assert.equal(reads, 1);assert.equal(writes, 0);
});
