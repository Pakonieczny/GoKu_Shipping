// Reads and bytes per ANONYMOUS request of the public storefront endpoints (Firebase cost emergency, FC18), measured with tests/cost/meter.cjs on an
// in-memory Firestore shaped like production, then projected to a crawler or bot that calls one endpoint once per second for an hour.
//
//   node tests/cost/fc18-public.cjs        prints the table; exits 1 when a budget or a behaviour check fails
//
// Endpoints: reviews.js (list, global, pending), productReviewsFeed.js (Google's review feed), skuChecked.js (the SKU console's ticks).
// No network, no secret, no paid call: the Shopify calls of reviews.js are never reached by the read paths.
'use strict';
const path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const meter = require('./meter.cjs');
const fn = f => path.join(root, 'netlify/functions', f);
const say = s => process.stdout.write(s + '\n');

const m = meter.create();
const db = m.db;

/* ── production-shaped fixtures (the structure is the code's, the sizes are an assumption, stated in the printout) ── */
const seed = {};
const lorem = 'The chain is beautiful and arrived quickly, exactly as pictured, and the clasp feels solid. I wear it every day and get compliments. '.repeat(2);
const HANDLES = []; for (let h = 0; h < 30; h++) HANDLES.push('charm-necklace-' + h);
HANDLES[0] = 'popular-charm-necklace';
let reviewTotal = 0, summaries = 0;
HANDLES.forEach((h, hi) => {
  const n = hi === 0 ? 120 : 30 + (hi * 7) % 60;                      // the popular product has 120 reviews, the rest 30 to 90
  let approved = 0;
  for (let i = 0; i < n; i++) {
    const d = { r: 3 + (i % 3), n: 'Buyer ' + hi + '-' + i, d: '2026-0' + (1 + (i % 9)) + '-1' + (i % 9), b: lorem.slice(0, 120 + (i * 13) % 200), v: i % 2, badge: i % 2 ? 'Verified Buyer' : 'Verified Reviewer', s: 'approved', imported: true, created: 1.7e12 };
    if (i % 5 === 0) d.pics = ['https://cdn.example.com/review-pics/' + hi + '/' + i + '-a.jpg', 'https://cdn.example.com/review-pics/' + hi + '/' + i + '-b.jpg'];
    if (i % 4 === 0) Object.assign(d, { email: 'buyer' + i + '@example.com', customerId: '71234567' + i, tier: 'buyer' });   // submitted on the site: private fields
    if (i % 17 === 16) d.s = 'pending';
    if (d.s === 'approved') approved++;
    seed['Brites_Reviews/' + h + '/items/r' + hi + '_' + i] = d; reviewTotal++;
  }
  seed['Brites_Reviews/' + h] = { count: approved, avg: 4.6, dist: { 1: 0, 2: 0, 3: 2, 4: 20, 5: approved - 22 }, updated: 1.7e12 }; summaries++;
});
for (let i = 0; i < 400; i++) seed['Brites_ProductIds/charm-necklace-' + i] = { sku: 'SKU' + i, gtin: '', title: 'A product title of average length ' + i, updatedAt: 1.7e12 };
for (let i = 0; i < 3000; i++) seed['sku_checked/' + (1000000000 + i)] = { checked: true, listing_id: 1000000000 + i, updated_at: 1.7e12 };
db.seed(seed);
m.install();
{ const Module = require('module'), load = Module._load;       // node-fetch is not installed in the offline test tree and the read paths never call it
  Module._load = function (req, ...r) { return req === 'node-fetch' ? async () => { throw new Error('fc18 test: no network'); } : load.call(this, req, ...r); }; }

const reviews = require(fn('reviews.js')), feed = require(fn('productReviewsFeed.js')), sku = require(fn('skuChecked.js'));
const ORIGIN = 'https://britesjewelry.com';
const get = (h, q, headers) => h.handler({ httpMethod: 'GET', headers: Object.assign({ origin: ORIGIN }, headers || {}), queryStringParameters: q || {}, body: null });
const rows = [];
async function one(name, perHourCalls, call, budget) {
  const a = m.snapshot();
  const res = await m.op(name, call);
  const d = m.since(a);
  rows.push({ name, reads: d.reads + d.aggs, bytes: d.bytes, perHourCalls });
  if (budget) meter.assertMax(d, budget, name);
  return res;
}
const body = r => JSON.parse(r.body);

(async () => {
  const L = 'reviews list popular (120 reviews)';
  // perHourCalls = calls that reach Firestore per hour for one caller at 1 request/s: before the change every call did (3600); after it only a miss does (once per TTL per instance)
  const first = await one(L + ' call 1', 30, () => get(reviews, { action: 'list', handle: 'popular-charm-necklace' }), { reads: 130, bytes: 40000 });
  const second = await one(L + ' call 2 (same instance, seconds later)', 0, () => get(reviews, { action: 'list', handle: 'popular-charm-necklace' }), { reads: 0, bytes: 0 });
  const rr = body(first);
  assert.strictEqual(first.statusCode, 200);
  assert.ok(rr.reviews.length > 100 && rr.summary.count > 100, 'the shopper still gets the reviews and the summary');
  assert.deepStrictEqual(body(second), rr, 'the second answer is identical to the first');
  for (const rv of rr.reviews) { assert.ok(!('email' in rv) && !('customerId' in rv)); }
  const byRating = body(await get(reviews, { action: 'list', handle: 'popular-charm-necklace', sort: 'rating' }));
  assert.ok(byRating.reviews[0].r >= byRating.reviews[byRating.reviews.length - 1].r, 'sort=rating still sorts');
  const lim = body(await get(reviews, { action: 'list', handle: 'popular-charm-necklace', limit: '10' }));
  assert.strictEqual(lim.reviews.length, 10, 'limit=10 still gives 10');
  await one('reviews list unknown handle (a bot)', 30, () => get(reviews, { action: 'list', handle: 'no-such-product-xyz' }));
  await one('reviews list unknown handle, 2nd call', 0, () => get(reviews, { action: 'list', handle: 'no-such-product-xyz' }), { reads: 0 });
  const bad = await get(reviews, { action: 'list', handle: 'a/b' });
  assert.ok(bad.statusCode === 400 || bad.statusCode === 200 || bad.statusCode === 500, 'a slash in a handle never reaches a document path as a crash of the process');
  // the owner approves a pending review: this instance serves it at once (the copy is forgotten), the summary is recomputed
  const pend = Object.keys(seed).find(k => /^Brites_Reviews\/popular-charm-necklace\/items\//.test(k) && seed[k].s === 'pending');
  assert.ok(pend, 'fixture has a pending review');
  const beforeN = body(await get(reviews, { action: 'list', handle: 'popular-charm-necklace' })).reviews.length;
  const mod = await reviews.handler({ httpMethod: 'POST', headers: { origin: ORIGIN }, queryStringParameters: { action: 'moderate' }, body: JSON.stringify({ action: 'moderate', handle: 'popular-charm-necklace', id: pend.split('/').pop(), decision: 'approve' }) });
  assert.strictEqual(mod.statusCode, 200, 'moderate answers');
  assert.strictEqual(body(await get(reviews, { action: 'list', handle: 'popular-charm-necklace' })).reviews.length, beforeN + 1, 'an approved review shows at once on the instance that approved it');
  for (const h of ['', 'a/b', '..', '__x__', 'x'.repeat(301)]) assert.strictEqual((await get(reviews, { action: 'list', handle: h })).statusCode, 400, 'refused before any read: ' + JSON.stringify(h.slice(0, 12)));
  const g1 = await one('reviews global call 1 (all reviews of all products)', 6, () => get(reviews, { action: 'global' }));
  await one('reviews global call 2 (cached)', 0, () => get(reviews, { action: 'global' }), { reads: 0 });
  const gb = body(g1);
  assert.ok(gb.summary.count > 1000 && gb.reviews.length === gb.summary.count && gb.reviews[0].h, 'global still lists every approved review with its handle');
  assert.ok(!gb.reviews.some(r => 'email' in r), 'global never carries an email');
  const f1 = await one('review feed XML call 1 (Google)', 6, () => get(feed, {}, { 'accept-encoding': 'gzip' }));
  await one('review feed XML call 2 (seconds later)', 0, () => get(feed, {}, { 'accept-encoding': 'gzip' }), { reads: 0 });
  assert.strictEqual(f1.statusCode, 200); assert.strictEqual(f1.headers['Content-Encoding'], 'gzip');
  const xml = require('zlib').gunzipSync(Buffer.from(f1.body, 'base64')).toString('utf8');
  assert.ok(xml.includes('<review_id>') && xml.includes('<reviews>'), 'feed still has reviews');
  const stats1 = body(await get(feed, { stats: '1' }));
  await one('review feed ?pretty=1 call 1 (a bot adds a parameter)', 0, () => get(feed, { pretty: '1' }), { reads: 0 });
  const p2 = await one('review feed ?pretty=1 call 2', 0, () => get(feed, { pretty: '1' }));
  assert.ok(p2.body.includes('\n  <review>'), 'pretty feed is still indented');
  assert.strictEqual(p2.body.replace(/\n\s*/g, '').replace(/ +$/g, ''), xml.replace(/\n\s*/g, ''), 'pretty and compact feeds carry the same reviews');
  const s1 = await one('sku console ticks GET (3,000 ticked listings)', 3600, () => get(sku, {}, { origin: 'https://sku.goldenspike.app' }), { bytes: 10000 });
  assert.strictEqual(body(s1).ids.length, 3000, 'all ticks still come back');
  assert.strictEqual(stats1.ok, true);

  say('');
  say('FC18 public endpoints: reads and bytes per call, and per hour for ONE caller at 1 request per second (list prices, USD, before free quota)');
  say(`fixture: ${reviewTotal} review documents in ${HANDLES.length} products (popular one 120), 400 product-id docs, 3,000 SKU ticks`);
  say('name'.padEnd(62) + 'reads'.padStart(8) + 'bytes'.padStart(11) + ' |  reads/h'.padStart(13) + 'MiB/h'.padStart(9) + 'USD/h'.padStart(9) + '   (per hour: calls that reach Firestore at 1 request/s)');
  for (const r of rows) {
    const h = meter.perHour({ reads: r.reads, aggs: 0, bytes: r.bytes, writes: 0, deletes: 0 }, r.perHourCalls);
    say(r.name.padEnd(62) + String(r.reads).padStart(8) + String(r.bytes).padStart(11) + ' | ' + String(Math.round(h.reads)).padStart(9) + (h.bytes / 1048576).toFixed(1).padStart(9) + h.usd.toFixed(3).padStart(9));
  }
  m.uninstall();
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
