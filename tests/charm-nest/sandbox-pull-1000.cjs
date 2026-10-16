// SBX1000 (Paul, 16 Oct 2026: "For the purpose of the sandbox test you can consider shipped orders as open. I need to be able to pull/test up-to 1000 of the
// most recent orders from etsy."). The SERVER half of 1000 orders, offline: an in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs), a FAKE Etsy
// (every request recorded, realistic 4-8 KB receipts with transactions) and the REAL charmNestLibrary, _charmNestSandboxPull and etsySandbox code. No network, no live record.
// It proves:
//   · 1000 receipts of 4 to 8 KB each are fetched (10 pages of 100, STARTED 110 ms apart), deduplicated, sorted newest first, stored as ONE file (about 6 MB) and answered, and how long that takes
//     on a fake Etsy that answers each page in 400 ms: printed, and held well inside the pull's clock (WORK_MS) and the function's 10 s;
//   · a receipt that came into the first page while the others were read (every later page shifted by one) shows once;
//   · SHIPPED RECEIPTS PLAY AS OPEN: every receipt that is paid and not cancelled plays, cancelled and unpaid ones are stored but left out and counted (closedBy), and a shipped receipt
//     reaches the order list the Design Station reads (listOpenOrders, with the stream off and with it playing, and read alone) as a normal open order: is_shipped / was_shipped false, status
//     "Paid", no shipments or tracking, no shipped time on its lines, with its receipt number, buyer, transactions and dates kept. The page's own "gone" rule (design-1.html orders.detail) reads
//     it as not gone, where the receipt as Etsy sent it is gone. The STORED file stays exactly as Etsy returned it;
//   · the emulated Etsy serves the 1000-order set: the list pages (100 each, under 1 MB), the stream's total and its pool, every order once, the status;
//   · a 429 with a retry-after of 2 s or less is asked ONCE more after that wait (and counted as a call); a longer one, a second 429, or no time left on the clock fails as before, with the
//     sandbox EMPTY and "Try again in a minute"; a daily limit is never retried;
//   · a slow Etsy: 1000 orders where one page does not answer inside the clock end as "Etsy did not answer (timed out … on page N of 10) … Press Start to try again": the sandbox EMPTY (no pointer, no
//     file, no stream), every call counted, never a half set; a slow but working Etsy still brings all 1000.
//   node tests/charm-nest/sandbox-pull-1000.cjs
'use strict';
const path = require('path'), fs = require('fs'), assert = require('assert'), Module = require('module');
const fnDir = path.join(__dirname, '../../netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const { store, blobs } = Fk;

/* ── a fake Etsy: 1100 receipts of 4-8 KB (1-3 transactions each, with listing text, variations, prices), newest = highest number ── */
const DAY = 86400, NOW = Math.floor(Date.now() / 1000), T0 = NOW - 1200 * 3600, N = 1100;
const LOREM = 'Handmade personalized initial charm, stamped and polished in our studio from solid 14k gold filled or sterling silver. Choose a letter, a birthstone or a zodiac sign, then a cable chain. ';
const SIZES = [];
const tx = (rid, i, j, created, shipped) => {
  const text = LOREM.repeat(1 + ((i + j) % 6)), listing = 1718000 + ((i + j) % 40);
  return { transaction_id: rid * 10 + j, title: `Personalized Initial Charm Necklace - ${['14k Gold Filled', 'Sterling Silver', 'Rose Gold'][(i + j) % 3]} Letter Pendant ${listing}`, description: text, seller_user_id: 111, buyer_user_id: 7000000 + i, create_timestamp: created, created_timestamp: created,
    paid_timestamp: created, shipped_timestamp: shipped ? created + 3 * DAY : null, quantity: 1 + (j % 2), listing_image_id: 5000000 + listing, receipt_id: rid, is_digital: false, listing_id: listing, sku: `BR-TST-0${1 + ((i + j) % 6)}`, product_id: 9000000 + listing, transaction_type: 'listing',
    price: { amount: 2495 + 100 * (j % 3), divisor: 100, currency_code: 'USD' }, shipping_cost: { amount: 0, divisor: 100, currency_code: 'USD' }, min_processing_days: 1, max_processing_days: 3, shipping_profile_id: 123456, shipping_method: 'USPS First Class', expected_ship_date: created + (2 + (i % 6)) * DAY,
    variations: [{ property_id: 513, value_id: 1 + j, formatted_name: 'Metal', formatted_value: ['14k Gold Filled', 'Sterling Silver', 'Rose Gold'][(i + j) % 3] }, { property_id: 54, value_id: 2, formatted_name: 'Personalization', formatted_value: ['ANNA', 'M', 'Leo', 'E & J', 'Sofia'][(i + j) % 5] }, { property_id: 200, value_id: 3, formatted_name: 'Chain length', formatted_value: '18 inches' }],
    product_data: [{ property_id: 513, property_name: 'Metal', scale_id: null, scale_name: null, value_ids: [1 + j], values: ['x'] }] };
};
function receipt(i) {
  const rid = 3800000000 + i, created = T0 + i * 3600, status = i % 11 === 0 ? 'cancel' : i % 29 === 0 ? 'unpaid' : i % 3 === 0 ? 'shipped' : 'open';
  const shipped = status === 'shipped', n = 1 + (i % 3);
  const r = { receipt_id: rid, receipt_type: 0, seller_user_id: 111, seller_email: 'shop@example.com', buyer_user_id: 7000000 + i, buyer_email: `buyer${i}@example.com`, name: 'Buyer ' + rid, first_line: `${100 + i} Maple Street`, second_line: i % 4 ? '' : 'Apt 2', city: 'Austin', state: 'TX', zip: '78701', formatted_address: `Buyer ${rid}\n${100 + i} Maple Street\nAustin, TX 78701\nUnited States`, country_iso: 'US',
    payment_method: 'cc', payment_email: '', message_from_seller: '', message_from_buyer: i % 5 ? '' : 'Please make the letter a little smaller. Thank you so much, it is a gift for my sister!', message_from_payment: '', is_paid: status !== 'unpaid', is_shipped: shipped,
    create_timestamp: created, created_timestamp: created, update_timestamp: created + 120, updated_timestamp: created + 120, is_gift: i % 7 === 0, gift_message: i % 7 ? '' : 'Happy birthday!', gift_sender: '', grandtotal: { amount: 2995, divisor: 100, currency_code: 'USD' }, subtotal: { amount: 2495, divisor: 100, currency_code: 'USD' },
    total_price: { amount: 2495, divisor: 100, currency_code: 'USD' }, total_shipping_cost: { amount: 500, divisor: 100, currency_code: 'USD' }, total_tax_cost: { amount: 0, divisor: 100, currency_code: 'USD' }, total_vat_cost: { amount: 0, divisor: 100, currency_code: 'USD' }, discount_amt: { amount: 0, divisor: 100, currency_code: 'USD' },
    gift_wrap_price: { amount: 0, divisor: 100, currency_code: 'USD' }, status: status === 'cancel' ? 'Canceled' : shipped ? 'Completed' : status === 'unpaid' ? 'Payment Processing' : 'Paid',
    shipments: shipped ? [{ receipt_shipping_id: 8100000 + i, shipment_notification_timestamp: created + 3 * DAY, carrier_name: 'usps', tracking_code: '9400111899' + rid }] : [],
    transactions: Array.from({ length: n }, (_, j) => tx(rid, i, j, created, shipped)) };
  if (status === 'cancel') r.is_canceled = true;
  // a seller's note fills each receipt out to 4-8 KB (4096 + a spread up to 3.8 KB), as real receipts with their listing text run
  const target = 4096 + (i * 997) % 3800, base = Buffer.byteLength(JSON.stringify(r));
  if (base < target) r.message_from_seller = 'Thank you for your order, it will be packed with care. '.repeat(Math.ceil((target - base) / 53)).slice(0, target - base);
  SIZES.push(Buffer.byteLength(JSON.stringify(r)));
  return r;
}
const SHOP = Array.from({ length: N }, (_, i) => receipt(i)).reverse();   // newest first, as Etsy lists them
assert(SIZES.every(x => x >= 4096 && x <= 8192), 'every fake receipt is 4-8 KB: ' + Math.min(...SIZES) + '..' + Math.max(...SIZES));
const sortedIds = list => list.slice().sort((a, b) => b.create_timestamp - a.create_timestamp || b.receipt_id - a.receipt_id);
const NEWEST = n => sortedIds(SHOP).slice(0, n);
const playsNow = r => r.is_paid !== false && !r.is_canceled && !/cancel/i.test(r.status);

const etsy = { calls: [], oauth: 0, delay: 0, delayAt: {}, answer429: {}, shift: 0, t0: 0 };
const res = (status, body, headers = {}) => ({ ok: status >= 200 && status < 300, status, headers: { get: k => headers[String(k).toLowerCase()] }, json: async () => body, text: async () => (typeof body === 'string' ? body : JSON.stringify(body)) });
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function fakeFetch(url, init = {}) {
  const u = new URL(url);
  if (u.pathname === '/v3/public/oauth/token') { etsy.oauth++; return res(200, { access_token: 'tok-refreshed', refresh_token: 'ref-2', expires_in: 3600 }); }
  const offset = +u.searchParams.get('offset'), limit = +u.searchParams.get('limit'), call = { path: u.pathname, offset, limit, at: Date.now(), n: 0 };
  call.n = etsy.calls.filter(c => c.offset === offset).length + 1; etsy.calls.push(call);
  const abortable = ms => new Promise((ok, no) => { const t = setTimeout(ok, ms), sig = init.signal; if (sig) sig.addEventListener('abort', () => { clearTimeout(t); no(Object.assign(new Error('The operation was aborted'), { name: 'AbortError' })); }); });
  const wait = etsy.delayAt[offset] != null ? etsy.delayAt[offset] : etsy.delay; if (wait) await abortable(wait);
  const rule = etsy.answer429[offset]; if (rule && call.n <= rule.times) { call.answered = Date.now(); call.status = 429; return res(429, rule.body || 'Too many requests', rule.retryAfter != null ? { 'retry-after': String(rule.retryAfter) } : {}); }
  const sorted = sortedIds(SHOP), from = offset ? Math.max(0, offset - etsy.shift) : 0;
  call.answered = Date.now(); call.status = 200;
  return res(200, { count: SHOP.length, results: JSON.parse(JSON.stringify(sorted.slice(from, from + limit))) });
}
const meterToken = { failNet() {}, fromHttp() {}, ok() {} };
const fakeMeter = { bump: () => meterToken, bumpSimple() {}, flushNow: async () => {} };
const realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return fakeFetch;
  if (/[\/]_etsyApiMeter(\.js)?$/.test(req)) return fakeMeter;
  return realLoad.call(this, req, ...rest);
};
const warnings = [], realWarn = console.warn; console.warn = (...a) => { warnings.push(a.map(String).join(' ')); };
const lib = require(path.join(fnDir, 'charmNestLibrary.js')), SP = require(path.join(fnDir, '_charmNestSandboxPull.js')), emulator = require(path.join(fnDir, 'etsySandbox.js')), Orders = require(path.join(fnDir, '_charmNestSandboxOrders.js'));
const post = b => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const repoStamp = require('./_repoToken.cjs');
const pullOp = async (startId, extra) => post(await repoStamp('charmNestLibrary', Object.assign({ op: 'sandboxPullOrders', sandbox: true, startId, by: 'tester' }, extra || {})));
const emu = async q => { const t = Date.now(), r = await emulator.handler({ httpMethod: 'GET', queryStringParameters: q }); return { status: r.statusCode, body: JSON.parse(r.body), bytes: Buffer.byteLength(r.body), ms: Date.now() - t }; };
const direct = (id, workMs) => SP.pull({ startId: id, by: 'tester' }, { stream: async () => ({ stream: null }) }, { workMs });
const CFG = 'config/charmNestSandboxPull';
const todayKey = () => new Date().toISOString().slice(0, 10);
const reset = () => { etsy.calls.length = 0; etsy.oauth = 0; etsy.delay = 0; etsy.delayAt = {}; etsy.answer429 = {}; etsy.shift = 0; store.set('Charm_Sandbox/pulls', { day: todayKey(), pulls: 0, calls: 0, tokenRefreshes: 0, starts: [], claim: null }); store.set('config/etsyOauth', { access_token: 'tok-fresh', refresh_token: 'ref-1', expires_at_ms: Date.now() + 3600e3 }); };
const empty = () => !store.has('Charm_Sandbox/current') && !store.has('Charm_Sandbox/stream') && ![...blobs.keys()].some(k => k.startsWith('charmnest/sandbox/orders-pull/'));
const stored = path => JSON.parse(blobs.get(path).buf.toString('utf8')).receipts;
const timings = {};

(async () => {
  store.set(CFG, { v: 1, limit: 1000, by: 'Paul', at: Date.now() });
  const WANT = NEWEST(1000), WANT_PLAY = WANT.filter(playsNow), WANT_SHIPPED = WANT_PLAY.filter(r => r.is_shipped);
  const wantCanceled = WANT.filter(r => r.is_canceled || /cancel/i.test(r.status)).length, wantUnpaid = WANT.filter(r => r.is_paid === false && !/cancel/i.test(r.status)).length;
  assert(WANT_SHIPPED.length > 200 && wantCanceled > 50 && wantUnpaid > 10, 'the fake shop has shipped, cancelled and unpaid receipts: ' + [WANT_SHIPPED.length, wantCanceled, wantUnpaid]);

  /* ═══ 1 · 1000 receipts of 4-8 KB: fetched, deduplicated, sorted, stored, answered ═══ */
  reset(); etsy.delay = 400;   // (every page takes 400 ms to answer; ten of them in flight together)
  let t = Date.now(), r = await pullOp('start-1000-0001');
  const total = Date.now() - t, a = r.body;
  assert(r.status === 200 && a.ok && a.count === 1000 && a.want === 1000 && a.calls === 10 && a.source === 'etsy-pull' && a.label === '1000 newest Etsy orders', '1000 orders, ten calls: ' + JSON.stringify(a).slice(0, 300));
  const asked = etsy.calls.slice().sort((x, y) => x.offset - y.offset);
  assert.deepStrictEqual(asked.map(c => [c.offset, c.limit]), Array.from({ length: 10 }, (_, i) => [i * 100, 100]), 'ten pages of 100');
  assert(asked.every((c, i) => c.at - asked[0].at >= i * SP.STAGGER_MS - 10), 'page k starts k x ' + SP.STAGGER_MS + ' ms after the first (timers may run late under load, never early): ' + asked.map(c => c.at - asked[0].at).join(','));
  const lastAnswer = Math.max(...etsy.calls.map(c => c.answered)), etsyPart = lastAnswer - etsy.calls[0].at, afterEtsy = Date.now() - lastAnswer;
  const file = stored(a.path), bytes = blobs.get(a.path).buf.length;
  assert.deepStrictEqual(file.map(x => x.receipt_id), WANT.map(x => x.receipt_id), 'the 1000 newest, once each, newest first');
  assert.deepStrictEqual(file[5], WANT[5], 'stored exactly as Etsy returned it');
  assert(bytes > 4e6 && bytes < 9e6 && store.get('Charm_Sandbox/current').bytes === bytes, 'one file of about 6 MB: ' + (bytes / 1e6).toFixed(2) + ' MB');
  assert(a.ms === undefined || a.ms < SP.WORK_MS, 'the answer says how long the pull took, inside the clock: ' + a.ms);
  assert(total < SP.WORK_MS / 2 && etsyPart < 400 + 10 * SP.STAGGER_MS + 600, `1000 orders pulled in ${total} ms (Etsy part ${etsyPart} ms with 400 ms per page, the rest ${afterEtsy} ms), well inside the ${SP.WORK_MS} ms clock and the 10 s limit`);
  Object.assign(timings, { pullMs: total, etsyMs: etsyPart, afterEtsyMs: afterEtsy, fileMB: +(bytes / 1e6).toFixed(2), perReceiptKB: +(bytes / 1000 / 1024).toFixed(1) });
  assert.strictEqual(a.open, WANT_PLAY.length, 'what plays: paid and not cancelled, shipped ones too'); assert.strictEqual(a.shipped, WANT_SHIPPED.length);
  assert.deepStrictEqual(a.closedBy, { canceled: wantCanceled, unpaid: wantUnpaid }, 'cancelled and unpaid are left out and counted'); assert.strictEqual(a.closed, wantCanceled + wantUnpaid); assert.strictEqual(a.open + a.closed, 1000);
  assert(a.stream && a.stream.total === WANT_PLAY.length, 'the stream will bring every one that plays: ' + JSON.stringify(a.stream));
  console.log('1 ok · 1000 receipts (' + (bytes / 1e6).toFixed(1) + ' MB):', JSON.stringify(timings));

  /* ═══ 2 · a receipt that came into the first page while the others were read shows once ═══ */
  reset(); etsy.shift = 1;
  r = await pullOp('start-1000-0002'); assert(r.status === 200 && r.body.calls === 10, 'ten calls');
  { const f = stored(r.body.path), ids = f.map(x => String(x.receipt_id)); assert.strictEqual(new Set(ids).size, ids.length, 'no receipt twice'); assert.strictEqual(ids.length, 999, 'one fewer distinct receipt, because every later page moved up by one');
    assert.deepStrictEqual(f.map(x => x.receipt_id), sortedIds(f).map(x => x.receipt_id), 'and still newest first'); }
  etsy.shift = 0;

  /* ═══ 3 · shipped receipts play as open: the emulated Etsy, stream off ═══ */
  reset(); r = await pullOp('start-1000-0003', { stream: false });
  assert(r.status === 200 && r.body.count === 1000 && r.body.stream === null, 'a pull without a stream');
  const cur = store.get('Charm_Sandbox/current'), storedFile = stored(cur.path);
  assert.deepStrictEqual(storedFile.map(x => x.receipt_id), WANT.map(x => x.receipt_id)); assert(storedFile.filter(x => x.is_shipped).length === WANT_SHIPPED.length && storedFile.filter(x => x.status === 'Completed').length === WANT_SHIPPED.length, 'the stored file still holds the shipped receipts as Etsy returned them');
  let listed = [], pages = [], tList = Date.now();
  for (let off = 0; ; off += 100) { const p = await emu({ fn: 'listOpenOrders', offset: off }); assert.strictEqual(p.status, 200); pages.push(p); listed.push(...p.body.results); if (p.body.results.length < 100) break; }
  timings.listSweepMs = Date.now() - tList;
  assert.strictEqual(listed.length, WANT_PLAY.length, 'the list holds every receipt that plays: ' + listed.length); assert.strictEqual(new Set(listed.map(x => x.receipt_id)).size, listed.length, 'each once');
  assert.strictEqual(pages.length, Math.floor(WANT_PLAY.length / 100) + 1, 'in pages of 100, the last one short'); assert(pages.slice(0, -1).every(p => p.body.results.length === 100 && p.bytes < 1.2e6), 'each full page is 100 receipts and under 1.2 MB (' + Math.max(...pages.map(p => p.bytes)) + ' bytes)');
  assert.strictEqual(pages[0].body.count, WANT_PLAY.length, 'and says how many'); assert(!listed.some(x => x.is_canceled || /cancel/i.test(x.status) || x.is_paid === false), 'cancelled and unpaid receipts are never listed');
  const byId = new Map(WANT.map(x => [String(x.receipt_id), x]));
  assert(listed.every(x => x.is_shipped === false && x.status === 'Paid' && !(x.shipments || []).length && x.transactions.every(tt => !tt.shipped_timestamp)), 'every listed order reads open: not shipped, status Paid, no shipments, no shipped time');
  const served = listed.filter(x => byId.get(String(x.receipt_id)).is_shipped);
  assert.strictEqual(served.length, WANT_SHIPPED.length, 'the shipped receipts are all there'); assert(served.every(x => x.sandbox_shipped_on_etsy === true));
  for (const x of served.slice(0, 40)) {   // its number, buyer, lines and dates are kept
    const was = byId.get(String(x.receipt_id));
    assert(x.receipt_id === was.receipt_id && x.name === was.name && x.buyer_email === was.buyer_email && x.city === was.city && x.create_timestamp === was.create_timestamp && x.update_timestamp === was.update_timestamp && x.transactions.length === was.transactions.length
      && x.transactions.every((tt, j) => tt.transaction_id === was.transactions[j].transaction_id && tt.sku === was.transactions[j].sku && tt.expected_ship_date === was.transactions[j].expected_ship_date && tt.create_timestamp === was.transactions[j].create_timestamp && JSON.stringify(tt.variations) === JSON.stringify(was.transactions[j].variations)), 'a shipped receipt keeps its number, buyer, lines and dates: ' + x.receipt_id);
  }
  assert(listed.filter(x => !byId.get(String(x.receipt_id)).is_shipped).every(x => !('sandbox_shipped_on_etsy' in x)), 'an order that was open is served untouched');
  // read alone: open too; a cancelled one is read as cancelled (the page settles it as gone), an unpaid one as unpaid
  { const sid = String(served[0].receipt_id), one = await emu({ fn: 'etsyOrderProxy', orderId: sid }); assert(one.status === 200 && one.body.receipt.is_shipped === false && one.body.receipt.status === 'Paid' && !(one.body.receipt.shipments || []).length && one.body.transactions.length === served[0].transactions.length && one.body.transactions.every(tt => !tt.shipped_timestamp), 'a shipped receipt read alone reads open: ' + JSON.stringify(one.body.receipt).slice(0, 200));
    const cid = String(WANT.find(x => x.is_canceled).receipt_id), c = await emu({ fn: 'etsyOrderProxy', orderId: cid }); assert(c.status === 200 && c.body.receipt.is_canceled === true, 'a cancelled receipt is read as cancelled'); }
  // the page's own rule (design-1.html, orders.detail): read as shipped or cancelled or unpaid = gone. It must still say what this test copies, and then it is applied to what is served
  { const html = fs.readFileSync(path.join(__dirname, '../../design-1.html'), 'utf8'), rule = "const gone = !!rc.is_shipped || !!rc.was_shipped || !!rc.is_canceled || !!rc.was_canceled || rc.is_paid === false || rc.was_paid === false || !!(rc.status && /cancel|fully\\s*refund/i.test(String(rc.status)));";
    assert(html.includes(rule), "design-1.html's gone rule is still the one this test applies");
    const gone = rc => !!rc.is_shipped || !!rc.was_shipped || !!rc.is_canceled || !!rc.was_canceled || rc.is_paid === false || rc.was_paid === false || !!(rc.status && /cancel|fully\s*refund/i.test(String(rc.status)));
    assert(WANT_SHIPPED.every(x => gone(x)), 'as Etsy sent them, the shipped receipts are gone to the page'); assert(listed.every(x => !gone(x)), 'as served, none of the 1000 that play is gone to the page'); }
  console.log('3 ok · shipped play as open: ' + WANT_SHIPPED.length + ' of ' + WANT_PLAY.length + ' served orders were shipped on Etsy; list sweep of ' + pages.length + ' pages ' + timings.listSweepMs + ' ms');

  /* ═══ 4 · the stream plays all of them, each once; a cold read of the 1000 set ═══ */
  reset(); r = await pullOp('start-1000-0004', { seed: 4242, speed: 50 });
  assert(r.status === 200 && r.body.stream.total === WANT_PLAY.length && r.body.stream.tick === 0, 'a stream for the 1000');
  const st0 = await emu({ fn: 'status' }); assert(st0.body.count === 1000 && st0.body.open === WANT_PLAY.length && st0.body.stream.total === WANT_PLAY.length && st0.body.stream.left === WANT_PLAY.length, 'status: ' + JSON.stringify(st0.body).slice(0, 300));
  let ticks = 0; for (let s = store.get('Charm_Sandbox/stream'); !s.done && ticks < 1000; ticks++) { const tk = await post({ op: 'sandboxStream', action: 'tick', sandbox: true, expect: s.simNow }); assert(tk.body.advanced, 'a step'); s = tk.body.stream; }
  const strm = store.get('Charm_Sandbox/stream'); assert(strm.done && strm.brought === WANT_PLAY.length, 'every order has come: ' + JSON.stringify({ tick: strm.tick, brought: strm.brought, total: strm.total }));
  assert(ticks >= WANT_PLAY.length / 5 && ticks <= WANT_PLAY.length / 2 + 2, '2 to 5 a step: ' + ticks + ' steps');
  const arrived = []; for (let off = 0; ; off += 100) { const p = await emu({ fn: 'listOpenOrders', offset: off }); arrived.push(...p.body.results); if (p.body.results.length < 100) break; }
  assert.strictEqual(arrived.length, WANT_PLAY.length, 'all of them are listed once the last step has come'); assert.strictEqual(new Set(arrived.map(x => x.receipt_id)).size, WANT_PLAY.length);
  assert(arrived.every(x => x.is_shipped === false && x.status === 'Paid' && !(x.shipments || []).length), 'streamed orders read open, shipped-on-Etsy ones included');
  assert(arrived.filter(x => byId.get(String(x.receipt_id)).is_shipped).length === WANT_SHIPPED.length, 'and the ones Etsy had shipped are among them');
  { const first = arrived[arrived.length - 1], oldestPlay = WANT_PLAY.slice().sort((x, y) => x.create_timestamp - y.create_timestamp)[0]; assert.strictEqual(String(first.receipt_id), String(oldestPlay.receipt_id), 'the oldest order came first'); }
  console.log('4 ok · stream of ' + WANT_PLAY.length + ' orders in ' + ticks + ' steps');

  /* ═══ 5 · a 429 with a short retry-after is asked once more ═══ */
  reset(); etsy.answer429 = { 100: { times: 1, retryAfter: 1 } };
  t = Date.now(); r = await pullOp('start-429-00001', { stream: false }); let took = Date.now() - t;
  assert(r.status === 200 && r.body.ok && r.body.count === 1000 && r.body.calls === 11, 'one page asked twice: 11 calls, all 1000 orders: ' + JSON.stringify(r.body).slice(0, 200));
  { const two = etsy.calls.filter(c => c.offset === 100).sort((x, y) => x.at - y.at); assert.strictEqual(two.length, 2, 'page 2 was asked twice'); assert(two[0].status === 429 && two[1].status === 200, 'first a 429, then the answer'); assert(two[1].at - two[0].answered >= 950, 'after its 1 s retry-after: ' + (two[1].at - two[0].answered) + ' ms');
    assert.strictEqual(etsy.calls.filter(c => c.offset !== 100).length, 9, 'the other nine pages once each'); assert.strictEqual(store.get('Charm_Sandbox/pulls').calls, 11, 'the ledger counts the retry as a call'); assert.deepStrictEqual(stored(r.body.path).map(x => x.receipt_id), WANT.map(x => x.receipt_id)); }
  assert(took < 5000, 'a retry costs about its wait: ' + took + ' ms');
  // retry-after 2 s is the longest that is asked again; no header at all is a per-second limit (1 s)
  reset(); etsy.answer429 = { 200: { times: 1, retryAfter: 2 } }; r = await pullOp('start-429-00002', { stream: false }); assert(r.status === 200 && r.body.calls === 11 && r.body.count === 1000, 'a retry-after of 2 s is asked again: ' + JSON.stringify(r.body).slice(0, 160));
  reset(); etsy.answer429 = { 300: { times: 1 } }; r = await pullOp('start-429-00003', { stream: false }); assert(r.status === 200 && r.body.calls === 11 && r.body.count === 1000, 'a 429 with no retry-after is asked again: ' + JSON.stringify(r.body).slice(0, 160));
  // a longer retry-after is not asked again: the sandbox is EMPTY, in the words it always had
  reset(); etsy.answer429 = { 100: { times: 1, retryAfter: 3 } }; r = await pullOp('start-429-00004');
  assert(r.status === 502 && r.body.ok === false && r.body.reason === 'etsy' && r.body.empty === true && r.body.calls === 10 && r.body.error === 'Etsy is limiting requests right now (HTTP 429), so the sandbox is empty. Try again in a minute.' && empty(), 'retry-after 3 s: no second try: ' + JSON.stringify(r.body).slice(0, 300));
  assert.strictEqual(etsy.calls.filter(c => c.offset === 100).length, 1, 'asked once');
  // a second 429 is final: one retry, no more
  reset(); etsy.answer429 = { 100: { times: 2, retryAfter: 1 } }; r = await pullOp('start-429-00005');
  assert(r.status === 502 && r.body.reason === 'etsy' && r.body.empty === true && r.body.calls === 11 && /HTTP 429/.test(r.body.error) && empty(), 'two 429s on one page: ONE retry, then it fails: ' + JSON.stringify(r.body).slice(0, 300)); assert.strictEqual(etsy.calls.filter(c => c.offset === 100).length, 2);
  // the daily limit is never retried
  reset(); etsy.answer429 = { 0: { times: 5, retryAfter: 1, body: 'You have exceeded your daily rate limit' } }; r = await pullOp('start-429-00006');
  assert(r.status === 502 && r.body.reason === 'limit' && /daily limit/.test(r.body.error) && empty() && etsy.calls.filter(c => c.offset === 0).length === 1, 'a daily limit: no retry: ' + JSON.stringify(r.body).slice(0, 200));
  // no time left on the clock: the retry is not started
  reset(); etsy.answer429 = { 100: { times: 1, retryAfter: 2 } }; r = await direct('start-429-00007', 2500);
  assert(r.ok === false && r.reason === 'etsy' && r.empty === true && r.calls === 10 && /HTTP 429/.test(r.error) && empty() && etsy.calls.filter(c => c.offset === 100).length === 1, 'a 2 s wait does not fit a 2.5 s clock: ' + JSON.stringify(r).slice(0, 300));
  console.log('5 ok · 429: one retry at most, only when the wait is short and the clock has room');

  /* ═══ 6 · a slow Etsy: the pull ends with the existing line and an EMPTY sandbox, never a half set ═══ */
  reset(); etsy.delayAt = { 500: 6000 };   // the sixth page does not answer inside the clock
  t = Date.now(); r = await direct('start-slow-00001', 2500); took = Date.now() - t;
  assert(r.ok === false && r.reason === 'etsy' && r.empty === true && r.calls === 10 && /^Etsy did not answer \(timed out after 2\.[3-9] s on page 6 of 10\), so the sandbox is empty\. Press Start to try again\.$/.test(r.error), 'a page that does not answer: ' + JSON.stringify(r).slice(0, 300));
  assert(empty() && !store.has('Charm_Sandbox/current'), 'no pointer, no file, no stream: not a half set'); assert(took < 4500, 'ended by the pull\'s clock: ' + took + ' ms');
  assert(store.get('Charm_Sandbox/pulls').claim === null && store.get('Charm_Sandbox/pulls').calls === 10, 'the claim is released and the ten calls are counted');
  { const rec = store.get('Charm_Sandbox/pulls').starts[0]; assert(rec.ok === false && rec.pages.length === 10 && rec.pages.some(x => x.o === 500 && x.s === 0 && x.ms >= 1800) && rec.pages.filter(x => x.s === 200).length === 9, 'the ledger names the slow page among all ten timings (it kept only the first eight before 1000 orders): ' + JSON.stringify(rec.pages)); }
  // every page slow but inside the clock: all 1000 come
  reset(); etsy.delay = 1500;
  t = Date.now(); r = await direct('start-slow-00002', 4000); took = Date.now() - t;
  assert(r.ok === true && r.count === 1000 && r.calls === 10 && !empty(), 'a slow but working Etsy still brings all 1000: ' + JSON.stringify(r).slice(0, 200)); assert(took < 4000, took + ' ms');
  // every page slower than the clock: the same line, the sandbox empty
  reset(); etsy.delay = 5000;
  r = await direct('start-slow-00003', 2000);
  assert(r.ok === false && r.empty === true && /^Etsy did not answer \(timed out after 2\.0 s on page \d+ of 10\)/.test(r.error) && empty(), 'all pages too slow: ' + JSON.stringify(r).slice(0, 300));
  console.log('6 ok · slow Etsy: "press Start to try again", the sandbox empty, never a half set');

  /* ═══ 7 · the rule in one place ═══ */
  assert(Orders.plays({ is_paid: true, status: 'Paid' }) && Orders.plays({ is_paid: true, is_shipped: true, status: 'Completed' }) && Orders.plays({}) && !Orders.plays({ status: 'Canceled' }) && !Orders.plays({ is_canceled: true }) && !Orders.plays({ was_canceled: true }) && !Orders.plays({ is_paid: false }) && !Orders.plays({ was_paid: false }) && !Orders.plays(null), 'plays: paid and not cancelled');
  assert.strictEqual(SP.isOpen, Orders.plays, 'the pull counts with the same rule'); assert.strictEqual(Orders.closedWhy({ status: 'Canceled' }), 'canceled'); assert.strictEqual(Orders.closedWhy({ is_paid: false }), 'unpaid'); assert.strictEqual(Orders.closedWhy({ is_paid: true, is_shipped: true }), null, 'shipped is not a reason to leave an order out');
  { const o = { receipt_id: 1, is_paid: true, status: 'Paid', is_shipped: false, transactions: [] }; assert.strictEqual(Orders.asOpen(o), o, 'an open receipt is the very same object'); const sh = { receipt_id: 2, is_paid: true, status: 'Completed', is_shipped: true, shipments: [{ tracking_code: 'x' }], transactions: [{ shipped_timestamp: 5 }] }, back = JSON.stringify(sh), op = Orders.asOpen(sh);
    assert.strictEqual(JSON.stringify(sh), back, 'asOpen copies: the receipt it is given is not changed'); assert(op !== sh && op.is_shipped === false && op.status === 'Paid' && op.shipments.length === 0 && op.transactions[0].shipped_timestamp === null && op.sandbox_shipped_on_etsy === true); }
  assert(Orders.asOpen({ status: 'Completed', is_paid: true }).status === 'Paid' && Orders.asOpen({ status: 'Completed', is_paid: true }).is_shipped === false, 'a Completed status alone is shipped too');
  const cancelledShipped = { is_canceled: true, is_shipped: true, status: 'Canceled' }; assert.strictEqual(Orders.asOpen(cancelledShipped), cancelledShipped, 'a cancelled receipt is never turned into an open one');

  console.warn = realWarn; Module._load = realLoad;
  console.log(`sandbox pull 1000 OK: ${JSON.stringify(timings)}; 429 retried once, slow Etsy ends empty`);
})().catch(e => { console.warn = realWarn; console.error(e); process.exit(1); });
