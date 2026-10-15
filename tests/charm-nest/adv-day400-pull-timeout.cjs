// ADV-DAY400 (final adversarial test, 15 Oct 2026): a cold pull of the 400 newest Etsy orders is 14-21 minutes under the Design Station's own
// Etsy pacing (60 reads a minute, 150 in ten minutes), and the sorter's call for it gave up after 20 whatever the station was doing: about one
// pull in five (by where in the ten-minute window it started) stopped the run with "did not answer in time" and a person had to press Resume.
// Every read reports progress, so the limit now counts from the station's last sign of work.
// Here a 40-order snapshot (the station starts a read every 250 ms: 10+ s) is asked for with a 4 s limit: it must still come back whole, and
// take longer than the limit. Offline: the repo's fake site, no Etsy, no network.
//   node tests/charm-nest/adv-day400-pull-timeout.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const assert = require('assert/strict');
const { open } = require('./day-sim/harness.cjs');

const N = 40, now = Math.floor(Date.now() / 1000);
const receipts = Array.from({ length: N }, (_, i) => ({ receipt_id: 4200000001 + i, order_number: 4200000001 + i, name: 'Buyer ' + i, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: now, create_timestamp: now, status: 'Paid', is_shipped: false,
  transactions: [{ transaction_id: Number(`${4200000001 + i}1`), listing_id: 1718001, receipt_id: 4200000001 + i, sku: 'BR-TST-01', title: 'BR-TST-01 charm', quantity: 1, expected_ship_date: now + 86400 * 3, variations: [{ formatted_name: 'Metal', formatted_value: '14k Gold Filled' }], is_personalized: false }] }));

(async () => {
  const H = await open({ receipts, masters: [{ count: 1 }], settings: { pollOrders: 'off' } });
  let failed = 0;
  const check = (name, fn) => { try { fn(); console.log('  ✓ ' + name); } catch (e) { failed++; console.log('  ✗ ' + name + '\n      ' + String(e.message).split('\n')[0]); } };
  try {
    const r = await H.page.evaluate(async () => {
      const t = Date.now(), log = [];
      try { const res = await DesignLink.call('orders.snapshot', { hydrate: true, refresh: true }, { timeoutMs: 4000, quiet: true, onProgress: p => log.push(p.done) }); return { ok: true, total: res.total, hydrated: res.hydrated, ms: Date.now() - t, progress: log.length }; }
      catch (e) { return { ok: false, err: e.message, ms: Date.now() - t, progress: log.length }; }
    });
    console.log('  snapshot of', N, 'orders with a 4 s limit:', JSON.stringify(r));
    check('a call that keeps reporting progress outlives its limit and comes back whole', () => { assert.equal(r.ok, true, r.err); assert.equal(r.hydrated, N); assert.equal(r.total, N); });
    check('it really ran past the limit (the station paces its reads)', () => assert(r.ms > 4500, `took only ${r.ms} ms`));
  } finally { await H.close(); }
  if (failed) { console.log(failed + ' failed'); process.exit(1); }
  console.log('ok');
})().catch(e => { console.log('FAILED', e && e.stack || e); process.exit(1); });
