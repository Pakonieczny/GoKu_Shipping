// Adversarial (ADV-CANCEL, 15 Oct 2026): which Etsy receipt statuses take an order out of production. The station's fresh read
// of one order (design-1.html, orders.detail: what the sorter's re-check "before labels / before commit" trusts) and the
// server's rule for Etsy's cancels (_orderCancel.isCancelled, the inbox mirror) must agree:
//   "Canceled" and "Fully Refunded" before shipping are cancels; "Partially Refunded" is a live order (a part of the money
//   went back, the pieces are still to be cut). The station used to match /refund/ and so called a partly refunded order
//   gone: the re-check dropped it from the run and took its pieces off every sheet, and an order found gone is never read again.
//   node tests/charm-nest/adv-cancel-refund.cjs
const fs = require('fs'), vm = require('vm'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const station = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8');
const OrderCancel = require(path.join(root, 'netlify/functions/_orderCancel.js'));

// the station's own handler, cut out of the page and run over a stand-in Etsy that returns the receipt we hand it
const a = station.indexOf('async "orders.detail"(a) {'), b = station.indexOf('async "orders.watch"');
assert(a > 0 && b > a, 'the orders.detail handler is in the page');
let receipt = null, reads = 0;
const ctx = {
  Cursor: { glide() {} }, etsyMeter: { total: 0 }, allOpenReceipts: [], currentReceipts: [], etsyBraked: () => false,
  pullEtsyOrderDetails: async () => { reads++; return { receipt: Object.assign({}, receipt), transactions: [] }; },
  decorateReceipt() {}, orderCache: {}, cachePut() {}, receiptStamp: () => 1, paintRow() {}, notesCache: new Map(), msgCache: new Map(),
  orderForBridge: async r => ({ receiptId: String(r.receipt_id) })
};
vm.createContext(ctx);
vm.runInContext('var H = {' + station.slice(a, b).trim().replace(/,\s*$/, '') + '};', ctx);
const goneOf = async r => { receipt = Object.assign({ receipt_id: '4800000001', status: 'Paid', is_paid: true, is_shipped: false }, r); return (await ctx.H['orders.detail']({ receiptId: '4800000001', fresh: true })).gone; };

(async () => {
  const ok = [];
  // 1 · a partial refund on a live order is not a cancel
  assert.strictEqual(await goneOf({ status: 'Partially Refunded' }), false, 'a partly refunded order is still to be made');
  assert.strictEqual(await goneOf({ status: 'partially refunded' }), false, 'whatever the spelling');
  ok.push('a partly refunded unshipped order is not gone (it used to be dropped from the run and taken off its sheets)');
  // 2 · the real cancels and the other reasons an order leaves production
  for (const r of [{ status: 'Canceled' }, { status: 'Cancelled' }, { status: 'Fully Refunded' }, { status: 'fully refunded' }, { status: 'Paid', is_canceled: true }, { status: 'Paid', was_canceled: true },
    { status: 'Paid', is_shipped: true }, { status: 'Paid', was_shipped: true }, { status: 'Paid', is_paid: false }, { status: 'Paid', was_paid: false }, { status: 'Partially Refunded', is_shipped: true }, { status: 'Completed', is_shipped: true }])
    assert.strictEqual(await goneOf(r), true, 'gone: ' + JSON.stringify(r));
  for (const st of ['Paid', 'Open', 'Payment Processing', '', undefined]) assert.strictEqual(await goneOf({ status: st }), false, 'live: ' + st);
  ok.push('canceled / fully refunded / the cancel flags / shipped / unpaid are gone; paid, open and processing are not');
  // 3 · the two rules agree on every unshipped status, so they cannot drift apart again
  for (const st of ['Paid', 'Open', 'Completed', 'Payment Processing', 'Canceled', 'Cancelled', 'canceled', 'Fully Refunded', 'fully refunded', 'Partially Refunded', 'partially refunded', 'Refunded', '', null])
    assert.strictEqual(await goneOf({ status: st }), OrderCancel.isCancelled({ status: st, is_shipped: false }), 'the station and the mirror agree on status ' + JSON.stringify(st));
  ok.push('the station and the inbox mirror (_orderCancel.isCancelled) agree on every unshipped status');
  console.log('adv-cancel-refund: all passed (' + reads + ' fresh reads)\n  ' + ok.join('\n  '));
})().catch(e => { console.error(e); process.exit(1); });
