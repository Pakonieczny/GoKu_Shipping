// Release hold answers to the same "on hold" the page draws (Paul, 16 Oct 2026: "when I try to release the dragon earrings order from the hold, I get an error that says this order is not on hold, but it's obviously on hold").
// The page marks a line "held" for a problem (a blocked SKU) or where the cloud says its pieces are on hold (Orders.heldNow) and draws its Release hold button on that; the engine read only the line's own `hold` flag.
// Offline, node only, the real charm-nest-order-release.js with small stand-ins for the page:  node tests/charm-nest/release-held-state.cjs
const path = require('node:path');
const assert = require('node:assert/strict');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

const rid = '4193111740';
const row = (key, o) => Object.assign({ key, order: { receiptId: rid }, state: 'pooled', hold: null, problems: [], spec: { designSku: 'DRAGON11', quantity: 1 }, line: { quantity: 1, sku: 'DRAGON11' }, poolIds: [] }, o || {});
const rows = [];
const repooled = [];
globalThis.Orders = { rows: () => rows, heldNow: r => !!r.hold || r.state === 'held' || r.cloudHold === true };
globalThis.Review = { repool: async r => { repooled.push(r.key); r.hold = null; r.state = 'pooled'; r.problems = []; } };
globalThis.B = { run: null };
require(path.join(__dirname, '../../charm-nest-order-release.js'));

(async () => {
  // a line the page marked "held" for a blocked SKU: no `hold` flag of its own, and the page offers Release hold on it
  rows.push(row('dragon', { state: 'held', reason: 'SKU DRAGON11 blocked: The charm outline is front artwork.', problems: [{ kind: 'blockedSku', sku: 'DRAGON11' }] }));
  let plan = globalThis.OrderHold.releasePlan(rid);
  ok(plan.canRelease === true && plan.held === true && !plan.blockedWhy, 'a line the page shows as held can be released: ' + JSON.stringify({ c: plan.canRelease, w: plan.blockedWhy }));
  let r = await globalThis.OrderHold.release(rid, { name: 'Paul' });
  ok(!/not on hold/.test(String(r.error || '')), 'pressing Release hold on it no longer says "This order is not on hold": ' + JSON.stringify({ ok: r.ok, e: r.error }));
  ok(repooled.includes('dragon'), 'the line is made up again like any released line (Review says what is still wrong with it)');

  // a hold only the cloud knows (no flag, no held state on the line): the same
  rows.length = 0; repooled.length = 0; rows.push(row('cloud', { cloudHold: true }));
  plan = globalThis.OrderHold.releasePlan(rid);
  ok(plan.canRelease === true, 'a hold the cloud keeps can be released');

  // the line's own flag still works as before
  rows.length = 0; repooled.length = 0; rows.push(row('flag', { hold: 'Held by Paul', state: 'held' }));
  ok(globalThis.OrderHold.releasePlan(rid).canRelease === true, 'a line held by its own flag can be released, as ever');

  // an order that is not on hold on the page is still refused, in the same words
  rows.length = 0; repooled.length = 0; rows.push(row('plain'));
  plan = globalThis.OrderHold.releasePlan(rid);
  ok(plan.canRelease === false && plan.blockedWhy === 'This order is not on hold.', 'an order that is really not on hold still says so: ' + plan.blockedWhy);
  r = await globalThis.OrderHold.release(rid, { name: 'Paul' });
  ok(r.ok === false && /not on hold/.test(r.error) && !repooled.length, 'and its release does nothing');

  console.log('release-held-state OK ·', n, 'checks');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
