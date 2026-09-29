// order-timeline.js in a Node vm, with a fake window, localStorage and fetch: nothing leaves the machine.
//   · an event recorded with by "" (nobody signed in at the station) keeps "": it does not take the configured person
//   · an event with no by at all still takes the configured person; an explicit name is kept as given
//   node tests/stations/st-by-fallback.cjs [path/to/order-timeline.js]
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert');
const src = fs.readFileSync(process.argv[2] || path.join(__dirname, '../../order-timeline.js'), 'utf8');
const store = {}, posts = [];
const ctx = {
  addEventListener() {},
  localStorage: { getItem: k => (k in store ? store[k] : null), setItem: (k, v) => { store[k] = String(v); } },
  location: { protocol: 'http:' },
  document: { addEventListener() {}, visibilityState: 'visible' },
  fetch: async (url, o) => { posts.push({ url, body: JSON.parse(o.body) }); return { ok: true, status: 200, json: async () => ({ ok: true }) }; },
  Blob, console, setTimeout, clearTimeout
};
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(src, ctx, { filename: 'order-timeline.js' });
const OT = ctx.OrderTimeline;
assert.ok(OT, 'OrderTimeline loaded');
OT.config({ mode: 'station', station: 'weld', device: 'w1', by: 'Alice' });
const nobody = OT.record({ orderId: '3521000001', type: 'scan', by: '', data: { signedIn: false } });
const missing = OT.record({ orderId: '3521000002', type: 'scan' });
const named = OT.record({ orderId: '3521000003', type: 'scan', by: 'Bob' });
assert.strictEqual(nobody && nobody.by, '', 'an event recorded with by "" keeps "" (got ' + JSON.stringify(nobody && nobody.by) + ')');
assert.strictEqual(missing && missing.by, 'Alice', 'an event with no by takes the configured person');
assert.strictEqual(named && named.by, 'Bob', 'an explicit name is kept');
(async () => {
  await OT.flush();
  const sent = posts.flatMap(p => p.body.timeline || []);
  assert.deepStrictEqual(Object.fromEntries(sent.map(e => [e.orderId, e.by])), { 3521000001: '', 3521000002: 'Alice', 3521000003: 'Bob' },
    'what is sent carries the same people');
  assert.ok(posts.length && posts.every(p => p.url === '/.netlify/functions/firebaseOrders'), 'only the fake fetch, station door');
  console.log('st-by-fallback: ok (by "" kept, missing by falls back, named kept)');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
