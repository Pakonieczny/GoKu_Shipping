// station-activity.js, the browser helper: nothing without a signed-in person, the event it builds, no PIN, the 600-byte fit,
// the queue (memory + localStorage, cap 500), the 10 s flush in batches of 50, retries with backoff and the same ids,
// pagehide / hidden (sendBeacon, keepalive fallback), online, permanent refusals, a reload that finds earlier events,
// the midnight sign-out that still sends what is queued. Fakes only: jsdom, a fake StationSession, fetch and sendBeacon
// stubs, a fake clock and scheduler; nothing touches the network.
//   JSDOM_DIR=<…/node_modules> node tests/stations/station-activity-client.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
let JSDOM;
for (const d of [process.env.JSDOM_DIR, process.argv[2], path.join(root, 'node_modules')].filter(Boolean)) { try { ({ JSDOM } = require(path.join(d, 'jsdom'))); break; } catch (_) {} }
if (!JSDOM) { try { ({ JSDOM } = require('jsdom')); } catch (_) { console.error('jsdom not found: set JSDOM_DIR'); process.exit(2); } }
const code = fs.readFileSync(path.join(root, 'station-activity.js'), 'utf8');
const T0 = Date.parse('2026-10-02T13:00:00Z');

/** a page: jsdom + fakes. Returns the handles the checks use. */
function page(opts = {}) {
  const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'http://station.test/weld-1.html', pretendToBeVisual: true, runScripts: 'outside-only' });
  const w = dom.window;
  const h = { w, now: opts.now || T0, calls: [], beacons: [], intervals: [], timeouts: [], reply: () => ({ status: 200 }), beacon: true, onLine: true,
    who: opts.who === undefined ? { person: 'Tess Welder', station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW', startAt: (opts.now || T0) - 30000, sandbox: !!opts.sandbox } : opts.who };
  for (const [k, v] of Object.entries(opts.ls || {})) w.localStorage.setItem(k, v);
  w.Date.now = () => h.now;
  w.setInterval = (fn, ms) => { h.intervals.push({ fn, ms }); return h.intervals.length; };
  w.setTimeout = (fn, ms) => { h.timeouts.push({ fn, ms }); return h.timeouts.length; };
  w.clearTimeout = () => {};
  w.TextEncoder = TextEncoder;
  w.Blob = function (parts) { this.parts = parts; };
  Object.defineProperty(w.navigator, 'onLine', { get: () => h.onLine, configurable: true });
  if (h.beacon) Object.defineProperty(w.navigator, 'sendBeacon', { value: (u, b) => { h.beacons.push({ url: u, body: JSON.parse(b.parts[0]) }); return true; }, configurable: true });
  w.fetch = (u, o) => { const r = h.reply(JSON.parse(o.body), u); h.calls.push({ url: u, body: JSON.parse(o.body), keepalive: !!o.keepalive }); return r instanceof Error ? Promise.reject(r) : Promise.resolve(r); };
  if (opts.session !== false) w.StationSession = { who: () => { if (h.whoThrows) throw new Error('boom'); return h.who; }, page: () => ({ station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', sandbox: !!opts.sandbox }) };
  w.eval(code);
  h.A = w.StationActivity;
  h.tick = () => h.intervals.find(i => i.ms === 10000).fn();       // the 10 s flush
  h.q = () => JSON.parse(w.localStorage.getItem('station_activity_q.weld-1') || '[]');
  h.settle = async () => { for (let i = 0; i < 20; i++) await new Promise(r => setImmediate(r)); };
  return h;
}

(async () => {
  /* 1 · nothing without a signed-in person; never throws */
  {
    let h = page({ who: null });
    assert.strictEqual(h.A.log('scan', { orderId: '3521000777' }), false, 'nobody signed in');
    assert.strictEqual(h.A.pending(), 0); assert.strictEqual(h.A.who(), null);
    h = page({ session: false });
    assert.strictEqual(h.A.log('scan'), false, 'no StationSession'); assert.strictEqual(h.A.who(), null);
    h = page(); h.whoThrows = true;
    assert.strictEqual(h.A.log('scan'), false, 'a throwing StationSession is not the page\'s problem');
    h.whoThrows = false;
    assert.strictEqual(h.A.log('poke'), false, 'unknown action'); assert.strictEqual(h.A.log(), false); assert.strictEqual(h.A.log({}, 5), false);
    h.who = Object.assign({}, h.who, { person: '123456' });
    assert.strictEqual(h.A.log('scan'), false, 'a digits-only name (a PIN) is never an identity');
    h.who = Object.assign({}, h.who, { person: 'Tess Welder', startAt: undefined });
    assert.strictEqual(h.A.log('scan'), true, 'a session with no start still logs');
    assert.strictEqual(h.calls.length, 0, 'log never touches the network');
    // an old station-session.js without who(): the helper stays quiet
    const w = page({ session: false }); w.w.StationSession = { init() {} };
    assert.strictEqual(w.A.log('scan'), false);
    console.log('1 nothing without a person or session, never throws');
  }

  /* 2 · the event */
  {
    const h = page();
    assert.strictEqual(h.A.log('scan', { orderId: ' 3521000777 ', line: '99', sku: 'GF-12', parts: 1.6, detail: '  QR\u0000 scan  ' }), true);
    h.now += 45000;
    assert.strictEqual(h.A.log('complete', { orderId: '3521000777', parts: 12, orders: 1 }), true);
    h.now += 2 * 3600e3;
    h.A.log('print', { parts: -4, orders: 7 }); h.A.log('note', { detail: 'x' });
    const q = h.q();
    assert.strictEqual(q.length, 4); assert.strictEqual(h.A.pending(), 4);
    assert.deepStrictEqual(q[0], { id: `weld-1_ABCD_1_${T0}`, station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW', person: 'Tess Welder', action: 'scan',
      orderId: '3521000777', line: '99', sku: 'GF-12', parts: 2, orders: 0, detail: 'QR scan', at: T0, seq: 1, sincePrevMs: 30000 });
    assert.strictEqual(q[1].sincePrevMs, 45000, 'the gap since this person\'s previous action'); assert.strictEqual(q[1].orders, 1); assert.strictEqual(q[1].parts, 12);
    assert.strictEqual(q[2].sincePrevMs, 3600000, 'a gap is capped at an hour'); assert.strictEqual(q[2].parts, 0); assert.strictEqual(q[2].orders, 1, 'orders is 0 or 1');
    assert.strictEqual(q[3].sincePrevMs, 0);
    assert.deepStrictEqual(q.map(e => e.seq), [1, 2, 3, 4], 'a per-device counter');
    assert.strictEqual(new Set(q.map(e => e.id)).size, 4);
    assert(q.every(e => /^[\w.:-]{8,100}$/.test(e.id)), 'ids are the server\'s shape');
    assert(!('sandbox' in q[0]), 'no sandbox flag in production');
    // another person on the same page: their gap starts at their own session
    h.who = Object.assign({}, h.who, { person: 'Ray Welder', session: 'weld-1-ABCD-k2-QQQQ', startAt: h.now - 10000 });
    h.A.log('scan'); assert.strictEqual(h.q()[4].sincePrevMs, 10000); assert.strictEqual(h.q()[4].person, 'Ray Welder');
    assert.strictEqual(h.A.who().person, 'Ray Welder');
    console.log('2 the event: identity, id, seq, gaps, clamps');
  }

  /* 3 · no PIN, and the 600 bytes */
  {
    const h = page();
    h.A.log('scan', { orderId: '123456', line: '482913', sku: '9999', detail: '654321' });
    h.A.log('note', { detail: 'typed pin 654321 by mistake, sheet 2 of 12', orderId: '3521000777' });
    const all = JSON.stringify(h.q());
    assert(!/123456|654321|482913|9999/.test(all), 'no PIN-looking value is queued: ' + all);
    assert.strictEqual(h.q()[0].orderId, ''); assert.strictEqual(h.q()[0].detail, '');
    assert.strictEqual(h.q()[1].detail, 'typed pin [#] by mistake, sheet 2 of 12');
    h.A.log('note', { detail: 'ä'.repeat(300), sku: 's'.repeat(100), line: 'l'.repeat(100), orderId: '3'.repeat(40) });
    const big = h.q()[2];
    assert(Buffer.byteLength(JSON.stringify(big)) <= 600, 'fits the server\'s 600 bytes: ' + Buffer.byteLength(JSON.stringify(big)));
    assert(big.detail.length <= 120);
    console.log('3 no PIN; 600-byte fit');
  }

  /* 4 · the flush: every 10 s, batches of 50, in order, acknowledged */
  {
    const h = page();
    assert(h.intervals.some(i => i.ms === 10000), 'a 10 s flush is scheduled');
    for (let i = 0; i < 120; i++) h.A.log('scan', { orderId: '3521000777', parts: 1 });
    const ids = h.q().map(e => e.id);
    h.tick(); await h.settle();
    assert.deepStrictEqual(h.calls.map(c => c.body.activity.length), [50, 50, 20], 'batches of at most 50');
    assert.deepStrictEqual(h.calls.flatMap(c => c.body.activity.map(e => e.id)), ids, 'in the order they happened');
    assert(h.calls.every(c => c.url === '/.netlify/functions/firebaseOrders' && !c.keepalive));
    assert.strictEqual(h.A.pending(), 0); assert.deepStrictEqual(h.q(), []); assert.strictEqual(h.w.localStorage.getItem('station_activity_q.weld-1'), null);
    h.tick(); await h.settle(); assert.strictEqual(h.calls.length, 3, 'nothing queued: no request');
    console.log('4 flush: 10 s, 50 a batch, in order, cleared on 200');
  }

  /* 5 · retries: 503, network error, offline, backoff, the same ids */
  {
    const h = page();
    for (let i = 0; i < 3; i++) h.A.log('scan', { parts: 1 });
    const ids = h.q().map(e => e.id);
    h.reply = () => ({ status: 503 });
    h.tick(); await h.settle();
    assert.strictEqual(h.calls.length, 1); assert.strictEqual(h.A.pending(), 3, 'kept after a 503'); assert.strictEqual(h.q().length, 3);
    h.now += 5000; h.tick(); await h.settle(); assert.strictEqual(h.calls.length, 1, 'backs off: not again within the backoff');
    h.now += 60000; h.reply = () => new Error('network down'); h.tick(); await h.settle();
    assert.strictEqual(h.calls.length, 2); assert.strictEqual(h.A.pending(), 3, 'kept after a network error');
    h.now += 600000; h.reply = () => ({ status: 429 }); h.tick(); await h.settle(); assert.strictEqual(h.calls.length, 3); assert.strictEqual(h.A.pending(), 3, 'kept after a 429');
    h.onLine = false; h.now += 600000; h.reply = () => ({ status: 200 }); h.tick(); await h.settle();
    assert.strictEqual(h.calls.length, 3, 'offline: no attempt'); assert.strictEqual(h.A.pending(), 3);
    h.onLine = true; h.w.dispatchEvent(new h.w.Event('online')); await h.settle();
    assert.strictEqual(h.calls.length, 4, 'back online: sent at once'); assert.strictEqual(h.A.pending(), 0);
    assert.deepStrictEqual(h.calls.map(c => c.body.activity.map(e => e.id)), [ids, ids, ids, ids], 'every retry carries the same ids');
    // events logged while a batch is in flight are not lost
    h.reply = () => ({ status: 200 }); h.A.log('scan'); const p = h.A.flush(); h.A.log('complete', { parts: 3 }); await p; await h.settle();
    assert.strictEqual(h.A.pending(), 0);
    assert.strictEqual(h.calls.flatMap(c => c.body.activity).filter(e => e.action === 'complete').length >= 1, true);
    console.log('5 retries: 503, network error, 429, offline, online, same ids, backoff');
  }

  /* 6 · permanent refusals and 413 */
  {
    const h = page();
    for (let i = 0; i < 4; i++) h.A.log('scan');
    h.reply = () => ({ status: 400 }); h.tick(); await h.settle();
    assert.strictEqual(h.A.pending(), 0, 'a 400 drops the batch for good (nothing retried forever)');
    for (let i = 0; i < 4; i++) h.A.log('scan');
    h.reply = b => ({ status: b.activity.length > 1 ? 413 : 200 }); h.tick(); await h.settle();
    assert.strictEqual(h.A.pending(), 0, 'a 413 is split until it passes');
    for (let i = 0; i < 2; i++) h.A.log('scan');
    h.reply = () => ({ status: 413 }); h.tick(); await h.settle();
    assert.strictEqual(h.A.pending(), 0, 'a single event the server calls too large is dropped');
    console.log('6 400 and 413');
  }

  /* 7 · pagehide / hidden: sendBeacon, kept until an answer, not repeated, keepalive fallback */
  {
    const h = page();
    h.A.log('scan', { parts: 1 }); h.A.log('complete', { parts: 2, orders: 1 });
    h.w.dispatchEvent(new h.w.Event('pagehide'));
    assert.strictEqual(h.beacons.length, 1); assert.strictEqual(h.beacons[0].url, '/.netlify/functions/firebaseOrders');
    assert.deepStrictEqual(h.beacons[0].body.activity.map(e => e.action), ['scan', 'complete']);
    assert.strictEqual(h.A.pending(), 2, 'kept until the server has answered (a repeat is stored once)');
    h.w.dispatchEvent(new h.w.Event('pagehide')); assert.strictEqual(h.beacons.length, 1, 'the same events are not beaconed twice');
    h.A.log('note'); h.w.dispatchEvent(new h.w.Event('pagehide')); assert.strictEqual(h.beacons.length, 2); assert.strictEqual(h.beacons[1].body.activity.length, 1);
    Object.defineProperty(h.w.document, 'visibilityState', { value: 'hidden', configurable: true });
    h.A.log('note'); h.w.document.dispatchEvent(new h.w.Event('visibilitychange')); assert.strictEqual(h.beacons.length, 3, 'a hidden page sends too');
    const g = page({ now: T0 + 1 }); delete g.w.navigator.sendBeacon; g.w.navigator.sendBeacon = undefined;
    g.A.log('scan'); g.w.dispatchEvent(new g.w.Event('pagehide'));
    assert(g.calls.length === 1 && g.calls[0].keepalive === true && g.calls[0].body.activity.length === 1, 'no sendBeacon: a keepalive request');
    console.log('7 pagehide/hidden: beacon, kept, once, keepalive fallback');
  }

  /* 8 · a reload finds the earlier events; the cap; sandbox; the midnight sign-out */
  {
    let h = page();
    for (let i = 0; i < 3; i++) h.A.log('scan', { parts: 1 });
    const stored = h.w.localStorage.getItem('station_activity_q.weld-1'), ids = h.q().map(e => e.id);
    const keys = {}; for (let i = 0; i < h.w.localStorage.length; i++) { const k = h.w.localStorage.key(i); keys[k] = h.w.localStorage.getItem(k); }
    const r = page({ now: T0 + 3600e3, ls: keys });                                // the same browser, a new page load, one hour on
    assert.strictEqual(r.A.pending(), 0, 'nothing loaded until the page is set up');
    assert(r.timeouts.some(t => t.ms === 4000), 'a flush a few seconds after load');
    r.A.log('scan', { parts: 1 });
    assert.strictEqual(r.A.pending(), 4, 'earlier events and the new one wait together');
    assert.strictEqual(r.q().filter(e => ids.includes(e.id)).length, 3);
    assert(r.q()[3].seq > 3, 'the counter goes on across loads');
    assert.strictEqual(r.q()[3].sincePrevMs, 3600000, 'the gap goes on across the reload (capped at an hour)');
    r.timeouts.find(t => t.ms === 4000).fn(); await r.settle();
    assert.deepStrictEqual(r.calls[0].body.activity.map(e => e.id).slice(0, 3), ids, 'sent again with the same ids'); assert.strictEqual(r.A.pending(), 0);
    // the cap
    h = page();
    for (let i = 0; i < 620; i++) h.A.log('scan');
    assert.strictEqual(h.A.pending(), 500, 'at most 500 wait'); assert.strictEqual(h.q().length, 500);
    assert.strictEqual(h.q()[0].seq, 121, 'the oldest go first');
    // sandbox
    h = page({ sandbox: true }); h.A.log('scan'); h.A.log('complete', { parts: 1 });
    assert(h.q().every(e => e.sandbox === true)); h.tick(); await h.settle();
    assert.strictEqual(h.calls[0].url, '/.netlify/functions/firebaseOrders?sandbox=1');
    // the midnight sign-out: nobody is signed in, what is queued still goes
    h = page(); h.A.log('complete', { parts: 2, orders: 1 }); h.who = null;
    assert.strictEqual(h.A.log('scan'), false, 'after sign-out nothing new is logged'); assert.strictEqual(h.A.pending(), 1);
    h.tick(); await h.settle(); assert.strictEqual(h.calls.length, 1); assert.strictEqual(h.A.pending(), 0, 'the queued event still goes after the sign-out');
    // localStorage that throws is not the page's problem
    h = page(); Object.defineProperty(h.w, 'localStorage', { get() { throw new Error('blocked'); }, configurable: true });
    assert.strictEqual(h.A.log('scan'), true); h.tick(); await h.settle(); assert.strictEqual(h.calls.length, 1);
    console.log('8 reload keeps events, cap 500, sandbox, sign-out still sends, no localStorage');
  }
})().then(() => console.log('activity client: all passed'), e => { console.error(e); process.exit(1); });
