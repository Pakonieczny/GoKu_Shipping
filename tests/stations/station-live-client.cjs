// The browser half of the live layer (station-activity.js: working / idle / touch), run by station-live.cjs against the REAL door
// (firebaseOrders {live}) and the REAL reader (employeeEfficiency {op:"live"}) over the in-memory Firestore: a page signs in,
// scans, keeps an order open, completes it, loses its sign-in, closes, fails, is slow, runs in the sandbox, and the board shows
// what it should at each step. The clock is shared and faked; jsdom, a fake StationSession, scripted timers; nothing touches a network.
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');

exports.run = async function run(X) {
  const { door, post, ask, fresh, station, tick, seed, root, say, logs, PIN, SB, getNow, getCur } = X;
  let JSDOM;
  for (const d of [process.env.JSDOM_DIR, process.argv[2], path.join(root, 'node_modules')].filter(Boolean)) { try { ({ JSDOM } = require(path.join(d, 'jsdom'))); break; } catch (_) {} }
  if (!JSDOM) { try { ({ JSDOM } = require('jsdom')); } catch (_) { throw new Error('jsdom not found: set JSDOM_DIR'); } }
  const code = fs.readFileSync(path.join(root, 'station-activity.js'), 'utf8');
  let ip = 0;
  const eq = (a, b, m) => assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m);       // (the page's arrays come from another realm)

  /** a station page: jsdom + a fake sign-in, scripted timers, a fetch that goes to the real door (or to h.reply) */
  function page(o = {}) {
    const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'http://station.test/weld-1.html', pretendToBeVisual: true, runScripts: 'outside-only' });
    const w = dom.window, me = '203.0.113.' + (100 + (++ip % 100));
    const h = { w, calls: [], beacons: [], intervals: [], timeouts: [], cleared: [], reply: null, onLine: true,
      who: o.who === undefined ? { person: 'Tess Welder', station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW', startAt: getNow() - 30000, sandbox: !!o.sandbox } : o.who };
    w.Date.now = () => getNow();
    w.setInterval = (fn, ms) => { h.intervals.push({ fn, ms, id: h.intervals.length + 1 }); return h.intervals.length; };
    w.clearInterval = id => { h.cleared.push(id); const i = h.intervals.find(x => x.id === id); if (i) i.dead = true; };
    w.setTimeout = (fn, ms) => { h.timeouts.push({ fn, ms }); return h.timeouts.length; };
    w.clearTimeout = () => {};
    w.TextEncoder = TextEncoder;
    w.Blob = function (parts) { this.parts = parts; };
    Object.defineProperty(w.navigator, 'onLine', { get: () => h.onLine, configurable: true });
    Object.defineProperty(w.navigator, 'sendBeacon', { value: (u, b) => { h.beacons.push({ url: u, body: JSON.parse(b.parts[0]) }); return true; }, configurable: true });
    w.fetch = (u, opt) => {
      const body = JSON.parse(opt.body); h.calls.push({ url: u, body, keepalive: !!opt.keepalive, at: getNow() });
      if (h.reply) { const r = h.reply(body, u, h.calls.length); if (r !== undefined) return r instanceof Error ? Promise.reject(r) : Promise.resolve(r); }
      return door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': me }, queryStringParameters: /sandbox=1/.test(u) ? { sandbox: '1' } : {}, body: opt.body })
        .then(r => ({ status: r.statusCode, json: () => Promise.resolve(JSON.parse(r.body || '{}')) }));
    };
    w.StationSession = { who: () => { if (h.whoThrows) throw new Error('boom'); return h.who; }, page: () => ({ station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', sandbox: !!o.sandbox }) };
    w.eval(code);
    h.A = w.StationActivity;
    h.settle = async () => { for (let i = 0; i < 40; i++) await new Promise(r => setImmediate(r)); };
    h.zero = async () => { for (let n = 0; n < 10; n++) { const z = h.timeouts.filter(t => t.ms === 0 || t.ms === undefined); h.timeouts = h.timeouts.filter(t => !z.includes(t)); if (!z.length) break; z.forEach(t => t.fn()); await h.settle(); } await h.settle(); };
    h.tick5 = async () => { const i = h.intervals.filter(x => x.ms === 5000 && !x.dead).pop(); if (i) i.fn(); await h.settle(); };
    h.live = () => h.calls.filter(c => c.body && c.body.live);
    h.kinds = () => h.live().map(c => c.body.live.event);
    return h;
  }
  const docKey = 'welding__weld-1__Tess Welder';
  const rows = (s, key) => station({ body: s }, key);

  /* 11 · nothing without a person; never throws; no network inside the call */
  {
    fresh(); seed(getCur());
    let h = page({ who: null });
    assert.strictEqual(h.A.working({ rid: '3521000777' }), false, 'nobody signed in: nothing is current'); await h.zero(); assert.strictEqual(h.calls.length, 0);
    h = page({ who: Object.assign({}, page().who, { person: '123456' }) });
    assert.strictEqual(h.A.working({ rid: '3521000777' }), false, 'a digits-only name (a PIN) is never an identity'); assert.strictEqual(h.A.idle(), false);
    h = page(); h.whoThrows = true; assert.strictEqual(h.A.working({ rid: '3521000777' }), false, 'a throwing sign-in is not the page\'s problem'); h.whoThrows = false;
    for (const bad of [undefined, null, 5, 'x', {}, { rid: '' }, { rid: 'abc' }, { rid: PIN }, { rid: '1234' }, { kind: 'sheet' }]) assert.strictEqual(h.A.working(bad), false, 'not an order: ' + JSON.stringify(bad));
    assert.strictEqual(h.A.idle(), false, 'nothing to end'); assert.strictEqual(h.A.idle({ station: 'laser' }), false); assert.strictEqual(h.A.touch(), false);
    await h.zero(); assert.strictEqual(h.calls.length, 0, 'nothing was sent for any of them');
    assert.strictEqual(h.A.working({ rid: '3521000777', station: 'nowhere' }), true, 'an unknown station name falls back to the page\'s own station'); eq(h.A.current().map(c => c.station), ['welding']); h.A.idle(); await h.zero();
    h = page({ who: null }); h.w.StationSession = { init() {} };
    assert.strictEqual(h.A.working({ rid: '3521000777' }), false, 'an old station-session.js without who(): quiet');
    h = page(); assert.strictEqual(h.A.working({ rid: '3521000777' }), true); assert.strictEqual(h.calls.length, 0, 'the call itself never touches the network'); assert.strictEqual(h.timeouts.length > 0, true);
    say('11 client: nothing without a person, never throws, no network inside the call');
  }

  /* 12 · scan -> work; keep-alive; completion -> idle; the board follows at each step */
  {
    const s = fresh(); seed(s);
    s.put('Station_Sessions', 's1', { person: 'Tess Welder', station: 'welding', device: 'weld-1', startAt: getNow() - 3600e3, lastSeenAt: getNow(), endAt: null });
    const h = page();
    assert.strictEqual(h.A.working({ rid: ' 3521000777 ', orderNumber: '3521000777', customer: 'Sam P.', note: 'phone scan', pieces: [{ id: '3521000777_9001_1', label: 'Stud earrings', sku: 'GF-12', listingId: 4455667788, size: 'L' }, { id: '3521000777_9001_2', label: 'Stud earrings', sku: 'GF-12', listingId: 4455667788 }] }), true);
    await h.zero();
    assert.deepStrictEqual(h.kinds(), ['work']); const b = h.live()[0];
    assert.strictEqual(b.url, '/.netlify/functions/firebaseOrders'); assert.strictEqual(b.keepalive, false);
    const L = b.body.live;
    assert.strictEqual(L.person, 'Tess Welder'); assert.strictEqual(L.station, 'welding'); assert.strictEqual(L.device, 'weld-1'); assert.strictEqual(L.session, 'weld-1-ABCD-k1-XYZW'); assert.strictEqual(L.startAt, getNow() - 30000);
    assert.strictEqual(L.order.rid, '3521000777'); assert.strictEqual(L.order.kind, 'order'); assert.strictEqual(L.order.scannedAt, getNow()); assert.strictEqual(L.order.pieces.length, 2); assert.strictEqual(L.order.pieces[0].listingId, '4455667788'); assert.strictEqual(L.order.pieceCount, 2);
    assert(!('sandbox' in L), 'no sandbox flag in production');
    assert(!JSON.stringify(b.body).includes(PIN), 'no PIN');
    eq(h.A.current().map(c => [c.station, c.rid, c.sent]), [['welding', '3521000777', true]]);
    assert(h.intervals.some(i => i.ms === 5000), 'a 5 s check runs while an order is current');
    let a = await ask(); let c = rows(a.body, 'welding').current[0];
    assert.strictEqual(c.rid, '3521000777'); assert.strictEqual(c.person, 'Tess Welder'); assert.strictEqual(c.pieces.length, 2); assert.strictEqual(c.pieces[0].thumbUrl.includes('gf-12-L'), true); assert.deepStrictEqual(c.qr, { text: '3521000777' }); assert.strictEqual(c.scannedAt, getNow());
    // nothing is due yet: the check sends nothing
    tick(10000); await h.tick5(); assert.deepStrictEqual(h.kinds(), ['work'], 'no beat before 30 s');
    // a beat every 30 s: tiny, and it is a one-field update
    tick(21000); await h.tick5(); assert.deepStrictEqual(h.kinds(), ['work', 'beat']);
    assert.deepStrictEqual(Object.keys(h.live()[1].body.live).sort(), ['device', 'event', 'person', 'station', 'v'], 'a beat names only the page and the person');
    assert.strictEqual(getCur().writesOf('Station_Live').map(w => w[2]).join(), 'set,update');
    tick(10000); await h.tick5(); assert.deepStrictEqual(h.kinds(), ['work', 'beat'], 'not again for 30 s');
    tick(20000); await h.tick5(); assert.deepStrictEqual(h.kinds(), ['work', 'beat', 'beat']);
    for (let i = 0; i < 3; i++) { tick(30000); await h.tick5(); }
    assert.deepStrictEqual(h.kinds().slice(3), ['beat', 'work', 'beat'], 'the whole order is sent again every 2 minutes (a safety refresh)');
    tick(1000); a = await ask(); assert.strictEqual(rows(a.body, 'welding').state, 'working', 'still on the board after 2 minutes of beats');
    // completion
    assert.strictEqual(h.A.idle('3521000777'), true); await h.zero();
    assert.strictEqual(h.kinds().pop(), 'idle'); const i = h.live().pop().body.live;
    assert.deepStrictEqual([i.ended.rid, i.ended.kind], ['3521000777', 'order']); assert(!('order' in i));
    eq(h.A.current(), []);
    tick(2500); a = await ask(); const wd = rows(a.body, 'welding'); assert.strictEqual(wd.state, 'idle', 'completed: the station is idle'); assert.strictEqual(wd.current.length, 0);
    const n = h.calls.length; tick(60000); await h.tick5(); await h.tick5(); assert.strictEqual(h.calls.length, n, 'nothing more is sent after idle');
    assert(h.cleared.length >= 1, 'the 5 s check stops when nothing is current');
    assert.strictEqual(h.A.idle('3521000777'), false, 'already idle');
    say('12 client: scan -> work, a beat every 30 s, a refresh every 2 min, completion -> idle; the board follows');
  }

  /* 13 · the same scan told twice, a new order, a rescan */
  {
    const s = fresh(); seed(s); const h = page();
    h.A.working({ rid: '3521000777' }); await h.zero(); const t0 = getNow();
    tick(2000); h.A.working({ rid: '3521000777' }); await h.zero();
    assert.deepStrictEqual(h.kinds(), ['work'], 'the same scan again inside 15 s: nothing new to say');
    tick(1000); h.A.working({ rid: '3521000777', customer: 'Sam P.', pieces: [{ id: '3521000777_1_1', sku: 'GF-12' }] }); await h.zero();
    assert.deepStrictEqual(h.kinds(), ['work', 'work'], 'more detail about the same scan is sent'); assert.strictEqual(h.live()[1].body.live.order.scannedAt, t0, 'it is still the same scan: the scan time is kept');
    h.A.working({ rid: '3521000777', note: 'x' }); await h.zero(); assert.strictEqual(h.live().length, 3);
    assert.strictEqual(h.live()[2].body.live.order.pieces.length, 1, 'a call that does not mention pieces keeps them');
    assert.strictEqual(h.live()[2].body.live.order.customer, 'Sam P.');
    tick(20000); h.A.working({ rid: '3521000777' }); await h.zero();
    assert.strictEqual(h.live().length, 4); assert.strictEqual(h.live()[3].body.live.order.scannedAt, getNow(), 'scanned again after 15 s: a new scan, a new time');
    assert.strictEqual(h.live()[3].body.live.order.pieces.length, 0, 'a new scan starts with what it says');
    tick(1000); h.A.working({ rid: '3521000778' }); await h.zero();
    assert.deepStrictEqual(h.kinds().slice(4), ['work'], 'another order replaces it with one write: no idle in between');
    assert.strictEqual(getCur().count('Station_Live'), 1); assert.strictEqual(getCur().get('Station_Live', docKey).rid, '3521000778');
    h.A.idle(); await h.zero();
    say('13 client: a repeat is not re-sent, more detail keeps the scan time, a rescan after 15 s is a new scan, a new order replaces one');
  }

  /* 14 · sign-out, another person, the quiet-time limit, pagehide */
  {
    const s = fresh(); seed(s); let h = page();
    h.A.working({ rid: '3521000777' }); await h.zero();
    h.who = null; tick(5000); await h.tick5();
    assert.strictEqual(h.kinds().pop(), 'idle', 'signed out: the order ends'); assert.strictEqual(h.live().pop().body.live.person, 'Tess Welder', 'under the name that was signed in');
    assert.strictEqual(getCur().get('Station_Live', docKey).state, 'idle'); eq(h.A.current(), []);
    // somebody else signs in at the same page: Tess's order ends under Tess's name; Ray starts clean
    h = page(); h.A.working({ rid: '3521000777' }); await h.zero();
    h.who = Object.assign({}, h.who, { person: 'Ray Welder', session: 'weld-1-ABCD-k2-QQQQ' }); tick(5000); await h.tick5();
    assert.strictEqual(h.live().pop().body.live.person, 'Tess Welder'); assert.strictEqual(h.kinds().pop(), 'idle'); eq(h.A.current(), []);
    assert.strictEqual(h.A.working({ rid: '3521000900' }), true); await h.zero(); assert.strictEqual(h.live().pop().body.live.person, 'Ray Welder');
    assert.strictEqual(getCur().get('Station_Live', 'welding__weld-1__Tess Welder').state, 'idle'); assert.strictEqual(getCur().get('Station_Live', 'welding__weld-1__Ray Welder').state, 'working');
    // quiet for longer than holdMs: the order ends by itself (welding scans ARE the completion, so the order is not "open" for long)
    h = page(); h.A.working({ rid: '3521000777', holdMs: 60000 }); await h.zero();
    tick(59000); await h.tick5(); assert.strictEqual(h.A.current().length, 1); h.A.touch(); tick(59000); await h.tick5(); assert.strictEqual(h.A.current().length, 1, 'touch() starts the quiet time again');
    tick(2000); await h.tick5(); assert.strictEqual(h.A.current().length, 0); assert.strictEqual(h.kinds().pop(), 'idle', 'quiet for holdMs: idle');
    h = page(); h.A.working({ rid: '3521000777' }); await h.zero(); for (let k = 0; k < 20; k++) { tick(30000); await h.tick5(); } assert.strictEqual(h.A.current().length, 0, 'the default quiet time is 10 minutes'); assert.strictEqual(h.kinds().pop(), 'idle');
    // closing the page: idle goes out by beacon at once
    h = page(); h.A.working({ rid: '3521000777' }); await h.zero(); const before = h.calls.length;
    h.w.dispatchEvent(new h.w.Event('pagehide'));
    assert.strictEqual(h.beacons.length, 1); assert.strictEqual(h.beacons[0].body.live.event, 'idle'); assert.strictEqual(h.beacons[0].url, '/.netlify/functions/firebaseOrders'); assert.strictEqual(h.calls.length, before);
    eq(h.A.current(), []);
    // (the beacon is the one request the test does not carry out itself: it reaches the door like any other)
    await post(h.beacons[0].body.live); assert.strictEqual(getCur().get('Station_Live', docKey).state, 'idle');
    // a page closed before its first request left the building owes nobody an idle
    h = page(); h.A.working({ rid: '3521000777' }); h.w.dispatchEvent(new h.w.Event('pagehide')); assert.strictEqual(h.beacons.length, 0);
    say('14 client: sign-out and another person end the order, quiet time ends it, pagehide sends idle by beacon');
  }

  /* 15 · failures, slowness, order of requests */
  {
    fresh(); let h = page(); let n = 0;
    h.reply = () => (++n <= 2 ? { status: 503, json: () => Promise.resolve({}) } : undefined);
    h.A.working({ rid: '3521000777' }); await h.zero(); assert.strictEqual(h.live().length, 1); assert.strictEqual(h.A.current()[0].sent, false, 'a 503 is not sent yet');
    await h.tick5(); assert.strictEqual(h.live().length, 1, 'backing off: not again at once');
    tick(6000); await h.tick5(); assert.strictEqual(h.live().length, 2, 'tried again after 5 s'); assert.strictEqual(h.A.current()[0].sent, false);
    tick(6000); await h.tick5(); assert.strictEqual(h.live().length, 2, 'the second failure backs off for 10 s');
    tick(5000); await h.tick5(); assert.strictEqual(h.live().length, 3); assert.strictEqual(h.A.current()[0].sent, true, 'the third try got through');
    assert.strictEqual(getCur().get('Station_Live', docKey).rid, '3521000777', 'the same order, still there');
    // a network error is the same
    h = page(); n = 0; h.reply = () => (++n === 1 ? new Error('network down') : undefined);
    h.A.working({ rid: '3521000777' }); await h.zero(); assert.strictEqual(h.A.current()[0].sent, false); tick(6000); await h.tick5(); assert.strictEqual(h.A.current()[0].sent, true);
    // a refusal (400) is final: not repeated
    h = page(); h.reply = () => ({ status: 400, json: () => Promise.resolve({ error: 'no' }) });
    h.A.working({ rid: '3521000777' }); await h.zero(); tick(60000); await h.tick5(); await h.tick5(); assert.strictEqual(h.live().filter(c => c.body.live.event === 'work').length, 1, 'a refused write is not sent again and again');
    // an idle that failed is kept and sent again; and the order of requests holds (one at a time)
    h = page(); h.A.working({ rid: '3521000777' }); await h.zero();
    let release; const slow = new Promise(r => { release = r; });
    h.reply = (body) => (body.live.event === 'idle' && !h.slowDone ? (h.slowDone = true, slow) : undefined);
    h.A.idle(); await h.zero(); assert.strictEqual(h.kinds().pop(), 'idle');
    h.A.working({ rid: '3521000999' }); await h.zero(); assert.strictEqual(h.live().length, 2, 'the next request waits for the one in flight');
    release({ status: 200, json: () => Promise.resolve({ success: true }) }); await h.settle(); await h.zero();
    assert.deepStrictEqual(h.kinds(), ['work', 'idle', 'work'], 'in order: the idle first, then the new order'); assert.strictEqual(getCur().get('Station_Live', docKey).rid, '3521000999');
    h.A.idle(); await h.zero();
    h = page(); n = 0; h.reply = body => (body.live.event === 'idle' && ++n === 1 ? { status: 500, json: () => Promise.resolve({}) } : undefined);
    h.A.working({ rid: '3521000777' }); await h.zero(); h.A.idle(); await h.zero(); assert.strictEqual(h.kinds().filter(k => k === 'idle').length, 1); tick(6000); await h.tick5();
    assert.strictEqual(h.kinds().filter(k => k === 'idle').length, 2, 'a lost idle is sent again'); assert.strictEqual(getCur().get('Station_Live', docKey).state, 'idle');
    // the server forgot the order (a beat answered "resend"): the whole order goes again at the next check
    h = page(); h.A.working({ rid: '3521000777' }); await h.zero(); getCur().colls.get('Station_Live').clear();
    tick(31000); await h.tick5(); assert.deepStrictEqual(h.kinds().slice(-2), ['beat', 'work'], 'the beat was answered "send it again": the whole order goes at once');
    assert.strictEqual(getCur().get('Station_Live', docKey).rid, '3521000777'); assert.strictEqual(h.A.current()[0].sent, true);
    // offline: the page keeps trying quietly and still never throws
    h = page(); h.reply = () => new Error('offline'); h.A.working({ rid: '3521000777' }); await h.zero(); assert.doesNotThrow(() => { h.A.working({ rid: '3521000778' }); h.A.idle(); h.A.touch(); });
    say('15 client: 503 and network errors back off and retry, a 400 is final, one request at a time in order, a lost idle is resent, a forgotten order is resent');
  }

  /* 16 · the sorter: two stations on one page, sandbox, an order of several pieces, and no PIN in anything sent */
  {
    const s = fresh(); seed(s);
    let h = page({ who: { person: 'Tess Welder', station: 'sorter', device: 'charm-nest-1', computer: 'pc-ABCDEFGHJKMN', session: 'sorter-ABCD-k1-XYZW', startAt: getNow() - 30000, sandbox: false } });
    h.A.working({ rid: '3521000003', pieces: [{ id: '3521000003_1_1', sku: 'GF-12' }] });
    h.A.working({ kind: 'sheet', station: 'laser', title: 'GF Sheet 2 · Set 4', orderNumber: 'GF Sheet 2' });
    await h.zero();
    assert.deepStrictEqual(h.live().map(c => c.body.live.station).sort(), ['laser', 'sorter'], 'the page\'s own station and the laser override, two documents');
    assert.strictEqual(h.live().find(c => c.body.live.station === 'laser').body.live.order.kind, 'sheet');
    let a = await ask(); assert.strictEqual(rows(a.body, 'sorting').current[0].rid, '3521000003'); assert.strictEqual(rows(a.body, 'laser').current[0].title, 'GF Sheet 2 · Set 4');
    assert.strictEqual(h.A.idle({ station: 'laser' }), true); await h.zero(); eq(h.A.current().map(c => c.station), ['sorter'], 'only the laser slot ended');
    tick(2500); a = await ask(); assert.strictEqual(rows(a.body, 'laser').current.length, 0); assert.strictEqual(rows(a.body, 'sorting').current.length, 1);
    h.A.idle(); await h.zero();
    // sandbox: its own door, its own collection, its own reader; the real board never sees it
    h = page({ sandbox: true, who: { person: 'Paul K.', station: 'sorter', device: 'charm-nest-1', computer: 'pc-ABCDEFGHJKMN', session: 'sorter-ABCD-k9-SSSS', startAt: getNow() - 30000, sandbox: true } });
    h.A.working({ rid: '3521000066' }); await h.zero();
    assert.strictEqual(h.live()[0].url, '/.netlify/functions/firebaseOrders?sandbox=1'); assert.strictEqual(h.live()[0].body.live.sandbox, true);
    assert.strictEqual(getCur().count('Station_Live'), 2, 'the real collection holds only the earlier two (the laser and sorter documents, now idle)'); assert.strictEqual(getCur().get(SB + 'Station_Live', 'sorter__charm-nest-1__Paul K.').rid, '3521000066');
    tick(2500); const real = await ask(), sand = await ask({ sandbox: true });
    assert.deepStrictEqual(rows(real.body, 'sorting').current.map(c => c.rid), []); assert.deepStrictEqual(rows(sand.body, 'sorting').current.map(c => c.rid), ['3521000066']);
    h.A.idle(); await h.zero(); assert.strictEqual(h.live().pop().url, '/.netlify/functions/firebaseOrders?sandbox=1');
    // an order of several pieces: every piece is listed (up to 24), the count is kept, a PIN-looking value never leaves the page
    h = page(); const many = Array.from({ length: 30 }, (_, i) => ({ id: `3521000555_1_${i + 1}`, label: i === 0 ? 'badge ' + PIN : 'Piece ' + (i + 1), sku: 'GF-12' }));
    h.A.working({ rid: '3521000555', pieces: many, pieceCount: 30, customer: PIN, note: 'scanned by ' + PIN }); await h.zero();
    const sent = h.live()[0].body.live.order;
    assert.strictEqual(sent.pieces.length, 24); assert.strictEqual(sent.pieceCount, 30); assert.strictEqual(sent.pieces[0].label, 'badge [#]'); assert.strictEqual(sent.customer, ''); assert.strictEqual(sent.note, 'scanned by [#]');
    assert(!JSON.stringify(h.calls).includes(PIN), 'no PIN in anything the page sent');
    tick(2500); a = await ask(); assert.strictEqual(rows(a.body, 'welding').current[0].pieces.length, 24); assert.strictEqual(rows(a.body, 'welding').current[0].pieceCount, 30);
    assert(!a.raw.includes(PIN) && !logs.join('\n').includes(PIN));
    say('16 client: sorter + laser on one page, sandbox on its own door, a 30-piece order, no PIN in what is sent');
  }
};
