// The live station layer: what each station is working on RIGHT NOW (plans/employee-hr/api.md).
//   client  station-activity.js  StationActivity.working() / idle() / touch(): a `work` write, a 30 s `beat`, an `idle`, one request
//           at a time, retries, sign-out, pagehide, sandbox, no PIN, never throws
//   door    firebaseOrders {live} -> _stationLive.write(): one small document per station + page + person in Station_Live
//           (Sandbox_Station_Live), overwritten; validation; coalescing; no PIN in anything stored
//   reader  employeeEfficiency {op:"live"} -> _stationLive.op(): the current orders with pieces, thumbnails and the QR text,
//           who is signed in, per-station state and counts; completion clears; a closed tab expires; two people at one
//           station; real and sandbox never mixed; the cost of a poll
// Everything offline: Firestore is an in-memory fake, the clock is faked, every passcode and PIN here is synthetic.
//   JSDOM_DIR=<...>/node_modules node tests/stations/station-live.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── fake Firestore: typed enough for where/orderBy/limit/select, doc get/set/update, getAll, counted reads and writes ── */
const store = () => {
  const colls = new Map(), reads = [], writes = [];
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const clone = v => v == null ? v : JSON.parse(JSON.stringify(v));
  const notFound = () => Object.assign(new Error('5 NOT_FOUND: No document to update'), { code: 5 });
  function query(name, filters, order, lim, sel) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
      limit: n => query(name, filters, order, n, sel),
      select: (...f) => query(name, filters, order, lim, f),
      get: async () => {
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || typeof x !== typeof v) return false;
          return op === '==' ? x === v : op === '>=' ? x >= v : op === '>' ? x > v : op === '<' ? x < v : op === '<=' ? x <= v : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (p.d[f] < q.d[f] ? -1 : p.d[f] > q.d[f] ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: docs.length, filters: filters.map(f => f[0] + f[1]), select: sel || null });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const ref = (name, id) => ({ id, name,
    get: async () => { reads.push({ name, doc: id }); const d = data(name).get(id); return { exists: !!d, id, data: () => clone(d) }; },
    set: async (v, o) => { if (name !== 'Station_Rev') writes.push([name, id, 'set']); const prev = data(name).get(id) || {}, nv = clone(v); for (const k of Object.keys(nv)) if (nv[k] && typeof nv[k] === 'object' && '__inc' in nv[k]) nv[k] = (typeof prev[k] === 'number' ? prev[k] : 0) + nv[k].__inc; data(name).set(id, o && o.merge ? Object.assign({}, prev, nv) : nv); },   // (a top-level increment adds, as Firestore does: the revision counters, FC5)
    update: async v => { writes.push([name, id, 'update']); if (!data(name).has(id)) throw notFound(); data(name).set(id, Object.assign({}, data(name).get(id), clone(v))); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null, null), { doc: id => ref(name, id) }),
    getAll: async (...a) => { const refs = a.filter(x => x && x.get); return Promise.all(refs.map(r => r.get())); },
    runTransaction: async fn => fn({ get: r => r.get(), getAll: (...rs) => Promise.all(rs.map(r => r.get())), set: (r, v, o) => r.set(v, o) })
  };
  return { db, put: (name, id, d) => data(name).set(id, clone(d)), get: (name, id) => data(name).get(id), all: name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, d)),
    reads, writes, readsOf: name => reads.filter(r => r.name === name), writesOf: name => writes.filter(w => w[0] === name), reset: () => { reads.length = 0; writes.length = 0; }, colls, count: name => data(name).size };
};

/* the modules under test, over a fake admin that hands every module the current store */
let cur = store();
const dbNow = { collection: n => cur.db.collection(n), getAll: (...a) => cur.db.getAll(...a), runTransaction: f => cur.db.runTransaction(f) };   // (what the door captured at load follows the store of the moment)
const fakeAdmin = { firestore: Object.assign(() => dbNow, { FieldValue: { serverTimestamp: () => 'ts', increment: n => ({ __inc: n }), delete: () => null }, Timestamp: { fromMillis: m => m } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const L = require(path.join(root, 'netlify/functions/_stationLive.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;

const PASS = 'synthetic-pass-9f3k';
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
console.warn = console.log = console.error = console.info = keepLog;
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const realNow = Date.now; let NOW = Date.parse('2026-10-05T15:00:00Z');       // 11:00 on 5 Oct in New York (EDT)
Date.now = () => NOW;
const tick = ms => { NOW += ms; };
const Z = iso => Date.parse(iso);
const PIN = '482915', PIN2 = '7340';                                              // synthetic stand-ins for an Employee Number
let ipN = 0;

const post = async (live, o = {}) => {
  const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': o.ip || '198.51.100.' + (++ipN % 250) }, queryStringParameters: o.sandbox ? { sandbox: '1' } : {}, body: o.raw != null ? o.raw : JSON.stringify({ live }) });
  return { status: r.statusCode, body: JSON.parse(r.body || '{}') };
};
const ask = async (body = {}, o = {}) => {
  const r = await eff._t.handle({ httpMethod: o.method || 'POST', headers: { 'x-nf-client-connection-ip': o.ip || '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(Object.assign({ op: 'live', key: PASS }, body)) }, cur.db);
  return { status: r.statusCode, headers: r.headers, raw: r.body, body: JSON.parse(r.body || '{}') };
};
const fresh = () => { cur = store(); EP.resetCache(); for (const k of Object.keys(L._t.seen)) delete L._t.seen[k]; L._t.seen.clear(); return cur; };
const station = (out, key) => out.body.stations.find(s => s.key === key);
const SB = 'Sandbox_';
const URL_GF = 'https://firebasestorage.googleapis.com/v0/b/shop/o/thumbs%2Fgf-12.png?alt=media&token=t1';
const URL_GF_L = 'https://firebasestorage.googleapis.com/v0/b/shop/o/thumbs%2Fgf-12-L.png?alt=media&token=t2';
const URL_PHOTO = 'https://i.etsystatic.com/111/r/il/abc/1/il_570xN.1_xyz.jpg';
const URL_ARCH = 'https://storage.googleapis.com/shop/design-archive/listing/aa.jpg';
const seed = s => {
  s.put('Charm_Master_Index', 'GF-12', { sku: 'GF-12', thumbUrl: URL_GF, sizes: { L: { thumbUrl: URL_GF_L } } });
  s.put('Charm_Master_Index', 'AB-7', { sku: 'AB-7', thumbUrl: '' });
  s.put('Etsy_Listing_Image_Cache', '4455667788', { images: [{ rank: 2, url_570xN: 'https://i.etsystatic.com/second.jpg' }, { rank: 1, url_570xN: URL_PHOTO }] });
  s.put('Design_Order_Archive', '3521000999', { buyer: { name: 'Dana Q.' }, items: [{ transactionId: '8001', sku: 'GF-12', mirrorUrl: URL_ARCH }, { transactionId: '8002', sku: 'AB-7', imageUrl: URL_PHOTO }] });
};
const W = (o = {}) => Object.assign({ v: 1, event: 'work', station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW', person: 'Tess Welder', startAt: NOW - 3600e3,
  order: { kind: 'order', rid: '3521000777', orderNumber: '3521000777', customer: 'Sam P.', scannedAt: NOW, note: 'phone scan',
    pieces: [{ id: '3521000777_9001_1', label: 'Stud earrings, gold', sku: 'GF-12', listingId: '4455667788', size: 'L' }, { id: '3521000777_9001_2', label: 'Stud earrings, gold', sku: 'GF-12', listingId: '4455667788' }], pieceCount: 2 } }, o);
const I = (o = {}) => Object.assign({ v: 1, event: 'idle', station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW', person: 'Tess Welder', startAt: NOW - 3600e3,
  ended: { kind: 'order', rid: '3521000777', orderNumber: '3521000777', scannedAt: NOW - 60000 } }, o);
const B = (o = {}) => Object.assign({ v: 1, event: 'beat', station: 'welding', device: 'weld-1', person: 'Tess Welder' }, o);

(async () => {
  /* 1 · a heartbeat becomes one small document; the reader shows the order with pieces, thumbnails and the QR text */
  {
    const s = fresh(); seed(s);
    const r = await post(W());
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.written, 1);
    assert.strictEqual(s.count('Station_Live'), 1, 'one document');
    const id = 'welding__weld-1__Tess Welder', d = s.get('Station_Live', id);
    assert(d, 'the document id is station__device__person');
    assert.strictEqual(d.state, 'working'); assert.strictEqual(d.rid, '3521000777'); assert.strictEqual(d.person, 'Tess Welder'); assert.strictEqual(d.beatAt, NOW);
    assert.strictEqual(d.pieces.length, 2); assert.strictEqual(d.pieces[0].sku, 'GF-12'); assert.strictEqual(d.pieces[0].listingId, '4455667788');
    assert(!('sandbox' in d), 'no sandbox flag in production');
    // sessions and rollups the reader joins
    s.put('Station_Sessions', 's1', { person: 'Tess Welder', station: 'welding', device: 'weld-1', startAt: NOW - 3600e3, lastSeenAt: NOW - 60000, endAt: null });
    s.put('Station_Sessions', 's2', { person: 'Ray Welder', station: 'assembly', device: 'assembly-2', startAt: NOW - 7200e3, lastSeenAt: NOW - 120000, endAt: null });
    s.put('Station_Sessions', 's3', { person: 'Old Hand', station: 'shipping', device: 'shipping-1', startAt: NOW - 9000e3, lastSeenAt: NOW - 40 * 60000, endAt: null });      // closed without an end: not on now
    s.put('Station_Sessions', 's4', { person: 'Gone Away', station: 'design', device: 'design-1', startAt: NOW - 9000e3, lastSeenAt: NOW - 3000e3, endAt: NOW - 3000e3 });
    s.put('Efficiency_Daily', '2026-10-05__Tess Welder', { day: '2026-10-05', person: 'Tess Welder', stations: { welding: { parts: 14, undoParts: 2, orders: 7, scans: 9, lastAt: NOW - 90000 } } });
    s.put('Efficiency_Daily', '2026-10-05__Ray Welder', { day: '2026-10-05', person: 'Ray Welder', stations: { welding: { parts: 3, orders: 1, scans: 2, lastAt: NOW - 600000 }, assembly: { parts: 5, orders: 5, scans: 6, lastAt: NOW - 300000 } } });
    const a = await ask();
    assert.strictEqual(a.status, 200); assert.strictEqual(a.body.ok, true); assert.strictEqual(a.body.mode, 'real'); assert.strictEqual(a.body.at, NOW);
    assert.strictEqual(a.headers['Cache-Control'], 'no-store');
    assert.deepStrictEqual(a.body.stations.map(x => x.key), ['sorting', 'welding', 'assembly', 'shipping', 'design', 'laser', 'inbox'], 'every station is listed, working or not (the Sorter app and the QR Printer are pages of Sorting, not stations of their own)');
    const wd = station(a, 'welding');
    assert.strictEqual(wd.label, 'Welding'); assert.strictEqual(wd.state, 'working'); assert.deepStrictEqual(wd.names, ['Tess Welder']);
    assert.strictEqual(wd.current.length, 1);
    const c = wd.current[0];
    assert.strictEqual(c.person, 'Tess Welder'); assert.strictEqual(c.rid, '3521000777'); assert.strictEqual(c.orderNumber, '3521000777'); assert.strictEqual(c.customer, 'Sam P.');
    assert.strictEqual(c.scannedAt, NOW); assert.strictEqual(c.device, 'weld-1'); assert.strictEqual(c.deviceLabel, 'Welding'); assert.strictEqual(c.note, 'phone scan'); assert.strictEqual(c.kind, 'order');
    assert.deepStrictEqual(c.qr, { text: '3521000777' }, 'the QR holds the order number, as the order sticker does');
    assert.strictEqual(c.pieces.length, 2); assert.strictEqual(c.pieceCount, 2);
    assert.strictEqual(c.pieces[0].thumbUrl, URL_GF_L, 'the vector design of that size'); assert.strictEqual(c.pieces[0].vectorUrl, URL_GF_L); assert.strictEqual(c.pieces[0].photoUrl, URL_PHOTO, 'the listing photo, rank 1');
    assert.strictEqual(c.pieces[1].thumbUrl, URL_GF, 'no size: the design\'s own thumbnail'); assert.strictEqual(c.pieces[1].label, 'Stud earrings, gold'); assert.strictEqual(c.pieces[1].id, '3521000777_9001_2');
    assert.strictEqual(c.thumbUrl, URL_GF_L, 'the order\'s thumbnail is its first piece\'s design'); assert.strictEqual(c.photoUrl, URL_PHOTO);
    assert(!('listingId' in c.pieces[0]) && !('size' in c.pieces[0]), 'the lookup hints do not leave the server');
    assert.deepStrictEqual(wd.counts, { partsToday: null, ordersToday: null, scansToday: 0 }, 'the Welding station is not counted in throughput: no pieces, no orders today (its matched scans are counted instead; see welding-portal.cjs)');
    assert.strictEqual(wd.lastEventAt, NOW, 'the current scan is the latest event');
    assert.strictEqual(wd.devices.find(x => x.device === 'weld-1').state, 'working');
    const as = station(a, 'assembly');
    assert.strictEqual(as.state, 'idle', 'signed in, nothing in hand'); assert.deepStrictEqual(as.names, ['Ray Welder']); assert.strictEqual(as.current.length, 0);
    assert.deepStrictEqual(as.devices.map(x => [x.device, x.state]), [['assembly-1', 'offline'], ['assembly-2', 'idle'], ['assembly-3', 'offline'], ['assembly-4', 'offline']]);
    assert.strictEqual(as.lastEventAt, NOW - 300000);
    assert.strictEqual(station(a, 'shipping').state, 'offline', 'a session that went quiet for 15 minutes is not on'); assert.strictEqual(station(a, 'design').state, 'offline', 'an ended session is not on');
    assert.deepStrictEqual(a.body.signedIn.map(x => [x.name, x.stationKey, x.device]).sort(), [['Ray Welder', 'assembly', 'assembly-2'], ['Tess Welder', 'welding', 'weld-1']]);
    assert(a.body.signedIn.every(x => x.since > 0 && x.lastSeenAt > 0));
    assert.strictEqual(a.body.keepAliveMs, 30000); assert.strictEqual(a.body.staleMs, 180000);
    assert(!a.body.partial, 'nothing partial');
    say('1 a heartbeat -> one document -> the board: order, pieces, thumbnails, QR, people, counts');
  }

  /* 2 · thumbnails and the customer come from stored data only; a bare order falls back to the archive; no Etsy */
  {
    const s = fresh(); seed(s);
    await post(W({ order: { kind: 'order', rid: '3521000999', scannedAt: NOW } }), {});
    const a = await ask(), c = station(a, 'welding').current[0];
    assert.strictEqual(c.customer, 'Dana Q.', 'the buyer\'s name from the saved order'); assert.strictEqual(c.pieces.length, 2, 'the saved items stand in for pieces');
    assert.strictEqual(c.pieces[0].id, '3521000999_8001_1'); assert.strictEqual(c.pieces[0].thumbUrl, URL_GF); assert.strictEqual(c.pieces[0].photoUrl, URL_ARCH);
    assert.strictEqual(c.pieces[1].thumbUrl, URL_PHOTO, 'a SKU with no design thumbnail shows the saved picture'); assert.strictEqual(c.pieces[1].vectorUrl, '');
    assert.strictEqual(c.thumbUrl, URL_GF);
    // an order nothing is stored for: no thumbnails, no customer, still a card
    await post(W({ person: 'Ray Welder', device: 'weld-1', order: { kind: 'order', rid: '3521000111', scannedAt: NOW } }));
    tick(3000); const b = await ask(), x = station(b, 'welding').current.find(q => q.rid === '3521000111');
    assert.strictEqual(x.thumbUrl, ''); assert.strictEqual(x.customer, ''); assert.deepStrictEqual(x.pieces, []); assert.deepStrictEqual(x.qr, { text: '3521000111' });
    // the lookups are kept: a second poll reads none of them again
    s.reset(); tick(3000); await ask();
    assert.strictEqual(s.readsOf('Charm_Master_Index').length + s.readsOf('Etsy_Listing_Image_Cache').length + s.readsOf('Design_Order_Archive').length, 0, 'thumbnails and archive are kept 15 minutes (a miss 1 minute)');
    say('2 thumbnails and the customer from stored data (master index, listing cache, saved order); lookups kept');
  }

  /* 3 · completion clears; the last order is kept for the card */
  {
    const s = fresh(); seed(s);
    s.put('Station_Sessions', 's1', { person: 'Tess Welder', station: 'welding', device: 'weld-1', startAt: NOW - 3600e3, lastSeenAt: NOW - 30000, endAt: null });
    await post(W()); tick(40000);
    let a = await ask(); assert.strictEqual(station(a, 'welding').state, 'working');
    const r = await post(I()); assert.strictEqual(r.status, 200); assert.strictEqual(r.body.written, 1);
    const d = s.get('Station_Live', 'welding__weld-1__Tess Welder');
    assert.strictEqual(d.state, 'idle'); assert.strictEqual(d.last.rid, '3521000777'); assert.strictEqual(d.rid, undefined, 'the order itself is gone from the document');
    tick(3000); a = await ask();
    const wd = station(a, 'welding'); assert.strictEqual(wd.state, 'idle', 'signed in, nothing in hand'); assert.strictEqual(wd.current.length, 0);
    assert.strictEqual(wd.lastEventAt, NOW - 3000, 'the completion is the latest event');
    // a new order after it works again, and replaces the document
    await post(W({ order: { kind: 'order', rid: '3521000888', scannedAt: NOW } })); tick(3000);
    a = await ask(); assert.strictEqual(station(a, 'welding').current[0].rid, '3521000888'); assert.strictEqual(s.count('Station_Live'), 1, 'still one document: overwritten, no growth');
    say('3 completion clears the order (idle, last order kept); a new order overwrites the same document');
  }

  /* 4 · a closed tab expires; a beat keeps an order alive; a woken tab comes back */
  {
    const s = fresh(); seed(s);
    s.put('Station_Sessions', 's1', { person: 'Tess Welder', station: 'welding', device: 'weld-1', startAt: NOW - 3600e3, lastSeenAt: NOW, endAt: null });
    await post(W());
    for (let i = 0; i < 5; i++) { tick(30000); const b = await post(B()); assert.strictEqual(b.status, 200); }          // 2.5 minutes of keep-alives
    let a = await ask(); assert.strictEqual(station(a, 'welding').state, 'working', 'a beat 30 s ago: still working');
    assert.strictEqual(s.get('Station_Live', 'welding__weld-1__Tess Welder').beatAt, NOW);
    tick(170000); a = await ask(); assert.strictEqual(station(a, 'welding').state, 'working', 'a beat 170 s ago: still inside the window');
    tick(15000); a = await ask(); const wd = station(a, 'welding');
    assert.strictEqual(wd.state, 'idle', 'no beat for 185 s: the order is gone (a closed tab leaves no ghost)'); assert.strictEqual(wd.current.length, 0);
    assert.strictEqual(a.body.signedIn.length, 1, 'the person is still signed in (the session says so)');
    // a beat for a document that is not there says "send it all again"
    const r = await post(B({ person: 'Nobody Here' })); assert.deepStrictEqual(r.body, { success: true, resend: true });
    assert.strictEqual(s.count('Station_Live'), 1, 'a beat never creates a document');
    // the same tab woke up (a throttled timer): its beat brings the order back
    tick(46000); await post(B()); a = await ask();   // (a keep-alive is not a counted change: the board looks at the live documents again within 45 s, FC5)
     assert.strictEqual(station(a, 'welding').state, 'working');
    say('4 a closed tab expires after 3 minutes; beats keep it alive; a beat for a missing document asks for a resend');
  }

  /* 5 · two people at one station, two stations for one person */
  {
    const s = fresh(); seed(s);
    s.put('Station_Sessions', 's1', { person: 'Tess Welder', station: 'assembly', device: 'assembly-1', startAt: NOW - 3600e3, lastSeenAt: NOW, endAt: null });
    s.put('Station_Sessions', 's2', { person: 'Ray Welder', station: 'assembly', device: 'assembly-2', startAt: NOW - 3000e3, lastSeenAt: NOW, endAt: null });
    s.put('Station_Sessions', 's3', { person: 'Mia Packer', station: 'assembly', device: 'assembly-3', startAt: NOW - 3000e3, lastSeenAt: NOW, endAt: null });
    await post(W({ station: 'assembly', device: 'assembly-1', person: 'Tess Welder', order: { kind: 'order', rid: '3521000001', scannedAt: NOW - 90000, pieces: [{ id: '3521000001_1_1', sku: 'GF-12' }] } }));
    await post(W({ station: 'assembly', device: 'assembly-2', person: 'Ray Welder', order: { kind: 'order', rid: '3521000002', scannedAt: NOW - 30000 } }));
    const a = await ask(), as = station(a, 'assembly');
    assert.strictEqual(as.state, 'working'); assert.strictEqual(as.current.length, 2); assert.deepStrictEqual(as.current.map(c => c.person), ['Ray Welder', 'Tess Welder'], 'newest scan first');
    assert.deepStrictEqual(as.names.sort(), ['Mia Packer', 'Ray Welder', 'Tess Welder']);
    assert.deepStrictEqual(as.devices.map(x => [x.device, x.state, x.person]), [['assembly-1', 'working', 'Tess Welder'], ['assembly-2', 'working', 'Ray Welder'], ['assembly-3', 'idle', 'Mia Packer'], ['assembly-4', 'offline', '']]);
    assert.strictEqual(s.count('Station_Live'), 2, 'one document per person and page');
    // one person, two stations on one page (the sorter and its laser): two documents, both shown
    await post(W({ station: 'sorter', device: 'charm-nest-1', person: 'Tess Welder', order: { kind: 'order', rid: '3521000003', scannedAt: NOW } }));
    await post(W({ station: 'laser', device: 'charm-nest-1', person: 'Tess Welder', order: { kind: 'sheet', title: 'GF Sheet 2', scannedAt: NOW } }));
    tick(3000); const b = await ask();
    assert.strictEqual(station(b, 'sorting').current[0].rid, '3521000003', 'the Sorter app is a page of Sorting'); assert.strictEqual(station(b, 'sorting').current[0].deviceLabel, 'Sorter (nesting)'); assert(!b.body.stations.some(x => x.key === 'sorter' || x.key === 'qr'), 'no Sorter or QR Printer station'); const lz = station(b, 'laser');
    assert.strictEqual(lz.state, 'working'); assert.strictEqual(lz.current[0].kind, 'sheet'); assert.strictEqual(lz.current[0].title, 'GF Sheet 2'); assert.strictEqual(lz.current[0].qr, null, 'a sheet has no order QR');
    assert.strictEqual(s.count('Station_Live'), 4);
    // the same name spelled two ways is one person on the board
    await post(W({ station: 'shipping', device: 'shipping-1', person: 'Michael_V', order: { kind: 'order', rid: '3521000004', scannedAt: NOW } }));
    await post(W({ station: 'shipping', device: 'shipping-2', person: 'Michael V.', order: { kind: 'order', rid: '3521000005', scannedAt: NOW } }));
    tick(3000); const c = await ask(); const sh = station(c, 'shipping');
    assert.deepStrictEqual(sh.names, ['Michael V.'], 'one person'); assert.deepStrictEqual(sh.current.map(x => x.person), ['Michael V.', 'Michael V.']);
    say('5 two people at one station, one person at two stations, one name spelled two ways');
  }

  /* 6 · real and sandbox never mix */
  {
    const s = fresh(); seed(s);
    const r1 = await post(W({ person: 'Real Rita', order: { kind: 'order', rid: '3521000010', scannedAt: NOW } }));
    const r2 = await post(W({ person: 'Sandy Box', sandbox: true, order: { kind: 'order', rid: '3521000020', scannedAt: NOW } }), { sandbox: true });
    assert.strictEqual(r1.status, 200); assert.strictEqual(r2.status, 200);
    assert.strictEqual(s.count('Station_Live'), 1); assert.strictEqual(s.count(SB + 'Station_Live'), 1);
    assert.strictEqual(s.get(SB + 'Station_Live', 'welding__weld-1__Sandy Box').sandbox, true);
    const real = await ask(), sand = await ask({ sandbox: true });
    assert.strictEqual(real.body.mode, 'real'); assert.strictEqual(sand.body.mode, 'sandbox');
    assert.deepStrictEqual(station(real, 'welding').current.map(c => c.person), ['Real Rita']); assert.deepStrictEqual(station(sand, 'welding').current.map(c => c.person), ['Sandy Box']);
    // an event that says one store cannot be written to the other door
    const x = await post(W({ person: 'Wrong Door', sandbox: true })); assert.strictEqual(x.status, 400);
    const y = await post(W({ person: 'Wrong Door', sandbox: false }), { sandbox: true }); assert.strictEqual(y.status, 400);
    assert.strictEqual(s.count('Station_Live'), 1); assert.strictEqual(s.count(SB + 'Station_Live'), 1);
    // a sandbox document that landed in the real collection (by hand) is not shown there
    s.put('Station_Live', 'welding__weld-1__Stray', { id: 'x', station: 'welding', device: 'weld-1', person: 'Stray', state: 'working', rid: '3521000030', scannedAt: NOW, beatAt: NOW, sandbox: true, pieces: [] });
    tick(3000); const again = await ask(); assert.deepStrictEqual(station(again, 'welding').current.map(c => c.person), ['Real Rita']);
    // the sandbox's sessions and numbers are its own
    s.put(SB + 'Station_Sessions', 'q1', { person: 'Sandy Box', station: 'welding', device: 'weld-1', startAt: NOW - 1000, lastSeenAt: NOW, endAt: null });
    tick(20000); const sa = await ask({ sandbox: true }), ra = await ask();
    assert.deepStrictEqual(sa.body.signedIn.map(p => p.name), ['Sandy Box']); assert.deepStrictEqual(ra.body.signedIn.map(p => p.name), ['Real Rita'], 'a person working is signed in there; the sandbox session is not');
    say('6 real and sandbox: separate collections, separate readers, a wrong-door event refused');
  }

  /* 7 · writes are coalesced and cheap */
  {
    const s = fresh(); seed(s);
    const w0 = W(); await post(w0); assert.strictEqual(s.writes.length, 1);
    tick(2000); const dup = await post(w0); assert.strictEqual(dup.body.coalesced, true); assert.strictEqual(s.writes.length, 1, 'the same scan again inside 15 s: not written again');
    tick(1000); const b1 = await post(B()); assert.strictEqual(b1.body.coalesced, true); assert.strictEqual(s.writes.length, 1, 'a beat inside 15 s of the last write: not written');
    tick(13000); const b2 = await post(B()); assert.strictEqual(b2.body.written, 1); assert.strictEqual(s.writes.length, 2, 'a beat after 15 s is one write');
    assert.deepStrictEqual(s.writes.map(w => w[2]), ['set', 'update'], 'a beat is a one-field update, not a rewrite');
    tick(1000); const b3 = await post(B()); assert.strictEqual(b3.body.coalesced, true);
    tick(1000); await post(W({ order: { kind: 'order', rid: '3521000778', scannedAt: NOW } })); assert.strictEqual(s.writes.length, 3, 'a different order is written at once');
    tick(500); await post(W({ order: { kind: 'order', rid: '3521000778', scannedAt: NOW - 500, customer: 'New Name', pieces: [{ id: 'a_1_1', sku: 'GF-12' }] } })); assert.strictEqual(s.writes.length, 4, 'more detail about the order is written (the order changed)');
    tick(1000); await post(I({ ended: { kind: 'order', rid: '3521000778', scannedAt: NOW - 2000 } })); assert.strictEqual(s.writes.length, 5);
    tick(1000); const dupIdle = await post(I({ ended: { kind: 'order', rid: '3521000778', scannedAt: NOW - 3000 } })); assert.strictEqual(dupIdle.body.coalesced, true, 'an idle sent twice is written once'); assert.strictEqual(s.writes.length, 5);
    // a flood from one address is slowed
    let limited = 0; for (let i = 0; i < 700; i++) { const x = await post(B({ person: 'Flood ' + (i % 5) }), { ip: '192.0.2.77' }); if (x.status === 429) limited++; }
    assert(limited > 0, 'the open door has a flood limit');
    say('7 coalescing: a repeat inside 15 s is not written, a beat is a one-field update, order changes are written at once, a flood is limited');
  }

  /* 8 · no PIN anywhere */
  {
    const s = fresh(); seed(s);
    for (const person of [PIN, PIN2, '12-34-56', '123 456', '+', '']) { const r = await post(W({ person })); assert.strictEqual(r.status, 400, `"${person}" is never a name`); }
    assert.strictEqual((await post(W({ order: { kind: 'order', rid: PIN2, scannedAt: NOW } }))).status, 400, 'a PIN-looking order id is refused');
    assert.strictEqual((await post(W({ order: { kind: 'order', rid: PIN, scannedAt: NOW } }))).status, 400);
    assert.strictEqual((await post(W({ order: { kind: 'order', rid: '', scannedAt: NOW } }))).status, 400, 'an order needs an id');
    assert.strictEqual((await post(W({ order: { kind: 'sheet', scannedAt: NOW } }))).status, 400, 'a sheet needs a title');
    assert.strictEqual(s.count('Station_Live'), 0, 'nothing stored for any of them');
    const r = await post(W({ order: { kind: 'order', rid: '3521000777', orderNumber: PIN, customer: PIN, note: `scanned by ${PIN}`, title: `x ${PIN} y`, scannedAt: NOW,
      pieces: [{ id: PIN2, label: `badge ${PIN}`, sku: PIN, listingId: PIN2 }, { id: '3521000777_1_1', label: PIN, sku: 'GF-12' }] } }));
    assert.strictEqual(r.status, 200);
    const d = s.get('Station_Live', 'welding__weld-1__Tess Welder'), text = JSON.stringify(d);
    assert(!text.includes(PIN) && !text.includes(PIN2), 'a PIN-looking value is blanked or hidden in every field of the document: ' + text);
    assert.strictEqual(d.orderNumber, '3521000777', 'the order number falls back to the order id'); assert.strictEqual(d.customer, ''); assert.strictEqual(d.note, 'scanned by [#]');
    assert.strictEqual(d.pieces.length, 2); assert.strictEqual(d.pieces[0].id, ''); assert.strictEqual(d.pieces[0].label, 'badge [#]'); assert(!('sku' in d.pieces[0]) && !('listingId' in d.pieces[0]));
    const a = await ask();
    assert(!a.raw.includes(PIN) && !a.raw.includes(PIN2), 'the board\'s answer has no PIN');
    assert(!logs.join('\n').includes(PIN) && !logs.join('\n').includes(PIN2), 'no PIN in the log');
    // unknown fields and a wrong shape
    assert.strictEqual((await post(W({ station: 'nowhere' }))).status, 400); assert.strictEqual((await post(W({ event: 'poke' }))).status, 400); assert.strictEqual((await post(W({ device: '' }))).status, 400);
    const n0 = s.count('Station_Live'); assert((await post(null, { raw: JSON.stringify({ live: [1] }) })).status < 500, 'an array is not a live event (it falls through to the old door, which stores nothing)'); assert.strictEqual(s.count('Station_Live'), n0);
    const big = await post(null, { raw: JSON.stringify({ live: W({ order: { kind: 'order', rid: '3521000777', note: 'x'.repeat(9000) } }) }) }); assert.strictEqual(big.status, 413);
    const many = await post(W({ order: { kind: 'order', rid: '3521000555', scannedAt: NOW, pieces: Array.from({ length: 60 }, (_, i) => ({ id: `3521000555_1_${i + 1}`, label: 'p' + i })), pieceCount: 60 } }));
    assert.strictEqual(many.status, 200); assert.strictEqual(s.get('Station_Live', 'welding__weld-1__Tess Welder').pieces.length, 24, 'at most 24 pieces are kept'); assert.strictEqual(s.get('Station_Live', 'welding__weld-1__Tess Welder').pieceCount, 60, 'the count says how many there are');
    say('8 no PIN: a digits-only name, id, customer, note and label are refused, blanked or hidden; limits hold');
  }

  /* 9 · the gate, and the cost of a poll */
  {
    const s = fresh(); seed(s);
    await post(W());
    s.put('Station_Sessions', 's1', { person: 'Tess Welder', station: 'welding', device: 'weld-1', startAt: NOW - 3600e3, lastSeenAt: NOW, endAt: null });
    const noKey = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.9' }, body: JSON.stringify({ op: 'live' }) }, cur.db);
    assert.strictEqual(noKey.statusCode, 401); assert(!noKey.body.includes('3521000777'), 'no data without the key');
    const bad = await ask({ key: 'nope' }); assert.strictEqual(bad.status, 401);
    const get = await ask({}, { method: 'GET' }); assert.strictEqual(get.status, 405);
    s.reset();
    await ask(); const first = s.reads.slice();
    assert.strictEqual(s.readsOf('Station_Live').length, 1, 'one small query for the live documents'); assert.deepStrictEqual(s.readsOf('Station_Live')[0].filters, ['beatAt>='], 'a single range: no index to create');
    assert.strictEqual(s.readsOf('Station_Sessions').length, 1); assert.strictEqual(s.readsOf('Efficiency_Daily').length, 1); assert.deepStrictEqual(s.readsOf('Efficiency_Daily')[0].select, ['day', 'person', 'stations', 'sandbox', 'touched', 'devices'], 'only the fields the board shows (the store flag, and the orders touched, so an order in hand counts; and the store flag, so a document of the other store never counts; the desks of Assembly and Shipping)');
    s.reset(); tick(1000); await ask(); assert.strictEqual(s.reads.filter(r => r.name !== 'Station_Rev').length, 0, 'a second poll inside 2 s reads nothing (but the revision probe, FC5)');
    // FC5: nothing was written (the revision has not moved), so nothing is read again: the live documents are kept 45 s, sessions 1 minute, today numbers 2 minutes (a keep-alive beat is not a change)
    tick(1500); s.reset(); await ask();
    assert.strictEqual(s.readsOf('Station_Live').length, 0, 'an unmoved revision: the live documents are kept'); assert.strictEqual(s.readsOf('Station_Sessions').length, 0, 'sessions are kept'); assert.strictEqual(s.readsOf('Efficiency_Daily').length, 0, 'today\'s numbers are kept');
    // a change (an order started) is read at once, and only what it can change
    await post(W({ order: { kind: 'order', rid: '3521000222', scannedAt: NOW } })); s.reset(); tick(3000); await ask();
    assert.strictEqual(s.readsOf('Station_Live').length, 1, 'a new order: the live documents are read again at once'); assert.strictEqual(s.readsOf('Station_Sessions').length, 0, 'sessions did not change'); assert.strictEqual(s.readsOf('Efficiency_Daily').length, 0, 'no event was counted');
    tick(45000); s.reset(); await ask(); assert.strictEqual(s.readsOf('Station_Live').length, 1, 'the live documents are looked at again after 45 s whatever the revision says (beats are not counted)'); assert.strictEqual(s.readsOf('Station_Sessions').length, 0);
    tick(100000); await post(B()); s.reset(); await ask(); assert.strictEqual(s.readsOf('Station_Sessions').length, 1, 'sessions: again after a minute at the latest'); assert.strictEqual(s.readsOf('Efficiency_Daily').length, 1, 'today\'s numbers: again after 2 minutes at the latest');
    // a failing piece does not blank the board
    s.db.collection = (orig => name => name === 'Efficiency_Daily' ? { where: () => ({ limit: () => ({ select: () => ({ get: async () => { throw new Error('14 UNAVAILABLE: synthetic'); } }), get: async () => { throw new Error('14 UNAVAILABLE: synthetic'); } }) }) } : orig(name))(s.db.collection);
    tick(130000); await post(B()); const part = await ask(); assert.strictEqual(part.status, 200); assert.strictEqual(part.body.partial, true); assert(part.body.errors.some(e => /^today:/.test(e)));
    assert.strictEqual(station(part, 'welding').current.length, 1, 'the order is still shown'); assert.deepStrictEqual(station(part, 'welding').counts, { partsToday: null, ordersToday: null, scansToday: null }, 'today\'s numbers are unknown: null (a dash on the board), never a 0');
    say('9 gate (401 without the key), one query a poll, kept while the revision is unmoved (live 45 s, sessions 1 min, numbers 2 min), a failed read leaves the rest');
  }

  /* 9b · an order in hand counts the moment it is scanned, as the Overview counts it (the board used to count only the finished ones and said 9 where the Overview said 10) */
  {
    const s = fresh(); seed(s);
    s.put('Efficiency_Daily', '2026-10-05__In Hand A', { day: '2026-10-05', person: 'In Hand A', stations: { sorting: { parts: 0, orders: 0, scans: 2, lastAt: NOW - 1000 }, assembly: { parts: 2, orders: 1, scans: 2, lastAt: NOW - 2000 } },
      touched: { '3521000901': { sorting: true }, '3521000902': { sorting: true, assembly: true }, '3521000903': { assembly: true } } });
    s.put('Efficiency_Daily', '2026-10-05__In Hand B', { day: '2026-10-05', person: 'In Hand B', stations: { sorting: { parts: 1, orders: 1, scans: 1, lastAt: NOW - 3000 } }, touched: { '3521000901': { sorting: true }, '3521000904': { sorting: true } } });   // (the same order scanned by two people counts once)
    s.put('Efficiency_Daily', '2026-10-05__Sandbox only', { day: '2026-10-05', person: 'Sandbox only', sandbox: true, stations: { sorting: { scans: 1 } }, touched: { '3521000999': { sorting: true } } });
    s.put('Efficiency_Daily', '2026-10-05__Old rollup', { day: '2026-10-05', person: 'Old rollup', stations: { shipping: { parts: 9, orders: 5, scans: 5, lastAt: NOW - 4000 } } });   // (a rollup that kept no touched list: the finished orders stand)
    const x = await ask(), so = station(x, 'sorting'), as = station(x, 'assembly');
    assert.strictEqual(station(x, 'shipping').counts.ordersToday, 5, 'a rollup with no touched list keeps its finished count');
    assert.deepStrictEqual(so.counts, { partsToday: 1, ordersToday: 3, scansToday: 3 }, 'sorting: orders 901, 902, 904 are worked (one finished), the sandbox one never counts: ' + JSON.stringify(so.counts));
    assert.strictEqual(as.counts.ordersToday, 2, 'assembly: 1 finished, 2 touched (902, 903): the larger: ' + JSON.stringify(as.counts));
    assert.strictEqual(station(x, 'design').counts.ordersToday, 0, 'a station nobody scanned at has none');
  }

  /* 10 · read cost per hour (printed for the report) */
  {
    const s = fresh(); seed(s);
    for (let i = 0; i < 6; i++) await post(W({ person: 'Person ' + 'ABCDEF'[i], device: 'assembly-' + (i % 4 + 1), station: 'assembly', order: { kind: 'order', rid: '35210001' + i + '0', scannedAt: NOW, pieces: [{ id: 'p_1_1', sku: 'GF-12' }] } }));
    for (let i = 0; i < 12; i++) s.put('Station_Sessions', 'z' + i, { person: 'Person ' + 'ABCDEF'[i % 6], station: 'assembly', device: 'assembly-' + (i % 4 + 1), startAt: NOW - 3600e3, lastSeenAt: NOW, endAt: null });
    for (let i = 0; i < 6; i++) s.put('Efficiency_Daily', 'd' + i, { day: '2026-10-05', person: 'Person ' + 'ABCDEF'[i], stations: { assembly: { parts: 3, orders: 3 } } });
    s.reset(); let polls = 0;
    for (let t = 0; t < 3600; t += 3) { tick(3000); const x = await ask(); polls++; assert.strictEqual(x.status, 200); if (t % 30 === 0) for (let i = 0; i < 6; i++) await post(B({ station: 'assembly', device: 'assembly-' + (i % 4 + 1), person: 'Person ' + 'ABCDEF'[i] })); }
    const docs = s.reads.reduce((n, r) => n + (r.n != null ? Math.max(1, r.n) : 1), 0);
    say(`10 an open board for one hour (${polls} polls at 3 s, 6 people working): ${s.reads.length} queries/gets, ${docs} document reads; ${s.writes.length} writes by six stations' beats`);
  }

  await require('./station-live-client.cjs').run({ door, L, post, ask, fresh, station, W, tick, getNow: () => NOW, setNow: v => { NOW = v; }, seed, root, say, logs, PIN, SB, eff, getCur: () => cur });
  Date.now = realNow;
  say('station-live: all checks passed');
})().catch(e => { Date.now = realNow; process.stdout.write('FAILED: ' + (e && e.stack || e) + '\n'); process.exit(1); });
