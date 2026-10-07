// Station numbers in the Employee efficiency console (Paul, 7 Oct 2026): Assembly is FOUR stations (assembly-1..4) and Shipping THREE (shipping-1..3), but the console
// showed one Assembly row and one Shipping row. The pages always signed in, logged and heartbeated under their own device ("assembly-2"); what was dropped was the number,
// by summing everything under the kind. This suite runs the REAL modules over one in-memory Firestore, offline, with a faked clock and synthetic names:
//   door     firebaseOrders {activity, live} -> _stationActivity.js (rollup with a `devices` map, `touched` naming the desk) and _stationLive.js
//   reader   employeeEfficiency {overview, live}: per-desk rows, people, feed, counts, "kind alone" remainder for records with no number
//   console  charm-nest-efficiency.js (the Overview's rows, chips, signed-in, feed) and charm-nest-efficiency-stations.js (the board) view models
// and counts the Firestore reads and writes the change adds (none).
//   node tests/stations/station-numbers.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module'), vm = require('vm'), fs = require('fs');
const root = path.join(__dirname, '../..');
const noNested = require('../charm-nest/_noNestedArrays.cjs');

/* ── one in-memory Firestore: nested increments, merge, transactions, counted reads and writes ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const SERVER_TS = { __ts: true }, inc = n => ({ __inc: n });
const colls = new Map(), reads = [], writes = [];
const col = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Ts) && v.__inc === undefined && !v.__ts;
const clone = v => v instanceof Ts ? new Ts(v.m) : Array.isArray(v) ? v.map(clone) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
function apply(prev, patch, merge, at) {
  const out = merge && prev ? clone(prev) : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v && v.__inc !== undefined) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = new Ts(at);
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge, at);
    else out[k] = clone(v);
  }
  return out;
}
const put = (c, id, d, o) => { const doc = apply(col(c).get(id), d, !!(o && o.merge), Date.now()); noNested(doc, c + '/' + id); col(c).set(id, doc); };
const kind = v => v instanceof Ts ? 'ts' : typeof v, val = v => v instanceof Ts ? v.m : v;
const refOf = (c, id) => ({ c, id, path: c + '/' + id });
const snapOf = r => { const d = col(r.c).get(r.id); reads.push({ c: r.c, doc: r.id }); return { id: r.id, exists: !!d, data: () => d ? clone(d) : undefined, ref: r }; };
function query(name, filters, order, lim, sel) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel), orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel), limit: n => query(name, filters, order, n, sel), select: (...f) => query(name, filters, order, lim, f),
    get: async () => {
      let docs = [...col(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => { const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false; const a = val(x), b = val(v); return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false; });
      if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
      if (lim != null) docs = docs.slice(0, lim);
      reads.push({ c: name, n: docs.length, select: sel || null });
      return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
    }
  };
}
const fakeDb = {
  collection: c => Object.assign(query(c, [], null, null, null), { doc: id => Object.assign(refOf(c, id), {
    get: async () => snapOf(refOf(c, id)),
    set: async (v, o) => { writes.push([c, id]); put(c, id, v, o); },
    update: async v => { writes.push([c, id]); if (!col(c).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); put(c, id, v, { merge: true }); } }) }),
  getAll: async (...a) => a.filter(x => x && x.c).map(snapOf),
  runTransaction: async fn => {
    const w = [], tx = { get: async r => snapOf(r), getAll: async (...rs) => rs.map(snapOf), set: (r, d, o) => { w.push([r, d, o]); return tx; } };
    const out = await fn(tx);
    for (const [r, d, o] of w) { writes.push([r.c, r.id]); put(r.c, r.id, d, o); }
    return out;
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { Timestamp: Ts, FieldValue: { serverTimestamp: () => SERVER_TS, increment: inc, delete: () => null } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;

const PASS = 'synthetic-numbers-pass-2k8w';
process.env.EDIT_PASSCODE = PASS;
const realNow = Date.now;
let NOW = Date.UTC(2026, 9, 7, 15, 0);                                            // 11:00 on 7 Oct in New York (EDT)
Date.now = () => NOW;
console.warn = console.log = console.info = () => {};
const say = s => process.stdout.write(s + '\n');
let ipN = 0;
const MIN = 60000, DAY = '2026-10-07';
const post = async (payload, o = {}) => { const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, queryStringParameters: o.sandbox ? { sandbox: '1' } : {}, body: JSON.stringify(payload) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const ask = async body => { const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ipN % 200) }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, fakeDb); assert.strictEqual(r.statusCode, 200, r.body.slice(0, 300)); return JSON.parse(r.body); };
const dropCaches = () => { const c = eff._t.cacheOf(fakeDb); c.memo.clear(); c.recent.clear(); c.fails.clear(); EP.resetCache(); };
const eq = (a, b, msg) => assert.deepStrictEqual(a, b, msg);

/* ── the shop: seven desks, one person each, plus records with NO number ── */
const DESKS = [['assembly', 'assembly-1', 'Ann A.'], ['assembly', 'assembly-2', 'Ben B.'], ['assembly', 'assembly-3', 'Cy C.'], ['assembly', 'assembly-4', 'Di D.'], ['shipping', 'shipping-1', 'Eve E.'], ['shipping', 'shipping-2', 'Giovanna C.'], ['shipping', 'shipping-3', 'Gus G.']];
let seq = 0;
const O = n => String(3521000000 + n);
const ev = (station, device, person, action, o = {}) => Object.assign({ id: `${device || 'old'}_${String(seq).padStart(4, '0')}_${++seq}_${NOW}`, station, device, computer: 'pc-ABCDEFGHJKMN', session: `${device || 'old'}-sess-0001`, person, action, orderId: '', line: '', sku: '', parts: 0, orders: 0, detail: '', at: NOW - 5 * MIN + seq * 1000, seq, sincePrevMs: 30000 }, o);

(async () => {
  /* ═══ 1 · the door: events from seven desks and from two pages with no number ═══ */
  DESKS.forEach(([st, dv, who], i) => {
    put('Station_Sessions', 's-' + dv, { person: who, station: st, device: dv, startAt: NOW - (90 - i) * MIN, lastSeenAt: NOW - 20000, endAt: null });
  });
  put('Station_Sessions', 's-old-asm', { person: 'Hal H.', station: 'assembly', device: 'assembly', startAt: NOW - 80 * MIN, lastSeenAt: NOW - 20000, endAt: null });     // a page with NO number (an old record)
  put('Station_Sessions', 's-old-ship', { person: 'Ina I.', station: 'shipping', startAt: NOW - 70 * MIN, lastSeenAt: NOW - 20000, endAt: null });                            // and one with no device at all
  const batch = [];
  DESKS.forEach(([st, dv, who], i) => {
    const n = i + 1, o1 = O(100 + n), o2 = O(200 + n);
    batch.push(ev(st, dv, who, 'scan', { orderId: o1, parts: n }), ev(st, dv, who, 'complete', { orderId: o1, parts: n, orders: 1 }));       // desk n: n pieces, order o1 finished
    if (n % 2) batch.push(ev(st, dv, who, 'scan', { orderId: o2, parts: 1 }));                                                               // odd desks have a second order in hand
  });
  batch.push(ev('assembly', 'assembly', 'Hal H.', 'scan', { orderId: O(900), parts: 5 }), ev('assembly', 'assembly', 'Hal H.', 'complete', { orderId: O(900), parts: 5, orders: 1 }));   // no number
  batch.push(ev('shipping', '', 'Ina I.', 'scan', { orderId: O(901), parts: 2 }), ev('shipping', '', 'Ina I.', 'complete', { orderId: O(901), parts: 2, orders: 1 }));
  for (let i = 0; i < batch.length; i += 40) { const r = await post({ activity: batch.slice(i, i + 40) }); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); assert.strictEqual(r.body.refused, 0, 'every event is accepted'); }
  // a desk's order says which desk touched it; a page with no number says `true`; the rollup has a devices map; no nested array is stored anywhere
  const roll = who => col('Efficiency_Daily').get(`${DAY}__${who}`);
  eq(roll('Ben B.').devices, { 'assembly-2': { events: 2, scans: 1, completes: 1, parts: 2, orders: 1, lastAt: roll('Ben B.').devices['assembly-2'].lastAt } }, 'a desk\'s own counters, in the rollup the station already writes');
  eq(roll('Ben B.').touched[O(102)], { assembly: 'assembly-2' }, 'the order names the desk that touched it');
  eq(roll('Hal H.').touched[O(900)], { assembly: true }, 'a page with no number leaves the order as before (true)');
  assert(!('devices' in roll('Hal H.')), 'a page with no number adds no desk counters');
  eq(roll('Ina I.').touched[O(901)], { shipping: true });
  for (const [c, m] of colls) for (const [id, d] of m) noNested(d, c + '/' + id);
  // an old rollup and an old session (written before desks were told apart): nothing is rewritten, and they read as the kind alone
  put('Station_Sessions', 's-old-day', { person: 'Old Timer', station: 'shipping', device: 'shipping-2', startAt: NOW - 30 * MIN, lastSeenAt: NOW - 20000, endAt: null });
  put('Efficiency_Daily', `${DAY}__Old Timer`, { day: DAY, person: 'Old Timer', v: 1, events: 3, firstAt: NOW - 40 * MIN, lastAt: NOW - 30 * MIN, stations: { shipping: { scans: 1, completes: 1, parts: 4, orders: 1, firstAt: NOW - 40 * MIN, lastAt: NOW - 30 * MIN } }, touched: { [O(777)]: { shipping: true } } });
  say('1 the door: each desk\'s counters and touched orders are in the rollup the station already writes; a page with no number is `true`; no nested arrays');

  /* ═══ 2 · heartbeats: an order in hand at Assembly 2 and at Shipping 3 ═══ */
  const W = (station, device, person, rid) => ({ v: 1, event: 'work', station, device, computer: 'pc-ABCDEFGHJKMN', session: `${device}-sess-0001`, person, startAt: NOW - 60 * MIN, order: { kind: 'order', rid, orderNumber: rid, customer: 'Sam P.', scannedAt: NOW - 2 * MIN, pieces: [{ id: rid + '_1_1', label: 'Stud' }], pieceCount: 1 } });
  assert.strictEqual((await post({ live: W('assembly', 'assembly-2', 'Ben B.', O(202)) })).status, 200);
  assert.strictEqual((await post({ live: W('shipping', 'shipping-3', 'Gus G.', O(207)) })).status, 200);
  say('2 heartbeats at Assembly 2 and Shipping 3');

  /* ═══ 3 · what the reader returns: the live answer and the overview ═══ */
  dropCaches(); reads.length = 0; writes.length = 0;
  const live = await ask({ op: 'live' });
  const liveReads = reads.map(r => r.c + (r.select ? '[' + r.select.join() + ']' : ''));
  const ls = (k) => live.stations.find(s => s.key === k);
  for (const [k, n, label] of [['assembly', 4, 'Assembly'], ['shipping', 3, 'Shipping']]) {
    const s = ls(k);
    eq(s.devices.map(d => d.device).filter(d => /-\d$/.test(d)), Array.from({ length: n }, (_, i) => `${k}-${i + 1}`), label + ': every desk of the shop is listed, idle or not');
  }
  const d = (k, dv) => ls(k).devices.find(x => x.device === dv);
  eq([d('assembly', 'assembly-1').counts, d('assembly', 'assembly-2').counts], [{ partsToday: 1, ordersToday: 2, scansToday: 2 }, { partsToday: 2, ordersToday: 1, scansToday: 1 }], 'desk counts: pieces finished, orders worked (an order in hand counts), scans');
  eq(ls('assembly').unassigned, { partsToday: 5, ordersToday: 1, scansToday: 1 }, 'Assembly: what no desk claims is the page with no number (5 pieces, 1 order, 1 scan)');
  eq(ls('shipping').unassigned, { partsToday: 2 + 4, ordersToday: 1 + 1, scansToday: 1 + 1 }, 'Shipping: the page with no device and the old rollup, both with no number');
  eq(d('shipping', 'shipping-2').counts, { partsToday: 6, ordersToday: 1, scansToday: 1 }, 'Giovanna C. at Shipping 2: only what shipping-2 logged (the old rollup of Old Timer is not hers or the desk\'s)');
  assert.strictEqual(d('assembly', 'assembly-2').state, 'working'); assert.strictEqual(d('assembly', 'assembly-3').state, 'idle'); assert.strictEqual(d('shipping', 'shipping-3').state, 'working');
  eq(live.signedIn.filter(p => p.stationKey === 'assembly').map(p => p.name + ':' + (p.device || '')).sort(), ['Ann A.:assembly-1', 'Ben B.:assembly-2', 'Cy C.:assembly-3', 'Di D.:assembly-4', 'Hal H.:assembly'], 'Signed in now names the desk (and the page with no number says none)');
  eq(live.stations.find(s => s.key === 'shipping').current.map(c => c.person + ':' + c.device), ['Gus G.:shipping-3']); eq(ls('assembly').current.map(c => c.person + ':' + c.device), ['Ben B.:assembly-2']);
  assert(d('assembly', 'assembly-2').lastEventAt > 0 && d('assembly', 'assembly-4').lastEventAt > 0);
  const sb = live.stations.find(s => s.key === 'welding'); assert(!('unassigned' in sb), 'only the numbered stations carry desks');
  // COST: the live answer asks for exactly what it asked before: one query on Station_Live, sessions, rollups (the same documents, `devices` is one more field of them), nothing else per desk
  eq(liveReads.filter(x => /^Efficiency_Daily/.test(x)), ['Efficiency_Daily[day,person,stations,sandbox,touched,devices]'], 'the live answer reads today\'s rollups ONCE, asking for the desks as one more field of the same documents');
  eq(reads.filter(r => r.c === 'Station_Live' || r.c === 'Station_Sessions').length, 2, 'one query each on the live documents and today\'s sessions');
  eq(writes.filter(w => !/^Station_Rev$|^Station_Sessions$/.test(w[0])).length, 0, 'the live read writes nothing');
  say('3 the live answer: all 7 desks listed, each with its people, order in hand, counts and last event; the remainder is "unassigned"; reads: the same queries');

  dropCaches(); reads.length = 0;
  const ov = await ask({ op: 'overview', day: DAY, days: 1 });
  const bs = k => ov.business.stations.find(s => s.station === k);
  eq(bs('assembly').devices.map(x => [x.device, x.parts, x.orders, x.peopleNow]), [['assembly-1', 1, 2, ['Ann A.']], ['assembly-2', 2, 1, ['Ben B.']], ['assembly-3', 3, 2, ['Cy C.']], ['assembly-4', 4, 1, ['Di D.']]], 'Overview Assembly: the right person and counts on each desk');
  eq(bs('shipping').devices.map(x => [x.device, x.parts, x.orders, x.peopleNow]), [['shipping-1', 5, 2, ['Eve E.']], ['shipping-2', 6, 1, ['Giovanna', 'Old Timer']], ['shipping-3', 7, 2, ['Gus G.']]]);
  eq(bs('assembly').unassigned, { parts: 5, scans: 1, orders: 1 }); eq(bs('shipping').unassigned, { parts: 6, scans: 2, orders: 2 });
  eq([bs('assembly').parts, bs('assembly').orders, bs('shipping').parts, bs('shipping').orders], [15, 7, 24, 7], 'the group totals are what they always were (kind rollups)');
  assert(!('devices' in bs('welding')) && !('devices' in bs('sorting')), 'Welding, Sorting, Design, Laser and Inbox stay as they are');
  const person = n => ov.people.find(p => p.name === n);
  eq([person('Ben B.').devices.map(x => x.device), person('Ben B.').nowDevices, person('Giovanna').nowDevices], [['assembly-2'], ['assembly-2'], ['shipping-2']], 'a person\'s chips know the desk (Giovanna C. is Giovanna by the alias map)');
  eq(person('Hal H.').devices, [], 'a person on a page with no number has no desk'); eq(person('Hal H.').nowAt, ['assembly']);
  const feedDev = ov.feed.filter(f => f.person === 'Ben B.').map(f => f.device); assert(feedDev.length && feedDev.every(x => x === 'assembly-2'), 'feed rows carry the desk');
  assert(ov.feed.filter(f => f.person === 'Hal H.' || f.person === 'Ina I.').every(f => !('device' in f)), 'a feed row of a page with no number has no desk: it reads as the kind alone');
  eq(reads.filter(r => r.c === 'Efficiency_Daily').length >= 1, true);
  say('4 the overview: per-desk people, counts, chips, feed rows; totals unchanged; the kind-alone remainder; other stations untouched');

  /* ═══ 5 · the console: the Overview rows ═══ */
  const mkWin = (extra) => { const win = Object.assign({ document: { getElementById: () => null, addEventListener() {}, createElement: () => ({}) }, console }, extra || {}); win.window = win; vm.createContext(win); return win; };
  const win = mkWin();
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), win);
  const Eff = win.Efficiency, J = o => JSON.parse(JSON.stringify(o));
  const Mraw = Eff.norm(J(ov)), M = J(Mraw);
  const rows = J(Eff.stationRowsOf(Mraw));
  eq(rows.filter(r => !r.head).map(r => r.key), ['shipping-1', 'shipping-2', 'shipping-3', 'shipping', 'assembly-1', 'assembly-2', 'assembly-3', 'assembly-4', 'assembly', 'welding', 'sorting', 'design', 'laser'], 'SEVEN desk rows (Shipping 1 to 3, Assembly 1 to 4), then the kind alone only where records have no number, then the other stations as they were');
  eq(rows.filter(r => r.head).map(r => [r.head, r.s.parts, r.s.orders]), [['shipping', 24, 7], ['assembly', 15, 7]], 'a group caption with the kind\'s own totals');
  eq(rows.filter(r => r.desk).map(r => r.key + ':' + r.s.now.join('+') + ':' + r.s.parts + ':' + r.s.orders), ['shipping-1:Eve E.:5:2', 'shipping-2:Giovanna+Old Timer:6:1', 'shipping-3:Gus G.:7:2', 'assembly-1:Ann A.:1:2', 'assembly-2:Ben B.:2:1', 'assembly-3:Cy C.:3:2', 'assembly-4:Di D.:4:1'], 'the right person on each desk');
  const rest = k => rows.find(r => r.rest && r.key === k);
  eq([rest('assembly').s.now, rest('assembly').s.parts, rest('shipping').s.now, rest('shipping').s.parts], [['Hal H.'], 5, ['Ina I.'], 6], 'a legacy page and a legacy rollup show the kind alone: "Assembly" and "Shipping", nothing lost');
  // a service that does not tell desks apart (an older answer) leaves the kind as one row
  const old = J(ov); for (const s of old.business.stations) { delete s.devices; delete s.unassigned; }
  eq(J(Eff.stationRowsOf(Eff.norm(old))).filter(r => !r.head).map(r => r.key).slice(0, 3), ['shipping', 'assembly', 'welding'], 'an answer with no desks reads exactly as it did');
  // people chips by desk, and the kind alone for a person with no desk
  const chips = n => J(Eff.chipsOf(M.people.find(p => p.name === n))).map(c => c.label + (c.now ? '*' : ''));
  eq([chips('Ben B.'), chips('Giovanna'), chips('Hal H.')], [['Assembly 2*'], ['Shipping 2*'], ['Assembly*']], 'People station chips name the number ("Assembly 2", "Shipping 2"); old sign-ins read "Assembly"');
  // the feed row says where
  eq([Eff.whereOf('assembly', 'assembly-2'), Eff.whereOf('assembly', ''), Eff.whereOf('welding', 'weld-1'), Eff.whereOf('shipping', 'assembly-2')], ['Assembly 2', 'Assembly', 'Welding', 'Shipping'], 'Live activity rows name the desk; a page of another kind or none names the kind');
  eq(M.feed.filter(f => f.person === 'Ben B.').every(f => f.device === 'assembly-2'), true);
  // Signed in now keeps the desk (two chips for one person at two desks)
  const L = J(Eff.normLive(J(live)));
  eq(L.signedIn.filter(p => p.stationKey === 'shipping').map(p => p.name + ':' + p.desk).sort(), ['Eve E.:shipping-1', 'Giovanna:shipping-2', 'Gus G.:shipping-3', 'Ina I.:', 'Old Timer:shipping-2'], 'Signed in now: the desk on every chip');
  const two = J(live); two.signedIn.push({ name: 'Eve E.', stationKey: 'shipping', device: 'shipping-3', since: NOW - MIN, lastSeenAt: NOW });
  eq(J(Eff.normLive(two)).signedIn.filter(p => p.name === 'Eve E.').map(p => p.desk).sort(), ['shipping-1', 'shipping-3'], 'one person at two desks of one station is two chips, never merged');
  eq(L.current.map(c => c.person + ':' + Eff.whereOf(c.station, c.raw.device)), ['Ben B.:Assembly 2', 'Gus G.:Shipping 3'], 'Now working on names the desk');
  say('5 the console\'s Overview: 7 desk rows with the right person, group totals, kind-alone remainder, chips, feed wording, signed-in chips; an older answer reads as it did');

  /* ═══ 6 · the Stations board ═══ */
  const win2 = mkWin(); win2.addEventListener = () => {};
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency-stations.js'), 'utf8'), win2);
  const ES = win2.EfficiencyStations;
  const B = J(ES.norm(J(live)));
  const keys = B.stations.map(s => s.key);
  eq(keys, ['sorting', 'welding', 'assembly-1', 'assembly-2', 'assembly-3', 'assembly-4', 'assembly', 'shipping-1', 'shipping-2', 'shipping-3', 'shipping', 'design', 'laser', 'inbox'], 'the board: one card per desk (Assembly 1 to 4, Shipping 1 to 3); the kind alone only for records with no number; the rest as they were');
  const bst = k => B.stations.find(s => s.key === k);
  eq([bst('assembly-2').label, bst('assembly-2').state, bst('assembly-2').people.map(p => p.name), bst('assembly-2').current.map(c => c.person + ':' + c.orderNumber), bst('assembly-2').counts], ['Assembly 2', 'working', ['Ben B.'], ['Ben B.:' + O(202)], { parts: 2, orders: 1, scans: 1 }], 'a desk card: who, the order in hand, its own counts');
  eq([bst('assembly-3').state, bst('assembly-3').people.map(p => p.name), bst('assembly-3').current.length, bst('assembly-3').counts.parts], ['idle', ['Cy C.'], 0, 3]);
  eq([bst('shipping-3').current.map(c => c.person), bst('shipping-3').group, bst('shipping-3').groupCounts.partsToday], [['Gus G.'], 'shipping', 24], 'the group\'s own day totals ride on its desks');
  eq([bst('assembly').label, bst('assembly').people.map(p => p.name), bst('assembly').counts], ['Assembly', ['Hal H.'], { parts: 5, orders: 1, scans: 1 }], 'the legacy page shows the kind alone');
  eq(bst('assembly-1').people[0].since > 0, true, 'a desk\'s person keeps the sign-in time');
  const idleLive = J(live); for (const s of idleLive.stations) if (s.key === 'assembly') { s.people = []; s.current = []; s.devices.forEach(x => { x.state = 'offline'; x.person = ''; x.counts = { partsToday: 0, ordersToday: 0, scansToday: 0 }; }); s.unassigned = { partsToday: 0, ordersToday: 0, scansToday: 0 }; }
  eq(J(ES.norm(idleLive)).stations.filter(s => /^assembly/.test(s.key)).map(s => s.key + ':' + s.state), ['assembly-1:offline', 'assembly-2:offline', 'assembly-3:offline', 'assembly-4:offline'], 'idle desks are still listed (the quiet dash), and no kind-alone card without records');
  const oldLive = J(live); for (const s of oldLive.stations) { delete s.unassigned; }
  eq(J(ES.norm(oldLive)).stations.map(s => s.key).filter(k => /^(assembly|shipping)/.test(k)), ['assembly', 'shipping'], 'an answer with no desk fields reads as one card per kind, as it did');
  eq(ES.deskLabel('shipping-3'), 'Shipping 3'); eq(ES.deskKey('assembly', 'Assembly-02'), 'assembly-2'); eq(ES.deskKey('assembly', 'shipping-1'), '');
  say('6 the Stations board: a card per desk with its people, order in hand and counts; the group totals ride along; an older answer reads as before');

  /* ═══ 7 · the cost of the change ═══ */
  // the writer: one transaction per batch, as before (the same documents; the desk counters are fields of the rollup it already updates)
  reads.length = 0; writes.length = 0;
  const one = [ev('assembly', 'assembly-2', 'Ben B.', 'scan', { orderId: O(303), parts: 1 }), ev('shipping', 'shipping-1', 'Eve E.', 'complete', { orderId: O(304), parts: 2, orders: 1 })];
  assert.strictEqual((await post({ activity: one })).status, 200);
  const w = writes.filter(x => x[0] !== 'Station_Rev').map(x => x[0]).sort();
  eq(w, ['Efficiency_Daily', 'Efficiency_Daily', 'Station_Activity', 'Station_Activity'], 'two events, two people: two event documents and two rollups, exactly the writes of before (no document for desks)');
  say('7 cost: no new document, no new read, no new write; the desk counters are fields of the documents already read and written');
  say('OK');
})().then(() => { Date.now = realNow; }, e => { Date.now = realNow; process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
