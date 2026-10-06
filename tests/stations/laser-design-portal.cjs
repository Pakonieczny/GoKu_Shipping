// Laser and Design are two stations of their own in the Employee efficiency portal (Paul, 6 Oct 2026, item 4; plans/stations-round2/plan.md R4,
// api.md "LD2"). A non-Admin person signs in at the Sorter app (device charm-nest-1) as Laser or Design, the Design Station pages keep logging as
// Design, and Admin signs in at the Sorter app with no role (station sorter, which the portal shows as Sorting).
// Everything runs over the REAL door (firebaseOrders: {session}, {activity}, {live}) and the REAL reader (employeeEfficiency: ops live, overview,
// person with and without a range) on an in-memory Firestore with a faked clock; then the REAL board (charm-nest-efficiency-stations.js) draws the
// real answer in jsdom. No network, no Etsy, no paid AI, no real name, PIN or passcode.
//   1 · one person Laser, then Design, in one day: hours and work split by station, one signed-in span, attendance, who is on now
//   2 · two people, one each: the two cards are independent (state, people, pages, counts, the sheet and the order in hand)
//   3 · Admin in the Sorter app (no role) stays Sorting; an old Sorter-app session and rollup of yesterday fold into Sorting, stored history untouched
//   4 · the whole shop: totals add up (people = stations = board = distinct orders) and nothing counts twice between the Sorter app's Design role
//       and the Design Station pages (one person at both: one signed-in span, one name on the card, one order, one set of pieces)
//   5 · the board in a browser (jsdom): the Laser and Design cards list who is signed in, where, since when and how long ago the last input was
//   JSDOM_DIR=<...>/node_modules node tests/stations/laser-design-portal.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── an in-memory Firestore: where / orderBy / limit / select, doc get/set/update, getAll, transactions, merges, increments, server times ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } toDate() { return new Date(this.m); } static fromMillis(m) { return new Ts(m); } static now() { return new Ts(Date.now()); } }
const INC = n => ({ __inc: n }), SRV = { __srv: 1 }, DEL = { __del: 1 };
const kind = v => v instanceof Ts ? 'ts' : typeof v === 'number' ? 'num' : typeof v === 'string' ? 'str' : 'other';
const val = v => v instanceof Ts ? v.m : v;
const plain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Ts) && v.__inc == null && !v.__srv && !v.__del;
const keep = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(keep) : plain(v) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, keep(x)])) : v;
function apply(prev, data, merge) {
  const out = merge && prev ? keep(prev) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v === undefined) continue;
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__srv) out[k] = new Ts(Date.now());
    else if (v && v.__del) delete out[k];
    else if (plain(v)) out[k] = apply(merge && plain(out[k]) ? out[k] : null, v, merge);
    else out[k] = keep(v);
  }
  return out;
}
function store() {
  const colls = new Map();
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  function query(name, filters, order, lim, sel) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
      limit: n => query(name, filters, order, n, sel),
      select: (...f) => query(name, filters, order, lim, f),
      get: async () => {
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        return { docs: docs.map(({ id, d }) => ({ id, exists: true, data: () => keep(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const ref = (name, id) => ({ id, path: name + '/' + id, name,
    get: async () => { const d = data(name).get(id); return { exists: !!d, id, data: () => keep(d) }; },
    set: async (v, o) => { data(name).set(id, apply(data(name).get(id), v, !!(o && o.merge))); },
    create: async v => { if (data(name).has(id)) throw Object.assign(new Error('6 ALREADY_EXISTS'), { code: 6 }); data(name).set(id, apply(null, v, false)); },
    update: async v => { if (!data(name).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); data(name).set(id, apply(data(name).get(id), v, true)); },
    delete: async () => { data(name).delete(id); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null, null), { doc: id => ref(name, id) }),
    getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
    runTransaction: async fn => {
      const w = [];
      const tx = { get: r => r.get(), getAll: (...rs) => Promise.all(rs.filter(x => x && x.get).map(r => r.get())), set: (r, d, o) => { w.push(['set', r, d, o]); return tx; }, create: (r, d) => { w.push(['create', r, d]); return tx; }, update: (r, d) => { w.push(['update', r, d]); return tx; }, delete: r => { w.push(['delete', r]); return tx; } };
      const out = await fn(tx);
      for (const [k, r, d, o] of w) await r[k](d, o);
      return out;
    }
  };
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), get: (name, id) => data(name).get(id), all: name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, keep(d))), count: name => data(name).size };
}

/* ── the real modules over a fake admin that hands every module the current store ── */
let cur = store();
const dbNow = { collection: n => cur.db.collection(n), getAll: (...a) => cur.db.getAll(...a), runTransaction: f => cur.db.runTransaction(f) };
const fakeAdmin = { firestore: Object.assign(() => dbNow, { Timestamp: Ts, FieldValue: { serverTimestamp: () => SRV, increment: INC, delete: () => DEL, arrayUnion: (...a) => a, arrayRemove: () => [] }, FieldPath: { documentId: () => '__name__' } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const L = require(path.join(root, 'netlify/functions/_stationLive.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const K = require(path.join(root, 'netlify/functions/_activityKinds.js'));
Module._load = realLoad;

const PASS = 'synthetic-pass-9f3k';
process.env.EDIT_PASSCODE = PASS;
const logs = [];
console.warn = console.log = console.error = console.info = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const realNow = Date.now;
let NOW = 0;
Date.now = () => NOW;
const DAY = '2026-10-06', YESTERDAY = '2026-10-05';
const at = (hm, day) => Date.parse(`${day || DAY}T${hm}:00-04:00`);           // the New York clock on that day (Eastern Daylight Time until 1 Nov)
const go = hm => { NOW = at(hm); };
let ipN = 0, sn = 0, seq = 0;

/* ── what the stations send ── */
const post = async (body, o = {}) => {
  const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ipN % 250) }, queryStringParameters: o.sandbox ? { sandbox: '1' } : {}, body: JSON.stringify(body) });
  return { status: r.statusCode, body: JSON.parse(r.body || '{}') };
};
const open = new Map();                                                              // key -> the session the page holds now
const beatBody = (s, event, extra) => ({ session: Object.assign({ id: s.id, event, person: s.person, employeeId: '', station: s.station, device: s.device, computerId: s.pc, computerLabel: '', at: NOW }, extra || {}) });
/** a page signs a person in (the station-session.js start) */
async function signIn(key, person, station, device, pc) {
  const s = { id: `${device}-${pc}-${++sn}`, person, station, device, pc };
  const r = await post(beatBody(s, 'start')); assert.strictEqual(r.status, 200, `${key} starts: ${JSON.stringify(r.body)}`);
  open.set(key, s); return s;
}
async function signOut(key, reason) {
  const s = open.get(key), r = await post(beatBody(s, 'end', { reason: reason || 'signOut' })); assert.strictEqual(r.status, 200, `${key} ends: ${JSON.stringify(r.body)}`); open.delete(key);
}
/** the clock runs to hm; every open page beats every 10 minutes (as the real one does every 5), so no session looks closed */
async function runTo(hm) {
  const to = at(hm);
  while (NOW < to) { NOW = Math.min(to, NOW + 10 * 60000); for (const s of open.values()) { const r = await post(beatBody(s, 'beat')); assert.strictEqual(r.status, 200); } }
}
/** the activity events the stations log (station-activity.js log()) */
const ev = o => ({ id: `${o.device}_${o.pc.slice(-5)}_${++seq}_${at(o.t)}`, station: o.station, device: o.device, computer: o.pc, session: '', person: o.person, action: o.action || 'complete', orderId: o.order || '', line: '', sku: '', parts: o.parts || 0, orders: o.orders || 0, detail: o.detail || '', at: at(o.t), seq, sincePrevMs: 120000 });
async function log(list) { const r = await post({ activity: list }); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); assert.strictEqual(r.body.written, list.length, 'every event is new: ' + JSON.stringify(r.body)); assert.strictEqual(r.body.refused, 0); }
/** what the page shows as the order or sheet in hand (station-activity.js working()) */
const live = (o, order) => post({ live: { v: 1, event: 'work', station: o.station, device: o.device, computer: o.pc, session: '', person: o.person, startAt: o.since, order: Object.assign({ scannedAt: NOW - 60000 }, order) } });
const order = (rid, extra) => Object.assign({ kind: 'order', rid, orderNumber: rid, customer: '', pieces: [{ id: rid + '_1_1', label: 'Charm' }], pieceCount: 1 }, extra || {});

/* ── what the portal reads (the gated function, a fresh clock each time so no cache answers) ── */
const ask = async body => {
  NOW += 20000;
  const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, cur.db);
  const j = JSON.parse(r.body || '{}'); j._status = r.statusCode; return j;
};
const fresh = () => { cur = store(); EP.resetCache(); L._t.seen.clear(); open.clear(); };
const stationOf = (b, key) => b.stations.find(s => s.key === key);
const names = st => st.people.map(p => (p && typeof p === 'object' ? p.name : p));                  // (the people of a card: names, or rows with a name)
const personOf = (ov, name) => ov.people.find(p => p.name === name);
const stRow = (list, key) => list.find(s => s.station === key);
const near = (a, b, m) => assert(Math.abs(a - b) < 2, `${m}: ${a} is not ${b} (within two minutes: each read moves the clock 20 s on)`);
const sum = (list, f) => list.reduce((n, x) => n + f(x), 0);

(async () => {
  /* 0 · the catalog: Laser has the Sorter app, Design has its own pages and the Sorter app; no Sorter or QR Printer station */
  {
    const laser = L.CATALOG.find(s => s.key === 'laser'), design = L.CATALOG.find(s => s.key === 'design');
    assert.deepStrictEqual(laser.devices, [['charm-nest-1', 'Sorter app (Laser)']], 'the Laser station has the Sorter app');
    assert.deepStrictEqual(design.devices.map(d => d[0]), ['design', 'design-1', 'design-message', 'design-message-1', 'charm-nest-1'], 'the Design Station pages and the Sorter app');
    assert.strictEqual(design.devices.find(d => d[0] === 'charm-nest-1')[1], 'Sorter app (Design)');
    assert.deepStrictEqual(L.CATALOG.map(s => s.key), ['sorting', 'welding', 'assembly', 'shipping', 'design', 'laser', 'inbox'], 'the Sorter and the QR Printer are not stations of the board');
    assert.strictEqual(K.displayStation('laser'), 'laser'); assert.strictEqual(K.displayStation('design'), 'design'); assert.strictEqual(K.displayStation('sorter'), 'sorting', 'only sorter and qr fold');
    say('0 the catalog: Laser has the Sorter app, Design its pages and the Sorter app; laser and design never fold into Sorting');
  }

  /* 1 · one person Laser, then Design, in one day */
  {
    fresh(); go('08:00');
    await signIn('dana-l', 'Dana S.', 'laser', 'charm-nest-1', 'pc-dana01');
    await runTo('11:00'); await signOut('dana-l'); await signIn('dana-d', 'Dana S.', 'design', 'charm-nest-1', 'pc-dana01');         // the role switch: the one session ends, the other starts
    await runTo('12:25');
    await log([
      ev({ t: '08:30', station: 'laser', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', parts: 3, orders: 1, order: '3521000101', detail: 'approved for laser cutting' }),
      ev({ t: '09:30', station: 'laser', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', parts: 2, orders: 1, order: '3521000102', detail: 'laser done' }),
      ev({ t: '11:30', station: 'design', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', parts: 2, orders: 1, order: '3521000104', detail: 'design decision' }),
      ev({ t: '12:00', station: 'design', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', parts: 5, orders: 1, order: '3521000105', detail: 'design decision' })]);
    await runTo('12:30');
    const day = cur.get('Efficiency_Daily', `${DAY}__Dana S.`);
    assert(day.stations.laser && day.stations.design && !day.stations.sorter, 'the rollup keeps the two roles as two stations');
    assert.strictEqual(day.stations.laser.parts, 5); assert.strictEqual(day.stations.design.parts, 7);
    // the board: Laser has nobody now (the laser session ended at the switch), Design has Dana; each card counts only its own work
    const b = await ask({ op: 'live' });
    const lc = stationOf(b, 'laser'), dc = stationOf(b, 'design');
    assert.strictEqual(lc.state, 'offline'); assert.deepStrictEqual(names(lc), []); assert.strictEqual(lc.counts.partsToday, 5); assert.strictEqual(lc.counts.ordersToday, 2);
    assert.strictEqual(dc.state, 'idle'); assert.deepStrictEqual(names(dc), ['Dana S.']); assert.strictEqual(dc.counts.partsToday, 7); assert.strictEqual(dc.counts.ordersToday, 2);
    const sorterApp = dc.devices.find(d => d.device === 'charm-nest-1');
    assert.strictEqual(sorterApp.label, 'Sorter app (Design)'); assert.strictEqual(sorterApp.state, 'idle'); assert.strictEqual(sorterApp.person, 'Dana S.'); assert.strictEqual(sorterApp.since, at('11:00'));
    assert.strictEqual(lc.devices.find(d => d.device === 'charm-nest-1').label, 'Sorter app (Laser)'); assert.strictEqual(lc.devices.find(d => d.device === 'charm-nest-1').state, 'offline');
    assert.deepStrictEqual(b.signedIn.map(x => [x.stationKey, x.name, x.device, x.since]), [['design', 'Dana S.', 'charm-nest-1', at('11:00')]], 'signed in now: Design only');
    // the Overview: her hours and work split by station, one signed-in span, on now at Design only
    const ov = await ask({ op: 'overview', days: 1, trend: false });
    const d = personOf(ov, 'Dana S.');
    assert.strictEqual(d.status, 'on'); assert.deepStrictEqual(d.nowAt, ['design']); assert.strictEqual(d.onSince, at('11:00')); assert.strictEqual(d.firstIn, at('08:00')); assert.strictEqual(d.lastOut, null);
    near(stRow(d.stations, 'laser').minutes, 180, 'Laser minutes'); near(stRow(d.stations, 'design').minutes, 90, 'Design minutes');
    assert.strictEqual(stRow(d.stations, 'laser').parts, 5); assert.strictEqual(stRow(d.stations, 'design').parts, 7); assert.strictEqual(stRow(d.stations, 'laser').orders, 2); assert.strictEqual(stRow(d.stations, 'design').orders, 2);
    near(d.totals.signedInMin, 270, 'the two roles are one continuous day, not 360'); assert.strictEqual(d.totals.parts, 12); assert.strictEqual(d.totals.orders, 4);
    assert.deepStrictEqual(ov.business.stations.filter(s => s.peopleNow.length).map(s => [s.station, s.peopleNow]), [['design', ['Dana S.']]]);
    // her page (the one-day op and the profile): Laser and Design are two rows with their own hours
    const pg = await ask({ op: 'person', name: 'Dana S.', days: 1 });
    assert.deepStrictEqual(pg.days[0].stations.map(s => s.station).sort(), ['design', 'laser']); near(stRow(pg.days[0].stations, 'laser').minutes, 180, 'page Laser'); near(stRow(pg.days[0].stations, 'design').minutes, 90, 'page Design');
    const pr = await ask({ op: 'person', name: 'Dana S.', range: 'week' });
    assert.strictEqual(pr._status, 200, JSON.stringify(pr).slice(0, 300));
    assert.deepStrictEqual(pr.stations.map(s => s.station).sort(), ['design', 'laser'], 'the profile splits the work by role'); assert.strictEqual(pr.stations.find(s => s.station === 'laser').label, 'Laser'); assert.strictEqual(pr.stations.find(s => s.station === 'design').label, 'Design');
    near(pr.stations.find(s => s.station === 'laser').minutes, 180, 'profile Laser'); near(pr.stations.find(s => s.station === 'design').minutes, 90, 'profile Design');
    assert.strictEqual(pr.stations.find(s => s.station === 'laser').parts, 5); assert.strictEqual(pr.stations.find(s => s.station === 'design').parts, 7);
    // attendance: one day worked, the signed-in time is both roles' time once
    const today = (pr.calendar || []).find(c => c.day === DAY);
    assert(today, 'the calendar has today'); assert.strictEqual(today.state, 'worked'); near(today.signedMs / 60000, 270, 'attendance: the signed-in time of both roles'); assert.strictEqual(today.firstIn, at('08:00')); assert.strictEqual(today.lastOut, null, 'still signed in'); assert.strictEqual(today.parts, 12, 'attendance sees the pieces of both roles');
    // what is stored is what the stations wrote: nothing renamed
    assert.strictEqual(cur.all('Station_Sessions').filter(s => s.person === 'Dana S.').map(s => s.station).sort().join(), 'design,laser');
    say('1 one person Laser then Design: two rows (3 h Laser, 1 h 30 Design), one 4 h 30 day, on now at Design only, attendance one day worked');
  }

  /* 2 · two people, one each: the cards are independent */
  {
    fresh(); go('08:15');
    await signIn('fay', 'Fay T.', 'laser', 'charm-nest-1', 'pc-fay01');
    await runTo('08:30'); await signIn('gus', 'Gus M.', 'design', 'design-message', 'pc-gus01');
    await runTo('12:20');
    await log([
      ev({ t: '09:00', station: 'laser', device: 'charm-nest-1', pc: 'pc-fay01', person: 'Fay T.', parts: 4, orders: 1, order: '3521000103', detail: 'approved for laser cutting' }),
      ev({ t: '10:00', station: 'design', device: 'design-message', pc: 'pc-gus01', person: 'Gus M.', action: 'scan', parts: 3, order: '3521000108', detail: 'phone scan' }),
      ev({ t: '10:30', station: 'design', device: 'design-message', pc: 'pc-gus01', person: 'Gus M.', parts: 3, orders: 1, order: '3521000108', detail: 'order chat message sent' })]);
    await runTo('12:28');
    await live({ station: 'laser', device: 'charm-nest-1', pc: 'pc-fay01', person: 'Fay T.', since: at('08:15') }, { kind: 'sheet', title: 'GF Sheet 2 · Set 4', pieces: [], pieceCount: 0 });
    const b = await ask({ op: 'live' });
    const lc = stationOf(b, 'laser'), dc = stationOf(b, 'design');
    assert.strictEqual(lc.state, 'working', 'a sheet in hand'); assert.deepStrictEqual(names(lc), ['Fay T.']); assert.strictEqual(dc.state, 'idle'); assert.deepStrictEqual(names(dc), ['Gus M.']);
    assert.strictEqual(lc.current.length, 1); assert.strictEqual(lc.current[0].kind, 'sheet'); assert.strictEqual(lc.current[0].title, 'GF Sheet 2 · Set 4'); assert.strictEqual(lc.current[0].person, 'Fay T.');
    assert.strictEqual(lc.current[0].device, 'charm-nest-1'); assert.strictEqual(lc.current[0].deviceLabel, 'Sorter app (Laser)'); assert.strictEqual(dc.current.length, 0, 'the sheet is not on the Design card');
    assert.strictEqual(lc.counts.partsToday, 4); assert.strictEqual(lc.counts.ordersToday, 1); assert.strictEqual(dc.counts.partsToday, 3); assert.strictEqual(dc.counts.ordersToday, 1);
    assert.strictEqual(dc.devices.find(x => x.device === 'design-message').person, 'Gus M.'); assert.strictEqual(dc.devices.find(x => x.device === 'design-message').label, 'Design messages');
    assert.strictEqual(dc.devices.find(x => x.device === 'charm-nest-1').state, 'offline', 'nobody is in the Sorter app as Design');
    assert.deepStrictEqual(b.signedIn.map(x => `${x.stationKey}:${x.name}`).sort(), ['design:Gus M.', 'laser:Fay T.']);
    for (const k of ['sorting', 'welding', 'assembly', 'shipping']) assert.strictEqual(stationOf(b, k).state, 'offline', k + ' is untouched');
    const ov = await ask({ op: 'overview', days: 1, trend: false });
    assert.deepStrictEqual(personOf(ov, 'Fay T.').nowAt, ['laser']); assert.deepStrictEqual(personOf(ov, 'Gus M.').nowAt, ['design']);
    near(stRow(personOf(ov, 'Fay T.').stations, 'laser').minutes, 253.5, 'Fay Laser minutes'); near(stRow(personOf(ov, 'Gus M.').stations, 'design').minutes, 238.5, 'Gus Design minutes');
    assert.strictEqual(personOf(ov, 'Fay T.').stations.length, 1); assert.strictEqual(personOf(ov, 'Gus M.').stations.length, 1);
    say('2 two people, one each: the Laser card has Fay and her sheet, the Design card has Gus and his page; the counts, pages and hours stay apart');
  }

  /* 3 · Admin in the Sorter app stays Sorting; yesterday's Sorter-app history folds, stored untouched */
  {
    fresh();
    // yesterday: Dana at the Sorter app before roles existed (station sorter), and her rollup under sorter
    cur.put('Station_Sessions', 'old-sorter-dana', { id: 'old-sorter-dana', person: 'Dana S.', station: 'sorter', device: 'charm-nest-1', computerId: 'pc-dana01', startAt: at('09:00', YESTERDAY), lastSeenAt: at('13:00', YESTERDAY), endAt: at('13:00', YESTERDAY), endReason: 'signOut', minutes: 240 });
    cur.put('Efficiency_Daily', `${YESTERDAY}__Dana S.`, { day: YESTERDAY, person: 'Dana S.', v: 1, events: 3, firstAt: at('09:30', YESTERDAY), lastAt: at('12:30', YESTERDAY), stations: { sorter: { completes: 2, parts: 7, orders: 2, activeMs: 360000, firstAt: at('09:30', YESTERDAY), lastAt: at('12:30', YESTERDAY) } }, touched: { '3521000090': { sorter: true }, '3521000091': { sorter: true } } });
    cur.put('Efficiency_Daily', `${YESTERDAY}__Fay T.`, { day: YESTERDAY, person: 'Fay T.', v: 1, events: 1, firstAt: at('10:00', YESTERDAY), lastAt: at('10:00', YESTERDAY), stations: { laser: { completes: 1, parts: 4, orders: 1, firstAt: at('10:00', YESTERDAY), lastAt: at('10:00', YESTERDAY) } }, touched: { '3521000092': { laser: true } } });
    // today: Paul K. (Admin) opens the Sorter app, no role asked
    go('08:00'); await signIn('paul', 'Paul K.', 'sorter', 'charm-nest-1', 'pc-paul01');
    await runTo('12:20');
    await log([ev({ t: '09:00', station: 'sorter', device: 'charm-nest-1', pc: 'pc-paul01', person: 'Paul K.', parts: 1, orders: 1, order: '3521000107', detail: 'sorted' })]);
    await runTo('12:28'); await live({ station: 'sorter', device: 'charm-nest-1', pc: 'pc-paul01', person: 'Paul K.', since: at('08:00') }, order('3521000109'));
    const b = await ask({ op: 'live' });
    const so = stationOf(b, 'sorting');
    assert.deepStrictEqual(names(so), ['Paul K.'], 'Admin in the Sorter app is on the Sorting card'); assert.strictEqual(so.state, 'working');
    assert.strictEqual(so.devices.find(d => d.device === 'charm-nest-1').label, 'Sorter (nesting)'); assert.strictEqual(so.devices.find(d => d.device === 'charm-nest-1').person, 'Paul K.');
    assert.strictEqual(so.current.length, 1); assert.strictEqual(so.current[0].rid, '3521000109'); assert.strictEqual(so.counts.partsToday, 1); assert.strictEqual(so.counts.ordersToday, 1);
    for (const k of ['laser', 'design']) { const c = stationOf(b, k); assert.strictEqual(c.state, 'offline', k + ' stays offline'); assert.deepStrictEqual(names(c), []); assert.strictEqual(c.counts.partsToday, 0); assert.deepStrictEqual(c.current, []); }
    assert(!b.stations.some(s => s.key === 'sorter' || s.key === 'qr'), 'no Sorter or QR Printer station'); assert.deepStrictEqual(b.signedIn.map(x => [x.stationKey, x.name]), [['sorting', 'Paul K.']]);
    const ov = await ask({ op: 'overview', days: 1, trend: false });
    const p = personOf(ov, 'Paul K.');
    assert.deepStrictEqual(p.nowAt, ['sorting']); assert.deepStrictEqual(p.stations.map(s => s.station), ['sorting']); near(p.stations[0].minutes, 268.5, 'Sorting minutes'); assert.strictEqual(p.stations[0].parts, 1);
    assert.deepStrictEqual(ov.business.stations.filter(s => s.peopleNow.length).map(s => [s.station, s.peopleNow]), [['sorting', ['Paul K.']]]);
    assert(!ov.business.stations.some(s => s.station === 'sorter'), 'no sorter row in the Overview');
    // yesterday folds into Sorting, and yesterday's Laser person stays Laser
    const y = await ask({ op: 'person', name: 'Dana S.', day: YESTERDAY, days: 1 });
    assert.deepStrictEqual(y.days[0].stations.map(s => s.station), ['sorting'], 'an old Sorter-app session and rollup are Sorting'); near(y.days[0].stations[0].minutes, 240, 'old session minutes'); assert.strictEqual(y.days[0].stations[0].parts, 7); assert.strictEqual(y.days[0].stations[0].orders, 2);
    const yf = await ask({ op: 'person', name: 'Fay T.', day: YESTERDAY, days: 1 });
    assert.deepStrictEqual(yf.days[0].stations.map(s => s.station), ['laser']);
    const yo = await ask({ op: 'overview', day: YESTERDAY, days: 1, trend: false });
    assert.deepStrictEqual(yo.business.stations.filter(s => s.parts).map(s => [s.station, s.parts]).sort(), [['laser', 4], ['sorting', 7]]);
    // history is untouched: the documents still say what the Sorter app wrote
    assert.strictEqual(cur.get('Station_Sessions', 'old-sorter-dana').station, 'sorter'); assert.strictEqual(cur.get('Station_Sessions', 'old-sorter-dana').endReason, 'signOut');
    assert(cur.get('Efficiency_Daily', `${YESTERDAY}__Dana S.`).stations.sorter && !cur.get('Efficiency_Daily', `${YESTERDAY}__Dana S.`).stations.sorting, 'the stored rollup keeps its key');
    assert.strictEqual(cur.all('Station_Sessions').find(s => s.person === 'Paul K.').station, 'sorter', 'what Admin\'s Sorter app wrote is stored as it was');
    say('3 Admin in the Sorter app is Sorting (card, hours, Overview); yesterday\'s sorter session and rollup read as Sorting, stored as sorter');
  }

  /* 4 · the whole shop: totals add up, and nothing counts twice between the Sorter app's Design role and the Design Station pages */
  let shop = null;
  {
    fresh(); go('08:00');
    await signIn('paul', 'Paul K.', 'sorter', 'charm-nest-1', 'pc-paul01');                // Admin, no role
    await signIn('dana-l', 'Dana S.', 'laser', 'charm-nest-1', 'pc-dana01');
    await signIn('eli-1', 'Eli R.', 'design', 'design-1', 'pc-eli01');                      // a Design Station page
    await runTo('08:15'); await signIn('fay', 'Fay T.', 'laser', 'charm-nest-1', 'pc-fay01');
    await runTo('10:00'); await signIn('eli-app', 'Eli R.', 'design', 'charm-nest-1', 'pc-eli02');         // the same person, also in the Sorter app as Design, on a second computer
    await runTo('11:00'); await signOut('dana-l'); await signIn('dana-d', 'Dana S.', 'design', 'charm-nest-1', 'pc-dana01');
    await runTo('12:25');
    await log([
      ev({ t: '08:30', station: 'laser', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', parts: 3, orders: 1, order: '3521000101' }),
      ev({ t: '09:30', station: 'laser', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', parts: 2, orders: 1, order: '3521000102' }),
      ev({ t: '09:00', station: 'laser', device: 'charm-nest-1', pc: 'pc-fay01', person: 'Fay T.', parts: 4, orders: 1, order: '3521000103' }),
      ev({ t: '09:00', station: 'design', device: 'design-1', pc: 'pc-eli01', person: 'Eli R.', parts: 6, orders: 1, order: '3521000106', detail: 'labels printed' }),
      ev({ t: '10:15', station: 'design', device: 'charm-nest-1', pc: 'pc-eli02', person: 'Eli R.', action: 'note', order: '3521000106', detail: 'design decision' }),      // the SAME order, a note: no pieces, no second completion
      ev({ t: '11:30', station: 'design', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', parts: 2, orders: 1, order: '3521000104' }),
      ev({ t: '12:00', station: 'design', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', parts: 5, orders: 1, order: '3521000105' }),
      ev({ t: '09:00', station: 'sorter', device: 'charm-nest-1', pc: 'pc-paul01', person: 'Paul K.', parts: 1, orders: 1, order: '3521000107', detail: 'sorted' })]);
    await runTo('12:28');
    await live({ station: 'laser', device: 'charm-nest-1', pc: 'pc-fay01', person: 'Fay T.', since: at('08:15') }, { kind: 'sheet', title: 'GF Sheet 2 · Set 4', pieces: [], pieceCount: 0 });
    await live({ station: 'design', device: 'charm-nest-1', pc: 'pc-dana01', person: 'Dana S.', since: at('11:00') }, order('3521000105'));
    const ov = await ask({ op: 'overview', days: 1, trend: false });
    const b = await ask({ op: 'live' });
    shop = { ov, b };
    // people: who is where, and for how long
    assert.deepStrictEqual(ov.people.map(p => p.name).sort(), ['Dana S.', 'Eli R.', 'Fay T.', 'Paul K.']);
    assert.deepStrictEqual(personOf(ov, 'Paul K.').nowAt, ['sorting']); assert.deepStrictEqual(personOf(ov, 'Fay T.').nowAt, ['laser']); assert.deepStrictEqual(personOf(ov, 'Dana S.').nowAt, ['design']); assert.deepStrictEqual(personOf(ov, 'Eli R.').nowAt, ['design']);
    // no double counting: Eli is at the Design page from 8:00 and in the Sorter app (Design) from 10:00, on two computers
    const eli = personOf(ov, 'Eli R.');
    near(eli.totals.signedInMin, 268.5, 'two overlapping sign-ins are one span (8:00 to 12:28), not 418'); assert.strictEqual(eli.stations.length, 1); near(eli.stations[0].minutes, 268.5, 'Design minutes: the two pages cover the same time once');
    assert.strictEqual(eli.stations[0].parts, 6, 'the order\'s 6 pieces once'); assert.strictEqual(eli.stations[0].orders, 1, 'the one order once'); assert.strictEqual(eli.totals.parts, 6); assert.strictEqual(eli.totals.orders, 1);
    assert.strictEqual(eli.onSince, at('08:00'));
    // totals add up three ways: the people, the stations, the board
    const dana = personOf(ov, 'Dana S.'), fay = personOf(ov, 'Fay T.'), paul = personOf(ov, 'Paul K.');
    assert.deepStrictEqual([dana, eli, fay, paul].map(p => p.totals.parts), [12, 6, 4, 1]);
    assert.strictEqual(ov.business.totals.parts, 23); assert.strictEqual(sum(ov.people, p => p.totals.parts), 23); assert.strictEqual(sum(ov.business.stations, s => s.parts), 23, 'the station rows add up to the pieces of the day');
    assert.strictEqual(ov.business.totals.orders, 7, 'seven different orders'); assert.strictEqual(ov.business.totals.people, 4);
    const biz = k => ov.business.stations.find(s => s.station === k);
    assert.strictEqual(biz('laser').parts, 9); assert.strictEqual(biz('design').parts, 13); assert.strictEqual(biz('sorting').parts, 1);
    assert.strictEqual(biz('laser').orders, 3); assert.strictEqual(biz('design').orders, 3, 'Design: orders 104, 105 and 106 (106 was handled at the page AND the Sorter app: one order)'); assert.strictEqual(biz('sorting').orders, 1);
    assert.deepStrictEqual(biz('laser').peopleNow, ['Fay T.']); assert.deepStrictEqual(biz('design').peopleNow, ['Dana S.', 'Eli R.']); assert.deepStrictEqual(biz('sorting').peopleNow, ['Paul K.']);
    assert(!ov.business.stations.some(s => s.station === 'sorter' || s.station === 'qr'));
    assert(ov.people.every(p => p.stations.every(s => s.station !== 'sorter')), 'no sorter row on any person');
    // the board agrees with the Overview, station by station
    for (const k of ['laser', 'design', 'sorting']) { assert.strictEqual(stationOf(b, k).counts.partsToday, biz(k).parts, k + ': board pieces = Overview pieces'); assert.strictEqual(stationOf(b, k).counts.ordersToday, biz(k).orders, k + ': board orders = Overview orders'); }
    assert.strictEqual(sum(b.stations, s => s.counts.partsToday), 23); assert.strictEqual(sum(b.stations, s => s.counts.ordersToday), 7);
    const dc = stationOf(b, 'design'), lc = stationOf(b, 'laser'), sc = stationOf(b, 'sorting');
    assert.deepStrictEqual(names(dc).sort(), ['Dana S.', 'Eli R.'], 'Eli is one person on the Design card, though he is on two pages'); assert.deepStrictEqual(names(lc), ['Fay T.']); assert.deepStrictEqual(names(sc), ['Paul K.']);
    assert.deepStrictEqual(b.signedIn.map(x => `${x.stationKey}:${x.name}`).sort(), ['design:Dana S.', 'design:Eli R.', 'laser:Fay T.', 'sorting:Paul K.'], 'one row per person and station');
    assert.deepStrictEqual(dc.devices.filter(d => d.state !== 'offline').map(d => d.device).sort(), ['charm-nest-1', 'design-1'], 'both Design sources show as pages of the Design card');
    assert.strictEqual(dc.devices.find(d => d.device === 'design-1').person, 'Eli R.'); assert.strictEqual(dc.devices.find(d => d.device === 'design-1').label, 'Design 1');
    assert.strictEqual(dc.state, 'working'); assert.strictEqual(dc.current.length, 1); assert.strictEqual(dc.current[0].rid, '3521000105'); assert.strictEqual(dc.current[0].deviceLabel, 'Sorter app (Design)');
    assert.strictEqual(lc.state, 'working'); assert.strictEqual(lc.current[0].kind, 'sheet'); assert.strictEqual(sc.state, 'idle');
    // the stored rollups: per person, per role, nothing summed across roles
    const dd = cur.get('Efficiency_Daily', `${DAY}__Eli R.`); assert.deepStrictEqual(Object.keys(dd.stations), ['design']); assert.strictEqual(dd.stations.design.parts, 6); assert.strictEqual(dd.stations.design.notes, 1); assert.deepStrictEqual(Object.keys(dd.touched), ['3521000106']);
    // the one order the two Design sources shared: its trail names both pages, once each, and counts one completion
    const tr = await ask({ op: 'orders', orderId: '3521000106' });
    assert.strictEqual(tr.steps.length, 1); assert.strictEqual(tr.steps[0].station, 'design'); assert.strictEqual(tr.steps[0].person, 'Eli R.'); assert.strictEqual(tr.steps[0].completes, 1); assert.strictEqual(tr.steps[0].parts, 6);
    assert.deepStrictEqual(tr.events.map(e => [e.station, e.device, e.action]).sort(), [['design', 'charm-nest-1', 'note'], ['design', 'design-1', 'complete']]);
    // the feed names the stations the people worked at
    assert(ov.feed.some(f => f.station === 'laser') && ov.feed.some(f => f.station === 'design') && ov.feed.some(f => f.station === 'sorter' || f.station === 'sorting'));
    say('4 the whole shop: 23 pieces and 7 orders by people = stations = board; Eli at the Design page and the Sorter app counts once (one span, one card name, one order, 6 pieces)');
  }

  /* 5 · the board in a browser: the Laser and Design cards list who is signed in, on which page, since when and how long ago the last input was */
  {
    let JSDOM;
    for (const d of [process.env.JSDOM_DIR, process.argv[2], path.join(root, 'node_modules')].filter(Boolean)) { try { ({ JSDOM } = require(path.join(d, 'jsdom'))); break; } catch (_) {} }
    if (!JSDOM) { try { ({ JSDOM } = require('jsdom')); } catch (_) { say('5 (jsdom not found: set JSDOM_DIR; the browser checks were not run)'); } }
    if (JSDOM) {
      const code = fs.readFileSync(path.join(root, 'charm-nest-efficiency-stations.js'), 'utf8');
      const board = async (answer) => {
        const dom = new JSDOM('<!doctype html><html><body><div id="host"></div></body></html>', { url: 'https://portal.test/', pretendToBeVisual: true, runScripts: 'outside-only' }), w = dom.window;
        w.Date.now = () => NOW;
        w.Element.prototype.getClientRects = function () { return [{ width: 1, height: 1 }]; };
        w.eval(code);
        const E = w.EfficiencyStations; E.options.pollMs = 600000; E.options.source = async () => JSON.parse(JSON.stringify(answer));
        const b = E.mount(w.document.getElementById('host'), { own: true, onPerson: false });
        for (let i = 0; i < 60 && !w.document.querySelector('.esSt'); i++) await new Promise(r => setImmediate(r));
        assert(w.document.querySelector('.esSt'), 'the board drew');
        return { w, b, E, row: k => w.document.querySelector(`.esSt[data-key="${k}"]`), close: () => { b.unmount(); w.close(); } };
      };
      const text = el => el.textContent.replace(/\s+/g, ' ').trim();
      // (a) the answer the real reader gave for the whole shop
      go('12:30');
      let h = await board(shop.b);
      assert.deepStrictEqual([...h.w.document.querySelectorAll('.esSt')].map(r => r.dataset.key), ['sorting', 'welding', 'assembly', 'shipping', 'design', 'laser', 'inbox'], 'the board lists the stations, Laser and Design apart');
      const laser = h.row('laser'), design = h.row('design'), sorting = h.row('sorting');
      assert.strictEqual(text(laser.querySelector('.esStName')), 'Laser'); assert.strictEqual(text(design.querySelector('.esStName')), 'Design');
      assert.deepStrictEqual([...laser.querySelectorAll('.esRoster .esRo')].map(text), ['Fay T. · Sorter app (Laser) · since 8:15 AM'], 'the Laser card lists Fay with her page and her start (no input time yet: the answer carries none)');
      const dRows = [...design.querySelectorAll('.esRoster .esRo')].map(text).sort();
      assert.strictEqual(dRows.length, 2, 'one line per person on the Design card: ' + dRows.join(' | ')); assert(/^Dana S\. · Sorter app \(Design\) · since 11:00 AM/.test(dRows[0]), dRows[0]); assert(/^Eli R\. · Design 1 · since 8:00 AM/.test(dRows[1]), dRows[1]);
      assert.strictEqual(sorting.querySelector('.esRoster'), null, 'the other cards are as they were: chips only');
      assert.deepStrictEqual([...laser.querySelectorAll('.esPer .esPn')].map(text), ['Fay T.']); assert.deepStrictEqual([...design.querySelectorAll('.esPer .esPn')].map(text).sort(), ['Dana S.', 'Eli R.']);
      const cnt = (row, k) => text(row.querySelector(`.esCnt [data-n="${k}"]`));
      assert.deepStrictEqual([cnt(laser, 'parts'), cnt(laser, 'orders'), cnt(design, 'parts'), cnt(design, 'orders'), cnt(sorting, 'parts')], ['9', '3', '13', '3', '1'], 'today per station');
      assert(/Laser sheet GF Sheet 2/.test(laser.querySelector('.esCard').getAttribute('aria-label')), 'the sheet in hand is a card on the Laser card'); assert.strictEqual(design.querySelectorAll('.esCard').length, 1);
      assert(/4 people on/.test(text(h.w.document.querySelector('.esSum'))), 'four people on: ' + text(h.w.document.querySelector('.esSum')));
      h.close();
      // (b) the people rows the live board is to get (plans/stations-round2/api.md, C4): the page, the start and the last input; the input time ticks on its own
      go('12:30');
      const base = JSON.parse(JSON.stringify(shop.b)); base.at = NOW;
      const dd = stationOf(base, 'design'), ll = stationOf(base, 'laser'); ll.current = []; ll.state = 'idle';
      ll.people = [{ name: 'Fay T.', role: 'laser', since: at('08:15'), lastSeenAt: at('12:29'), lastInputAt: at('12:27'), device: 'charm-nest-1', deviceLabel: 'Sorter app (Laser)' }];
      dd.people = [{ name: 'Eli R.', since: at('08:00'), lastSeenAt: at('12:29'), lastInputAt: at('12:20'), device: 'design-1', deviceLabel: 'Design 1' },
        { name: 'Eli R.', role: 'design', since: at('10:00'), lastSeenAt: at('12:29'), lastInputAt: NOW - 1000, device: 'charm-nest-1', deviceLabel: 'Sorter app (Design)' },
        { name: 'Dana S.', role: 'design', since: at('11:00'), lastSeenAt: at('12:29'), lastInputAt: null, device: 'charm-nest-1', deviceLabel: 'Sorter app (Design)' }];
      h = await board(base);
      assert.deepStrictEqual([...h.row('laser').querySelectorAll('.esRoster .esRo')].map(text), ['Fay T. · Sorter app (Laser) · since 8:15 AM · last input 3 m ago']);
      assert.deepStrictEqual([...h.row('design').querySelectorAll('.esRoster .esRo')].map(text), ['Eli R. · Design 1 · since 8:00 AM · last input 10 m ago', 'Eli R. · Sorter app (Design) · since 10:00 AM · last input just now', 'Dana S. · Sorter app (Design) · since 11:00 AM'],
        'oldest sign-in first; a person on two pages has two lines; no input time, none shown');
      assert.strictEqual(h.row('design').querySelectorAll('.esPer').length, 2, 'the chips are still one per person');
      assert.strictEqual(text(h.row('design').querySelector('.esStState')), 'Working · 2 people', 'a person on two pages is one person');
      NOW += 4 * 60000; await new Promise(r => setTimeout(r, 600));                         // the board's own clock moves the times on without a request
      assert(/last input 7 m ago/.test(text(h.row('laser').querySelector('.esRoster .esRo'))), 'the input time ticks: ' + text(h.row('laser').querySelector('.esRoster')));
      assert(/last input 4 m ago/.test(text([...h.row('design').querySelectorAll('.esRoster .esRo')][1])), 'ticks on every line: ' + text(h.row('design').querySelector('.esRoster')));
      // the hover card of a person says where and when, too
      const chip = h.row('laser').querySelector('.esPer'); const spec = chip._esTip();
      const rowsOf = spec.rows.map(r => [r.k, r.v]);
      assert(rowsOf.some(([k, v]) => k === 'Signed in at' && v === 'Sorter app (Laser)'), JSON.stringify(rowsOf)); assert(rowsOf.some(([k, v]) => k === 'Last input' && /7 m ago/.test(v)), JSON.stringify(rowsOf)); assert(rowsOf.some(([k]) => k === 'Signed in since'));
      // the answer changes: Fay signs out, the Laser card's roster goes away with her and the card goes quiet
      const gone = JSON.parse(JSON.stringify(base)); const lg = stationOf(gone, 'laser'); lg.people = []; lg.state = 'offline'; lg.current = []; gone.signedIn = gone.signedIn.filter(x => x.name !== 'Fay T.'); gone.at = NOW;
      h.E.options.source = async () => JSON.parse(JSON.stringify(gone)); await h.b.refresh(); for (let i = 0; i < 40; i++) await new Promise(r => setImmediate(r));
      assert.strictEqual(h.row('laser').querySelector('.esRoster'), null, 'nobody signed in: no roster'); assert(/Offline/.test(text(h.row('laser').querySelector('.esIdle'))));
      h.close();
      say('5 the board draws Laser and Design apart: who is on, which page, since when, last input ticking; one line per person per page, the chips and counts as before');
    }
  }

  assert(!logs.some(l => /\b\d{6}\b/.test(l) && /pin/i.test(l)), 'no PIN in any log');
  const everything = JSON.stringify([...cur.all('Station_Sessions'), ...cur.all('Station_Activity'), ...cur.all('Efficiency_Daily'), ...cur.all('Station_Live'), shop && shop.ov, shop && shop.b]);
  assert(!/employeeId":"[0-9]+"/.test(everything), 'no PIN-like id anywhere');
  Date.now = realNow;
  say('laser-design-portal: all checks passed');
})().catch(e => { Date.now = realNow; process.stdout.write('FAILED: ' + (e && e.stack || e) + '\n'); process.exit(1); });
