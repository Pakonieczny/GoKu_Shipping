// The Welding station on the portal (Paul, 6 Oct 2026, stations round 2 item 2): two tasks (Welding | Matching), two people at the
// station at once, and the Welding station is NOT counted in throughput (R2). Everything is driven through the real doors over a fake
// Firestore: the session door and the activity door (firebaseOrders), then the console's own reads (employeeEfficiency ops live,
// overview, person, personOrders, attendance).
//   1 · the session door keeps `task` for the Welding station only; two people at once; one person in both tasks; a page that does
//       not know tasks (no `task`) still works; a beat never adds a task to an old session; sandbox is its own store
//   2 · the activity door: `matched` (and a scan made as Matching), the person's task, nobody in Matching = "Unattributed",
//       the rollup's matched counter, a resend counts once
//   3 · the Stations board: Welding as two groups (people with task, time on task, last input), the matched list with order
//       thumbnails, no pieces / orders today, Assembly unchanged, no event read when there is nothing matched
//   4 · totals exclude Welding: Overview totals, orders, per hour, the trend, the ranking; the legacy welding days read as 0 pieces
//   5 · the person view for day / week / month / quarter / year: hours per task, matched, the charts' buckets, the same person in
//       both tasks, an old task-less session as Welding time with task unknown, KPIs without Welding
//   6 · the matched-orders list of a person (personOrders {matched:true}) and the order trace
//   7 · attendance: two task sessions at once are one stretch of time
//   8 · history: no stored document of an earlier day or an old session changed; the console's reads wrote nothing; no PIN or passcode anywhere
//   JSDOM_DIR is not needed: no page is opened here.   node tests/stations/welding-portal.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── a Firestore fake: typed where / orderBy / limit / select, doc get / set(merge) / update, getAll, transactions that apply their
      writes at the end, nested merges, increments, Timestamps; every read and write is counted ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const INC = n => ({ __inc: n });
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Ts) && v.__inc === undefined;
const val = v => v instanceof Ts ? v.m : v;
const kindOf = v => v instanceof Ts ? 'ts' : typeof v;
const keep = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(keep) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, keep(x)])) : v;
function apply(prev, data, merge) {
  const out = merge && prev ? keep(prev) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v && v.__inc !== undefined) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge);
    else out[k] = keep(v);
  }
  return out;
}
function store() {
  const colls = new Map(), reads = [], writes = [], failing = new Set();
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const notFound = () => Object.assign(new Error('5 NOT_FOUND: No document to update'), { code: 5 });
  function query(name, filters, order, lim, sel) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
      limit: n => query(name, filters, order, n, sel),
      select: (...f) => query(name, filters, order, lim, f),
      get: async () => {
        if (failing.has(name)) throw Object.assign(new Error('14 UNAVAILABLE: synthetic outage of ' + name), { code: 14 });
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kindOf(x) !== kindOf(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: docs.length, filters: filters.map(f => f[0] + f[1]) });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => keep(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const ref = (name, id) => ({ id, path: name + '/' + id,
    get: async () => { reads.push({ name, doc: id, n: 1 }); const d = data(name).get(id); return { exists: !!d, id, data: () => keep(d) }; },
    set: async (v, o) => { writes.push([name, id, 'set']); data(name).set(id, apply(data(name).get(id), v, !!(o && o.merge))); },
    create: async v => { writes.push([name, id, 'create']); data(name).set(id, apply(null, v, false)); },
    update: async v => { writes.push([name, id, 'update']); if (!data(name).has(id)) throw notFound(); data(name).set(id, apply(data(name).get(id), v, true)); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null, null), { doc: id => ref(name, id) }),
    getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
    runTransaction: async fn => {
      const pend = [];
      const out = await fn({ get: r => r.get(), getAll: (...rs) => Promise.all(rs.filter(x => x && x.get).map(r => r.get())), set: (r, v, o) => pend.push([r, v, o]) });
      for (const [r, v, o] of pend) await r.set(v, o);
      return out;
    }
  };
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), get: (name, id) => data(name).get(id), all: name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, d)),
    fail: n => failing.add(n), heal: n => failing.delete(n), reads, writes, readsOf: name => reads.filter(r => r.name === name), writesOf: name => writes.filter(w => w[0] === name), clear: () => { reads.length = 0; writes.length = 0; }, colls,
    snapshot: () => JSON.stringify([...colls].map(([n, m]) => [n, [...m]]).sort(), (k, v) => v instanceof Ts ? { __ts: v.m } : v) };
}

/* ── the doors and the readers under test, over a fake admin that hands every module the current store ── */
let NOW = Date.parse('2026-10-05T15:00:00Z');
let cur = store();
const dbNow = { collection: n => cur.db.collection(n), getAll: (...a) => cur.db.getAll(...a), runTransaction: f => cur.db.runTransaction(f) };
const fakeAdmin = { firestore: Object.assign(() => dbNow, { FieldValue: { serverTimestamp: () => Ts.fromMillis(NOW), increment: INC, delete: () => null }, Timestamp: Ts }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const KIND = require(path.join(root, 'netlify/functions/_activityKinds.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;

const PASS = 'synthetic-pass-wp-7h2', PIN = '482915';                       // synthetic stand-ins; no real passcode or Employee Number is in this file
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
console.warn = console.log = console.error = console.info = keepLog;
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
Date.now = () => NOW;
const Z = iso => Date.parse(iso);
const MIN = 60000, HOUR = 3600000;
let ipN = 0;
const bodies = [];

const post = async (payload, o = {}) => {
  const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ipN % 250) }, queryStringParameters: o.sandbox ? { sandbox: '1' } : {}, body: JSON.stringify(payload) });
  return { status: r.statusCode, body: JSON.parse(r.body || '{}') };
};
const ask = async (body = {}) => {
  const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, cur.db);
  bodies.push(r.body);
  return { status: r.statusCode, body: JSON.parse(r.body || '{}') };
};
const live = (o = {}) => ask(Object.assign({ op: 'live' }, o));
const person = (name, range, extra) => ask(Object.assign({ op: 'person', name, range }, extra || {}));
const station = (out, key) => out.body.stations.find(s => s.key === key);
const fresh = () => { cur = store(); EP.resetCache(); return cur; };

/* ── the people and their pages ── */
const S = {
  tessW: { id: 'welding__weld-1__tess__welding__k1', person: 'Tess Welder', task: 'welding' },
  tessM: { id: 'welding__weld-1__tess__matching__k2', person: 'Tess Welder', task: 'matching' },
  rayM: { id: 'welding__weld-1__ray__matching__k3', person: 'Ray Matcher', task: 'matching' },
  lou: { id: 'welding__weld-1__lou__notask__k4', person: 'Legacy Lou' },                       // a page that does not know tasks: sends none
  ann: { id: 'assembly__assembly-1__ann__k5', person: 'Ann Assembler', station: 'assembly', device: 'assembly-1', computerId: 'pc-ASSM0001' },
  maxA: { id: 'assembly__assembly-2__max__k6', person: 'Max Mixed', station: 'assembly', device: 'assembly-2', computerId: 'pc-ASSM0002' },
  maxM: { id: 'welding__weld-1__max__matching__k7', person: 'Max Mixed', task: 'matching' }
};
const running = new Set();
let idN = 0;
const sessEvent = (s, event, extra) => post({ session: Object.assign({ id: s.cur || s.id, event, station: s.station || 'welding', device: s.device || 'weld-1', computerId: s.computerId || 'pc-WELD0001', computerLabel: 'Station PC', person: s.person, at: NOW }, s.task ? { task: s.task } : {}, extra || {}) });
/** the clock to `iso`; the open pages beat every 10 minutes meanwhile (the door ends a session that went quiet for 15) */
async function advance(iso) {
  const to = Z(iso);
  if (to < NOW) { NOW = to; return; }
  while (running.size && NOW + 10 * MIN < to) { NOW += 10 * MIN; for (const s of running) assert.strictEqual((await sessEvent(s, 'beat')).status, 200); }
  NOW = to; for (const s of running) assert.strictEqual((await sessEvent(s, 'beat')).status, 200);
}
async function start(s, iso) { await advance(iso); s.cur = s.id + '-' + (++idN); const r = await sessEvent(s, 'start'); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); running.add(s); return r.body; }
async function stop(s, iso) { await advance(iso); running.delete(s); const r = await sessEvent(s, 'end', { reason: 'signOut' }); assert.strictEqual(r.status, 200); return r.body; }
let evN = 0;
const E = (o = {}) => { evN++; return Object.assign({ id: `weld-1_ABCD_${evN}_${NOW}`, station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW', person: 'Ray Matcher', action: 'matched', orderId: '3521000001', line: '', sku: '', parts: 0, orders: 0, detail: '', at: NOW, seq: evN, sincePrevMs: 0 }, o); };
const emit = async (iso, o) => { await advance(iso); const r = await post({ activity: [E(o)] }); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); return r.body; };

const eq = assert.deepStrictEqual, is = assert.strictEqual;
const near = (a, b, tol, msg) => assert(Math.abs(a - b) <= tol, `${msg || ''} ${a} is not within ${tol} of ${b}`);
const URL_GF = 'https://firebasestorage.googleapis.com/v0/b/shop/o/thumbs%2Fgf-12.png?alt=media&token=t1';
const URL_PHOTO = 'https://i.etsystatic.com/111/r/il/abc/1/il_570xN.1_xyz.jpg';
const URL_ARCH = 'https://storage.googleapis.com/shop/design-archive/listing/aa.jpg';
const JSONS = v => JSON.stringify(v, (k, x) => x instanceof Ts ? { __ts: x.m } : x);
/** runs fn over an empty store of its own, then gives the world back */
async function scratch(fn) { const keepStore = cur; cur = store(); EP.resetCache(); try { return await fn(cur); } finally { cur = keepStore; EP.resetCache(); } }
const tp = (p, t) => p.find(x => x.task === t);

(async () => {
  /* ── the world: earlier days first (the clock only moves forward), then today ── */
  // 10 Feb: Ray matches for an hour, one order
  await start(S.rayM, '2026-02-10T15:00:00Z');
  await emit('2026-02-10T15:30:00Z', { person: 'Ray Matcher', orderId: '3521000050', task: 'matching' });
  await stop(S.rayM, '2026-02-10T16:00:00Z');
  // 20 Aug: an hour and a half, two orders
  await start(S.rayM, '2026-08-20T14:00:00Z');
  await emit('2026-08-20T14:30:00Z', { person: 'Ray Matcher', orderId: '3521000060', task: 'matching' });
  await emit('2026-08-20T15:00:00Z', { person: 'Ray Matcher', orderId: '3521000061', task: 'matching', sincePrevMs: 30 * MIN });
  await stop(S.rayM, '2026-08-20T15:30:00Z');
  // 2 Oct (Friday): Tess welds 09:00-13:00 and ALSO matches 11:00-13:00: her two task sessions overlap (no scans that day)
  await start(S.tessW, '2026-10-02T13:00:00Z'); await start(S.tessM, '2026-10-02T15:00:00Z');
  await stop(S.tessM, '2026-10-02T17:00:00Z'); await stop(S.tessW, '2026-10-02T17:00:00Z');
  // 3 Oct (Saturday): two hours, two orders
  await start(S.rayM, '2026-10-03T14:00:00Z');
  await emit('2026-10-03T14:10:00Z', { person: 'Ray Matcher', orderId: '3521000070', task: 'matching' });
  await emit('2026-10-03T14:50:00Z', { person: 'Ray Matcher', orderId: '3521000071', task: 'matching', sincePrevMs: 40 * MIN });
  await stop(S.rayM, '2026-10-03T16:00:00Z');
  // 4 Oct (Sunday): an OLD welding session and rollup, written the way the station wrote them before tasks existed (no `task`; welding parts and orders counted)
  const OLD_SESSION = { id: 'welding__weld-1__oldhand__j9', person: 'Old Hand', employeeId: '', station: 'welding', device: 'weld-1', computerId: 'pc-OLDOLD01', computerLabel: 'Welding PC', startAt: Z('2026-10-04T13:00:00Z'), lastSeenAt: Z('2026-10-04T16:00:00Z'), endAt: Z('2026-10-04T16:00:00Z'), endReason: 'signOut', minutes: 180 };
  const OLD_ROLLUP = { day: '2026-10-04', person: 'Old Hand', events: 5, firstAt: Z('2026-10-04T13:05:00Z'), lastAt: Z('2026-10-04T15:55:00Z'), stations: { welding: { scans: 4, scanParts: 20, completes: 5, parts: 20, orders: 5, activeMs: 900000, idleMs: 0, firstAt: Z('2026-10-04T13:05:00Z'), lastAt: Z('2026-10-04T15:55:00Z') } },
    hours: { '09': { scans: 4, parts: 20, by: { welding: { scans: 4, parts: 20 } } } }, touched: { 3521000090: { welding: true }, 3521000091: { welding: true } } };
  cur.put('Station_Sessions', OLD_SESSION.id, OLD_SESSION); cur.put('Efficiency_Daily', '2026-10-04__Old Hand', OLD_ROLLUP);
  const HISTORY = new Map();                                   // every stored document so far is history: nothing below may change one byte of it
  for (const n of ['Station_Sessions', 'Efficiency_Daily', 'Station_Activity']) for (const d of cur.all(n)) HISTORY.set(n + '/' + d._id, JSONS(cur.get(n, d._id)));
  const OLD_JSON = HISTORY.get('Station_Sessions/' + OLD_SESSION.id);
  // 5 Oct (Monday), today: Tess welds from 08:00; Max assembles 08:00-09:00 then matches 09:00-10:00; Ray matches from 09:00; Tess ALSO matches from 10:00;
  // Lou's page does not know tasks (sends none) from 10:00; Ann assembles all day
  await start(S.tessW, '2026-10-05T12:00:00Z'); await start(S.ann, '2026-10-05T12:00:00Z'); await start(S.maxA, '2026-10-05T12:00:00Z');
  await emit('2026-10-05T12:20:00Z', { person: 'Tess Welder', action: 'scan', orderId: '3521000100', parts: 12 });                                    // the old way: a welding scan and completion with pieces and an order
  await emit('2026-10-05T12:30:00Z', { person: 'Tess Welder', action: 'complete', orderId: '3521000100', parts: 12, orders: 1, sincePrevMs: 10 * MIN });
  await emit('2026-10-05T12:30:00Z', { person: 'Max Mixed', station: 'assembly', device: 'assembly-2', action: 'complete', orderId: '3521000102', parts: 6, orders: 1 });
  await stop(S.maxA, '2026-10-05T13:00:00Z'); await start(S.maxM, '2026-10-05T13:00:00Z'); await start(S.rayM, '2026-10-05T13:00:00Z');
  await emit('2026-10-05T13:00:00Z', { person: 'Ann Assembler', station: 'assembly', device: 'assembly-1', action: 'complete', orderId: '3521000101', parts: 5, orders: 1 });
  await emit('2026-10-05T13:20:00Z', { person: 'Max Mixed', orderId: '3521000006', task: 'matching' });
  await emit('2026-10-05T13:30:00Z', { person: 'Ray Matcher', orderId: '3521000001', task: 'matching' });
  await emit('2026-10-05T13:30:08Z', { person: 'Ray Matcher', action: 'scan', orderId: '3521000001', parts: 2, detail: 'phone scan', sincePrevMs: 8000 });     // the desk page's own scan of that same phone scan (what weld-1 wrote before WS1 stopped it)
  await emit('2026-10-05T13:40:00Z', { person: 'Ray Matcher', orderId: '3521000002', task: 'matching', sincePrevMs: 10 * MIN });
  await emit('2026-10-05T13:42:00Z', { person: 'Ray Matcher', action: 'scan', orderId: '3521000003', task: 'matching', sincePrevMs: 2 * MIN });           // a scan made as Matching counts as matched too
  await emit('2026-10-05T13:50:00Z', { person: 'Max Mixed', orderId: '3521000007', task: 'matching', sincePrevMs: 30 * MIN });
  await stop(S.maxM, '2026-10-05T14:00:00Z'); await start(S.tessM, '2026-10-05T14:00:00Z'); await start(S.lou, '2026-10-05T14:00:00Z');
  await emit('2026-10-05T14:20:00Z', { person: 'Ray Matcher', orderId: '3521000001', task: 'matching', detail: 'scanned again', sincePrevMs: 38 * MIN });
  await emit('2026-10-05T14:30:00Z', { person: '', unattributed: true, orderId: '3521000004', task: 'matching' });                                           // scanned with nobody in Matching
  await emit('2026-10-05T14:35:00Z', { person: 'Tess Welder', orderId: '3521000005', task: 'matching' });
  await emit('2026-10-05T14:40:00Z', { person: 'Ray Matcher', action: 'scan', orderId: '3521000300', detail: 'typed', sincePrevMs: 20 * MIN });                          // an order typed at the desk: a scan of its own, no matched event beside it
  await advance('2026-10-05T15:00:00Z');
  // what the stations' pages and the shop's data hold for the order thumbnails
  cur.put('Charm_Master_Index', 'GF-12', { sku: 'GF-12', thumbUrl: URL_GF });
  cur.put('Design_Order_Archive', '3521000001', { buyer: { name: 'Dana Q.' }, items: [{ transactionId: '8001', sku: 'GF-12', mirrorUrl: URL_ARCH }] });
  cur.put('Design_Order_Archive', '3521000002', { buyer: { name: 'Lee R.' }, items: [{ transactionId: '8002', sku: 'AB-7', imageUrl: URL_PHOTO }] });

  const world = cur, WRITES0 = world.writes.length;             // from here the console only reads

  /* 1 · the session door */
  {
    const doc = s => world.get('Station_Sessions', s.cur);
    is(doc(S.tessW).task, 'welding'); is(doc(S.tessM).task, 'matching'); is(doc(S.rayM).task, 'matching');
    assert(!('task' in doc(S.lou)), 'a page that sends no task stores none (as every session did before)'); assert(!('task' in doc(S.ann)), 'no task at another station');
    const open = world.all('Station_Sessions').filter(d => d.station === 'welding' && d.endAt == null).map(d => [d.person, d.task || '']).sort();
    eq(open, [['Legacy Lou', ''], ['Ray Matcher', 'matching'], ['Tess Welder', 'matching'], ['Tess Welder', 'welding']], 'four sessions open at the Welding station at once: two people, one of them in both tasks');
    assert.notStrictEqual(doc(S.tessW).id, doc(S.tessM).id, 'one session per person AND task');
    is(world.all('Station_Sessions').filter(d => d.person === 'Tess Welder' && d.endAt == null).length, 2);
    await scratch(async s => {
      const p0 = { id: 'welding__weld-1__ivy__x1', event: 'start', station: 'welding', device: 'weld-1', computerId: 'pc-WELD0001', person: 'Ivy Test', at: NOW };
      is((await post({ session: Object.assign({}, p0, { task: 'packing' }) })).status, 200); assert(!('task' in s.get('Station_Sessions', p0.id)), 'a task that is not welding or matching is dropped');
      is((await post({ session: Object.assign({}, p0, { id: 'welding__weld-1__ivy__x2', task: 'Matching' }) })).status, 200); assert(!('task' in s.get('Station_Sessions', 'welding__weld-1__ivy__x2')), 'the word is exact');
      is((await post({ session: Object.assign({}, p0, { id: 'assembly__a-1__ivy__x3', station: 'assembly', device: 'assembly-1', task: 'welding' }) })).status, 200); assert(!('task' in s.get('Station_Sessions', 'assembly__a-1__ivy__x3')), 'a task belongs to the Welding station alone');
      is((await post({ session: Object.assign({}, p0, { id: 'welding__weld-1__ivy__x4', task: 'matching', person: 'Ivy 482915', employeeId: PIN }) })).status, 200);
      const d4 = s.get('Station_Sessions', 'welding__weld-1__ivy__x4'); is(d4.task, 'matching'); is(d4.person, 'Ivy'); is(d4.employeeId, '', 'a PIN is never a name nor an id, with or without a task');
      // a beat never adds a task to a session that has none, and never changes one
      is((await post({ session: Object.assign({}, p0, { event: 'beat', task: 'matching' }) })).status, 200); assert(!('task' in s.get('Station_Sessions', p0.id)), 'a beat cannot give an old session a task');
      NOW += 60000; is((await post({ session: Object.assign({}, p0, { id: 'welding__weld-1__ivy__x4', event: 'beat', task: 'welding' }) })).status, 200); is(s.get('Station_Sessions', 'welding__weld-1__ivy__x4').task, 'matching', 'nor change one');
      NOW -= 60000;
      // sandbox is a store of its own
      const sb = await post({ session: Object.assign({}, p0, { id: 'welding__weld-1__ivy__sb1', task: 'matching' }) }, { sandbox: true }); is(sb.status, 200);
      is(s.get('Sandbox_Station_Sessions', 'welding__weld-1__ivy__sb1').task, 'matching'); assert(!s.get('Station_Sessions', 'welding__weld-1__ivy__sb1'), 'a sandbox session never lands in production');
    });
    // the old session was not touched by a beat or end for it (it is over)
    const r = await sessEvent({ id: OLD_SESSION.id, person: 'Old Hand', computerId: 'pc-OLDOLD01', task: 'matching' }, 'beat'); is(r.status, 200); is(JSONS(world.get('Station_Sessions', OLD_SESSION.id)), OLD_JSON, 'an old session stays as written');
    say('1 sessions: task kept at the Welding station only, two people at once, one person in both tasks, no task still works, a beat never adds one, sandbox apart, no PIN');
  }

  /* 2 · the activity door */
  {
    const ev = (n, id) => world.get('Station_Activity', id);
    const evs = world.all('Station_Activity').filter(d => d.day === '2026-10-05');
    const m1 = evs.find(d => d.orderId === '3521000001' && !/again/.test(d.detail));
    is(m1.action, 'matched'); is(m1.task, 'matching'); is(m1.station, 'welding'); is(m1.person, 'Ray Matcher');
    const un = evs.find(d => d.orderId === '3521000004');
    is(un.person, 'Unattributed', 'a scan with nobody in Matching is stored under Unattributed'); is(un.unattributed, true); is(un.task, 'matching');
    const sc = evs.find(d => d.orderId === '3521000003'); is(sc.action, 'scan'); is(sc.task, 'matching');
    const legacy = evs.filter(d => d.orderId === '3521000100'); eq(legacy.map(d => d.action).sort(), ['complete', 'scan']); assert(legacy.every(d => !('task' in d)), 'the old welding events carry no task');
    const roll = (day, p) => world.get('Efficiency_Daily', `${day}__${p}`);
    const ray = roll('2026-10-05', 'Ray Matcher').stations.welding;
    is(ray.matched, 4, 'three matched events and a scan made as Matching'); is(ray.scans, 6, 'as written: 4 matched + the desk page\'s scan of one phone scan + a typed scan'); is(ray.x_phone, 1, 'the desk page\'s "phone scan" is counted apart, so a reader can count that phone scan once'); assert(!ray.completes && !ray.parts && !ray.orders, 'matched is scanned work, never a completion');
    const echoDoc = evs.find(d => d.action === 'scan' && d.orderId === '3521000001'); is(echoDoc.detail, 'phone scan'); is(echoDoc.parts, 2, 'the echo is stored as it was sent: nothing is dropped or rewritten');
    is(roll('2026-10-05', 'Tess Welder').stations.welding.matched, 1); is(roll('2026-10-05', 'Unattributed').stations.welding.matched, 1);
    is(roll('2026-10-05', 'Max Mixed').stations.assembly.parts, 6); is(roll('2026-10-05', 'Max Mixed').stations.welding.matched, 2);
    const t = roll('2026-10-05', 'Tess Welder').stations.welding; is(t.completes, 1); is(t.parts, 12); is(t.orders, 1);                  // (the old way is stored as it always was; only the reading changed)
    await scratch(async s => {
      NOW = Z('2026-10-05T15:00:00Z');
      const batch = [E({ person: 'Zed Matcher', orderId: '3521000200', task: 'matching', parts: 3, orders: 1, role: 'laser' }), E({ person: 'Zed Matcher', station: 'assembly', device: 'assembly-1', action: 'matched', orderId: '3521000201', task: 'matching' }),
        E({ person: 'Zed Matcher', orderId: '3521000202', task: 'packing' }), E({ person: '482915', orderId: '3521000203' })];
      const r1 = await post({ activity: batch }); is(r1.status, 200); is(r1.body.written + r1.body.refused, 4, JSON.stringify(r1.body));
      const stored = s.all('Station_Activity'); const z0 = stored.find(d => d.orderId === '3521000200'), z1 = stored.find(d => d.orderId === '3521000201'), z2 = stored.find(d => d.orderId === '3521000202');
      is(z0.task, 'matching'); is(z0.role, 'laser'); assert(!('task' in z1), 'a task belongs to the Welding station alone'); assert(!('task' in z2), 'a task that is not welding or matching is dropped');
      assert(!stored.some(d => d.orderId === '3521000203'), 'a digits-only name is refused whatever else the event says');
      const z = s.get('Efficiency_Daily', '2026-10-05__Zed Matcher').stations; assert(!z.welding.completes && !z.welding.orders, 'a matched event that says orders: 1 completes nothing'); is(z.welding.matched, 2, 'two matched events (the one with a task that is not welding or matching is still a matched event, with no task)');
      const before = JSONS([...s.colls]);
      const r2 = await post({ activity: batch }); is(r2.body.written, 0, 'a resend stores nothing'); is(JSONS([...s.colls]), before, 'a resend counts nothing twice');
    });
    say('2 activity: matched / task / role / Unattributed stored, matched counted as scanned work (no completion, parts or orders), a resend counts once');
  }

  /* 3 · the Stations board */
  {
    world.reads.length = 0;
    const a = await live(); is(a.status, 200); is(a.body.ok, true);
    const wd = station(a, 'welding');
    is(wd.noThroughput, true, 'the Welding card says it has no pieces / orders'); eq(wd.counts, { partsToday: null, ordersToday: null, scansToday: 8 }, 'no pieces or orders today; the matched scans stand in the count (the desk page\'s own scan of a phone scan is not a ninth)');
    eq(wd.today, { day: '2026-10-05', matched: 8, unattributed: 1, taskMs: { welding: 3 * HOUR, matching: 2 * HOUR, unknown: HOUR } }, 'time on task per task (two people in Matching at once count once)');
    is(wd.people.length, 4, 'two people at once, one of them twice (a row per person and task)');
    eq(wd.names, ['Tess Welder', 'Ray Matcher', 'Legacy Lou'], 'a person in both tasks is one name');
    const welding = wd.people.filter(p => p.task === 'welding'), matching = wd.people.filter(p => p.task === 'matching'), none = wd.people.filter(p => !p.task);
    eq(welding.map(p => p.name), ['Tess Welder']); eq(matching.map(p => p.name), ['Ray Matcher', 'Tess Welder']); eq(none.map(p => p.name), ['Legacy Lou'], 'an old page: no task');
    const tw = welding[0], ray = matching[0], tm = matching[1], lou = none[0];
    is(tw.since, Z('2026-10-05T12:00:00Z')); is(tw.todayMs, 3 * HOUR); is(tw.lastInputAt, null, 'welding has no scans: no input time but the beat'); is(tw.lastSeenAt, NOW);
    is(ray.since, Z('2026-10-05T13:00:00Z')); is(ray.todayMs, 2 * HOUR); is(ray.lastInputAt, Z('2026-10-05T14:20:00Z'), 'the last input of a Matching person is their latest matched scan');
    is(tm.todayMs, HOUR); is(tm.lastInputAt, Z('2026-10-05T14:35:00Z')); is(lou.todayMs, HOUR, 'an old session without a task is Welding time with the task unknown'); is(lou.lastInputAt, null);
    for (const p of wd.people) { is(p.device, 'weld-1'); is(p.deviceLabel, 'Welding'); assert(!('role' in p)); }
    const dv = wd.devices.find(x => x.device === 'weld-1'); is(dv.person, 'Tess Welder, Ray Matcher, Legacy Lou', 'the page lists everybody signed in at it, a person once'); is(dv.state, 'idle');
    eq(a.body.signedIn.filter(x => x.stationKey === 'welding').map(x => [x.name, x.task || '']).sort(), [['Legacy Lou', ''], ['Ray Matcher', 'matching'], ['Tess Welder', 'matching'], ['Tess Welder', 'welding']]);
    // the matched list: newest first, every scan, the person (or nobody), thumbnails like a current order
    eq(wd.matched.map(m => m.rid), ['3521000005', '3521000004', '3521000001', '3521000007', '3521000003', '3521000002', '3521000001', '3521000006'], 'a re-scan is a second row, newest first');
    eq(wd.matched.map(m => m.person), ['Tess Welder', '', 'Ray Matcher', 'Max Mixed', 'Ray Matcher', 'Ray Matcher', 'Ray Matcher', 'Max Mixed']);
    eq(wd.matched.map(m => m.unattributed), [false, true, false, false, false, false, false, false], 'scanned with nobody in Matching');
    eq(wd.matched.map(m => m.note), ['', 'Scanned with nobody in Matching', '', '', '', '', '', ''], 'the board says it in words');
    eq(wd.matched.map(m => m.at), ['14:35', '14:30', '14:20', '13:50', '13:42', '13:40', '13:30', '13:20'].map(h => Z(`2026-10-05T${h}:00Z`)));
    const o1 = wd.matched.find(m => m.rid === '3521000001'), o2 = wd.matched.find(m => m.rid === '3521000002');
    is(o1.thumbUrl, URL_GF, 'the vector design of the order\'s first item'); is(o1.photoUrl, URL_ARCH); is(o1.customer, 'Dana Q.'); is(o2.thumbUrl, URL_PHOTO, 'no vector design: the saved picture');
    is(wd.matched.find(m => m.rid === '3521000005').thumbUrl, '', 'an order with nothing saved has no picture');
    is(world.readsOf('Station_Activity').length, 1, 'one small read for the matched list'); eq(world.readsOf('Station_Activity')[0].filters, ['day==', 'station==']);
    // Assembly is unchanged: pieces and orders today, and nobody is "matched" there
    const as = station(a, 'assembly'); eq(as.counts, { partsToday: 11, ordersToday: 2, scansToday: 0 }); assert(!as.noThroughput && !as.matched && !as.today); eq(as.names, ['Ann Assembler']);
    assert(as.people.every(p => !('task' in p) && !('todayMs' in p)));
    // nothing matched today: no read of the activity events at all; a read that fails is named, the counts from the rollups stay
    await scratch(async s => {
      s.put('Station_Sessions', 'welding__weld-1__a1', { id: 'welding__weld-1__a1', person: 'Tess Welder', station: 'welding', device: 'weld-1', startAt: NOW - HOUR, lastSeenAt: NOW - 30000, endAt: null, task: 'welding' });
      s.put('Efficiency_Daily', '2026-10-05__Tess Welder', { day: '2026-10-05', person: 'Tess Welder', stations: { welding: { scans: 2, parts: 9, orders: 3, completes: 3 } }, touched: { 3521000100: { welding: true } } });
      const q = await live(); const w = station(q, 'welding');
      is(s.readsOf('Station_Activity').length, 0, 'no matched scans today: the events are not read'); eq(w.matched, []); eq(w.today, { day: '2026-10-05', matched: 0, unattributed: 0, taskMs: { welding: HOUR, matching: 0, unknown: 0 } });
      eq(w.counts, { partsToday: null, ordersToday: null, scansToday: 0 }, 'an old rollup\'s welding pieces and orders are not shown');
      s.put('Efficiency_Daily', '2026-10-05__Ray Matcher', { day: '2026-10-05', person: 'Ray Matcher', stations: { welding: { scans: 2, matched: 2 } } });
      NOW += 120000; s.fail('Station_Activity'); const q2 = await live(); const w2 = station(q2, 'welding'); NOW -= 120000;
      is(q2.body.partial, true); assert(q2.body.errors.some(e => /^matched:/.test(e)), JSON.stringify(q2.body.errors)); is(w2.today.matched, 2, 'the count comes from the rollups'); eq(w2.matched, []);
    });
    // an old store: sessions with no task, no role, no input time, rollups with no matched counter: all still read
    await scratch(async s => {
      s.put('Station_Sessions', 'old1', { person: 'Pat Old', station: 'welding', device: 'weld-1', startAt: NOW - 2 * HOUR, lastSeenAt: NOW - 60000, endAt: null });
      s.put('Station_Sessions', 'old2', { person: 'Quin Old', station: 'welding', device: 'weld-1', startAt: NOW - 3 * HOUR, lastSeenAt: NOW - HOUR, endAt: NOW - HOUR });
      s.put('Efficiency_Daily', '2026-10-05__Pat Old', { day: '2026-10-05', person: 'Pat Old', stations: { welding: { scans: 3, parts: 8, orders: 2 } } });
      const q = await live(); const w = station(q, 'welding');
      eq(w.people.map(p => [p.name, p.task || '', p.todayMs]), [['Pat Old', '', 2 * HOUR]]); eq(w.today.taskMs, { welding: 0, matching: 0, unknown: 3 * HOUR });
      is(w.counts.scansToday, 0); is(q.body.partial, undefined);
    });
    say('3 board: Welding as people with task, time on task and last input; matched list with thumbnails and Unattributed; no pieces or orders; Assembly unchanged; old sessions read');
  }

  /* 4 · totals exclude Welding */
  {
    const o = await ask({ op: 'overview', day: '2026-10-05' }); is(o.status, 200);
    const B = o.body.business;
    eq(B.totals, { parts: 11, scans: 9, orders: 2, people: 5 }, 'only Assembly counts: Tess\'s 12 welded pieces, her order, and everybody\'s matched orders are not throughput');
    eq(Object.keys(B.perHour), ['assembly'], 'no welding in the per-hour chart'); eq(B.perHour.assembly.map((v, h) => [h, v]).filter(x => x[1]), [[8, 6], [9, 5]]);
    const wr = B.stations.find(s => s.station === 'welding'), ar = B.stations.find(s => s.station === 'assembly');
    eq({ parts: wr.parts, orders: wr.orders, scans: wr.scans, matched: wr.matched, taskMin: wr.taskMin }, { parts: 0, orders: 0, scans: 9, matched: 7, taskMin: { welding: 180, matching: 240, unknown: 60 } }, 'the card shows minutes per task and the matched count');
    eq(wr.peopleNow, ['Legacy Lou', 'Ray Matcher', 'Tess Welder']); eq({ parts: ar.parts, orders: ar.orders }, { parts: 11, orders: 2 }); assert(!('matched' in ar));
    eq(o.body.people.map(p => p.name), ['Ann Assembler', 'Legacy Lou', 'Ray Matcher', 'Tess Welder', 'Max Mixed'], 'the ranking is by counted pieces: Tess\'s welded pieces do not lift her above anybody');
    const P = n => o.body.people.find(p => p.name === n);
    eq([P('Tess Welder').totals.parts, P('Tess Welder').totals.orders, P('Tess Welder').totals.scans], [0, 0, 2]); eq([P('Max Mixed').totals.parts, P('Max Mixed').totals.orders, P('Max Mixed').totals.scans], [6, 1, 2]);
    eq(P('Tess Welder').stations[0].taskMin, { welding: 180, matching: 60, unknown: 0 }); eq(P('Ray Matcher').stations[0].taskMin, { welding: 0, matching: 120, unknown: 0 }); eq(P('Legacy Lou').stations[0].taskMin, { welding: 0, matching: 0, unknown: 60 });
    is(P('Ray Matcher').stations[0].matched, 4); assert(!o.body.people.some(p => p.name === 'Unattributed'), 'a scan made with nobody in Matching is nobody\'s: the board counts it, no person has it');
    // somebody who worked at Welding alone has no pieces or orders to count: the answer says so (the console shows a dash on the People card, as the person page does, with time on task and matched instead); a mixed day does not
    eq(['Tess Welder', 'Ray Matcher', 'Legacy Lou', 'Ann Assembler', 'Max Mixed'].map(n => [n, P(n).noThroughput === true]), [['Tess Welder', true], ['Ray Matcher', true], ['Legacy Lou', true], ['Ann Assembler', false], ['Max Mixed', false]], 'noThroughput: Welding alone, never a mixed day or an Assembly person');
    eq(B.trend.slice(-4).map(d => [d.day, d.parts, d.orders, d.people, d.source]), [['2026-10-02', 0, 0, 1, 'sessions'], ['2026-10-03', 0, 0, 1, 'events'], ['2026-10-04', 0, 0, 1, 'events'], ['2026-10-05', 11, 2, 5, 'events']],
      'the old welding day (20 pieces, 5 orders written then) reads as 0 now; the stored rollup is unchanged');
    // the feed: the typed scan is there; the desk page's scan of the phone scan is not (the matched event is that scan)
    assert(o.body.feed.some(f => f.orderId === '3521000300' && f.action === 'scan'), 'a typed scan is a scan'); assert(!o.body.feed.some(f => f.orderId === '3521000001' && f.action === 'scan'), 'the echo of a phone scan is not a second scan in the feed'); assert(o.body.feed.some(f => f.orderId === '3521000001' && f.action === 'matched'));
    const w = await ask({ op: 'overview', day: '2026-10-05', days: 7 });
    const W7 = w.body.business, wr7 = W7.stations.find(s => s.station === 'welding');
    eq(W7.totals, { parts: 11, scans: 15, orders: 2, people: 6 }, 'a week: the old welding day adds scans only'); eq({ matched: wr7.matched, taskMin: wr7.taskMin, parts: wr7.parts, orders: wr7.orders }, { matched: 9, taskMin: { welding: 420, matching: 480, unknown: 240 }, parts: 0, orders: 0 });
    eq(Object.keys(W7.perHour), ['assembly']);
    say('4 totals: Overview totals, orders, per hour, trend and ranking leave Welding out; time per task and matched shown instead; an old welding day reads 0 pieces');
  }

  /* 5 · the person view */
  {
    const HRS = { day: [1, 3], week: [7, 3], month: [30, 3], quarter: [90, 3], year: [365, 3] };
    const RAY = { day: [2, 4, 5], week: [4, 6, 7], month: [4, 6, 7], quarter: [5.5, 8, 9], year: [6.5, 9, 10] };       // [matching hours, matched, scans]
    for (const range of ['day', 'week', 'month', 'quarter', 'year']) {
      const r = await person('Ray Matcher', range); is(r.status, 200, range);
      const w = r.body.welding; assert(w, 'a welding block in ' + range);
      eq([w.hours.matching, w.matched, w.hours.welding, w.hours.unknown], [RAY[range][0], RAY[range][1], 0, 0], 'Ray, ' + range);
      eq([w.metrics.matchingHours.value, w.metrics.matchedOrders.value, w.metrics.weldingHours.value], [RAY[range][0], RAY[range][1], 0]);
      is(r.body.kpis.scans.value, RAY[range][2], 'scans in ' + range + ': 4 matched + 1 typed today, a phone scan once'); is(r.body.stations[0].scans, RAY[range][2]);
      is(r.body.days, HRS[range][0]);
      // the chart's buckets are the same buckets as the profile's own series and add up to the totals
      eq(w.series.map(x => x.day), r.body.series.map(x => x.day), 'the same buckets as the other charts');
      eq(r.body.granularity, range === 'year' ? 'week' : 'day');
      const sum = k => w.series.reduce((n, x) => n + (x[k] || 0), 0);
      near(sum('matchingMs') / HOUR, w.hours.matching, 0.051, range + ' matching'); is(sum('matched'), w.matched, range + ' matched'); is(sum('weldingMs'), 0); is(sum('unknownMs'), 0);
      assert(w.series.every(x => (x.matchingMs === null) === (x.matched === null)), 'a bucket with nothing at the station is empty, not 0');
      const k = r.body.kpis; eq([k.parts.value, k.orders.value, k.ordersCompleted.value], [null, null, null], 'a person who only matched has no pieces or orders (a dash, never a 0)');
      eq(r.body.stations.map(s => [s.station, s.parts, s.orders, s.completes, s.shareParts, s.perActiveHour]), [['welding', 0, 0, 0, null, null]]); is(r.body.stations[0].matched, w.matched);
      assert(r.body.series.every(x => x.parts === null && x.orders === null), 'no pieces or orders in the charts');
    }
    const day = await person('Ray Matcher', 'day'); eq(day.body.welding.series, [{ day: '2026-10-05', to: '2026-10-05', days: 1, weldingMs: 0, matchingMs: 2 * HOUR, unknownMs: 0, matched: 4 }]);
    // the same person in both tasks: each task's hours, the station's time once
    const tess = await person('Tess Welder', 'day'), tw = tess.body.welding;
    eq(tw.hours, { welding: 3, matching: 1, unknown: 0, station: 3 }, 'welding and matching overlap from 10:00: the station\'s own time counts it once'); is(tw.matched, 1);
    const tw7 = (await person('Tess Welder', 'week')).body; eq(tw7.welding.hours, { welding: 7, matching: 3, unknown: 0, station: 7 }, '2 Oct: 4 h of welding, of which 2 h also matching'); is(tw7.welding.matched, 1);
    eq(tw7.welding.series.map(x => [x.day, x.weldingMs, x.matchingMs, x.matched]).filter(x => x[1] !== null), [['2026-10-02', 4 * HOUR, 2 * HOUR, 0], ['2026-10-05', 3 * HOUR, HOUR, 1]]);
    is(tw7.kpis.signedHours.value, 7, 'signed-in time is the union, not 10 h'); is(tw.metrics.weldingHours.prev, 0); is(tess.body.kpis.parts.value, null, 'her old welded 12 pieces are not pieces');
    is(tw7.kpis.piecesScanned.value, 12, 'scanned work stays: the old scan still shows as a scan');
    // an old session without a task: Welding time with the task unknown
    const lou = (await person('Legacy Lou', 'day')).body; eq(lou.welding.hours, { welding: 0, matching: 0, unknown: 1, station: 1 }); is(lou.welding.matched, 0);
    const old = (await person('Old Hand', 'week')).body; eq(old.welding.hours, { welding: 0, matching: 0, unknown: 3, station: 3 }, 'the 4 Oct session, written before tasks existed'); is(old.kpis.parts.value, null, 'the 20 pieces written that day are not counted now');
    eq(old.stations.map(s => [s.station, s.parts, s.orders, s.completes, s.scans]), [['welding', 0, 0, 0, 4]]);
    // a person who also works a counted station: only that station's time makes the rates
    const max = (await person('Max Mixed', 'day')).body;
    eq([max.kpis.parts.value, max.kpis.orders.value, max.kpis.signedHours.value, max.kpis.partsPerSignedHour.value], [6, 1, 2, 6], '6 pieces in the 1 h at Assembly, not in the 2 h signed in');
    eq(max.stations.map(s => [s.station, s.parts, s.shareParts]), [['assembly', 6, 100], ['welding', 0, null]]); eq(max.welding.hours, { welding: 0, matching: 1, unknown: 0, station: 1 }); is(max.welding.matched, 2);
    // nobody else has a Welding block
    const ann = (await person('Ann Assembler', 'day')).body; is(ann.welding, null); is(ann.kpis.parts.value, 5);
    const nobody = await person('Unattributed', 'week'); is(nobody.body.found, false, 'Unattributed is nobody');
    say('5 person view: hours per task and matched for day / week / month / quarter / year, chart buckets add up, both tasks at once, old task-less time, KPIs without Welding, mixed day');
  }

  /* 6 · the matched-orders list and the order trace */
  {
    const m = await ask({ op: 'personOrders', name: 'Ray Matcher', matched: true, from: '2026-02-01' }); is(m.status, 200);
    is(m.body.total, 8); eq(m.body.orders.map(o => o.rid), ['3521000001', '3521000003', '3521000002', '3521000071', '3521000070', '3521000061', '3521000060', '3521000050'], 'newest day first, then the last scan');
    const r1 = m.body.orders[0]; is(r1.matched, 2, 'scanned twice'); is(r1.scans, 2, 'two scans of this order: the desk page\'s scan of the first one is not a third'); is(r1.matchedAt, Z('2026-10-05T14:20:00Z')); eq(r1.stations, ['welding']); is(r1.completes, 0); is(r1.parts, 0); assert(r1.issues.some(i => i.kind === 'rescan'), 'a second scan is the "scanned again" issue');
    const tess = await ask({ op: 'personOrders', name: 'Tess Welder', matched: true, from: '2026-02-01' }); eq(tess.body.orders.map(o => o.rid), ['3521000005'], 'her old welding scan and completion are not matched orders');
    const plain = await ask({ op: 'personOrders', name: 'Ray Matcher', from: '2026-02-01' }); is(plain.body.total, 0, 'the ordinary list is orders worked at counted stations: matched orders are not in it');
    const maxo = await ask({ op: 'personOrders', name: 'Max Mixed', from: '2026-10-01' }); eq(maxo.body.orders.map(o => o.rid), ['3521000102'], 'a mixed day: the Assembly order only');
    const mm = await ask({ op: 'personOrders', name: 'Max Mixed', matched: true, from: '2026-10-01' }); eq(mm.body.orders.map(o => o.rid), ['3521000007', '3521000006']);
    const un = await ask({ op: 'personOrders', name: 'Unattributed', matched: true, from: '2026-10-01' }); is(un.body.found, false); eq(un.body.orders, []);
    const tr = await ask({ op: 'orders', orderId: '3521000004' }); eq(tr.body.steps.map(s => [s.station, s.person, s.scans, s.completes]), [['welding', 'Unattributed', 1, 0]], 'the order trace names the scan, and who: nobody');
    const t1 = await ask({ op: 'orders', orderId: '3521000001' }); eq(t1.body.steps.map(s => [s.station, s.person, s.scans, s.completes]), [['welding', 'Ray Matcher', 2, 0]]); eq(t1.body.events.map(e => [e.action, !!e.echo]), [['matched', false], ['scan', true], ['matched', false]], 'the trace lists the echo (history is kept) and marks it; it is no scan of the step');
    say('6 matched list: every order the person matched, newest first, scans per order, not in the ordinary list; the order trace shows matched and Unattributed');
  }

  /* 6b · a physical phone scan counts once: the matched event, never also the desk page's own scan of it */
  {
    const M = (id, order, at, o) => Object.assign({ id, station: 'welding', action: 'matched', orderId: order, at: Z('2026-10-05T16:00:00Z') + at, detail: 'phone scan', task: 'matching' }, o || {});
    const P = (id, order, at, o) => Object.assign({ id, station: 'welding', action: 'scan', orderId: order, at: Z('2026-10-05T16:00:00Z') + at, detail: 'phone scan', task: '' }, o || {});
    const ids = list => [...KIND.echoScans(list)].map(e => e.id).sort();
    eq(ids([M('m1', 'A', 0), P('p1', 'A', 8000)]), ['p1'], 'a plain scan 8 s after the matched scan of the same order is its echo');
    eq(ids([P('p1', 'A', 0), M('m1', 'A', 8000)]), ['p1'], 'whichever came first');
    eq(ids([M('m1', 'A', 0), P('p1', 'A', 15000)]), ['p1'], '15 seconds is inside'); eq(ids([M('m1', 'A', 0), P('p1', 'A', 15001)]), [], 'a second later it is a scan of its own');
    eq(ids([M('m1', 'A', 0), P('p1', 'B', 1000)]), [], 'another order is another scan'); eq(ids([P('p1', 'A', 0)]), [], 'with no matched event beside it (every old scan) a plain scan stays');
    eq(ids([M('m1', 'A', 0), P('p1', 'A', 1000, { station: 'assembly' })]), [], 'only at the Welding station'); eq(ids([M('m1', 'A', 0), P('p1', 'A', 1000, { task: 'matching' })]), [], 'a scan that says it is Matching is a matched scan, not an echo');
    eq(ids([M('m1', 'A', 0), P('p1', 'A', 1000, { detail: 'typed' })]), ['p1'], 'a typed order within 15 s of the phone scan of the same order is the same scan');
    eq(ids([M('m1', 'A', 0), M('m2', 'A', 3000)]), [], 'two matched scans are two matched scans (the scanner app has its own duplicate window)');
    // the rollup: the echoes counted apart come off the scans, one for each matched scan to pair with; an old rollup reads as it was written
    eq(KIND.readStationCounters('welding', { scans: 6, matched: 4, x_phone: 1, parts: 9, orders: 2, completes: 2 }), { scans: 5, matched: 4, x_phone: 1, parts: 0, orders: 0, completes: 0, undoParts: 0, undoOrders: 0 });
    eq(KIND.readStationCounters('welding', { scans: 3, x_phone: 3 }).scans, 3, 'echoes with no matched scan to pair with (the days before the scanner app) stay'); eq(KIND.readStationCounters('welding', { scans: 9, matched: 2, x_phone: 5 }).scans, 7, 'never more taken off than there are matched scans');
    eq(KIND.readStationCounters('assembly', { scans: 6, matched: 4, x_phone: 1 }), { scans: 6, matched: 4, x_phone: 1 }, 'no other station is touched');
    say('6b one phone scan is one scan: the matched event; the desk page\'s own scan of it (15 s, same order) is listed, never counted; typed scans and old scans stay');
  }

  /* 7 · attendance and hours use the task sessions */
  {
    const w = (await person('Tess Welder', 'week')).body, cal = d => w.calendar.find(x => x.day === d);
    is(cal('2026-10-02').signedMs, 4 * HOUR, 'two task sessions at once are one stretch of time: 4 h, not 6'); is(cal('2026-10-02').firstIn, Z('2026-10-02T13:00:00Z')); is(cal('2026-10-02').lastOut, Z('2026-10-02T17:00:00Z')); is(cal('2026-10-02').endedBy, 'signOut');
    is(cal('2026-10-05').signedMs, 3 * HOUR); is(cal('2026-10-05').parts, null, 'a day at the Welding station has no pieces'); is(cal('2026-10-05').orders, null); is(cal('2026-10-05').others, 4, 'the others in that day: Unattributed is not a person');
    is(w.attendance.daysWorked + w.attendance.extraDays, 2); is(w.attendance.medianStart, 480, 'the first sign-in of either task starts the day');
    const m = (await person('Max Mixed', 'week')).body.calendar.find(x => x.day === '2026-10-05'); is(m.signedMs, 2 * HOUR, 'Assembly then Matching, one after the other'); is(m.parts, 6); is(m.orders, 1);
    const l = (await person('Legacy Lou', 'week')).body.calendar.find(x => x.day === '2026-10-05'); is(l.signedMs, HOUR); is(l.parts, null);
    const o = (await person('Old Hand', 'week')).body.calendar.find(x => x.day === '2026-10-04'); is(o.signedMs, 3 * HOUR, 'an old session without a task is signed-in time as before'); is(o.parts, null, 'its 20 pieces are not shown'); is(o.orders, null);
    say('7 attendance: task sessions overlapping count once, the Welding day has no pieces or orders, Unattributed is nobody');
  }

  /* 8 · nothing in history rewritten, the console wrote nothing, no PIN or passcode anywhere */
  {
    is(world.writes.length, WRITES0, 'the console\'s reads wrote no document');
    let changed = 0; for (const [k, json] of HISTORY) { const [n, ...id] = k.split('/'); if (JSONS(world.get(n, id.join('/'))) !== json) changed++; }
    is(changed, 0, `${HISTORY.size} stored documents from before today (sessions, rollups, events) are byte for byte as they were written`);
    is(JSONS(world.get('Station_Sessions', OLD_SESSION.id)), OLD_JSON); is(JSONS(world.get('Efficiency_Daily', '2026-10-04__Old Hand')), JSONS(OLD_ROLLUP), 'the old welding day keeps its pieces and orders in the store');
    const everything = bodies.join('\n') + '\n' + logs.join('\n');
    assert(!everything.includes(PASS), 'the passcode is in no answer and no log line'); assert(!everything.includes(PIN), 'no Employee Number');
    assert(!/"person":"\d+"/.test(everything), 'no digits-only person');
    say('8 history: ' + HISTORY.size + ' stored documents unchanged, the reads wrote nothing, no passcode or PIN in ' + bodies.length + ' answers and ' + logs.length + ' log lines');
  }
  say('welding-portal: all checks passed');
})().catch(e => { process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
