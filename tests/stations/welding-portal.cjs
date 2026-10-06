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
  const colls = new Map(), reads = [], writes = [];
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
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
    reads, writes, readsOf: name => reads.filter(r => r.name === name), writesOf: name => writes.filter(w => w[0] === name), clear: () => { reads.length = 0; writes.length = 0; }, colls,
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
  ann: { id: 'assembly__assembly-1__ann__k5', person: 'Ann Assembler', station: 'assembly', device: 'assembly-1', computerId: 'pc-ASSM0001' }
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
  // 4 Oct (Sunday): an OLD welding session and rollup, written the way the station wrote them before tasks existed (no `task`, welding parts and orders counted)
  const OLD_SESSION = { id: 'welding__weld-1__oldhand__j9', person: 'Old Hand', employeeId: '', station: 'welding', device: 'weld-1', computerId: 'pc-OLDOLD01', computerLabel: 'Welding PC', startAt: Z('2026-10-04T13:00:00Z'), lastSeenAt: Z('2026-10-04T16:00:00Z'), endAt: Z('2026-10-04T16:00:00Z'), endReason: 'signOut', minutes: 180 };
  const OLD_ROLLUP = { day: '2026-10-04', person: 'Old Hand', events: 5, firstAt: Z('2026-10-04T13:05:00Z'), lastAt: Z('2026-10-04T15:55:00Z'), stations: { welding: { scans: 4, scanParts: 20, completes: 5, parts: 20, orders: 5, activeMs: 900000, idleMs: 0, firstAt: Z('2026-10-04T13:05:00Z'), lastAt: Z('2026-10-04T15:55:00Z') } },
    hours: { '09': { scans: 4, parts: 20, by: { welding: { scans: 4, parts: 20 } } } }, touched: { 3521000090: { welding: true }, 3521000091: { welding: true } } };
  cur.put('Station_Sessions', OLD_SESSION.id, OLD_SESSION); cur.put('Efficiency_Daily', '2026-10-04__Old Hand', OLD_ROLLUP);
  const HISTORY = cur.snapshot();                          // everything written so far is history: nothing below may change it
  const histDocs = ['Station_Sessions', 'Efficiency_Daily', 'Station_Activity'].map(n => [n, cur.all(n).filter(d => !/2026-10-05|k[1-5]$/.test(d._id) || /oldhand/.test(d._id))]);
  void histDocs;
  // 5 Oct (Monday), today: Tess welds from 08:00, Ray matches from 09:00, Tess ALSO matches from 10:00, a page with no tasks (Lou) from 10:00, Ann assembles
  await start(S.tessW, '2026-10-05T12:00:00Z'); await start(S.ann, '2026-10-05T12:00:00Z');
  await emit('2026-10-05T12:20:00Z', { person: 'Tess Welder', action: 'scan', orderId: '3521000100', parts: 12 });                                  // the old way: a welding scan and completion with pieces and an order
  await emit('2026-10-05T12:30:00Z', { person: 'Tess Welder', action: 'complete', orderId: '3521000100', parts: 12, orders: 1, sincePrevMs: 10 * MIN });
  await start(S.rayM, '2026-10-05T13:00:00Z');
  await emit('2026-10-05T13:00:00Z', { person: 'Ann Assembler', station: 'assembly', device: 'assembly-1', action: 'complete', orderId: '3521000101', parts: 5, orders: 1 });
  await emit('2026-10-05T13:30:00Z', { person: 'Ray Matcher', orderId: '3521000001', task: 'matching' });
  await emit('2026-10-05T13:40:00Z', { person: 'Ray Matcher', orderId: '3521000002', task: 'matching', sincePrevMs: 10 * MIN });
  await emit('2026-10-05T13:42:00Z', { person: 'Ray Matcher', action: 'scan', orderId: '3521000003', task: 'matching', sincePrevMs: 2 * MIN });        // a scan made as Matching counts as matched too
  await start(S.tessM, '2026-10-05T14:00:00Z'); await start(S.lou, '2026-10-05T14:00:00Z');
  await emit('2026-10-05T14:20:00Z', { person: 'Ray Matcher', orderId: '3521000001', task: 'matching', detail: 'scanned again', sincePrevMs: 38 * MIN });
  await emit('2026-10-05T14:30:00Z', { person: '', unattributed: true, orderId: '3521000004', task: 'matching' });                                         // scanned with nobody in Matching
  await emit('2026-10-05T14:35:00Z', { person: 'Tess Welder', orderId: '3521000005', task: 'matching' });
  await advance('2026-10-05T15:00:00Z');

  if (process.env.DUMP) {
    const r = await ask(JSON.parse(process.env.DUMP));
    process.stdout.write(JSON.stringify(r.body, null, 1) + '\n', () => process.exit(0));
    return;
  }
  say('world built', cur.all('Station_Sessions').length, 'sessions', cur.all('Efficiency_Daily').length, 'rollups', cur.all('Station_Activity').length, 'events', HISTORY.length);
})().catch(e => { process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
