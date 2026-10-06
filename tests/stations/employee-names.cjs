// The REAL employee names (Paul's PIN list) from every station to the console, offline. The seven names exactly as they are
// stored in the list: "Giovanna C.", "Empress D.", "Michael_V", "Michelle_R", "Ivy_Y", "Ana_M", "Paul_K" (the underscore style
// next to the older "Giovanna C." style), plus the spellings people get typed or created elsewhere: "Michael V." and
// "MICHAEL V" (design pages, sorter, sorting), "Ana M.", "Paul K" and "Giovanna" (inbox accounts), and two different Michaels
// ("Michael T." and a bare "Michael") who must NOT be merged into Michael V.
//   The real modules over ONE in-memory Firestore (fake clock, fake passcode, fake PINs made up when this runs and never
//   printed): the login door {pinLogin} returns each name exactly as stored -> sessions through the open door -> the real
//   station-activity.js in a vm page per station -> events and rollups (_stationActivity.js) -> employeeEfficiency.js
//   (overview, person, orders) -> charm-nest-efficiency.js (the console's view model).
//   1 · the write side: every name passes the door and the session op as sent (underscore, trailing period, doubled space
//       collapsed), is never a PIN, and makes a valid Firestore rollup id; two spellings of one person write separate rollup docs
//   2 · the day: 14 pages, 9 people. The reader shows the seven names once each, with nice names (Michael V., Ana M., Paul K.),
//       the two other Michaels apart, every figure hand-computed (parts, scans, orders, signed-in time counted once across two
//       spellings and across two stations in the same hour, active and idle never above the time signed in)
//   3 · asking by any spelling (op person) gives the same answer; the live feed and the order trace use the nice name
//   4 · the sums: the rollup documents add up to the people, the people to the business, the hours to the parts
//   5 · config/employeeAliases: its own spelling is shown as written (underscore and all), and its alias entries fold the same way
//   6 · the console's view model shows the same nine people, the same totals
//   node tests/stations/employee-names.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module'), vm = require('vm'), fs = require('fs');
const root = path.join(__dirname, '../..');

/* ═══════════════════════════ one in-memory Firestore ═══════════════════════════ */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const SERVER_TS = { __ts: true }, DEL = { __del: true }, inc = n => ({ __inc: n });
const colls = new Map(), vers = new Map();
const col = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Ts) && v.__inc === undefined && !v.__ts && !v.__del;
const clone = v => v instanceof Ts ? new Ts(v.m) : Array.isArray(v) ? v.map(clone) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
function apply(prev, patch, merge, at) {
  const out = merge && prev ? clone(prev) : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v && v.__inc !== undefined) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = new Ts(at);
    else if (v && v.__del) delete out[k];
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge, at);
    else out[k] = clone(v);
  }
  return out;
}
const kind = v => v instanceof Ts ? 'ts' : typeof v, val = v => v instanceof Ts ? v.m : v;
const refOf = (c, id) => ({ c, id, path: c + '/' + id });
const snapOf = r => { const d = col(r.c).get(r.id); return { id: r.id, exists: !!d, data: () => d ? clone(d) : undefined, ref: r }; };
function query(name, filters, order, lim) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim),
    orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim),
    limit: n => query(name, filters, order, n),
    get: async () => {
      let docs = [...col(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
        const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
        const a = val(x), b = val(v);
        return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
      });
      if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
      if (lim != null) docs = docs.slice(0, lim);
      return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(d) })), size: docs.length, empty: !docs.length };
    }
  };
}
const hooks = { beforeCommit: null, failTx: null, txRuns: 0 };
const fakeDb = {
  collection: c => Object.assign(query(c, [], null, null), {
    doc: id => Object.assign(refOf(c, id), {
      get: async () => snapOf(refOf(c, id)),
      set: async (v, o) => { col(c).set(id, apply(col(c).get(id), v, !!(o && o.merge), Date.now())); vers.set(c + '/' + id, (vers.get(c + '/' + id) || 0) + 1); }
    })
  }),
  /* optimistic concurrency like Firestore: a transaction whose reads were overtaken by another commit runs again */
  runTransaction: async fn => {
    for (let attempt = 1; attempt <= 6; attempt++) {
      const seen = new Map(), writes = [];
      const touch = r => seen.set(r.path, vers.get(r.path) || 0);
      const tx = { get: async r => { touch(r); return snapOf(r); }, getAll: async (...rs) => rs.map(r => { touch(r); return snapOf(r); }), set: (r, d, o) => { writes.push([r, d, o]); return tx; } };
      hooks.txRuns++;
      if (hooks.failTx && hooks.failTx(attempt)) throw new Error('14 UNAVAILABLE: synthetic outage');
      const out = await fn(tx);
      if (hooks.beforeCommit) await hooks.beforeCommit(attempt, hooks.txRuns);
      if ([...seen].some(([p, v]) => (vers.get(p) || 0) !== v)) continue;
      const at = Date.now();
      for (const [r, d, o] of writes) { col(r.c).set(r.id, apply(col(r.c).get(r.id), d, !!(o && o.merge), at)); vers.set(r.path, (vers.get(r.path) || 0) + 1); }
      return out;
    }
    throw new Error('10 ABORTED: too much contention');
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { Timestamp: Ts, FieldValue: { serverTimestamp: () => SERVER_TS, increment: inc, delete: () => DEL } }) };
const resetStore = () => { colls.clear(); vers.clear(); hooks.beforeCommit = null; hooks.failTx = null; hooks.txRuns = 0; };

/* ═══════════════════════════ the real modules over it ═══════════════════════════ */
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const reader = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const ACT = require(path.join(root, 'netlify/functions/_stationActivity.js'));
const AK = require(path.join(root, 'netlify/functions/_activityKinds.js'));
Module._load = realLoad;
const T = reader._t;
const PASS = 'synthetic-names-pass-5k2x';
process.env.EDIT_PASSCODE = PASS;
const realNow = Date.now, realErr = console.error, realWarn = console.warn;
let NOW = Date.UTC(2026, 8, 30, 12, 0);
Date.now = () => NOW;
const EDT = (y, m, d, hh, mm = 0, ss = 0) => Date.UTC(y, m - 1, d, hh + 4, mm, ss);      // New York wall clock in daylight time
const D0 = (hh, mm = 0, ss = 0) => EDT(2026, 9, 30, hh, mm, ss), D1 = (hh, mm = 0, ss = 0) => EDT(2026, 10, 1, hh, mm, ss), D2 = (hh, mm = 0, ss = 0) => EDT(2026, 10, 2, hh, mm, ss);
const MIN = 60000;
let ipN = 0;
const post = (payload, o = {}) => door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': o.ip || '203.0.113.' + (++ipN % 250) }, queryStringParameters: o.sandbox ? { sandbox: '1' } : {}, body: typeof payload === 'string' ? payload : JSON.stringify(payload) })
  .then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const read = async (body, o = {}) => {
  const r = await reader.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': o.ip || '198.51.100.' + (++ipN % 200) }, body: JSON.stringify(Object.assign({ op: 'overview', key: PASS }, body)) });
  return { status: r.statusCode, body: JSON.parse(r.body || '{}'), raw: r.body };
};
const ask = async (body, o) => { const r = await read(body, o); assert.strictEqual(r.status, 200, JSON.stringify(r.body).slice(0, 300)); return r.body; };
const dropCaches = () => { const c = T.cacheOf(fakeDb); c.memo.clear(); c.recent.clear(); c.fails.clear(); EP.resetCache(); };
const eq = (a, b, msg) => assert.deepStrictEqual(a, b, msg);
const say = s => process.stdout.write(s + '\n');
const settle = async () => { for (let i = 0; i < 40; i++) await new Promise(r => setImmediate(r)); };
const docsOf = c => [...col(c).entries()].map(([id, d]) => Object.assign({ _id: id }, d));
const eventsOf = person => docsOf('Station_Activity').filter(e => e.person === person);
/* ═══════════════════════════ a station's browser page: the real station-activity.js in a vm ═══════════════════════════ */
const CLIENT_SRC = fs.readFileSync(path.join(root, 'station-activity.js'), 'utf8');
let pageN = 0;
function openPage(init, o = {}) {
  const ls = o.ls || new Map(), listeners = {}, docListeners = {}, intervals = [], timeouts = [];
  const pg = { station: init.station, device: init.device, computer: init.computer, who: init.who || null, warns: [], ls, intervals, timeouts, listeners, calls: [], mode: 'ok', lsBroken: !!o.lsBroken, sandbox: !!o.sandbox, ip: '192.0.2.' + (++pageN), beacons: [], onLine: true };
  const need = () => { if (pg.lsBroken) throw new Error('storage blocked'); };
  const storage = { getItem: k => { need(); return ls.has(k) ? ls.get(k) : null; }, setItem: (k, v) => { need(); ls.set(k, String(v)); }, removeItem: k => { need(); ls.delete(k); } };
  const send = (url, body) => door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': pg.ip }, queryStringParameters: /sandbox=1/.test(url) ? { sandbox: '1' } : {}, body });
  const ctx = {
    console: { warn: (...a) => pg.warns.push(a.join(' ')), log() {}, error() {} }, localStorage: storage, TextEncoder, AbortController,
    navigator: { get onLine() { return pg.onLine; }, sendBeacon: o.beacon ? (url, blob) => { pg.beacons.push(JSON.parse(blob.parts[0])); send(url, blob.parts[0]); return true; } : undefined },
    document: { visibilityState: 'visible', addEventListener: (t, f) => { (docListeners[t] = docListeners[t] || []).push(f); } },
    addEventListener: (t, f) => { (listeners[t] = listeners[t] || []).push(f); },
    setInterval: (f, ms) => { intervals.push({ f, ms }); return intervals.length; }, setTimeout: (f, ms) => { timeouts.push({ f, ms }); return timeouts.length; }, clearTimeout() {},
    Blob: class { constructor(p) { this.parts = p; } },
    fetch: (url, opt) => {
      const sent = JSON.parse(opt.body); pg.calls.push({ url, ids: sent.activity.map(e => e.id), keepalive: !!opt.keepalive });
      if (pg.mode === 'down') return Promise.reject(new Error('network down'));
      if (typeof pg.mode === 'number') return Promise.resolve({ status: pg.mode });
      return send(url, opt.body).then(r => (pg.mode === 'lost' ? Promise.reject(new Error('the answer was lost')) : { status: r.statusCode }));
    },
    StationSession: { who: () => pg.who, page: () => ({ station: init.station, device: init.device, computer: init.computer, sandbox: pg.sandbox }) }
  };
  ctx.window = ctx; vm.createContext(ctx);
  ctx.__now = () => NOW; vm.runInContext('Date.now = () => __now();', ctx);
  vm.runInContext(CLIENT_SRC, ctx);
  pg.A = ctx.StationActivity;
  pg.log = (action, opts) => { const ok = pg.A.log(action, opts); assert.strictEqual(ok, true, `${init.device}: ${action} was not queued`); return ok; };
  pg.tick = async () => { intervals.find(i => i.ms === 10000).f(); await settle(); };
  pg.fire = async (type) => { for (const f of listeners[type] || []) f({}); await settle(); };
  pg.pending = () => pg.A.pending();
  pg.queued = () => JSON.parse(ls.get('station_activity_q.' + init.device) || '[]');
  return pg;
}

/* ═══════════════════════════ the plan runner: steps in time order, the clock set for each ═══════════════════════════ */
let plan = [];
const at = (t, fn, late) => plan.push({ t, fn, late: !!late });
async function run() { const list = plan.map((s, i) => [s, i]).sort((a, b) => a[0].t - b[0].t || a[0].late - b[0].late || a[1] - b[1]); plan = []; for (const [s] of list) { NOW = s.t; await s.fn(); } }
const PAGES = {};

/* ═══════════════════════════ the cast ═══════════════════════════ */
const pinDoor = require(path.join(root, 'netlify/functions/_stationPinLogin.js'));
pinDoor.deps.sleep = async () => {};                                            // a wrong try's pause is not waited for (there is none here)
const PIN_NAMES = ['Giovanna C.', 'Empress D.', 'Michael_V', 'Michelle_R', 'Ivy_Y', 'Ana_M', 'Paul_K'];   // the values of Brites_Orders/Employee Numbers
const fakePins = new Map();                                                     // this run only; never printed (every assertion below compares booleans or names)
const fakePin = () => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && ![...fakePins.values()].includes(p)) return p; } };
PIN_NAMES.forEach(n => fakePins.set(n, fakePin()));
col('Brites_Orders').set('Employee Numbers', Object.fromEntries([...fakePins].map(([n, p]) => [p, n])));

const validFirestoreId = s => typeof s === 'string' && s.length > 0 && Buffer.byteLength(s) <= 1500 && !s.includes('/') && s !== '.' && s !== '..' && !/^__.*__$/.test(s);
const tidy = s => String(s).replace(/\s+/g, ' ').trim();
const O = n => String(3521000000 + n);

const signIn = (t, key, person, sid) => at(t, async () => {
  const p = PAGES[key];
  const r = await post({ session: { id: sid, event: 'start', person, station: p.station, device: p.device, computerId: p.computer, computerLabel: p.device, at: NOW } });
  assert.strictEqual(r.status, 200, 'sign-in ' + sid);
  p.who = { person: tidy(person), station: p.station, device: p.device, computer: p.computer, session: sid, startAt: r.body.startAt, sandbox: false };
});
const signOut = (t, key, sid) => at(t, async () => {
  const p = PAGES[key];
  const r = await post({ session: { id: sid, event: 'end', reason: 'signOut', station: p.station, computerId: p.computer, at: NOW } });
  assert.strictEqual(r.status, 200, 'sign-out ' + sid); p.who = null;
}, true);
const evs = (key, list) => list.forEach(([h, m, action, o]) => at(D2(h, m), () => PAGES[key].log(action, o)));
const flush = (t, key) => at(t, async () => { await PAGES[key].tick(); });
const near = (a, b, msg) => assert(Math.abs(a - b) < 1e-9, msg + ' (' + a + ' vs ' + b + ')');

let MAIN;
(async () => {
  /* ═══════════ 1 · the write side ═══════════ */
  NOW = D2(7, 50);
  const who = {};                                                               // the name the login door answers, per name on the list
  for (const n of PIN_NAMES) {
    const r = await post({ pinLogin: fakePins.get(n) });
    assert.strictEqual(r.status === 200 && r.body.ok === true, true, 'the login door knows ' + n);
    who[n] = r.body.name;
  }
  eq(PIN_NAMES.map(n => who[n]), PIN_NAMES, 'the login door answers each name exactly as stored: no underscore turned into a space, no period added or dropped');

  const ALL_NAMES = [...PIN_NAMES, 'Michael V.', 'MICHAEL V', 'Ana M.', 'Paul K', 'Giovanna', 'Michael T.', 'Michael'];
  ALL_NAMES.forEach((n, i) => {
    const r = ACT.clean({ id: `weld-1_AAAA_${i + 1}_1700000000000`, station: 'welding', action: 'scan', person: n, at: NOW }, NOW, '');
    assert.strictEqual(!r.refused && r.doc.person === n, true, 'the activity door accepts the name as sent: ' + n);
    const id = ACT.rollupId('2026-10-02', n);
    assert.strictEqual(id, '2026-10-02__' + n, 'the rollup id is day__name, untouched: ' + n);
    assert.strictEqual(validFirestoreId(id), true, 'a valid Firestore document id: ' + id);
  });
  eq(ACT.clean({ id: 'weld-1_AAAA_99_1700000000000', station: 'welding', action: 'scan', person: 'MICHAEL  V', at: NOW }, NOW, '').doc.person, 'MICHAEL V', 'a doubled space is collapsed by the door, nothing else');
  for (const bad of ['___', '. .', '_', '123_456', '12-34-56', '+', '.']) assert.strictEqual(ACT.clean({ id: 'weld-1_AAAA_98_1700000000000', station: 'welding', action: 'scan', person: bad, at: NOW }, NOW, '').refused, true, 'no letter, no person: ' + bad);
  assert.strictEqual(new Set(['Michael_V', 'Michael V.', 'MICHAEL V'].map(n => ACT.rollupId('2026-10-02', n))).size, 3, 'three spellings of one person are three rollup documents (the reader merges them)');
  // a PIN sent as the session's employeeId is dropped by the door (the sandbox copy is used so no real sign-in is made)
  {
    const r = await post({ session: { id: 'sess-sandbox-pin1', event: 'start', person: who['Ivy_Y'], employeeId: fakePins.get('Ivy_Y'), station: 'welding', device: 'weld-2', computerId: 'pc-SANDBOX1', at: NOW } }, { sandbox: true });
    assert.strictEqual(r.status, 200);
    const d = col('Sandbox_Station_Sessions').get('sess-sandbox-pin1');
    assert.strictEqual(!!d && d.person === 'Ivy_Y' && d.employeeId === '', true, 'the session keeps the name (underscore and all) and never the PIN');
  }

  /* ═══════════ 2 · the day, 2 Oct (EDT) ═══════════ */
  // key, device, station, computer, the name as signed in, sign-in, sign-out
  const CAST = [
    ['G1', 'assembly-1', 'assembly', 'pc-G1G1G1G1', who['Giovanna C.'], D2(8), D2(12)],
    ['G2', 'etsy-mail-1', 'inbox', 'pc-G2G2G2G2', 'Giovanna', D2(8, 30), D2(9, 30)],
    ['E', 'design-1', 'design', 'pc-EEEEEEEE', who['Empress D.'], D2(9), D2(11)],
    ['M1', 'weld-1', 'welding', 'pc-M1M1M1M1', who['Michael_V'], D2(8), D2(9)],
    ['M2', 'design-2', 'design', 'pc-M2M2M2M2', 'Michael V.', D2(8), D2(9)],
    ['M3', 'sorting-1', 'sorting', 'pc-M3M3M3M3', 'MICHAEL  V', D2(8, 30), D2(9, 30)],
    ['R', 'shipping-1', 'shipping', 'pc-RRRRRRRR', who['Michelle_R'], D2(8), D2(10)],
    ['I', 'weld-2', 'welding', 'pc-IIIIIIII', who['Ivy_Y'], D2(9), D2(11)],
    ['A1', 'assembly-2', 'assembly', 'pc-A1A1A1A1', who['Ana_M'], D2(8), D2(11)],
    ['A2', 'sorting-2', 'sorting', 'pc-A2A2A2A2', 'Ana M.', D2(10), D2(10, 30)],
    ['P1', 'shipping-2', 'shipping', 'pc-P1P1P1P1', who['Paul_K'], D2(8), D2(9)],
    ['P2', 'etsy-mail-2', 'inbox', 'pc-P2P2P2P2', 'Paul K', D2(8, 30), D2(9, 30)],
    ['T', 'weld-3', 'welding', 'pc-TTTTTTTT', 'Michael T.', D2(8), D2(9)],
    ['B', 'etsy-mail-3', 'inbox', 'pc-BBBBBBBB', 'Michael', D2(8), D2(8, 30)]
  ];
  for (const [key, device, station, computer, person, tin, tout] of CAST) {
    PAGES[key] = openPage({ device, station, computer });
    signIn(tin, key, person, 'sess-' + key.toLowerCase() + '-1002');
    signOut(tout, key, 'sess-' + key.toLowerCase() + '-1002');
    flush(D2(13), key);
  }
  // Giovanna C. at the PIN station and Giovanna in the inbox, overlapping 08:30-09:30. Gaps: 2 min each; inbox 5 min (exactly work) and 1.
  evs('G1', [[8, 2, 'scan', { orderId: O(1), parts: 6 }], [8, 4, 'complete', { orderId: O(1), parts: 6, orders: 1 }], [8, 6, 'scan', { orderId: O(2), parts: 4 }], [8, 8, 'complete', { orderId: O(2), parts: 4, orders: 1 }]]);
  evs('G2', [[8, 35, 'complete', { detail: 'conversation done' }], [8, 36, 'note', { detail: 'reply sent' }]]);
  // Empress D.: a design station
  evs('E', [[9, 2, 'print', { detail: 'labels' }], [9, 4, 'complete', { orderId: O(3), parts: 3, orders: 1 }], [9, 6, 'complete', { orderId: O(4), parts: 2, orders: 1 }]]);
  // Michael V.: THREE spellings on THREE computers in the same hour (weld-1 "Michael_V", design-2 "Michael V.", sorting-1 "MICHAEL V"); order 10 at two stations
  evs('M1', [[8, 2, 'scan', { orderId: O(10), parts: 4 }], [8, 4, 'complete', { orderId: O(10), parts: 4, orders: 1 }], [8, 50, 'scan', { orderId: O(11), parts: 3 }], [8, 52, 'complete', { orderId: O(11), parts: 3, orders: 1 }]]);
  evs('M2', [[8, 3, 'scan', { orderId: O(12), parts: 2 }], [8, 5, 'complete', { orderId: O(12), parts: 2, orders: 1 }], [8, 55, 'complete', { orderId: O(13), parts: 1, orders: 1 }]]);
  evs('M3', [[8, 32, 'scan', { orderId: O(10), parts: 4 }], [9, 0, 'complete', { orderId: O(14), parts: 2, orders: 1 }]]);
  evs('R', [[8, 2, 'scan', { orderId: O(20), parts: 5 }], [8, 3, 'print', { orderId: O(20), detail: 'label' }], [8, 4, 'complete', { orderId: O(20), parts: 5, orders: 1 }]]);
  evs('I', [[9, 3, 'scan', { orderId: O(30), parts: 8 }], [9, 5, 'complete', { orderId: O(30), parts: 8, orders: 1 }]]);
  evs('A1', [[8, 2, 'scan', { orderId: O(40), parts: 6 }], [8, 5, 'complete', { orderId: O(40), parts: 6, orders: 1 }]]);
  evs('A2', [[10, 2, 'scan', { orderId: O(41), parts: 2 }], [10, 4, 'complete', { orderId: O(41), parts: 2, orders: 1 }]]);
  evs('P1', [[8, 2, 'scan', { orderId: O(50), parts: 3 }], [8, 3, 'complete', { orderId: O(50), parts: 3, orders: 1 }]]);
  evs('P2', [[8, 40, 'note', { detail: 'reply sent' }], [8, 41, 'complete', { detail: 'conversation done' }]]);
  evs('T', [[8, 2, 'scan', { orderId: O(60), parts: 2 }], [8, 4, 'complete', { orderId: O(60), parts: 2, orders: 1 }]]);
  evs('B', [[8, 10, 'note', { detail: 'reply sent' }], [8, 11, 'complete', { detail: 'conversation done' }]]);

  await run();
  NOW = D2(15, 30);
  for (const k of Object.keys(PAGES)) assert.strictEqual(PAGES[k].pending(), 0, 'every page delivered everything: ' + k);

  /* the store, exactly as the stations wrote it */
  assert.strictEqual(col('Station_Activity').size, 35, 'one document per event: 4+2+3+4+3+2+3+2+2+2+2+2+2+2');
  const stored = n => docsOf('Station_Activity').filter(e => e.person === n).length;
  eq(['Giovanna C.', 'Giovanna', 'Empress D.', 'Michael_V', 'Michael V.', 'MICHAEL V', 'Michelle_R', 'Ivy_Y', 'Ana_M', 'Ana M.', 'Paul_K', 'Paul K', 'Michael T.', 'Michael'].map(stored), [4, 2, 3, 4, 3, 2, 3, 2, 2, 2, 2, 2, 2, 2], 'every event carries the name as the station signed in (nothing is tidied on the way in)');
  eq(docsOf('Station_Sessions').map(d => d.person).sort(), ['Ana M.', 'Ana_M', 'Empress D.', 'Giovanna', 'Giovanna C.', 'Ivy_Y', 'MICHAEL V', 'Michael', 'Michael T.', 'Michael V.', 'Michael_V', 'Michelle_R', 'Paul K', 'Paul_K'].sort(), 'the sessions carry the same names');
  const ids = docsOf('Efficiency_Daily').map(d => d._id).sort();
  eq(ids, ['Giovanna C.', 'Giovanna', 'Empress D.', 'Michael_V', 'Michael V.', 'MICHAEL V', 'Michelle_R', 'Ivy_Y', 'Ana_M', 'Ana M.', 'Paul_K', 'Paul K', 'Michael T.', 'Michael'].map(n => '2026-10-02__' + n).sort(), 'fourteen rollup documents: two or three for each person with more than one spelling');
  assert(ids.every(validFirestoreId), 'every rollup id is a valid Firestore id');
  assert(docsOf('Efficiency_Daily').every(d => d._id === '2026-10-02__' + d.person), 'the id and the person field agree');
  eq(docsOf('Efficiency_Daily').reduce((n, d) => n + d.events, 0), 35, 'the rollups counted every event once');
  const md = col('Efficiency_Daily').get('2026-10-02__Michael_V').stations.welding;
  eq([md.scans, md.completes, md.parts, md.scanParts, md.orders, md.activeMs, md.idleMs], [2, 2, 7, 7, 2, 6 * MIN, 46 * MIN], 'Michael_V at weld-1: two scans, two completes, 7 parts; 6 min work, a 46 min gap');

  /* the live overview at 15:30 */
  dropCaches();
  const A = await ask({ day: '2026-10-02' });
  const EXPECT = {                      // hand-computed (see the cast above): parts, scans, scanParts, orders, active min, idle min, signed-in min, rate, sec per scan
    'Michael V.': { t: [5, 4, 13, 4, 13, 77, 90, 42.9, 195], hours: { 8: 3, 9: 2 }, st: { welding: 0, design: 3, sorting: 2 } },
    'Giovanna': { t: [10, 2, 10, 2, 14, 0, 240, 42.9, 420], hours: { 8: 10 }, st: { assembly: 10, inbox: 0 } },
    'Ana M.': { t: [8, 2, 8, 2, 9, 0, 180, 53.3, 270], hours: { 8: 6, 10: 2 }, st: { assembly: 6, sorting: 2 } },
    'Ivy Y.': { t: [0, 1, 8, 0, 5, 0, 120, 0, 300], hours: {}, st: { welding: 0 } },
    'Empress D.': { t: [5, 0, 0, 2, 6, 0, 120, 50, 0], hours: { 9: 5 }, st: { design: 5 } },
    'Michelle R.': { t: [5, 1, 5, 1, 4, 0, 120, 75, 240], hours: { 8: 5 }, st: { shipping: 5 } },
    'Paul K.': { t: [3, 1, 3, 1, 4, 10, 90, 45, 240], hours: { 8: 3 }, st: { shipping: 3, inbox: 0 } },
    'Michael T.': { t: [0, 1, 2, 0, 4, 0, 60, 0, 240], hours: {}, st: { welding: 0 } },
    'Michael': { t: [0, 0, 0, 0, 1, 10, 30, 0, 0], hours: {}, st: { inbox: 0 } }
  };
  const SHOWN = Object.keys(EXPECT);
  // (RG2: ranked by the pieces that COUNT, and the Welding station does not count since 6 Oct (plans/stations-round2 R2 / WS2: time on task and matched scans there, no completions): so the
  //  three people whose work was welding (Michael V. 5 of 12, Ivy Y., Michael T.) drop in the order; ties by name)
  eq(A.people.map(p => p.name), ['Giovanna', 'Ana M.', 'Empress D.', 'Michael V.', 'Michelle R.', 'Paul K.', 'Ivy Y.', 'Michael', 'Michael T.'],
    'nine people, each once: the seven names with nice display names (Michael V., Ana M., Paul K., Michelle R., Ivy Y.), Giovanna C. + Giovanna one person under the alias spelling, the other two Michaels apart');
  assert(!A.people.some(p => /_/.test(p.name)), 'no underscore on the screen');
  for (const p of A.people) {
    const x = EXPECT[p.name], t = p.totals;
    eq([t.parts, t.scans, t.scanParts, t.orders, t.activeMin, t.idleMin, t.signedInMin, t.rate, t.secPerScan], x.t, p.name + ': totals');
    eq(Object.fromEntries(p.perHour.map((v, h) => [h, v]).filter(([, v]) => v > 0)), x.hours, p.name + ': parts per hour');
    eq(Object.fromEntries(p.stations.map(s => [s.station, s.parts])), x.st, p.name + ': parts per station');
    assert(t.activeMin + t.idleMin <= t.signedInMin + 1e-9, p.name + ': active plus idle never exceeds the time signed in');
    assert.strictEqual(p.status, 'out'); assert.strictEqual(p.source, 'events');
  }
  const mv = A.people.find(p => p.name === 'Michael V.');
  eq([mv.firstIn, mv.lastOut], [D2(8), D2(9, 30)], 'Michael V.: first in 08:00 on the first computer, out 09:30 on the last (one person, three spellings)');
  eq(mv.stations.map(s => [s.station, s.minutes]), [['design', 60], ['sorting', 60], ['welding', 60]], 'minutes per station stay per station (Welding, with no counted pieces, is listed last); the 90 signed-in minutes count the overlap once');
  eq(mv.orders.map(o => o.orderId).sort(), [O(10), O(11), O(12), O(13), O(14)], 'five distinct orders (order 10 was worked at two stations and counts once)');
  // the business
  eq(A.business.totals, { parts: 36, scans: 12, orders: 12, people: 9 }, 'the business adds up the nine people (the 17 pieces and 3 orders that only the Welding station touched are not throughput)');
  eq(A.people.reduce((n, p) => n + p.totals.parts, 0), 36); eq(A.people.reduce((n, p) => n + p.totals.scans, 0), 12);
  eq(Object.fromEntries(A.business.stations.filter(s => s.parts > 0).map(s => [s.station, s.parts])), { sorting: 4, assembly: 16, shipping: 8, design: 8 }, 'parts per station (Welding counts none)');
  eq(A.business.perHour, { assembly: hourArr({ 8: 16 }), sorting: hourArr({ 9: 2, 10: 2 }), shipping: hourArr({ 8: 8 }), design: hourArr({ 8: 3, 9: 5 }) }, 'hours per station (no Welding: it adds nothing to the pieces per hour)');
  function hourArr(o) { const a = new Array(24).fill(0); for (const [h, v] of Object.entries(o)) a[+h] = v; return a; }
  eq(A.people.reduce((a, p) => a.map((v, h) => v + p.perHour[h]), new Array(24).fill(0)).reduce((n, v) => n + v, 0), 36, 'the hours add up to the parts');
  // the live feed: every line carries the one display name
  eq(A.feed.length, 35);
  const feedBy = {}; for (const f of A.feed) feedBy[f.person] = (feedBy[f.person] || 0) + 1;
  eq(feedBy, { 'Michael V.': 9, 'Giovanna': 6, 'Ana M.': 4, 'Ivy Y.': 2, 'Empress D.': 3, 'Michelle R.': 3, 'Paul K.': 4, 'Michael T.': 2, 'Michael': 2 }, 'the feed lines are under the nine display names, never under an underscore or a second spelling');

  /* ═══════════ 3 · any spelling asks for the same person ═══════════ */
  const personOf = async name => (await ask({ op: 'person', name, day: '2026-10-02', days: 3 }));
  const base = await personOf('Michael V.');
  eq(base.name, 'Michael V.'); eq(base.totals.parts, 5);
  eq(base.days.map(d => [d.day, d.parts, d.scans, d.orders, d.signedInMin, d.activeMin, d.idleMin]), [['2026-09-30', 0, 0, 0, 0, 0, 0], ['2026-10-01', 0, 0, 0, 0, 0, 0], ['2026-10-02', 5, 4, 4, 90, 13, 77]], 'the person op shows the same day (the 7 pieces at the Welding station are not counted)');
  for (const spelling of ['Michael_V', 'michael v.', 'MICHAEL  V', 'Michael V', ' michael_v. ', 'MICHAEL_V']) {
    const o = await personOf(spelling);
    eq([o.name, o.totals, o.days], [base.name, base.totals, base.days], 'asking for "' + spelling + '" gives the same person');
  }
  eq((await personOf('Michael T.')).totals.parts, 0, 'Michael T. is another person (his only work was at the Welding station: no counted pieces)'); eq((await personOf('Michael')).totals.parts, 0, 'so is a bare Michael');
  eq((await personOf('Ana_M')).name, 'Ana M.'); eq((await personOf('paul k')).name, 'Paul K.'); eq((await personOf('Giovanna C.')).name, 'Giovanna');
  // the order trace
  const ox = await ask({ op: 'orders', orderId: O(10) });
  eq(ox.steps.map(s => [s.station, s.person]), [['welding', 'Michael V.'], ['sorting', 'Michael V.']], 'order 10: worked at welding as Michael_V and at sorting as MICHAEL V, shown as one person twice');
  eq([...new Set(ox.events.map(e => e.person))], ['Michael V.'], 'the trace lists one display name'); eq(ox.totals.people, 1, 'one person on the order');

  /* ═══════════ 4 · the sums, from the rollup documents themselves ═══════════ */
  {
    const al = T.buildAliases(null), key = n => { const k = T.fold(n); return al.map.get(k) || k; };
    const groups = new Map();
    for (const d of docsOf('Efficiency_Daily')) {
      const g = groups.get(key(d.person)) || { parts: 0, scans: 0, scanParts: 0, docs: 0 };
      // (RG2: the stored rollups still hold the Welding station's pieces, as written; readers sum them through readStationCounters, which leaves Welding's completions out)
      for (const [st, v] of Object.entries(d.stations)) { const s = AK.readStationCounters(st, v); g.parts += (s.parts || 0) - (s.undoParts || 0); g.scans += s.scans || 0; g.scanParts += s.scanParts || 0; }
      g.docs++; groups.set(key(d.person), g);
    }
    eq(groups.size, 9, 'the fourteen rollup documents are nine people');
    for (const p of A.people) { const g = groups.get(key(p.name)); eq([p.totals.parts, p.totals.scans, p.totals.scanParts], [g.parts, g.scans, g.scanParts], p.name + ': the card equals the sum of that person\'s rollup documents'); }
    eq([...groups.values()].map(g => g.docs).sort(), [1, 1, 1, 1, 1, 2, 2, 2, 3], 'Michael V. has three documents, Giovanna, Ana M. and Paul K. two, the rest one');
  }

  /* ═══════════ 5 · config/employeeAliases ═══════════ */
  col('config').set('employeeAliases', { 'Mike V.': ['Michael_V'], 'Shelly_R': ['Michelle R.'] });
  dropCaches();
  const B = await ask({ day: '2026-10-02' });
  eq(B.people.map(p => p.name), ['Giovanna', 'Ana M.', 'Empress D.', 'Mike V.', 'Shelly_R', 'Paul K.', 'Ivy Y.', 'Michael', 'Michael T.'], 'the alias doc\'s own spelling wins and is shown as written (Shelly_R keeps its underscore); its aliases fold like names (alias "Michelle R." caught "Michelle_R")');
  eq(B.people.map(p => p.totals.parts), A.people.map(p => p.totals.parts), 'the same figures under the new names');
  eq(B.business.totals, A.business.totals, 'the same business');
  assert(B.feed.every(f => ['Mike V.', 'Giovanna', 'Ana M.', 'Ivy Y.', 'Empress D.', 'Shelly_R', 'Paul K.', 'Michael T.', 'Michael'].includes(f.person)), 'the feed follows');
  eq((await personOf('Michael V.')).name, 'Mike V.', 'asking by the typed spelling finds the aliased person'); eq((await personOf('michelle_r')).name, 'Shelly_R');
  col('config').delete('employeeAliases'); dropCaches();
  eq((await ask({ day: '2026-10-02' })).people.map(p => p.name), A.people.map(p => p.name), 'without the doc the names are the nice ones again');

  /* ═══════════ no PIN anywhere ═══════════ */
  {
    const dump = JSON.stringify([...colls].filter(([k]) => k !== 'Brites_Orders')) + JSON.stringify(A) + JSON.stringify(B) + JSON.stringify(ox) + JSON.stringify(base);
    for (const [n, pin] of fakePins) assert.strictEqual(dump.includes(pin), false, 'the PIN of ' + n + ' is in a stored document or an answer');
  }
  MAIN = { A };
  say('1-5 the seven names + typed variants: door, sessions, rollups (14 documents, valid ids), reader (9 people, nice names, sums, no double counting), any spelling, aliases');

  /* ═══════════ 6 · the console's view model ═══════════ */
  {
    const win = { document: { getElementById: () => null, addEventListener() {} }, console }; win.window = win; vm.createContext(win);
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), win);
    const Eff = win.Efficiency, j = o => JSON.parse(JSON.stringify(o));
    const M = j(Eff.norm(j(A)));
    eq(M.people.map(p => p.name), A.people.map(p => p.name), 'the screen shows the same nine people, spelled as the reader spelled them');
    assert.strictEqual(new Set(M.people.map(p => p.name)).size, 9, 'each once'); assert(!M.people.some(p => /_/.test(p.name)));
    for (const p of A.people) { const q = M.people.find(x => x.name === p.name); eq([q.t.parts, q.t.scans, q.t.orders, q.t.activeMin, q.t.idleMin, q.t.signedInMin, q.perHour], [p.totals.parts, p.totals.scans, p.totals.orders, p.totals.activeMin, p.totals.idleMin, p.totals.signedInMin, p.perHour], p.name + ': the card shows the reader\'s numbers'); }
    assert.strictEqual(M.biz.people, 9); assert.strictEqual(M.biz.parts, 36); assert.strictEqual(M.biz.scans, 12); assert.strictEqual(M.biz.orders, 12);   // (RG2: Welding is not throughput)
    assert.strictEqual(M.biz.hours.reduce((a, b) => a + b, 0), 36, 'the hourly graph adds to the parts');
    eq(M.feed.map(f => f.person).filter((v, i, a) => a.indexOf(v) === i).sort(), SHOWN.slice().sort(), 'the live feed shows the same nine names');
    const H = j(Eff.normHist(base));
    assert.strictEqual(H.parts, 5); assert.strictEqual(H.signedInMin, 90); assert.strictEqual(H.days[H.days.length - 1].parts, 5);
    const Od = j(Eff.normOrder(ox)); eq(Od.steps.map(s => s.person), ['Michael V.', 'Michael V.']);
    say('6 the console view model: nine people, nice names, the same totals, hours add to the parts, a person\'s days, an order trace');
  }
  say('OK');
})().then(() => { Date.now = realNow; console.error = realErr; }, e => { Date.now = realNow; console.error = realErr; process.stdout.write('FAIL ' + String(e && e.stack || e).replace(/\b\d{6}\b/g, '#') + '\n'); process.exit(1); });
