// Employee efficiency, the DATA side cross-checked end to end, offline. One realistic business day goes through the REAL
// modules: station-activity.js (the browser helper, in a vm page per station) -> firebaseOrders {activity, session} ->
// _stationActivity.js (events + rollups) -> employeeEfficiency.js (overview / person / orders) -> charm-nest-efficiency.js
// (the view model). Firestore is ONE in-memory fake (typed fields, queries, increments, merge, transactions that re-run when
// another commit touched what they read), the clock is faked, every passcode and number here is synthetic.
//   1 · the day (2 Oct, EDT): 10 people, shipping/assembly/welding/sorting/design/laser/sorter/inbox, sign-ins, a lock-out and a
//       return, one person at two stations at once, name variants (Giovanna / Giovanna C., Jose Perez in two cases), cancelled
//       orders, an undo, a lost response + retry, a 503 outage with out-of-order delivery, a seals-only older day, a day with
//       sign-ins only. Every figure is hand-computed below (the arithmetic is in the comments).
//   2 · the New York midnight: events queued before it and sent after it keep their day, the midnight sign-out, the next day
//   3 · daylight saving: the fall-back day (two 01:00 hours share bucket "01") and the spring day (no hour 02)
//   4 · the door: 413s, malformed events, allow lists, PIN-like values, order independence (shuffled batches give the same
//       rollup), duplicates, the live feed never skips an event that commits late
//   5 · the browser helper: outage and retry keep every event exactly once, blocked localStorage, reload, beacon, the cap
//   6 · the console's view model from the real answers
//   node tests/stations/efficiency-crosscheck.cjs
'use strict';
require(require('path').join(__dirname, '../../netlify/functions/_activityKinds.js')).NO_THROUGHPUT.clear();   // this suite uses 'welding' as a plain fixture station for the generic arithmetic: the Welding station's own rule (not counted in throughput, R2 of stations round 2) is tested in welding-portal.cjs

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
Module._load = realLoad;
const T = reader._t;
const PASS = 'synthetic-crosscheck-pass-4m7q';
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
const addPage = (key, device, station, computer) => (PAGES[key] = openPage({ device, station, computer }));
const sessionOf = {};
function signIn(t, key, person, sid) {
  at(t, async () => {
    const p = PAGES[key];
    const r = await post({ session: { id: sid, event: 'start', person, station: p.station, device: p.device, computerId: p.computer, computerLabel: p.device, at: NOW } });
    assert.strictEqual(r.status, /\p{L}/u.test(person) ? 200 : 400, 'sign-in ' + sid + (/\p{L}/u.test(person) ? '' : ': a digits-only name is a PIN: the door refuses it, nothing is stored')); sessionOf[sid] = { key, person };
    p.who = { person, station: p.station, device: p.device, computer: p.computer, session: sid, startAt: r.body.startAt, sandbox: false };
  });
}
function signOut(t, key, sid, reason) {
  at(t, async () => {
    const p = PAGES[key];
    const r = await post({ session: { id: sid, event: 'end', reason: reason || 'signOut', station: p.station, computerId: p.computer, at: NOW } });
    assert.strictEqual(r.status, 200, 'sign-out ' + sid); p.who = null;
  }, true);                                                    // (a sign-out at the same instant as an action comes after it)
}
function beats(key, sid, person, from, to) {
  for (let t = from; t <= to; t += 5 * MIN) at(t, async () => { const p = PAGES[key]; const r = await post({ session: { id: sid, event: 'beat', person, station: p.station, device: p.device, computerId: p.computer, at: NOW } }); assert.strictEqual(r.status, 200); });
}
const ev = (t, key, action, o) => at(t, () => PAGES[key].log(action, o));
const flush = (t, key, mode) => at(t, async () => { const p = PAGES[key]; if (mode !== undefined) p.mode = mode; await p.tick(); });
const setMode = (t, key, mode) => at(t, () => { PAGES[key].mode = mode; });
const seal = (id, orderId, type, by, station, t, milestone) => col('Order_Timeline').set(id, { orderId, type, at: t, by, source: 'station', station, device: station + '-1', milestone: !!milestone, text: 'x' });

const O = n => String(3521000000 + n);          // an Etsy receipt id (10 digits)
const OX = O(99);                                // the order scanned at three stations and completed at one

let MAIN;
(async () => {
  /* ═══════════ 1 · the day ═══════════ */
  addPage('T', 'weld-1', 'welding', 'pc-AAAAAAAA'); addPage('R', 'weld-2', 'welding', 'pc-BBBBBBBB'); addPage('A', 'assembly-1', 'assembly', 'pc-CCCCCCCC');
  addPage('S1', 'sorting-1', 'sorting', 'pc-DDDDDDDD'); addPage('S2', 'charm-nest-1', 'sorter', 'pc-EEEEEEEE'); addPage('H', 'shipping-1', 'shipping', 'pc-FFFFFFFF');
  addPage('D', 'design-1', 'design', 'pc-GGGGGGGG'); addPage('G1', 'sorting-2', 'sorting', 'pc-HHHHHHHH'); addPage('G2', 'etsy-mail-1', 'inbox', 'pc-IIIIIIII');
  addPage('J1', 'assembly-2', 'assembly', 'pc-JJJJJJJJ'); addPage('J2', 'weld-3', 'welding', 'pc-KKKKKKKK'); addPage('L', 'laser-1', 'laser', 'pc-LLLLLLLL');
  addPage('Q', 'shipping-2', 'shipping', 'pc-MMMMMMMM'); addPage('X', 'weld-4', 'welding', 'pc-PPPPPPPP'); addPage('N', 'sorting-3', 'sorting', 'pc-NNNNNNNN');

  // history: 30 Sep (Tess in 08:00-12:00 with sealed work, Ann sealed work only), 1 Oct (Tess signed in an hour, nothing else)
  signIn(D0(8), 'T', 'Tess Welder', 'sess-tess-0930'); signOut(D0(12), 'T', 'sess-tess-0930');
  signIn(D1(8), 'T', 'Tess Welder', 'sess-tess-1001'); signOut(D1(9), 'T', 'sess-tess-1001');
  at(D0(13), () => {
    seal('s1', O(80), 'scan', 'Tess Welder', 'welding', D0(8, 59), false); seal('s2', O(80), 'welded', 'Tess Welder', 'welding', D0(9), true); seal('s3', O(80), 'labelPrinted', 'Tess Welder', 'welding', D0(9, 1), false);
    seal('s4', O(81), 'scan', 'Tess Welder', 'welding', D0(9, 29), false); seal('s5', O(81), 'welded', 'Tess Welder', 'welding', D0(9, 30), true); seal('s6', O(82), 'welded', 'Tess Welder', 'welding', D0(10), true);
    seal('s7', O(80), 'assembled', 'Ann Assembler', 'assembly', D0(11), true);
    seal('s8', O(83), 'arrived', 'Etsy', 'sorter', D0(8), true);            // Etsy is not a person
    seal('s9', O(84), 'welded', '424242', 'welding', D0(9, 40), true);      // a digits-only name is never a person
    seal('s10', O(85), 'queued', 'Tess Welder', 'welding', D0(9, 50), false); // not a milestone, scan or print
  });

  // the day, 2 Oct (EDT)
  signIn(D2(7, 45), 'A', 'Ann Assembler', 'sess-ann-1002'); beats('A', 'sess-ann-1002', 'Ann Assembler', D2(7, 50), D2(15, 25)); at(D2(15, 28), () => post({ session: { id: 'sess-ann-1002', event: 'beat', person: 'Ann Assembler', station: 'assembly', device: 'assembly-1', computerId: 'pc-CCCCCCCC' } }));
  signIn(D2(8), 'T', 'Tess Welder', 'sess-tess-1002'); signIn(D2(8), 'G1', 'Giovanna C.', 'sess-gio-1'); signIn(D2(8), 'J1', 'José Pérez', 'sess-jose-1');
  signIn(D2(8, 30), 'H', 'Shane Shipper', 'sess-shane-1'); signIn(D2(8, 30), 'G2', 'Giovanna', 'sess-gio-2');
  signIn(D2(9), 'S1', 'Sam Sorter', 'sess-sam-1'); signIn(D2(9), 'D', 'Dana Designer', 'sess-dana-1'); signIn(D2(9), 'Q', 'Quinn Quiet', 'sess-quinn-1'); signIn(D2(9), 'X', '424242', 'sess-pin-bad');
  beats('Q', 'sess-quinn-1', 'Quinn Quiet', D2(9, 5), D2(9, 20));                     // then silence: closed at the last beat, 20 minutes
  signIn(D2(9, 30), 'S2', 'Sam Sorter', 'sess-sam-2'); signIn(D2(10), 'L', 'Leo Laser', 'sess-leo-1'); signIn(D2(10, 30), 'R', 'Ray Welder', 'sess-ray-1');
  signOut(D2(9), 'G1', 'sess-gio-1'); signOut(D2(9), 'J1', 'sess-jose-1'); signOut(D2(9, 30), 'G2', 'sess-gio-2'); signOut(D2(10), 'S1', 'sess-sam-1'); signOut(D2(10, 30), 'S2', 'sess-sam-2');
  signOut(D2(10, 30), 'L', 'sess-leo-1'); signOut(D2(11), 'D', 'sess-dana-1'); signOut(D2(12), 'H', 'sess-shane-1'); signOut(D2(12, 30), 'T', 'sess-tess-1002'); signOut(D2(14), 'R', 'sess-ray-1');
  signIn(D2(13), 'H', 'Shane Shipper', 'sess-shane-2'); beats('H', 'sess-shane-2', 'Shane Shipper', D2(13, 5), D2(15, 25)); at(D2(15, 29), () => post({ session: { id: 'sess-shane-2', event: 'beat', person: 'Shane Shipper', station: 'shipping', device: 'shipping-1', computerId: 'pc-FFFFFFFF' } }));
  signIn(D2(14), 'J2', 'JOSE PEREZ', 'sess-jose-2'); signOut(D2(14, 30), 'J2', 'sess-jose-2');

  // Tess, weld-1. Gaps from her sign-in at 08:00.   active: 120+60+90+90+60+30 + 60+60+60+120+60 = 810 s   idle: 750 + 3600 = 4350 s
  ev(D2(8, 2), 'T', 'scan', { orderId: O(1), parts: 4, detail: 'phone scan' });
  ev(D2(8, 3), 'T', 'complete', { orderId: O(1), parts: 4, orders: 1 });
  ev(D2(8, 4, 30), 'T', 'scan', { orderId: O(2), parts: 6 });
  ev(D2(8, 6), 'T', 'complete', { orderId: O(2), parts: 6, orders: 1 });
  ev(D2(8, 7), 'T', 'scan', { orderId: O(3), parts: 2 });
  ev(D2(8, 7, 30), 'T', 'reject', { orderId: O(3), detail: 'cancelled order 3521000003, hold code 123456' });   // a cancelled order: no parts, no finished order (the 6-digit number is hidden, the order number is not)
  ev(D2(8, 20), 'T', 'scan', { orderId: O(4), parts: 3 });                                          // 12.5 min: idle
  ev(D2(8, 21), 'T', 'complete', { orderId: O(4), parts: 3, orders: 1 });
  ev(D2(8, 22), 'T', 'undo', { orderId: O(4), parts: 3, orders: 1 });                               // reversed at once: net 0 for that order
  ev(D2(8, 23), 'T', 'error', { detail: 'order not found' });
  ev(D2(8, 25), 'T', 'note', { detail: 'manual correction' });
  flush(D2(8, 30), 'T');
  ev(D2(10, 25), 'T', 'scan', { orderId: O(5), parts: 5 });                                         // 2 h quiet: capped at 1 h, idle
  ev(D2(10, 26), 'T', 'complete', { orderId: O(5), parts: 5, orders: 1 });
  flush(D2(10, 30), 'T');

  // Ray, weld-2: a lost answer (the server stored it, the page does not know), then a 503 outage, delivered late and out of order
  ev(D2(10, 32), 'R', 'scan', { orderId: O(10), parts: 8 });
  ev(D2(10, 34), 'R', 'complete', { orderId: O(10), parts: 8, orders: 1 });
  flush(D2(10, 35), 'R', 'lost');
  ev(D2(10, 36), 'R', 'scan', { orderId: O(11), parts: 5 });
  ev(D2(10, 38), 'R', 'complete', { orderId: O(11), parts: 5, orders: 1 });
  flush(D2(10, 39), 'R', 'ok');                                                                      // the first two are sent again: stored once
  ev(D2(11), 'R', 'scan', { orderId: O(12), parts: 3 });                                            // 22 min: idle
  ev(D2(11, 1), 'R', 'complete', { orderId: O(12), parts: 3, orders: 1 });
  flush(D2(11, 2), 'R', 503); flush(D2(11, 2, 10), 'R');                                            // the second is inside the 20 s back-off: no request
  flush(D2(11, 20), 'R', 'ok');

  // Ann, assembly-1 (gaps from 07:45).   active: 120+180+120+180+60 = 660 s   idle: 44 min + 1 h (capped) = 6240 s
  ev(D2(7, 47), 'A', 'scan', { orderId: O(20), parts: 6 });
  ev(D2(7, 50), 'A', 'complete', { orderId: O(20), parts: 6, orders: 1 });
  ev(D2(7, 52), 'A', 'scan', { orderId: O(21), parts: 4 });
  ev(D2(7, 55), 'A', 'complete', { orderId: O(21), parts: 4, orders: 1 });
  ev(D2(7, 56), 'A', 'note', { orderId: O(21), detail: 'stamp again: Done' });
  flush(D2(8, 5), 'A');
  ev(D2(8, 40), 'A', 'reject', { orderId: O(22), detail: 'cancelled order alert' });
  flush(D2(8, 45), 'A');
  ev(D2(13), 'A', 'scan', { orderId: OX, parts: 10, detail: 'phone scan' });                         // OX at assembly: scanned, not completed here
  flush(D2(13, 5), 'A'); flush(D2(15, 25), 'A');

  // Sam: sorting-1 09:00-10:00 and the Charm Sorter 09:30-10:30 at the same time. Every gap is 2 minutes.
  for (let k = 0; k < 15; k++) {
    ev(D2(9, 2 + 4 * k), 'S1', 'scan', { orderId: O(300 + k), parts: 2 }); ev(D2(9, 4 + 4 * k), 'S1', 'complete', { orderId: O(300 + k), parts: 2, orders: 1 });
    ev(D2(9, 32 + 4 * k), 'S2', 'scan', { orderId: O(310 + k), parts: 1 }); ev(D2(9, 34 + 4 * k), 'S2', 'complete', { orderId: O(310 + k), parts: 1, orders: 1 });
  }
  flush(D2(9, 31), 'S1'); flush(D2(10, 5), 'S1'); flush(D2(10, 6), 'S2'); flush(D2(10, 35), 'S2');

  // Shane, shipping-1; locked out at 12:00 and back at 13:00
  ev(D2(8, 33), 'H', 'scan', { orderId: O(30), parts: 3 });
  ev(D2(8, 35), 'H', 'print', { orderId: O(30), detail: 'Chit Chats label' });
  ev(D2(8, 37), 'H', 'complete', { orderId: O(30), parts: 3, orders: 1 });
  ev(D2(8, 40), 'H', 'scan', { orderId: O(31), parts: 2 });
  ev(D2(8, 41), 'H', 'reject', { orderId: O(31), detail: 'cancelled order' });
  ev(D2(8, 50), 'H', 'error', { detail: 'Etsy did not answer' });                                    // 9 min: idle
  flush(D2(8, 55), 'H');
  ev(D2(11, 55), 'H', 'note', { detail: 'message to the order chat' });                             // 3 h 05 quiet: capped at 1 h
  flush(D2(11, 56), 'H');
  ev(D2(13, 3), 'H', 'scan', { orderId: O(32), parts: 5 });
  ev(D2(13, 5), 'H', 'print', { orderId: O(32), detail: 'Chit Chats label' });
  ev(D2(13, 6), 'H', 'complete', { orderId: O(32), parts: 5, orders: 1 });
  flush(D2(13, 10), 'H');
  ev(D2(14), 'H', 'scan', { orderId: OX, parts: 10 });                                              // 54 min: idle
  ev(D2(14, 2), 'H', 'complete', { orderId: OX, parts: 10, orders: 1 });
  flush(D2(14, 5), 'H');

  // Dana, design-1 (undo then complete again)
  ev(D2(9, 10), 'D', 'print', { detail: 'labels' });
  ev(D2(9, 10, 30), 'D', 'complete', { orderId: O(40), parts: 4, orders: 1 });
  ev(D2(9, 11), 'D', 'complete', { orderId: O(41), parts: 2, orders: 1 });
  ev(D2(9, 12), 'D', 'undo', { orderId: O(41), parts: 2, orders: 1 });
  ev(D2(9, 13), 'D', 'complete', { orderId: O(41), parts: 2, orders: 1 });
  flush(D2(9, 20), 'D');

  // Giovanna: "Giovanna C." at the PIN station, "Giovanna" in the inbox, overlapping 08:30-09:00. Gaps of exactly 5:00 are work, 5:01 is not.
  ev(D2(8, 5), 'G1', 'scan', { orderId: OX, parts: 10, detail: 'sheet QR' });
  ev(D2(8, 10, 1), 'G1', 'complete', { orderId: O(50), parts: 7, orders: 1 });
  ev(D2(8, 12), 'G1', 'print', { orderId: O(50), detail: 'sticker' });
  ev(D2(8, 35), 'G2', 'complete', { detail: 'conversation done' });
  ev(D2(8, 36), 'G2', 'note', { detail: 'reply sent' });
  flush(D2(8, 40), 'G1'); flush(D2(8, 40), 'G2');

  // José Pérez / JOSE PEREZ: two spellings, two computers, one person
  ev(D2(8, 3), 'J1', 'scan', { orderId: O(60), parts: 2 }); ev(D2(8, 5), 'J1', 'complete', { orderId: O(60), parts: 2, orders: 1 }); flush(D2(8, 10), 'J1');
  ev(D2(14, 3), 'J2', 'scan', { orderId: O(61), parts: 4 }); ev(D2(14, 5), 'J2', 'complete', { orderId: O(61), parts: 4, orders: 1 }); flush(D2(14, 10), 'J2');

  // Leo, laser-1
  ev(D2(10, 2), 'L', 'scan', { orderId: O(70), parts: 12 }); ev(D2(10, 4), 'L', 'complete', { orderId: O(70), parts: 12, orders: 1 }); flush(D2(10, 10), 'L');

  await run();
  NOW = D2(15, 30);
  for (const k of Object.keys(PAGES)) assert.strictEqual(PAGES[k].pending(), 0, 'every page has delivered everything: ' + k);

  /* the store: one document per event, every retry stored once */
  assert.strictEqual(col('Station_Activity').size, 114, 'one document per event: 13 Tess, 6 Ray, 7 Ann, 60 Sam, 12 Shane, 5 Dana, 5 Giovanna, 4 Jose, 2 Leo');
  assert.strictEqual(eventsOf('Ray Welder').length, 6, 'the lost answer and its retry stored Ray\'s events once');
  const rr = PAGES.R.calls; assert(rr.some(c => c.ids.length === 4 && c.ids.slice(0, 2).every(i => rr[0].ids.includes(i))), 'the retry after the lost answer carried the same ids');
  assert.strictEqual(rr.filter(c => c.ids.length === 2 && c.ids[0] === rr[rr.length - 1].ids[0]).length >= 1, true, 'the outage batch was tried again');
  eq(rr.map(c => c.ids.length), [2, 4, 2, 2], 'Ray: lost answer (2), retry with two more (4), 503 (2), and one try after the back-off (2): the second tick inside the back-off made no request');
  assert(!JSON.stringify([...colls]).includes('424242'), 'the digits-only session name is refused at the door: stored nowhere, in no session and in no event');
  assert.strictEqual(docsOf('Efficiency_Daily').filter(d => d.person === '424242').length, 0, 'no rollup under a PIN');

  /* the live overview at 15:30 */
  dropCaches();
  const A = await ask({ day: '2026-10-02' });
  const by = n => A.people.find(p => p.name === n);
  eq(A.people.map(p => p.name), ['Shane Shipper', 'Ann Assembler', 'Sam Sorter', 'Ray Welder', 'Tess Welder', 'Leo Laser', 'Giovanna', 'Dana Designer', 'José Pérez', 'Quinn Quiet'],
    'on first (parts desc), then parts desc, then name; one Giovanna, one José; the digits-only name is not a person');
  const totals = (parts, scanParts, scans, orders, rejects, errors, activeMin, idleMin, signedInMin, rate, secPerScan) => ({ parts, scanParts, scans, orders, rejects, errors, activeMin, idleMin, signedInMin, rate, secPerScan });
  const WANT = {
    // net parts 18-3=15 · scans O1..O5 · pieces scanned 4+6+2+3+5 · orders touched O1..O5 (the cancelled one and the undone one count) · 13.5 min active, 72.5 idle · 15 / 0.225 h · 810 s / 5
    'Tess Welder': totals(15, 20, 5, 5, 1, 1, 13.5, 72.5, 270, 66.7, 162),
    // 8+5+3 · 3 scans · 16 pieces · 3 orders · 540 s active, 1320 s idle · 16 / 0.15 h · 540 / 3
    'Ray Welder': totals(16, 16, 3, 3, 0, 0, 9, 22, 210, 106.7, 180),
    // 6+4 · 3 scans (O20, O21, OX) · 6+4+10 · O20, O21, O22 (rejected), OX · 660 s / 6240 s · 10 / 0.18333 h · 660 / 3 · in since 07:45 to 15:30
    'Ann Assembler': totals(10, 20, 3, 4, 1, 0, 11, 104, 465, 54.5, 220),
    // two computers at once: 60 min of gaps at each = 120 min, but only 90 min signed in (union of 09:00-10:00 and 09:30-10:30): active is capped at 90
    // 30 + 15 parts · 15 + 15 scans · 30 + 15 pieces · orders 300..324 (310..314 at both) = 25 · 45 / 1.5 h · 5400 s / 30
    'Sam Sorter': totals(45, 45, 30, 25, 0, 0, 90, 0, 90, 30, 180),
    // 3+5+10 · 4 scans · 3+2+5+10 · O30, O31 (cancelled), O32, OX · 1140 s / 7380 s · signed in 210 + 150 (locked out 12:00-13:00) · 18 / 0.31667 h · 1140 / 4
    'Shane Shipper': totals(18, 20, 4, 4, 1, 1, 19, 123, 360, 56.8, 285),
    // 4+2+2 produced, 2 undone = 6 · no scans · O40, O41 · 180 s / 600 s · 6 / 0.05 h
    'Dana Designer': totals(6, 0, 0, 2, 0, 0, 3, 10, 120, 120, 0),
    // 7 parts · 1 scan of 10 pieces · OX, O50 · 779 s active (5:00 counts, 5:01 does not) / 301 s idle · 7 / 0.21639 h · 779 s per scan · 08:00-09:30 = 90 (overlap once)
    'Giovanna': totals(7, 10, 1, 2, 0, 0, 13, 5, 90, 32.3, 779),
    // 2 + 4 · 2 scans · 6 pieces · O60, O61 · 600 s · 6 / 0.16667 h · 600 / 2 · 08:00-09:00 and 14:00-14:30
    'José Pérez': totals(6, 6, 2, 2, 0, 0, 10, 0, 90, 36, 300),
    'Leo Laser': totals(12, 12, 1, 1, 0, 0, 4, 0, 30, 180, 240),
    'Quinn Quiet': totals(0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0)
  };
  for (const [n, w] of Object.entries(WANT)) eq(by(n).totals, w, n + ': totals');
  eq(A.people.map(p => [p.name, p.status, p.source]), [['Shane Shipper', 'on', 'events'], ['Ann Assembler', 'on', 'events'], ['Sam Sorter', 'out', 'events'], ['Ray Welder', 'out', 'events'], ['Tess Welder', 'out', 'events'],
    ['Leo Laser', 'out', 'events'], ['Giovanna', 'out', 'events'], ['Dana Designer', 'out', 'events'], ['José Pérez', 'out', 'events'], ['Quinn Quiet', 'out', 'sessions']], 'who is on, where the numbers come from');
  // first login, lock-out, return
  const times = n => { const p = by(n); return [p.firstIn, p.lastOut, p.onSince, p.nowAt, p.inDay]; };
  eq(times('Shane Shipper'), [D2(8, 30), null, D2(13), ['shipping'], '2026-10-02'], 'Shane: first in 08:30, locked out 12:00, back at 13:00 (on: no lock-out)');
  eq(times('Ann Assembler'), [D2(7, 45), null, D2(7, 45), ['assembly'], '2026-10-02']);
  eq(times('Tess Welder'), [D2(8), D2(12, 30), null, [], '2026-10-02']);
  eq(times('Sam Sorter'), [D2(9), D2(10, 30), null, [], '2026-10-02'], 'two computers: first in 09:00, last out 10:30');
  eq(times('Giovanna'), [D2(8), D2(9, 30), null, [], '2026-10-02']);
  eq(times('José Pérez'), [D2(8), D2(14, 30), null, [], '2026-10-02']);
  eq(times('Quinn Quiet'), [D2(9), D2(9, 20), null, [], '2026-10-02'], 'a session left quiet closes at its last beat');
  // stations per person (parts desc), minutes = time signed in at that page
  eq(by('Tess Welder').stations, [{ station: 'welding', minutes: 270, parts: 15, scanParts: 20, scans: 5, completes: 4, prints: 0, orders: 5 }]);
  eq(by('Sam Sorter').stations, [{ station: 'sorting', minutes: 90, parts: 45, scanParts: 45, scans: 30, completes: 30, prints: 0, orders: 30 }]);
  eq(by('Shane Shipper').stations, [{ station: 'shipping', minutes: 360, parts: 18, scanParts: 20, scans: 4, completes: 3, prints: 2, orders: 4 }]);
  eq(by('José Pérez').stations, [{ station: 'welding', minutes: 30, parts: 4, scanParts: 4, scans: 1, completes: 1, prints: 0, orders: 1 }, { station: 'assembly', minutes: 60, parts: 2, scanParts: 2, scans: 1, completes: 1, prints: 0, orders: 1 }]);
  eq(by('Giovanna').stations, [{ station: 'sorting', minutes: 60, parts: 7, scanParts: 10, scans: 1, completes: 1, prints: 1, orders: 2 }, { station: 'inbox', minutes: 60, parts: 0, scanParts: 0, scans: 0, completes: 1, prints: 0, orders: 0 }]);
  // hourly buckets in New York time (parts produced minus undone, per hour)
  const hr = (o) => { const a = new Array(24).fill(0); for (const [h, v] of Object.entries(o)) a[+h] = v; return a; };
  eq(by('Tess Welder').perHour, hr({ 8: 10, 10: 5 }), 'Tess: 4+6+3 produced at 08, 3 undone at 08 = 10; 5 at 10');
  eq(by('Sam Sorter').perHour, hr({ 9: 35, 10: 10 }), 'Sam: sorting 28 + sorter 7 at 09; 2 + 8 at 10');
  eq(by('Dana Designer').perHour, hr({ 9: 6 }), 'Dana: 8 produced, 2 undone in the same hour');
  eq(by('Shane Shipper').perHour, hr({ 8: 3, 13: 5, 14: 10 }));
  eq(by('Ann Assembler').perHour, hr({ 7: 10 }), 'a scan at 13:00 produces nothing');
  assert(A.people.every(p => p.perHour.reduce((a, b) => a + b, 0) === p.totals.parts), 'each person: the hourly graph sums to the net parts');
  // recent orders (newest first, net of undo)
  eq(by('Tess Welder').orders.map(o => [o.orderId, o.parts]), [[O(5), 5], [O(4), 0], [O(3), 0], [O(2), 6], [O(1), 4]], 'the undone and the cancelled order carry no parts');
  eq(by('Ann Assembler').orders.map(o => [o.orderId, o.parts, o.stations]), [[OX, 0, ['assembly']], [O(22), 0, ['assembly']], [O(21), 4, ['assembly']], [O(20), 6, ['assembly']]]);
  eq(by('Sam Sorter').orders.length, 25, 'orders list = the orders worked (25, under the 30 cap)');
  assert(!JSON.stringify(A).includes('424242'), 'a digits-only name never appears');

  // the business: the sum of the people (parts, scans), distinct orders, per station
  eq(A.business.totals, { parts: 135, scans: 49, orders: 46, people: 10 }, 'parts and scans are the sum of the people; orders are distinct (OX is worked at 4 stations by 4 people and counts once: 48 - 2 = 46)');
  assert.strictEqual(A.business.totals.parts, A.people.reduce((n, p) => n + p.totals.parts, 0)); assert.strictEqual(A.business.totals.scans, A.people.reduce((n, p) => n + p.totals.scans, 0));
  assert.strictEqual(A.people.reduce((n, p) => n + p.totals.orders, 0), 48, 'the people\'s rows add to 48: OX is on three of them');
  eq(A.business.stations, [
    { station: 'sorting', parts: 52, scans: 31, orders: 27, peopleNow: [] }, { station: 'welding', parts: 35, scans: 9, orders: 9, peopleNow: [] },
    { station: 'assembly', parts: 12, scans: 4, orders: 5, peopleNow: ['Ann Assembler'] }, { station: 'shipping', parts: 18, scans: 4, orders: 4, peopleNow: ['Shane Shipper'] },
    { station: 'design', parts: 6, scans: 0, orders: 2, peopleNow: [] }, { station: 'laser', parts: 12, scans: 1, orders: 1, peopleNow: [] },
    { station: 'inbox', parts: 0, scans: 0, orders: 0, peopleNow: [] }], 'per station, in the stations\' order');
  assert.strictEqual(A.business.stations.reduce((n, s) => n + s.parts, 0), A.business.totals.parts, 'the stations add up to the business'); assert.strictEqual(A.business.stations.reduce((n, s) => n + s.scans, 0), A.business.totals.scans);
  eq(A.business.perHour, { sorting: hr({ 8: 7, 9: 35, 10: 10 }), welding: hr({ 8: 10, 10: 18, 11: 3, 14: 4 }), assembly: hr({ 7: 10, 8: 2 }), shipping: hr({ 8: 3, 13: 5, 14: 10 }), design: hr({ 9: 6 }), laser: hr({ 10: 12 }) }, 'per station per hour; the inbox produced nothing and is not drawn');
  const bizHours = new Array(24).fill(0); for (const arr of Object.values(A.business.perHour)) arr.forEach((v, i) => { bizHours[i] += v; });
  eq(bizHours, A.people.reduce((acc, p) => acc.map((v, i) => v + p.perHour[i]), new Array(24).fill(0)), 'the business hours are the people\'s hours'); assert.strictEqual(bizHours.reduce((a, b) => a + b, 0), 135);
  const tr = A.business.trend; assert.strictEqual(tr.length, 14);
  eq(tr[13], { day: '2026-10-02', parts: 135, orders: 46, people: 10, source: 'events' }, 'today\'s trend point is the overview');
  eq(tr[12], { day: '2026-10-01', parts: 0, orders: 0, people: 1, source: 'sessions' }, 'a day with only a sign-in');
  eq(tr[11], { day: '2026-09-30', parts: 0, orders: 3, people: 2, source: 'seals' }, 'the older day from seals: orders, no parts');
  eq(A.sources, { events: true, seals: false, sessions: true }, 'the day itself has no sealed people (the trend\'s older days do)'); assert.strictEqual(A.partial, undefined);

  // the feed: one line per event, under the display name, no repeats
  const F = await ask({ day: '2026-10-02', after: '0~' });
  assert.strictEqual(F.delta, true); assert.strictEqual(F.feed.length, 114, 'every event once (retries did not add lines)'); assert.strictEqual(new Set(F.feed.map(f => f.id)).size, 114);
  assert(F.feed.every(f => !['Giovanna C.', 'JOSE PEREZ'].includes(f.person)), 'the feed uses one display name');
  assert.strictEqual(A.feed.length, 40); eq(A.feed.map(f => f.id), F.feed.slice(0, 40).map(f => f.id), 'the full answer is the newest 40');
  eq(F.feed.filter(f => f.orderId === OX).map(f => [f.person, f.station, f.action]).sort(), [['Ann Assembler', 'assembly', 'scan'], ['Giovanna', 'sorting', 'scan'], ['Shane Shipper', 'shipping', 'complete'], ['Shane Shipper', 'shipping', 'scan']], 'OX: three scans and one completion');

  // parts produced vs parts scanned for OX (10 pieces): scanned 3 times (30 pieces), produced once
  const oxScanParts = ['Giovanna', 'Ann Assembler', 'Shane Shipper'].map(n => by(n).orders.find(o => o.orderId === OX)).filter(Boolean);
  assert.strictEqual(oxScanParts.length, 3);
  const oxProduced = docsOf('Station_Activity').filter(e => e.orderId === OX && e.action === 'complete').reduce((n, e) => n + e.parts, 0), oxScanned = docsOf('Station_Activity').filter(e => e.orderId === OX && e.action === 'scan').reduce((n, e) => n + e.parts, 0);
  assert.deepStrictEqual([oxProduced, oxScanned], [10, 30], 'OX: 30 pieces scanned, 10 produced');
  assert.strictEqual(docsOf('Station_Activity').filter(e => e.orderId === OX && e.parts > 0 && e.action === 'complete').length, 1, 'OX is completed once, at shipping (Shane\'s 18 = 3 + 5 + 10)');

  // the person op = the overview's person, field by field; and the 7-day range
  for (const p of A.people) {
    const P = await ask({ op: 'person', name: p.name, day: '2026-10-02', days: 1 }), d = P.days[0];
    eq(P.totals, p.totals, p.name + ': person op totals = overview totals');
    eq([d.day, d.source, d.parts, d.scanParts, d.scans, d.orders, d.rejects, d.errors, d.activeMin, d.idleMin, d.signedInMin], ['2026-10-02', p.source, p.totals.parts, p.totals.scanParts, p.totals.scans, p.totals.orders, p.totals.rejects, p.totals.errors, p.totals.activeMin, p.totals.idleMin, p.totals.signedInMin], p.name + ': the day row');
    eq([d.firstIn, d.lastOut, d.stations, d.perHour], [p.firstIn, p.lastOut, p.stations, p.perHour], p.name + ': in/out, stations, hours are the same on both screens');
    assert.strictEqual(P.name, p.name);
  }
  for (const spelling of ['giovanna c.', 'GIOVANNA', 'Giovanna C.']) assert.strictEqual((await ask({ op: 'person', name: spelling, days: 1 })).totals.parts, 7, 'any spelling finds the same person: ' + spelling);
  assert.strictEqual((await ask({ op: 'person', name: 'JOSE  perez', days: 1 })).totals.parts, 6); assert.strictEqual((await ask({ op: 'person', name: 'Jose Perez', days: 1 })).name, 'José Pérez');
  const TW = await ask({ op: 'person', name: 'Tess Welder', day: '2026-10-02', days: 7 });
  eq(TW.days.map(d => [d.day, d.source]), [['2026-09-26', 'none'], ['2026-09-27', 'none'], ['2026-09-28', 'none'], ['2026-09-29', 'none'], ['2026-09-30', 'seals'], ['2026-10-01', 'sessions'], ['2026-10-02', 'events']], 'every day, oldest first, each labelled by where it comes from');
  eq([TW.days[4].parts, TW.days[4].scans, TW.days[4].orders, TW.days[4].signedInMin, TW.days[4].perHour[9], TW.days[4].perHour[10]], [0, 2, 3, 240, 2, 1], '30 Sep from seals: 3 orders, 2 scans, no parts, steps by hour');
  eq(TW.days[4].stations, [{ station: 'welding', minutes: 240, parts: 0, scanParts: 0, scans: 2, completes: 3, prints: 1, orders: 3 }]);
  eq([TW.days[5].signedInMin, TW.days[5].parts, TW.days[5].orders], [60, 0, 0], '1 Oct: signed in an hour, nothing else');
  eq(TW.totals, totals(15, 20, 7, 8, 1, 1, 13.5, 72.5, 570, 66.7, 162), '7 days: parts from events only; scans 5 + 2 from seals; 8 distinct orders; secs per scan uses only the logged scans (810 / 5), not the seal scans');
  eq(TW.sources, { events: true, seals: true, sessions: true });
  const A7 = await ask({ day: '2026-10-02', days: 7 }), tw7 = A7.people.find(p => p.name === 'Tess Welder');
  eq(tw7.totals, TW.totals, 'the 7-day overview row = the person op over the same days'); assert.strictEqual(tw7.source, 'mixed');
  assert.strictEqual(A7.business.totals.parts, 135); assert.strictEqual(A7.business.totals.orders, 49, '46 + the 3 sealed orders of 30 Sep (O80 is also Ann\'s)');
  assert.strictEqual(A7.business.totals.people, 10); eq(A7.sources, { events: true, seals: true, sessions: true }); assert(A7.notes.some(n => /seals/.test(n)), 'sealed history is labelled');

  // the order trace
  const OXt = await ask({ op: 'orders', orderId: OX });
  eq(OXt.steps.map(s => [s.station, s.person, s.firstAt, s.lastAt, s.workMs, s.waitMs, s.scans, s.completes, s.prints, s.parts, s.source]), [
    ['sorting', 'Giovanna', D2(8, 5), D2(8, 5), 5 * MIN, 0, 1, 0, 0, 0, 'events'],
    ['assembly', 'Ann Assembler', D2(13), D2(13), 0, D2(13) - D2(8, 5), 1, 0, 0, 0, 'events'],
    ['shipping', 'Shane Shipper', D2(14), D2(14, 2), 2 * MIN, 60 * MIN, 1, 1, 0, 10, 'events']], 'OX: work = the gaps of 5 min or less leading to each action; wait = the time since the previous step ended');
  eq(OXt.totals, { firstAt: D2(8, 5), lastAt: D2(14, 2), spanMs: D2(14, 2) - D2(8, 5), workMs: 7 * MIN, people: 3, stations: 3 });
  eq(OXt.events.map(e => e.at), OXt.events.map(e => e.at).slice().sort((a, b) => a - b)); assert.strictEqual(OXt.events.length, 4);
  const O3 = await ask({ op: 'orders', orderId: O(3) });
  eq(O3.steps.map(s => [s.station, s.person, s.scans, s.completes, s.parts, s.workMs]), [['welding', 'Tess Welder', 1, 0, 0, 90000]], 'the cancelled order: scanned, rejected, no part, 90 s of work (60 + 30)');
  eq(O3.events.map(e => [e.action, e.detail]), [['scan', ''], ['reject', 'cancelled order 3521000003, hold code [#]']], 'a 6-digit number in a detail is hidden, the 10-digit order number stays');
  const O4 = await ask({ op: 'orders', orderId: O(4) });
  eq(O4.steps.map(s => [s.scans, s.completes, s.parts, s.workMs]), [[1, 1, 0, 2 * MIN]], 'completed then undone: its 3 parts are taken back, as in her order list; the 12.5-minute gap is not work');
  const O31 = await ask({ op: 'orders', orderId: O(31) });
  eq(O31.steps.map(s => [s.station, s.scans, s.completes, s.parts, s.workMs]), [['shipping', 1, 0, 0, 4 * MIN]]);
  const O80 = await ask({ op: 'orders', orderId: O(80) });
  eq(O80.steps.map(s => [s.station, s.person, s.source, s.workMs, s.scans, s.completes, s.prints, s.parts]), [['welding', 'Tess Welder', 'seals', 0, 1, 1, 1, 0], ['assembly', 'Ann Assembler', 'seals', 0, 0, 1, 0, 0]], 'a sealed order: steps from seals, no parts, no work time');
  eq([O80.steps[1].waitMs, O80.sources], [D0(11) - D0(9, 1), { events: false, seals: true }]);
  eq((await ask({ op: 'orders', orderId: O(98) })).steps, [], 'an unknown order is empty, not an error');

  // sandbox never mixes
  const sb = await post({ activity: [{ id: 'sb-weld-1_AAAA_1_1', station: 'welding', device: 'weld-1', computer: 'pc-AAAAAAAA', session: '', person: 'Sandy Box', action: 'complete', orderId: O(777), parts: 500, orders: 1, at: NOW, seq: 1, sincePrevMs: 1000, sandbox: true }] }, { sandbox: true });
  assert.strictEqual(sb.body.written, 1); dropCaches();
  assert.strictEqual((await ask({ day: '2026-10-02' })).business.totals.parts, 135, 'production is untouched by a sandbox event');
  eq((await ask({ day: '2026-10-02', sandbox: true })).people.map(p => p.name), ['Sandy Box']); NOW = D2(15, 30);
  col('Sandbox_Station_Activity').clear(); col('Sandbox_Efficiency_Daily').clear();
  MAIN = { A, F, TW, OXt };
  say('1 the day: 114 events from 15 pages, 10 people, hand-computed totals, hours, stations, trend, feed, person = overview, orders trace (cancelled, undone, sealed, scanned at 3 stations)');

  /* ═══════════ 2 · the New York midnight ═══════════ */
  // 23:59:58 a manager has the screen open (today's rollup is read and cached); events queued before midnight arrive after it.
  signIn(D2(23, 30), 'N', 'Nina Night', 'sess-nina-1'); beats('N', 'sess-nina-1', 'Nina Night', D2(23, 35), D2(23, 55));
  ev(D2(23, 40), 'N', 'scan', { orderId: O(90), parts: 3 });                                           // 10 min: idle
  ev(D2(23, 41), 'N', 'complete', { orderId: O(90), parts: 3, orders: 1 });
  flush(D2(23, 42), 'N');
  setMode(D2(23, 56), 'N', 'down');
  ev(D2(23, 58), 'N', 'scan', { orderId: O(91), parts: 2 });                                           // 17 min: idle
  ev(D2(23, 59, 50), 'N', 'note', { detail: 'sheet check' });                                          // 110 s: work
  at(D2(23, 59, 58), async () => { dropCaches(); const o = await ask({ day: '2026-10-02' }); assert.strictEqual(o.business.totals.parts, 138); });   // the 23:41 completion is already in
  signOut(D2(24, 0, 2), 'A', 'sess-ann-1002', 'midnight'); signOut(D2(24, 0, 2), 'H', 'sess-shane-2', 'midnight');
  at(D2(24, 0, 1), () => { PAGES.N.who = null; });                                                      // the midnight sign-out: nothing new is logged
  at(D2(24, 0, 3), () => { assert.strictEqual(PAGES.N.A.log('scan', { orderId: O(95) }), false, 'nobody signed in after midnight'); });
  at(D2(24, 0, 40), () => post({ session: { id: 'sess-nina-1', event: 'end', reason: 'midnight', station: 'sorting', computerId: 'pc-NNNNNNNN', at: D2(24) } }).then(r => assert.strictEqual(r.body.endAt, D2(24), 'ended at midnight')));
  setMode(D2(24, 0, 30), 'N', 'ok');
  flush(D2(24, 1), 'N');                                                                                // the queued events are sent after midnight
  at(D2(24, 1, 10), async () => {
    const o = await ask({ day: '2026-10-02' });                                                         // the screen, now looking at yesterday
    assert.strictEqual(o.business.totals.scans, 51, 'the events queued at 23:58 and 23:59:50 are in 2 Oct at 00:01 (a cached read of the live day must not be kept as a finished day)');
    const nina = o.people.find(p => p.name === 'Nina Night'); assert.strictEqual(nina.totals.scanParts, 5);
    const ann = o.people.find(p => p.name === 'Ann Assembler'); eq([ann.lastOut, ann.totals.signedInMin], [D2(15, 28), 463], 'Ann\'s page went silent after its beat at 15:28: since AD2 (6 Oct 2026) the first read after 15 quiet minutes ends the session "closed" at that beat for good (a non-Admin without a reported input keeps the old 15-minute rule), so the midnight end that comes later changes nothing');
  });
  signIn(D2(24, 5), 'N', 'Nina Night', 'sess-nina-2');
  ev(D2(24, 7), 'N', 'scan', { orderId: O(92), parts: 4 });
  ev(D2(24, 8), 'N', 'complete', { orderId: O(92), parts: 4, orders: 1 });
  flush(D2(24, 9), 'N');
  beats('N', 'sess-nina-2', 'Nina Night', D2(24, 10), D2(24, 25)); at(D2(24, 28), () => post({ session: { id: 'sess-nina-2', event: 'beat', person: 'Nina Night', station: 'sorting', device: 'sorting-3', computerId: 'pc-NNNNNNNN' } }));
  await run();
  NOW = D2(24, 30); dropCaches();

  const B2 = await ask({ day: '2026-10-02' }), B3 = await ask({ day: '2026-10-03' });
  eq(B2.people.map(p => p.name), ['Sam Sorter', 'Ray Welder', 'Tess Welder', 'Shane Shipper', 'Nina Night', 'Leo Laser', 'Giovanna', 'Ann Assembler', 'Dana Designer', 'José Pérez', 'Quinn Quiet'].sort((a, b) => B2.people.findIndex(p => p.name === a) - B2.people.findIndex(p => p.name === b)));
  const nina2 = B2.people.find(p => p.name === 'Nina Night'), nina3 = B3.people.find(p => p.name === 'Nina Night');
  // 2 Oct: 23:40 scan (idle 600 s), 23:41 complete (60 s), 23:58 scan (idle 1020 s), 23:59:50 note (110 s): 2 scans, 3+2 pieces, 3 parts, O90 and O91, 170 s active, 1620 s idle
  eq(nina2.totals, totals(3, 5, 2, 2, 0, 0, 2.8, 27, 30, 63.5, 85), 'Nina\'s day 2 Oct: signed in 23:30-24:00');
  eq([nina2.firstIn, nina2.lastOut, nina2.status, nina2.perHour[23], nina2.perHour[0]], [D2(23, 30), D2(24), 'out', 3, 0], 'locked out at midnight; the hour 23 holds her 3 parts');
  // 3 Oct: signed in again 00:05 (a new session: her gap starts there), 00:07 scan (120 s), 00:08 complete (60 s)
  eq(nina3.totals, totals(4, 4, 1, 1, 0, 0, 3, 0, 25, 80, 180), 'Nina\'s 3 Oct: 00:05-00:30 (the open session counts to now)');
  eq([nina3.firstIn, nina3.status, nina3.perHour[0], nina3.perHour[23]], [D2(24, 5), 'on', 4, 0], 'the first hour of the new day is bucket 00');
  eq(B3.people.map(p => p.name), ['Nina Night'], 'Ann and Shane were ended AT midnight: nothing of theirs on the next day');
  eq(B3.business.totals, { parts: 4, scans: 1, orders: 1, people: 1 });
  eq(B2.business.totals, { parts: 138, scans: 51, orders: 48, people: 11 }, '2 Oct after midnight: + Nina\'s 3 parts, 2 scans, 2 orders');
  const ann2 = B2.people.find(p => p.name === 'Ann Assembler'), shane2 = B2.people.find(p => p.name === 'Shane Shipper');
  eq([ann2.status, ann2.lastOut, ann2.totals.signedInMin, ann2.nowAt, shane2.totals.signedInMin, shane2.lastOut, shane2.onSince], ['out', D2(15, 28), 463, [], 359, D2(15, 29), null], 'a past day shows nobody on: Ann 07:45-15:28 = 463 min, Shane 210 + 149 (13:00 to his last beat at 15:29) = 359 (both pages went silent; see the 00:01 read above)');
  eq(B2.business.stations.every(s => s.peopleNow.length === 0), true);
  eq(B3.business.trend.slice(-2).map(t => [t.day, t.parts, t.orders, t.people]), [['2026-10-02', 138, 48, 11], ['2026-10-03', 4, 1, 1]]);
  for (const n of ['Tess Welder', 'Ray Welder', 'Sam Sorter', 'Dana Designer', 'Leo Laser', 'Giovanna', 'José Pérez']) eq(B2.people.find(p => p.name === n).totals, WANT[n], n + ' is the same after midnight');
  const NP = await ask({ op: 'person', name: 'nina night', day: '2026-10-03', days: 2 });
  eq(NP.days.map(d => [d.day, d.parts, d.signedInMin, d.perHour[23], d.perHour[0]]), [['2026-10-02', 3, 30, 3, 0], ['2026-10-03', 4, 25, 0, 4]]);
  eq([NP.totals.parts, NP.totals.signedInMin, NP.totals.orders], [7, 55, 3]);
  say('2 midnight: events queued before it keep their day, a cached live day is not kept as finished, the midnight sign-out, no overlap onto the next day, hour 23 and 00');

  /* ═══════════ 3 · daylight saving ═══════════ */
  resetStore(); dropCaches();
  let n = 0;
  const E = (o = {}) => { n++; return Object.assign({ id: `weld-1_DDDD_${n}_${o.at || n}`, station: 'welding', device: 'weld-1', computer: 'pc-DDDDDDDD', session: 'sess-dee-0001', person: 'Dee Dst', action: 'complete', orderId: '', line: '', sku: '', parts: 1, orders: 0, detail: '', at: NOW, seq: n, sincePrevMs: 60000 }, o); };
  const Z = iso => Date.parse(iso);
  NOW = Z('2026-11-02T05:30:00Z');                                                                      // the day after fall-back (EST)
  let r = await post({ activity: [E({ at: Z('2026-11-01T04:30:00Z'), parts: 1 }), E({ at: Z('2026-11-01T05:30:00Z'), parts: 2 }), E({ at: Z('2026-11-01T06:30:00Z'), parts: 3 }), E({ at: Z('2026-11-01T07:30:00Z'), parts: 4 }),
    E({ at: Z('2026-11-02T04:59:59Z'), parts: 5 }), E({ at: Z('2026-11-02T05:00:00Z'), parts: 6 })] });
  eq([r.body.written, r.body.refused], [6, 0]);
  const fb = col('Efficiency_Daily').get('2026-11-01__Dee Dst');
  eq(Object.keys(fb.hours).sort(), ['00', '01', '02', '23'], 'fall back: 00:30 EDT, 01:30 EDT, 01:30 EST, 02:30 EST, 23:59:59 EST');
  eq([fb.hours['00'].parts, fb.hours['01'].parts, fb.hours['02'].parts, fb.hours['23'].parts], [1, 5, 4, 5], 'both 01:00 hours share bucket "01" (2 + 3)');
  eq(col('Efficiency_Daily').get('2026-11-02__Dee Dst').hours['00'].parts, 6, '00:00 EST on the 2nd is the next day, hour 00');
  let f = await ask({ day: '2026-11-01' }), dee = f.people[0];
  eq(dee.perHour, hr({ 0: 1, 1: 5, 2: 4, 23: 5 }), 'the reader draws the same hours'); assert.strictEqual(dee.totals.parts, 15);
  assert.strictEqual((await ask({ day: '2026-11-02' })).people[0].totals.parts, 6);
  assert.strictEqual(T.nyMidnight('2026-11-02') - T.nyMidnight('2026-11-01'), 25 * 3600e3, 'the fall-back day is 25 hours');
  resetStore(); dropCaches(); n = 0;
  NOW = Z('2026-03-09T05:00:00Z');                                                                      // the day after spring-forward (EDT)
  r = await post({ activity: [E({ at: Z('2026-03-08T05:30:00Z'), parts: 1 }), E({ at: Z('2026-03-08T06:30:00Z'), parts: 2 }), E({ at: Z('2026-03-08T07:30:00Z'), parts: 3 }), E({ at: Z('2026-03-09T03:59:59Z'), parts: 4 }), E({ at: Z('2026-03-09T04:00:00Z'), parts: 5 })] });
  eq([r.body.written, r.body.refused], [5, 0]);
  const sp = col('Efficiency_Daily').get('2026-03-08__Dee Dst');
  eq(Object.keys(sp.hours).sort(), ['00', '01', '03', '23'], 'spring forward: there is no hour 02');
  eq(col('Efficiency_Daily').get('2026-03-09__Dee Dst').hours['00'].parts, 5);
  NOW = Z('2026-03-08T06:00:00Z'); await post({ session: { id: 'sess-dee-spring', event: 'start', person: 'Dee Dst', station: 'welding', device: 'weld-1', computerId: 'pc-DDDDDDDD', at: NOW } });
  NOW = Z('2026-03-08T08:00:00Z'); await post({ session: { id: 'sess-dee-spring', event: 'end', reason: 'signOut', station: 'welding', computerId: 'pc-DDDDDDDD', at: NOW } });
  NOW = Z('2026-03-09T05:00:00Z'); dropCaches();
  f = await ask({ day: '2026-03-08' }); dee = f.people[0];
  eq(dee.perHour, hr({ 0: 1, 1: 2, 3: 3, 23: 4 })); assert.strictEqual(dee.perHour[2], 0);
  eq([dee.totals.signedInMin, dee.firstIn, dee.lastOut], [120, Z('2026-03-08T06:00:00Z'), Z('2026-03-08T08:00:00Z')], '01:00 EST to 04:00 EDT is 120 real minutes');
  assert.strictEqual(T.nyMidnight('2026-03-09') - T.nyMidnight('2026-03-08'), 23 * 3600e3, 'the spring-forward day is 23 hours');
  say('3 daylight saving: bucket 01 shared on the 25-hour day, no hour 02 on the 23-hour day, the 23:59:59 / 00:00 boundary, a session across the jump = real minutes');

  /* ═══════════ 4 · the door ═══════════ */
  resetStore(); dropCaches(); n = 0; NOW = Date.parse('2026-10-02T15:00:00Z'); const t0 = Date.parse('2026-10-02T13:05:00Z');
  const mk = (o = {}) => E(Object.assign({ id: undefined }, o, { id: o.id || `weld-1_DDDD_${++n}_${(o.at || NOW)}`, at: o.at || NOW }));
  // limits
  r = await post({ activity: Array.from({ length: 51 }, () => mk({ action: 'scan' })) }); assert.strictEqual(r.status, 413); assert.strictEqual(col('Station_Activity').size, 0, 'a request of 51 events is refused whole');
  r = await post(JSON.stringify({ activity: [mk()], pad: 'z'.repeat(40001) })); assert.strictEqual(r.status, 413, 'a body over 40000 characters');
  r = await post({ activity: Array.from({ length: 50 }, () => mk({ action: 'scan' })) }); eq([r.status, r.body.written], [200, 50], '50 events pass');
  resetStore();
  r = await post({ activity: [mk({ detail: 'x'.repeat(100) }), Object.assign(mk({ id: 'big-event-0001' }), { note: 'y'.repeat(700) })] }); eq([r.body.written, r.body.refused], [1, 1], 'an event over 600 bytes is refused, the rest is written');
  resetStore();
  const junk = [mk({ station: 'kitchen' }), mk({ action: 'poke' }), mk({ id: 'x' }), mk({ id: 5 }), mk({ id: '__reserved__' }), mk({ id: '........' }), mk({ person: '' }), mk({ person: '123456' }), mk({ person: ' 654321 ' }), mk({ person: '123 456' }), mk({ person: '12-34-56' }),
    null, 'junk', [1], 7, mk({ sandbox: true })];
  r = await post({ activity: junk.concat([mk({ action: 'scan', parts: 1 })]) });
  eq([r.status, r.body.written, r.body.refused], [200, 1, junk.length], 'unknown station / action, bad ids (a reserved Firestore id would fail the whole batch with a 5xx), names without a letter, junk and a sandbox event at the real door are all refused; the good one is written');
  assert.strictEqual(col('Station_Activity').size, 1); assert.strictEqual(col('Efficiency_Daily').size, 1);
  assert.strictEqual((await post({ activity: 'not a list' })).status, 400, 'a non-list is a 400 (the helper drops it), never a 500');
  assert.strictEqual((await post({ activity: [] })).status, 200);
  // PIN-like values
  resetStore();
  r = await post({ activity: [mk({ orderId: '123456', line: '482913', sku: '9999', detail: '654321' }), mk({ action: 'note', detail: 'typed pin 654321 by mistake, order 3521000777', orderId: O(500) }), mk({ action: 'note', detail: 'sheet 2 of 12, 480 mm', orderId: O(501) })] });
  eq([r.body.written, r.body.scrubbed], [3, 2]);
  assert(!/123456|654321|482913/.test(JSON.stringify([...colls])), 'no PIN-looking value is stored anywhere');
  eq(docsOf('Station_Activity').find(e => e.orderId === O(500)).detail, 'typed pin [#] by mistake, order 3521000777', 'a 10-digit order id in a detail stays readable');
  // clock: far past refused, future clamped
  resetStore();
  r = await post({ activity: [mk({ at: NOW - 8 * 86400e3 }), mk({ at: NOW + 3 * 3600e3, action: 'scan' }), mk({ at: 'soon', action: 'note' })] }); eq([r.body.written, r.body.refused], [2, 1]);
  assert(docsOf('Station_Activity').every(e => e.at === NOW), 'an event is never in the future');
  // order independence + duplicates: the same 40 events in three orders and splits give identical rollups
  const base = [];
  const people = ['Pat Pal', 'Pat Pal', 'Quin Que'], devs = [['weld-1', 'welding'], ['assembly-1', 'assembly'], ['shipping-1', 'shipping']];
  let seed = 7; const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  for (let i = 0; i < 40; i++) {
    const d = devs[i % 3], at = t0 + i * 3 * MIN + Math.floor(rnd() * 60) * 1000, act = ['scan', 'complete', 'complete', 'print', 'reject', 'undo', 'error', 'note'][Math.floor(rnd() * 8)];
    base.push({ id: `${d[0]}_ABCD_${i + 1}_${at}`, station: d[1], device: d[0], computer: 'pc-ABCDEFGH', session: 'sess-pat-0001', person: people[i % 3], action: act, orderId: O(600 + (i % 9)), line: '', sku: '', parts: 1 + Math.floor(rnd() * 9), orders: act === 'complete' ? 1 : 0, detail: '', at, seq: i + 1, sincePrevMs: Math.floor(rnd() * 8) * 60000 });
  }
  NOW = t0 + 3 * 3600e3;
  const strip = d => JSON.parse(JSON.stringify(d, (k, v) => (v instanceof Ts ? 'TS' : v)));
  const rolls = () => Object.fromEntries([...col('Efficiency_Daily')].map(([k, v]) => [k, strip(v)]));
  const feed = [];
  for (const how of ['in order', 'reversed', 'shuffled, uneven batches']) {
    resetStore();
    let list = base.slice();
    if (how === 'reversed') list.reverse();
    if (how.startsWith('shuffled')) { for (let i = list.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [list[i], list[j]] = [list[j], list[i]]; } }
    const cuts = how === 'in order' ? [40] : how === 'reversed' ? [7, 13, 40] : [3, 4, 19, 33, 40];
    let from = 0; for (const c of cuts) { r = await post({ activity: list.slice(from, c) }); assert.strictEqual(r.status, 200); from = c; NOW += 1000; }
    const once = rolls(); feed.push(once);
    r = await post({ activity: base.slice(0, 25) }); eq([r.body.written, r.body.duplicate], [0, 25], how + ': a resend of 25 changes nothing'); eq(rolls(), once);
    r = await post({ activity: base.slice(25).concat(base.slice(25, 40), base.slice(0, 20)) }); eq([r.body.written, r.body.duplicate], [0, 50], how + ': ids twice in one request and already stored change nothing'); eq(rolls(), once);
    assert.strictEqual(col('Station_Activity').size, 40);
  }
  eq(feed[1], feed[0], 'reversed batches give the same rollups'); eq(feed[2], feed[0], 'shuffled batches give the same rollups');
  // ... and those rollups are right: recount the 40 events by hand-simple code
  { const want = {}; for (const e of base) { const k = ACT.nyDayHour(e.at).day + '__' + e.person; const w = want[k] || (want[k] = { events: 0, scans: 0, scanParts: 0, completes: 0, parts: 0, undoParts: 0, rejects: 0, prints: 0, errors: 0, notes: 0, active: 0, idle: 0, touched: new Set() }); w.events++; if (e.orderId) w.touched.add(e.orderId);
      if (e.action === 'scan') { w.scans++; w.scanParts += e.parts; } if (e.action === 'complete') { w.completes++; w.parts += e.parts; } if (e.action === 'undo') w.undoParts += e.parts; if (e.action === 'reject') w.rejects++; if (e.action === 'print') w.prints++; if (e.action === 'error') w.errors++; if (e.action === 'note') w.notes++;
      if (e.sincePrevMs <= 300000) w.active += e.sincePrevMs; else w.idle += e.sincePrevMs; }
    for (const [k, w] of Object.entries(want)) { const d = feed[0][k]; const sum = f2 => Object.values(d.stations).reduce((n, s) => n + (s[f2] || 0), 0);
      eq([d.events, Object.keys(d.touched).length, sum('scans'), sum('scanParts'), sum('completes'), sum('parts'), sum('undoParts'), sum('rejects'), sum('prints'), sum('errors'), sum('notes'), sum('activeMs'), sum('idleMs')],
        [w.events, w.touched.size, w.scans, w.scanParts, w.completes, w.parts, w.undoParts, w.rejects, w.prints, w.errors, w.notes, w.active, w.idle], 'recount ' + k);
      eq(Object.values(d.hours).reduce((a, h) => a + (h.parts || 0) - (h.undoParts || 0), 0), w.parts - w.undoParts, 'the hours add to the net parts ' + k); } }
  // the feed never skips an event that commits late: A starts first but commits after B (it re-runs because B changed the same rollup)
  resetStore(); dropCaches(); NOW = Date.parse('2026-10-02T15:00:00Z');
  const evA = mk({ person: 'Late Larry', action: 'scan', at: NOW - 1000 }), evB = mk({ person: 'Late Larry', device: 'weld-2', action: 'scan', at: NOW - 900 });
  let gate; const gated = new Promise(res => { gate = res; });
  hooks.beforeCommit = async (attempt, run) => { if (run === 1 && attempt === 1) await gated; };
  const pA = post({ activity: [evA] }); await settle();
  NOW += 100; r = await post({ activity: [evB] }); assert.strictEqual(r.body.written, 1);
  NOW += 100; const first = await ask({ day: '2026-10-02' }); eq(first.feed.map(x => x.id), [evB.id]); const cur = first.cursor;
  NOW += 100; gate(); r = await pA; assert.strictEqual(r.body.written, 1, 'A\'s transaction re-ran and committed after B');
  assert(hooks.txRuns >= 3, 'A ran twice'); hooks.beforeCommit = null;
  NOW += 6000; const next = await ask({ day: '2026-10-02', after: cur });
  eq(next.feed.map(x => x.id), [evA.id], 'the poll after B still shows A (it commits later than B although its server time is older)');
  eq(next.people[0].totals.scans, 2);
  // a failing store answers a 5xx and counts nothing; the same ids go through afterwards
  resetStore(); hooks.failTx = () => true; console.error = () => {};
  const evC = mk({ action: 'complete', parts: 4, orders: 1 }); r = await post({ activity: [evC] }); console.error = realErr;
  assert(r.status >= 500); assert.strictEqual(col('Station_Activity').size, 0); hooks.failTx = null;
  r = await post({ activity: [evC] }); eq([r.status, r.body.written], [200, 1]); eq(col('Efficiency_Daily').get('2026-10-02__Dee Dst').stations.welding.parts, 4);
  // reader limits and the gate
  assert.strictEqual((await read({ day: '2026-10-02' }, {})).status, 200);
  assert.strictEqual((await read({ key: 'nope' })).status, 401); assert.strictEqual((await read({ key: undefined })).status, 401); assert.strictEqual((await read({ pad: 'z'.repeat(8100) })).status, 413);
  assert.strictEqual((await read({ day: '2026-02-31' })).status, 400);
  say('4 the door: 413 at 51 events / 40000 chars, malformed and PIN-like refused, reserved ids refused, order independence (3 orderings, duplicates) = a recount, the feed shows a late commit, a store error counts nothing');

  /* ═══════════ 5 · the browser helper ═══════════ */
  resetStore(); dropCaches(); NOW = Date.parse('2026-10-02T14:00:00Z');
  const who = (person, sid) => ({ person, station: 'welding', device: 'weld-1', computer: 'pc-AAAAAAAA', session: sid, startAt: NOW - 30000, sandbox: false });
  const mkPage = (opts, person = 'Cleo Client', sid = 'sess-cleo-0001') => openPage({ station: 'welding', device: 'weld-1', computer: 'pc-AAAAAAAA', who: who(person, sid) }, opts);
  // 5a a 503 outage keeps everything (and says nothing is lost), then every event arrives once
  let pg = mkPage(); const sent = [];
  for (let i = 0; i < 4; i++) { NOW += 60000; pg.log('scan', { orderId: O(700 + i), parts: 2 }); NOW += 60000; pg.log('complete', { orderId: O(700 + i), parts: 2, orders: 1 }); }
  eq([pg.pending(), pg.queued().length], [8, 8], 'queued in memory and in localStorage');
  pg.mode = 503; await pg.tick(); eq(pg.pending(), 8, 'a 503 keeps the batch'); const callsAfter503 = pg.calls.length;
  NOW += 5000; await pg.tick(); assert.strictEqual(pg.calls.length, callsAfter503, 'the back-off holds the next try');
  for (const status of [404, 403, 401, 405, 408, 409, 429, 500, 502]) { NOW += 200000; pg.mode = status; await pg.tick(); assert.strictEqual(pg.pending(), 8, 'status ' + status + ' keeps the events (a proxy, a captive portal or a deploy in progress can answer it)'); }
  NOW += 200000; pg.mode = 'down'; await pg.tick(); assert.strictEqual(pg.pending(), 8, 'a network error keeps them');
  NOW += 200000; pg.mode = 'lost'; await pg.tick(); assert.strictEqual(pg.pending(), 8, 'a lost answer keeps them (the server has them)'); assert.strictEqual(eventsOf('Cleo Client').length, 8);
  NOW += 200000; pg.mode = 'ok'; await pg.tick(); eq([pg.pending(), eventsOf('Cleo Client').length], [0, 8], 'after the outage: delivered, each once');
  const ids = new Set(pg.calls.flatMap(c => c.ids)); assert.strictEqual(ids.size, 8, 'every retry carried the same 8 ids');
  const cd = col('Efficiency_Daily').get('2026-10-02__Cleo Client').stations.welding; eq([cd.scans, cd.completes, cd.parts, cd.scanParts, cd.orders], [4, 4, 8, 8, 4], 'nothing counted twice after 14 attempts');
  // 5b a 400 is dropped for good, and says so
  pg = mkPage({}, 'Cleo Client', 'sess-cleo-0002'); NOW += 1000; pg.log('scan', { parts: 1 }); pg.mode = 400; await pg.tick(); eq(pg.pending(), 0); assert(pg.warns.some(w => /refused for good/.test(w)), 'a refusal for good is reported');
  // 5c blocked localStorage: ids stay unique, gaps stay real
  pg = mkPage({ lsBroken: true }, 'Blocked Bea', 'sess-bea-00001'); NOW += 120000;
  pg.log('scan', { orderId: O(800), parts: 1 }); pg.log('complete', { orderId: O(800), parts: 1, orders: 1 });                          // two in the same millisecond
  NOW += 90000; pg.log('scan', { orderId: O(801), parts: 1 });
  NOW += 2 * 3600e3; pg.log('note', { detail: 'after a long break' }); NOW += 60000; pg.log('scan', { orderId: O(802), parts: 1 });
  const bq = pg.A.pending(); assert.strictEqual(bq, 5);
  await pg.tick(); eq([pg.pending(), eventsOf('Blocked Bea').length], [0, 5], 'two events in one millisecond are two events (the id carries a counter even without localStorage)');
  const bea = eventsOf('Blocked Bea').sort((a, b) => a.seq - b.seq);
  eq(bea.map(e => e.sincePrevMs), [120000 + 30000, 0, 90000, 3600000, 60000], 'gaps come from the page\'s memory when localStorage is blocked: 2.5 min after sign-in, then 0, 90 s, an hour (capped), 60 s');
  const bd = col('Efficiency_Daily').get('2026-10-02__Blocked Bea').stations.welding; eq([bd.activeMs, bd.idleMs], [150000 + 0 + 90000 + 60000, 3600000], 'work 150 + 90 + 60 s, quiet 1 h');
  // 5d reload after a lost answer: the same events, sent again, are stored once
  pg = mkPage({}, 'Rae Reload', 'sess-rae-00001'); NOW += 60000; pg.log('scan', { orderId: O(810), parts: 3 }); NOW += 60000; pg.log('complete', { orderId: O(810), parts: 3, orders: 1 });
  pg.mode = 'lost'; await pg.tick(); eq([pg.pending(), eventsOf('Rae Reload').length], [2, 2]);
  const reloaded = openPage({ station: 'welding', device: 'weld-1', computer: 'pc-AAAAAAAA', who: pg.who }, { ls: pg.ls }); NOW += 10000;
  reloaded.timeouts.find(t => t.ms === 4000).f(); await settle(); eq([reloaded.pending(), eventsOf('Rae Reload').length], [0, 2], 'a reload sends what was left; the repeat is stored once');
  assert.strictEqual(col('Efficiency_Daily').get('2026-10-02__Rae Reload').stations.welding.completes, 1);
  // 5e leaving the page: the beacon reaches the server, the events stay until a 200, and the repeat changes nothing
  pg = mkPage({ beacon: true }, 'Bo Beacon', 'sess-bo-000001'); NOW += 60000; pg.log('scan', { orderId: O(820), parts: 2 }); NOW += 60000; pg.log('complete', { orderId: O(820), parts: 2, orders: 1 });
  await pg.fire('pagehide'); await settle(); eq([pg.beacons.length, pg.beacons[0].activity.length, eventsOf('Bo Beacon').length, pg.pending()], [1, 2, 2, 2], 'beaconed: stored, still queued');
  await pg.tick(); eq([pg.pending(), eventsOf('Bo Beacon').length, col('Efficiency_Daily').get('2026-10-02__Bo Beacon').stations.welding.completes], [0, 2, 1], 'the later 200 clears the queue, nothing double counted');
  // 5f an overflow is reported, not silent
  pg = mkPage({}, 'Max Many', 'sess-max-00001'); pg.mode = 'down';
  for (let i = 0; i < 520; i++) { NOW += 1000; pg.log('note'); }
  eq(pg.pending(), 500, 'at most 500 wait'); assert(pg.warns.some(w => /dropped/.test(w)), 'the oldest being dropped is warned about');
  // 5g a name that is only digits, or digits and spaces, is never an identity
  pg = mkPage({}, '123456', 'sess-pin-00001'); assert.strictEqual(pg.A.log('scan'), false); pg.who.person = '123 456'; assert.strictEqual(pg.A.log('scan'), false); pg.who.person = 'Ann 2'; assert.strictEqual(pg.A.log('scan'), true);
  say('5 the helper: outages and retries keep every event exactly once (503, 4xx, network, lost answers, reload, beacon), blocked localStorage, a 400 is reported, the cap warns, no digit-only identity');

  /* ═══════════ 6 · the console's view model from the real answers ═══════════ */
  {
    const win = { document: { getElementById: () => null, addEventListener() {} }, console }; win.window = win; vm.createContext(win);
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), win);
    const Eff = win.Efficiency, j = o => JSON.parse(JSON.stringify(o));
    const M = j(Eff.norm(JSON.parse(JSON.stringify(MAIN.A))));
    eq(M.people.map(p => p.name), MAIN.A.people.map(p => p.name), 'the screen shows the same people');
    assert.strictEqual(M.biz.parts, 135); assert.strictEqual(M.biz.scans, 49); assert.strictEqual(M.biz.orders, 46); assert.strictEqual(M.biz.people, 10); assert.strictEqual(M.biz.on, 2);
    for (const p of MAIN.A.people) { const q = M.people.find(x => x.name === p.name); eq([q.t.parts, q.t.scanParts, q.t.scans, q.t.orders, q.t.activeMin, q.t.idleMin, q.t.signedInMin, q.t.rate, q.t.secPerScan, q.perHour], [p.totals.parts, p.totals.scanParts, p.totals.scans, p.totals.orders, p.totals.activeMin, p.totals.idleMin, p.totals.signedInMin, p.totals.rate, p.totals.secPerScan, p.perHour], p.name + ': the card shows the server\'s numbers'); }
    assert.strictEqual(M.biz.hours.reduce((a, b) => a + b, 0), 135, 'the hourly graph adds to the KPI');
    const sumActive = MAIN.A.people.reduce((n, p) => n + p.totals.activeMin, 0);
    assert(Math.abs(M.biz.rate - 135 / (sumActive / 60)) < 1e-9, 'the business rate is parts over everybody\'s active hours');
    assert(M.biz.rate > 20 && M.biz.rate < 80);
    const H = j(Eff.normHist(MAIN.TW)); eq(H.days.map(d => d.parts), [0, 0, 0, 0, 0, 0, 15]); assert.strictEqual(H.parts, 15); assert.strictEqual(H.orders, 8); assert.strictEqual(H.signedInMin, 570); assert.strictEqual(H.worked, 3, '30 Sep, 1 Oct and 2 Oct were worked days');
    const Od = j(Eff.normOrder(MAIN.OXt)); eq(Od.steps.map(s => s.station), ['sorting', 'assembly', 'shipping']); assert.strictEqual(Od.workMs, 7 * MIN); assert.strictEqual(Od.spanMs, MAIN.OXt.totals.spanMs);
    say('6 the console view model: the same people, the same totals, hours add to the KPI, the business rate, a person\'s days, an order trace');
  }
  say('OK');
})().then(() => { Date.now = realNow; console.error = realErr; }, e => { Date.now = realNow; console.error = realErr; process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
