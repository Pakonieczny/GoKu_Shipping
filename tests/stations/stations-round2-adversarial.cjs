// An attempt to BREAK the sign-in side of stations round 2 (ST2), offline: auto sign-out (Rule A idle 10 min, Rule B 17:00 Toronto,
// Admin exempt), two-person Welding (Welding | Matching), the Laser or Design question, hostile input at the station doors, and the
// one Sorting station. The REAL station-session.js / station-activity.js run in jsdom against the REAL doors (firebaseOrders.js with
// _stationActivity.js, _stationLive.js, employeeEfficiency.js and whatever the round-2 workers added) over an in-memory Firestore,
// under a fake two-part clock (a monotonic one that timers use, a wall one that Date.now reads; the wall one can be set backwards or
// forwards, and a computer can sleep). Nothing real is touched: no network, no Firebase, no Etsy, no paid AI, no PIN, no live site.
//   1 · auto sign-out        sleep and resume, background tab, clock backwards / forwards, DST days, a reload mid-idle, two tabs, a relayed
//                            scan as the only input, the Admin lookup (spelling, spaces, offline), two stations at once, the server
//                            ending a session whose page died (end = last input, idempotent, never twice, never an Admin), sandbox
//                            and real apart, midnight New York beside the new rules
//   2 · two-person welding   every sign-in / sign-out order, one person in both tasks, a reload with two sessions, a third person, the
//                            double tap on a chip, the scanner credit rule (one matcher, two, none), offline replay, a crashed page
//   3 · Laser or Design      Admin not asked, role switch races, reload, sign-out clears it, an event written with no role
//   4 · hostile input        PIN-like and hostile names, station keys, tasks and roles (never stored raw, never a PIN), oversized
//                            bodies, replayed and reordered beats
//   5 · the Sorting fold     old sorter and qr events, sessions and rollups still read, nothing counted twice
// A check whose feature a round-2 worker has not pushed yet is listed as PENDING (it runs as soon as the file or the function exists).
// A defect that is known and left is listed as KNOWN (with its id in /mnt/project-files/plans/stations-round2/st2-findings.md); it does
// not fail the run. Everything else failing is a defect.
//   JSDOM_DIR=<...>/node_modules node tests/stations/stations-round2-adversarial.cjs [playwright-core dir]
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');
let JSDOM;
for (const d of [process.env.JSDOM_DIR, process.argv[2], path.join(root, 'node_modules')].filter(Boolean)) { try { ({ JSDOM } = require(path.join(d, 'jsdom'))); break; } catch (_) {} }
if (!JSDOM) { try { ({ JSDOM } = require('jsdom')); } catch (_) { console.error('jsdom not found: set JSDOM_DIR'); process.exit(2); } }
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const exists = f => fs.existsSync(path.join(root, f));

/* ═════════════════════════ fake Firestore: where / orderBy / limit / select, transactions (one at a time), increments, batches, outages ═════════════════════════ */
const INC = n => ({ __inc: n }), TS = { __ts: true };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && v.__inc == null && !v.__ts;
const clone = v => v == null ? v : JSON.parse(JSON.stringify(v));
function apply(prev, data, merge) {
  const out = merge && prev ? clone(prev) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = Date.now();
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge);
    else out[k] = clone(v);
  }
  return out;
}
function store() {
  const colls = new Map(), reads = [], writes = [], failing = new Set();
  let chain = Promise.resolve();
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const down = n => Object.assign(new Error('14 UNAVAILABLE: synthetic outage of ' + n), { code: 14 });
  const notFound = () => Object.assign(new Error('5 NOT_FOUND: No document to update'), { code: 5 });
  const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
  function query(name, filters, order, lim, sel) {
    const q = {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
      limit: n => query(name, filters, order, n, sel),
      select: (...f) => query(name, filters, order, lim, f),
      get: async () => {
        if (failing.has(name)) throw down(name);
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f];
          if (op === 'in') return Array.isArray(v) && v.includes(x);
          if (op === 'array-contains') return Array.isArray(x) && x.includes(v);
          if (op === '!=') return x !== undefined && x !== v;
          if (x === undefined || typeof x !== typeof v) return false;
          return op === '==' ? x === v : op === '>=' ? x >= v : op === '>' ? x > v : op === '<' ? x < v : op === '<=' ? x <= v : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, r) => cmp(p.d[f], r.d[f]) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: Math.max(1, docs.length), filters: filters.map(f => f[0] + f[1]) });
        return { docs: docs.map(({ id, d }) => ({ id, ref: ref(name, id), exists: true, data: () => clone(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length, forEach(fn) { this.docs.forEach(fn); } };
      }
    };
    return q;
  }
  const ref = (name, id) => ({ id, name, path: name + '/' + id,
    get: async () => { reads.push({ name, doc: id, n: 1 }); if (failing.has(name)) throw down(name); const d = data(name).get(id); return { exists: !!d, id, ref: ref(name, id), data: () => clone(d) }; },
    set: async (v, o) => { if (failing.has(name)) throw down(name); writes.push([name, id, 'set']); data(name).set(id, apply(data(name).get(id), v, !!(o && o.merge))); },
    update: async v => { if (failing.has(name)) throw down(name); writes.push([name, id, 'update']); if (!data(name).has(id)) throw notFound(); data(name).set(id, apply(data(name).get(id), v, true)); },
    create: async v => { writes.push([name, id, 'create']); if (data(name).has(id)) throw Object.assign(new Error('6 ALREADY_EXISTS'), { code: 6 }); data(name).set(id, apply(null, v, false)); },
    delete: async () => { writes.push([name, id, 'delete']); data(name).delete(id); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null, null), { doc: id => ref(name, id), add: async v => { const id = 'auto' + data(name).size + Math.random().toString(36).slice(2, 6); await ref(name, id).set(v); return ref(name, id); } }),
    getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
    batch: () => { const q = []; const b = { set: (r, v, o) => (q.push(() => r.set(v, o)), b), update: (r, v) => (q.push(() => r.update(v)), b), delete: r => (q.push(() => r.delete()), b), create: (r, v) => (q.push(() => r.create(v)), b), commit: async () => { for (const f of q) await f(); } }; return b; },
    runTransaction: fn => {                                  // one at a time, like contended Firestore transactions that retry; writes land when the function returns
      const run = async () => {
        const q = [];
        const tx = { get: r => (r.get ? r.get() : r), getAll: (...rs) => Promise.all(rs.filter(x => x && x.get).map(r => r.get())),
          set: (r, v, o) => (q.push(() => r.set(v, o)), tx), update: (r, v) => (q.push(() => r.update(v)), tx), create: (r, v) => (q.push(() => r.create(v)), tx), delete: r => (q.push(() => r.delete()), tx) };
        const out = await fn(tx);
        for (const f of q) await f();
        return out;
      };
      const p = chain.then(run, run); chain = p.then(() => {}, () => {});
      return p;
    }
  };
  return { db, put: (n, id, d) => data(n).set(id, clone(d)), get: (n, id) => clone(data(n).get(id)), all: n => [...data(n)].map(([id, d]) => Object.assign({ _id: id }, clone(d))), colls, count: n => data(n).size,
    fail: n => failing.add(n), heal: n => failing.delete(n), reads, writes, readsOf: n => reads.filter(r => r.name === n), writesOf: n => writes.filter(w => w[0] === n), reset: () => { reads.length = 0; writes.length = 0; },
    dump: () => JSON.stringify([...colls].map(([n, m]) => [n, [...m]])) };
}
let cur = store();
const dbNow = { collection: n => cur.db.collection(n), getAll: (...a) => cur.db.getAll(...a), runTransaction: f => cur.db.runTransaction(f), batch: () => cur.db.batch() };   // (what a door captured at load follows the store of the moment)
const fakeAdmin = { firestore: Object.assign(() => dbNow, { FieldValue: { serverTimestamp: () => TS, increment: INC, delete: () => null, arrayUnion: (...a) => a }, Timestamp: { fromMillis: m => m, now: () => Date.now() } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const tryReq = f => { try { return require(path.join(root, f)); } catch (e) { if (e && e.code === 'MODULE_NOT_FOUND' && String(e.message).includes(f.replace(/^.*\//, ''))) return null; throw e; } };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const LIVE = require(path.join(root, 'netlify/functions/_stationLive.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const KINDS = require(path.join(root, 'netlify/functions/_activityKinds.js'));
const ACT = require(path.join(root, 'netlify/functions/_stationActivity.js'));
const ADMINS = tryReq('netlify/functions/_stationAdmins.js');             // AD2 (null until it is pushed)
Module._load = realLoad;
const PASS = 'synthetic-pass-st2-4q';
process.env.EDIT_PASSCODE = PASS;

/* ═════════════════════════ the clock: monotonic (timers, performance.now) + wall (Date.now), shared by the server and every page ═════════════════════════ */
const REAL_NOW = Date.now.bind(Date);
const T0 = Date.parse('2026-10-07T13:00:00Z');                // 09:00 EDT, Wednesday 7 Oct 2026
const clock = { mono: 0, off: T0, timers: [], seq: 0, tabs: new Set() };
const wall = () => clock.mono + clock.off;
Date.now = wall;
const nextTick = () => new Promise(r => setImmediate(r));
async function settle(n = 6) { for (let i = 0; i < n; i++) await nextTick(); }
const MIN = 60000, HOUR = 3600000;
/** the effective due time of a timer: a hidden tab runs its timers at most once a minute (browsers throttle background tabs) */
const dueOf = t => (t.tab && t.tab.hidden && t.tab.throttle ? Math.ceil(t.due / 60000) * 60000 : t.due);
/** moves the clock forward `ms` of real (monotonic and wall) time, running every due timer in order, the network answering in between */
async function advance(ms) {
  const end = clock.mono + ms;
  for (;;) {
    let best = null;
    for (const t of clock.timers) if (!t.dead && dueOf(t) <= end && (!best || dueOf(t) < dueOf(best) || (dueOf(t) === dueOf(best) && t.id < best.id))) best = t;
    if (!best) break;
    clock.mono = Math.max(clock.mono, dueOf(best));
    if (best.every != null) best.due = clock.mono + best.every; else best.dead = true;
    clock.timers = clock.timers.filter(t => !t.dead);
    try { best.fn(); } catch (_) {}
    await settle(3);
  }
  clock.mono = end;
  await settle(3);
}
/** the computer sleeps: the wall clock runs on, the monotonic one stands still; nothing runs until the next advance (like Linux and macOS) */
const sleepFor = async ms => { clock.off += ms; await settle(2); };
/** the computer is frozen or suspended and every clock jumps; each due timer fires ONCE on wake (like Windows) */
async function freeze(ms) {
  clock.mono += ms;
  for (const t of clock.timers.filter(t => !t.dead && dueOf(t) <= clock.mono)) { if (t.every != null) t.due = clock.mono + t.every; else t.dead = true; try { t.fn(); } catch (_) {} await settle(2); }
  clock.timers = clock.timers.filter(t => !t.dead);
}
/** somebody sets the computer's clock (wall only: timers do not notice) */
const setWall = async t => { clock.off += t - wall(); await settle(1); };
/** jump to a wall time (the server and the pages agree), with a monotonic jump that runs the timers on the way */
async function goTo(t, step = 30000) { while (wall() < t) await advance(Math.min(step, t - wall())); }
const iso = t => new Date(t).toISOString();
const Z = s => Date.parse(s);

/* New York time, with the zone data of this machine (Toronto is the same zone) */
const nyFmt = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hourCycle: 'h23', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
const ny = t => { const o = {}; for (const p of nyFmt.formatToParts(new Date(t))) o[p.type] = p.value; return { y: +o.year, m: +o.month, d: +o.day, h: +o.hour % 24, mi: +o.minute, s: +o.second, day: `${o.year}-${o.month}-${o.day}` }; };
/** the UTC ms of a New York wall time (day "YYYY-MM-DD", h, mi) */
function nyAt(day, h, mi = 0, s = 0) {
  const [y, m, d] = day.split('-').map(Number), wallT = Date.UTC(y, m - 1, d, h, mi, s);
  let u = wallT - 5 * HOUR; for (let i = 0; i < 3; i++) { const p = ny(u); u += wallT - Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi, p.s); }
  return u;
}
const TODAY = ny(T0).day;

/* ═════════════════════════ checks: PASS / FAIL / KNOWN / PENDING ═════════════════════════ */
const results = { pass: 0, fail: [], known: [], pending: [] };
let SECTION = '';
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
async function check(name, fn, o = {}) {
  const was = { mono: clock.mono, off: clock.off };
  try { await fn(); results.pass++; }
  catch (e) {
    const msg = String((e && e.message) || e).split('\n')[0].slice(0, 400);
    if (o.known) results.known.push({ section: SECTION, name, id: o.known, msg });
    else results.fail.push({ section: SECTION, name, msg, stack: String((e && e.stack) || '').split('\n').slice(1, 4).join(' | ') });
  }
  void was;
}
const pending = (what, why) => results.pending.push({ section: SECTION, what, why });
const section = async (title, fn) => { SECTION = title; say(title); try { await fn(); } catch (e) { results.fail.push({ section: title, name: '(section crashed)', msg: String((e && e.message) || e).split('\n')[0], stack: String((e && e.stack) || '').split('\n').slice(1, 4).join(' | ') }); } };
const ok = (c, m) => assert(c, m);
const eq = (a, b, m) => assert.deepStrictEqual(a, b, m);
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
const realConsole = { log: console.log, warn: console.warn, error: console.error, info: console.info };
console.warn = console.log = console.error = console.info = keepLog;

/* ═════════════════════════ the doors, as a station page reaches them ═════════════════════════ */
let ipN = 0;
const ipNext = () => '198.51.100.' + (1 + (++ipN % 240));
async function doorPost(payload, o = {}) {
  const r = await door.handler({ httpMethod: o.method || 'POST', headers: { 'x-nf-client-connection-ip': o.ip || ipNext() }, queryStringParameters: o.sandbox ? { sandbox: '1' } : {}, body: o.raw != null ? o.raw : JSON.stringify(payload) });
  let b = {}; try { b = JSON.parse(r.body || '{}'); } catch (_) { b = { _unparsable: true, raw: r.body }; }
  return { status: r.statusCode, body: b };
}
let sessN = 0;
const SESS = (o = {}) => Object.assign({ id: 'weld-1-ABCD-k' + (++sessN) + 'x' + sessN, event: 'start', person: 'Tess Welder', employeeId: '', station: 'welding', device: 'weld-1', computerId: 'pc-ABCDEFGHJKMN', computerLabel: 'Welding weld-1 · ABCD', at: wall() }, o);
const sess = (o, d) => doorPost({ session: o }, d);
let evN = 0;
const EV = (o = {}) => Object.assign({ id: 'evt-st2-' + (++evN) + '-' + wall(), station: 'welding', device: 'weld-1', person: 'Tess Welder', action: 'scan', orderId: '3521000' + String(100 + evN), parts: 0, orders: 0, at: wall(), seq: evN, sincePrevMs: 1000, detail: '' }, o);
const acts = (list, d) => doorPost({ activity: list }, d);
async function board(body = {}, o = {}) {                       // the portal's reads (manager passcode synthetic)
  const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (1 + (++ipN % 200)) }, body: JSON.stringify(Object.assign({ op: 'live', key: PASS }, body)) }, o.db || cur.db);
  let j = {}; try { j = JSON.parse(r.body || '{}'); } catch (_) { j = { _unparsable: true }; }
  return { status: r.statusCode, body: j, raw: r.body || '' };
}
function fresh() {                                              // a clean shop at the start of Wednesday morning; the clock stays where it is unless said
  cur = store(); EP.resetCache(); try { LIVE._t.seen.clear(); } catch (_) {}
  return cur;
}
const stationDocs = () => cur.all('Station_Sessions');
const sdoc = id => cur.get('Station_Sessions', id);

/* a synthetic "Employee Number" that must never come back out of anything */
const PIN = '482915', PIN2 = '271828';
const STORES = [];                                              // every store of the run: the PIN canary reads them all at the end
const freshKeep = () => { const s = fresh(); STORES.push(s); return s; };
const statOf = o => Object.assign({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0, firstAt: 0, lastAt: 0 }, o);
const rollDoc = (day, who, stations, rids, extra) => Object.assign({ day, person: who, v: 1, events: 10, firstAt: nyAt(day, 9, 5), lastAt: nyAt(day, 14, 50), stations, hours: { '10': { parts: 1, scans: 1, undoParts: 0, by: {} } },
  touched: Object.fromEntries((rids || []).map(r => [r, Object.fromEntries(Object.keys(stations).map(s => [s, true]))])) }, extra || {});
const sessionDoc = (id, who, station, startAt, endAt, extra) => Object.assign({ id, person: who, station, device: station + '-1', computerId: 'pc-' + id.replace(/\W/g, '').slice(0, 8).toUpperCase().padEnd(8, 'X'), computerLabel: '',
  startAt, lastSeenAt: endAt || wall() - 60000, endAt: endAt || null, endReason: endAt ? 'signOut' : null, minutes: 0 }, extra || {});
const ids = (base, n) => Array.from({ length: n }, (_, i) => String(base + i));

/* ═════════════════════════ 4 · hostile input at the station doors ═════════════════════════ */
async function hostile() {
  const REASONS_OK = new Set(['signOut', 'midnight', 'switched', 'closed', 'idle', 'closing']);
  await section('4 · hostile input at the station doors', async () => {
    await check('session door: an odd station key is refused and nothing is stored', async () => {
      freshKeep();
      for (const station of ['__proto__', 'constructor', 'toString', 'hasOwnProperty', 'valueOf', '', ' welding', 'WELDING', 'welding\n', 'welding/../x', 5, null, [], {}, ['welding'], { a: 1 }, true, 'sorter ', 'laser\u0000', 'Design']) {
        const r = await sess(SESS({ station })); ok(r.status === 400, `station ${JSON.stringify(station)}: ${r.status}`);
      }
      eq(stationDocs().length, 0, 'nothing stored for a refused station');
    });
    await check('session door: a task is kept only as exactly welding or matching, and only at the Welding station', async () => {
      freshKeep();
      for (const task of ['__proto__', 'constructor', 'Matching', 'MATCHING', ' matching', 'matching ', 'matching\u0000', ['matching'], { a: 1 }, 5, true, null, 'welding,matching', '"matching"', 'matching/../x', 'x'.repeat(3000)]) {
        const o = SESS({ task }); const r = await sess(o); ok(r.status === 200 || r.status === 413, `task ${JSON.stringify(task).slice(0, 30)}: ${r.status}`);
        const d = sdoc(o.id); if (r.status === 200) ok(d && d.task === undefined, `task ${JSON.stringify(task).slice(0, 30)} stored as ${d && d.task}`);
      }
      for (const task of ['welding', 'matching']) { const o = SESS({ task }); await sess(o); eq(sdoc(o.id).task, task); }
      for (const station of ['assembly', 'sorting', 'design', 'inbox', 'laser']) { const o = SESS({ task: 'matching', station, device: station + '-1' }); await sess(o); ok(sdoc(o.id) && sdoc(o.id).task === undefined, `a task at ${station} is dropped`); }
      ok(!/__proto__|constructor/.test(cur.dump().replace(/"constructor":/g, '')), 'no prototype name was stored');
    });
    await check('session door: a role is kept only as exactly laser or design, never raw', async () => {
      freshKeep();
      for (const role of ['__proto__', 'constructor', 'Laser', 'LASER', ' design', 'design ', ['laser'], { a: 1 }, 5, true, null, 'laser\u0000', 'admin', 'x'.repeat(2000)]) {
        const o = SESS({ role, station: 'laser', device: 'charm-nest-1' }); const r = await sess(o); ok(r.status === 200 || r.status === 413, `role ${JSON.stringify(role).slice(0, 20)}: ${r.status}`);
        const d = sdoc(o.id); if (r.status === 200) ok(d && (d.role === undefined || d.role === 'laser' || d.role === 'design'), `role ${JSON.stringify(role).slice(0, 20)} stored as ${d && d.role}`);
      }
    });
    await check('session door: an end reason is one of the known words, never raw', async () => {
      freshKeep();
      for (const reason of ['__proto__', 'constructor', 'IDLE', ' idle', 'idle\n', 'x'.repeat(2000), 5, {}, [], null, 'midnight; drop', 'Closing', '17:00']) {
        const o = SESS(); await sess(o); tickWall(1000);
        const r = await sess({ id: o.id, event: 'end', person: o.person, station: o.station, computerId: o.computerId, reason, at: wall() }); ok(r.status === 200 || r.status === 413, `reason ${JSON.stringify(reason).slice(0, 20)}: ${r.status}`);
        const d = sdoc(o.id); if (r.status === 200) ok(d.endReason == null || REASONS_OK.has(d.endReason), `reason ${JSON.stringify(reason).slice(0, 20)} stored as ${d.endReason}`);
      }
    });
    await check('session door: names, ids, computers and devices that are PINs or paths never become documents or numbers', async () => {
      freshKeep();
      for (const person of [PIN, `Paul ${PIN}`, `${PIN}Paul`, 'Pa 48 29 15', '   ', '', '\u0000\u0001', 'a/b/c', '../../etc', '__proto__', 'x'.repeat(5000), '<img src=x onerror=alert(1)>', 'Paul‮K', 'Ünïcödé 名前 🙂']) {
        const o = SESS({ person }); const r = await sess(o); ok(r.status === 200 || r.status === 400 || r.status === 413, `person ${JSON.stringify(person).slice(0, 24)}: ${r.status}`);
      }
      for (const id of ['', 'x', '../x', 'a/b/c/d/e/f', '__x__', '.', '..', 'a'.repeat(300), '1234567', 5, null, {}, ['abcdefgh']]) { const r = await sess(SESS({ id })); ok(r.status === 400, `id ${JSON.stringify(id).slice(0, 20)}: ${r.status}`); }
      for (const computerId of ['', 'x', '../../x', 'a b c d e f g', 'a'.repeat(200), 5, null, {}]) { const r = await sess(SESS({ computerId })); ok(r.status === 400, `computerId ${JSON.stringify(computerId).slice(0, 20)}: ${r.status}`); }
      const dv = SESS({ device: '../../etc/passwd\u0000<b>' }); await sess(dv); ok(/^[\w .:-]*$/.test(sdoc(dv.id).device), 'device is cleaned: ' + sdoc(dv.id).device);
      for (const d of stationDocs()) { ok(!/\d{4}/.test(d.person) && /\p{L}/u.test(d.person), `stored person ${JSON.stringify(d.person)}`); ok(!d.employeeId || !/^\d+$/.test(d.employeeId), 'no digits-only employee id stored'); }
      ok(!cur.dump().includes(PIN), 'the PIN is nowhere in the store');
    });
    await check('every door: a body that is not an object is a 4xx, never a 5xx', async () => {
      freshKeep(); const bad = [];
      for (const raw of ['null', '[]', '"x"', '12', 'true', '{', '', '{"session":null}', '{"session":[]}', '{"session":"x"}', '{"session":5}', '{"activity":{}}', '{"activity":"x"}', '{"activity":[null,5,"x",[]]}', '{"live":[]}', '{"live":"x"}', '{"live":5}', '{"timeline":{}}', '{"timeline":[null]}', '{"pinLogin":{}}', '{"pinLogin":[]}', '{"pinLogin":null}', '{"__proto__":{"x":1}}', '{"constructor":{"prototype":{"y":1}}}']) {
        const r = await doorPost(null, { raw }); if (r.status >= 500) bad.push(`${JSON.stringify(raw).slice(0, 40)} -> ${r.status} ${JSON.stringify(r.body).slice(0, 80)}`);
      }
      ok(!bad.length, bad.join(' ; '));
      ok(({}).x === undefined && ({}).y === undefined, 'Object.prototype was not polluted');
    });
    await check('oversized bodies: a session over 4096 characters, 51 events, a 40,000-character batch and an 8,000-character live event are refused whole', async () => {
      freshKeep();
      let r = await sess(SESS({ computerLabel: 'x'.repeat(5000) })); eq(r.status, 413);
      eq(stationDocs().length, 0);
      r = await acts(Array.from({ length: 51 }, () => EV())); eq(r.status, 413);
      r = await doorPost({ activity: [EV({ detail: 'y'.repeat(41000) })] }); eq(r.status, 413);
      r = await doorPost({ live: { v: 1, event: 'work', station: 'welding', device: 'weld-1', person: 'Tess Welder', order: { kind: 'order', rid: '3521000001', note: 'z'.repeat(9000) } } }); eq(r.status, 413);
      r = await acts([EV({ detail: 'q'.repeat(700) })]); ok(r.status === 200 && r.body.written === 0 && r.body.refused === 1, 'a 700-byte event is refused, the batch is not: ' + JSON.stringify(r.body));
      eq(cur.count('Station_Activity'), 0);
    });
    await check('activity door: station, action, task, role and the unattributed flag are validated; a valid batch beside a bad event still lands', async () => {
      freshKeep();
      const good = EV({ action: 'matched', task: 'matching', detail: 'phone scan' });
      const bad = [EV({ station: '__proto__' }), EV({ station: 'constructor' }), EV({ action: '__proto__' }), EV({ action: 'MATCHED' }), EV({ person: PIN }), EV({ person: '' }), EV({ id: 'x' }), 5, null, 'x', [], EV({ at: wall() - 8 * 86400000 })];
      const r = await acts(bad.concat([good])); ok(r.status === 200 && r.body.written === 1 && r.body.refused === 12, 'one good event among bad ones: ' + JSON.stringify(r.body));
      const stored = cur.all('Station_Activity'); eq(stored.length, 1); eq(stored[0].task, 'matching'); eq(stored[0].action, 'matched');
      for (const at of [1, -5, 'now', wall() + 3 * HOUR, 1e18]) { const e = EV({ at }); await acts([e]); const d = cur.get('Station_Activity', e.id); ok(d && d.at <= wall() && d.at > 1e12, `at ${at} is clamped to now, never the future: ${d && d.at}`); }
      for (const task of ['__proto__', 'Matching', ['matching'], { a: 1 }, 5, 'x'.repeat(500), null]) { const e = EV({ task }); await acts([e]); const d = cur.get('Station_Activity', e.id); ok(!d || d.task === undefined, `task ${JSON.stringify(task).slice(0, 20)} stored as ${d && d.task}`); }
      for (const role of ['__proto__', 'Laser', 'admin', ['laser'], { a: 1 }, 5, 'x'.repeat(500)]) { const e = EV({ role, station: 'laser', device: 'charm-nest-1' }); await acts([e]); const d = cur.get('Station_Activity', e.id); ok(!d || d.role === undefined, `role ${JSON.stringify(role).slice(0, 20)} stored as ${d && d.role}`); }
      for (const role of ['laser', 'design']) { const e = EV({ role, station: role, device: 'charm-nest-1', action: 'complete', parts: 1, orders: 1 }); await acts([e]); eq(cur.get('Station_Activity', e.id).role, role); }
      // no role at all: an event with no role is stored with no role (never a guess)
      const nr = EV({ station: 'laser', device: 'charm-nest-1', action: 'complete', parts: 1, orders: 1 }); await acts([nr]); eq(cur.get('Station_Activity', nr.id).role, undefined, 'an event written with no role has none');
    });
    await check('activity door: a scan with nobody in Matching is stored as Unattributed (never as a welder), and only at the Welding station', async () => {
      freshKeep();
      const a = EV({ action: 'matched', task: 'matching', unattributed: true, person: '' }), b = EV({ action: 'matched', task: 'matching', unattributed: true, person: 'Tess Welder' }), c = EV({ action: 'matched', unattributed: true, person: PIN });
      const r = await acts([a, b, c]); eq(r.body.written, 3);
      for (const e of [a, b, c]) { const d = cur.get('Station_Activity', e.id); eq(d.person, 'Unattributed'); eq(d.unattributed, true); eq(d.task, 'matching'); }
      ok(cur.get('Efficiency_Daily', `${TODAY}__Unattributed`) && !cur.get('Efficiency_Daily', `${TODAY}__Tess Welder`), 'the rollup is the Unattributed one, never the welder\'s');
      const d = EV({ station: 'assembly', device: 'assembly-1', action: 'scan', unattributed: true, person: 'Ray Welder' }); await acts([d]);
      const x = cur.get('Station_Activity', d.id); ok(x && x.person === 'Ray Welder' && x.unattributed === undefined, 'the unattributed flag at another station is ignored, the named person stays: ' + JSON.stringify(x && [x.person, x.unattributed]));
    });
    await check('live door: odd stations, devices and people are refused or cleaned, a PIN in a name never reaches a document id', async () => {
      freshKeep();
      const L = o => ({ v: 1, event: 'work', station: 'welding', device: 'weld-1', person: 'Tess Welder', order: { kind: 'order', rid: '3521000777', scannedAt: wall() }, ...o });
      for (const station of ['__proto__', 'constructor', 5, null, '', 'WELDING']) { const r = await doorPost({ live: L({ station }) }); ok(r.status === 400, `station ${JSON.stringify(station)}: ${r.status}`); }
      for (const person of [PIN, '   ', 'a/b', '__x__', '..']) { const r = await doorPost({ live: L({ person }) }); ok(r.status === 400 || r.status === 200, `person ${JSON.stringify(person)}: ${r.status}`); }
      for (const d of cur.all('Station_Live')) ok(!/\d{4}/.test(d._id) && !d._id.includes('/'), 'a live document id: ' + d._id);
      const dv = await doorPost({ live: L({ device: '../../x\u0000' }) }); ok(dv.status === 200 || dv.status === 400, 'a path as a device: ' + dv.status);
    });
    await check('a replayed, reordered or foreign beat changes nothing it must not: identity, start, end, computer', async () => {
      freshKeep();
      const o = SESS({ task: 'matching' }); let r = await sess(o); eq(r.status, 200);
      const d0 = sdoc(o.id);
      tickWall(60000);
      r = await sess({ id: o.id, event: 'beat', person: 'Mallory', station: 'sorting', device: 'sorting-1', task: 'welding', role: 'design', employeeId: 'x', computerId: o.computerId, at: 1 });
      const d1 = sdoc(o.id);
      ok(d1.person === d0.person && d1.station === d0.station && d1.device === d0.device && d1.task === d0.task && d1.startAt === d0.startAt && d1.role === d0.role, 'a beat cannot change who, where, which task, or the start: ' + JSON.stringify(d1));
      eq((await sess({ id: o.id, event: 'beat', computerId: 'pc-OTHERCOMPUTR', person: o.person, station: o.station })).status, 409, 'another computer cannot beat it');
      eq((await sess({ id: o.id, event: 'end', computerId: 'pc-OTHERCOMPUTR', person: o.person, station: o.station })).status, 409, 'another computer cannot end it');
      // a start replayed after the end does not bring it back; an end replayed does nothing; a late beat after the end does nothing
      tickWall(60000);
      r = await sess({ id: o.id, event: 'end', person: o.person, station: o.station, computerId: o.computerId, reason: 'signOut', at: wall() }); eq(r.body.ended, true);
      const dEnd = sdoc(o.id);
      for (const ev of ['start', 'beat', 'end', 'end', 'start']) { tickWall(5000); const rr = await sess(Object.assign({}, o, { event: ev, reason: 'idle', at: wall() })); eq(rr.body.ended, true, ev + ' after the end'); }
      eq(sdoc(o.id).endAt, dEnd.endAt, 'the end time never moves'); eq(sdoc(o.id).endReason, dEnd.endReason, 'the end reason never changes');
      // the end arriving BEFORE the start (the start request was slow): the later start must not leave a session that lives forever
      const q = SESS(); r = await sess({ id: q.id, event: 'end', person: q.person, station: q.station, computerId: q.computerId, reason: 'signOut', at: wall() }); ok(r.status === 200, 'an end with no start: ' + r.status);
      r = await sess(q); ok(r.status === 200, 'then the late start');
      const g = sdoc(q.id); ok(g, 'the late start made a document');
      tickWall(30 * MIN);
      r = await sess(Object.assign({}, q, { event: 'beat' }));
      ok(sdoc(q.id).endAt != null, 'a session whose end arrived first is closed by the next beat after 15 quiet minutes (not open forever): ' + JSON.stringify(sdoc(q.id)));
    });
  });
}

/* the server's wall clock and every page's, forward together (no timers: the doors do not run any) */
function tickWall(ms) { clock.mono += ms; }

/* ═════════════════════════ runner ═════════════════════════ */
(async () => {
  const t0 = REAL_NOW();
  await hostile();
  // the PIN canary: nothing stored, logged or sent anywhere in the run carries a synthetic Employee Number
  await section('0 · the PIN canary', async () => {
    await check('no synthetic PIN in any store or log of the run', async () => {
      for (const s of STORES) ok(!s.dump().includes(PIN) && !s.dump().includes(PIN2), 'a PIN is stored');
      ok(!logs.join('\n').includes(PIN) && !logs.join('\n').includes(PIN2), 'a PIN is in a log line');
    });
  });
  Date.now = REAL_NOW;
  const out = realConsole.log;
  out(`\n${results.pass} checks passed, ${results.fail.length} failed, ${results.known.length} known defects left, ${results.pending.length} pending  (${((REAL_NOW() - t0) / 1000).toFixed(1)} s)`);
  for (const p of results.pending) out(`  PENDING [${p.section}] ${p.what}: ${p.why}`);
  for (const k of results.known) out(`  KNOWN ${k.id} [${k.section}] ${k.name}: ${k.msg}`);
  for (const f of results.fail) out(`  FAIL [${f.section}] ${f.name}\n      ${f.msg}\n      ${f.stack}`);
  process.exit(results.fail.length ? 1 : 0);
})().catch(e => { realConsole.error(e); process.exit(1); });
