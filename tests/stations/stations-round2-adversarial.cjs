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
const AUTO = tryReq('netlify/functions/_stationAutoSignout.js');
const SWEEPFN = tryReq('netlify/functions/stationSessionsSweepCron.js');
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
  if (process.env.ONLY && !name.includes(process.env.ONLY)) return;                  // ONLY="text" runs the checks whose name has that text (debugging)
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

/* ═════════════════════════ pages: jsdom + the real scripts, one storage per computer, the real doors behind a fake network ═════════════════════════ */
const SCRIPTS = { session: process.env.ST2_SESSION_JS ? fs.readFileSync(process.env.ST2_SESSION_JS, 'utf8') : read('station-session.js'), activity: read('station-activity.js'), queue: read('station-scan-queue.js') };
const extraScript = f => (exists(f) ? read(f) : null);
let pcN = 0;
/** a clean world at a chosen moment: every page gone, no timers, an empty shop, the wall clock at `start` */
function world(start = '2026-10-07T13:00:00Z') {
  for (const t of [...clock.tabs]) t.kill();
  clock.timers = []; clock.off = Date.parse(start) - clock.mono;
  return freshKeep();
}
/** a computer: one browser profile (localStorage shared by its tabs), one address on the network */
function computer(label) {
  const mem = new Map(), tabs = new Set();
  const pc = { label, mem, tabs, ip: '10.' + ((++pcN >> 8) & 255) + '.' + (pcN & 255) + '.7', ls: k => (mem.has(k) ? mem.get(k) : null), set: (k, v) => mem.set(k, String(v)), del: k => mem.delete(k) };
  pc.storage = tab => ({
    getItem: k => (mem.has(String(k)) ? mem.get(String(k)) : null),
    setItem(k, v) { k = String(k); const old = mem.has(k) ? mem.get(k) : null; mem.set(k, String(v)); pc.fire(tab, k, old, String(v)); },
    removeItem(k) { k = String(k); const old = mem.has(k) ? mem.get(k) : null; if (mem.delete(k)) pc.fire(tab, k, old, null); },
    clear() { mem.clear(); }, key: i => [...mem.keys()][i], get length() { return mem.size; } });
  pc.fire = (src, key, oldValue, newValue) => {            // a storage event reaches the OTHER tabs of the computer, a moment later
    for (const t of tabs) if (t !== src && !t.dead) Promise.resolve().then(() => { try { t.w.dispatchEvent(new t.w.StorageEvent('storage', { key, oldValue, newValue, url: t.w.location.href })); } catch (_) {} });
  };
  return pc;
}
const FAIL_FETCH = () => Object.assign(new TypeError('Failed to fetch'), {});
/** opens a page on a computer. o: { page, station, device, multi, people, person, signOut, sandbox, scripts, init(w, tab) } */
function openTab(pc, o = {}) {
  const page = o.page || 'weld-1.html';
  const dom = new JSDOM('<!doctype html><html><head><meta charset="utf-8"></head><body><div id="host"></div></body></html>', { url: 'https://goldenspike.app/' + page + (o.sandbox ? '?sandbox=1' : ''), pretendToBeVisual: true, runScripts: 'outside-only' });
  const w = dom.window;
  const tab = { pc, w, dom, hidden: false, throttle: o.throttle !== false, skew: o.skew || 0, dead: false, online: true, forceStatus: null, hold: null, parked: [], signOuts: [], reqs: [], sessions: [], acts: [], errors: [], ip: o.ip || pc.ip, page };
  pc.tabs.add(tab); clock.tabs.add(tab);
  Object.defineProperty(w, 'localStorage', { value: pc.storage(tab), configurable: true });
  Object.defineProperty(w.document, 'visibilityState', { get: () => (tab.hidden ? 'hidden' : 'visible'), configurable: true });
  Object.defineProperty(w.document, 'hidden', { get: () => tab.hidden, configurable: true });
  Object.defineProperty(w.navigator, 'onLine', { get: () => tab.online, configurable: true });
  const D = w.Date;
  class FD extends D { constructor(...a) { if (a.length === 0) super(wall() + tab.skew); else super(...a); } static now() { return wall() + tab.skew; } }
  w.Date = FD;
  w.performance = { now: () => clock.mono, timeOrigin: 0 };           // (the monotonic clock: a page can tell a clock that was set from time that passed)
  const addTimer = (fn, ms, every, args) => { const t = { id: ++clock.seq, tab, fn: () => fn(...args), due: clock.mono + Math.max(0, Number(ms) || 0), every }; clock.timers.push(t); return t.id; };
  w.setTimeout = (fn, ms, ...a) => addTimer(typeof fn === 'function' ? fn : () => {}, ms, null, a);
  w.setInterval = (fn, ms, ...a) => addTimer(typeof fn === 'function' ? fn : () => {}, Math.max(1, Number(ms) || 1), Math.max(1, Number(ms) || 1), a);
  w.clearTimeout = w.clearInterval = id => { for (const t of clock.timers) if (t.id === id) t.dead = true; };
  w.TextEncoder = TextEncoder; w.Blob = function (parts) { this.parts = parts; };
  w.fetch = async (url, init) => {
    const u = new URL(String(url), w.location.href), text = init && init.body ? String(init.body) : '';
    let body = null; try { body = text ? JSON.parse(text) : null; } catch (_) {}
    const rec = { url: u.pathname + u.search, method: (init && init.method) || 'GET', body, at: wall(), pageAt: wall() + tab.skew, status: null };
    tab.reqs.push(rec);
    if (body && body.session) tab.sessions.push(body.session);
    if (body && Array.isArray(body.activity)) tab.acts.push(...body.activity);
    if (!tab.online) { rec.status = 'offline'; throw FAIL_FETCH(); }
    const deliver = async () => {
      const forced = tab.forceStatus || (tab.failIf && tab.failIf(rec));          // failIf(rec) -> a status to answer that ONE request with (the Admin lookup, say), or falsy
      if (forced) { rec.status = forced; return { status: forced, ok: false, json: async () => ({}), text: async () => '{}' }; }
      if (u.pathname === '/.netlify/functions/firebaseOrders') {
        const r = await door.handler({ httpMethod: rec.method, headers: { 'x-nf-client-connection-ip': tab.ip }, queryStringParameters: Object.fromEntries(u.searchParams), body: text || undefined });
        rec.status = r.statusCode; rec.reply = r.body;
        return { status: r.statusCode, ok: r.statusCode < 400, json: async () => JSON.parse(r.body || '{}'), text: async () => r.body || '' };
      }
      const custom = o.route && await o.route(u, rec, tab);
      if (custom) { rec.status = custom.status; return { status: custom.status, ok: custom.status < 400, json: async () => custom.body, text: async () => JSON.stringify(custom.body) }; }
      rec.status = 404; return { status: 404, ok: false, json: async () => ({}), text: async () => '{}' };
    };
    if (tab.hold && tab.hold(rec)) return new Promise(res => tab.parked.push({ rec, go: () => deliver().then(res) }));
    return deliver();
  };
  tab.release = async (order) => { const list = tab.parked.splice(0); const seq = order ? order.map(i => list[i]) : list; for (const p of seq) { await p.go(); await settle(2); } };
  w.addEventListener('error', e => tab.errors.push(String(e && e.message)));
  for (const f of o.scripts || ['session', 'activity']) w.eval(SCRIPTS[f] || read(f));
  tab.SS = w.StationSession;
  const page_ = {
    station: o.station || 'welding', device: o.device || 'weld-1',
    ...(o.multi ? { multi: true, people: o.people || (() => { try { return JSON.parse(pc.ls('fx_people') || '[]'); } catch (_) { return []; } }),
      signOut: (reason, who) => { tab.signOuts.push([reason, who]); const l = JSON.parse(pc.ls('fx_people') || '[]'); tab.storage().setItem('fx_people', JSON.stringify(l.filter(p => !(who && p.name === who.name && (p.task || '') === (who.task || ''))))); if (o.signOut) o.signOut(reason, who, tab); } }
      : { person: o.person || (() => { const n = pc.ls('employee_name'); return pc.ls('employee_id') && n ? { name: n, id: null } : null; }),
          signOut: reason => { tab.signOuts.push(reason); tab.storage().removeItem('employee_id'); tab.storage().removeItem('employee_name'); if (o.signOut) o.signOut(reason, null, tab); } }),
    sandbox: !!o.sandbox };
  tab.storage = () => w.localStorage;
  tab.init = extra => { try { w.StationSession.init(Object.assign({}, page_, o.initExtra || {}, extra || {})); } catch (e) { tab.errors.push('init: ' + e.message); } };
  if (o.init !== false) tab.init();
  /* what a person does on the page */
  tab.login = (name, task) => {                              // the page's own sign-in (its keys), then StationSession.signedIn
    if (o.multi) { const l = JSON.parse(pc.ls('fx_people') || '[]'); l.push({ name, task: task || '' }); tab.storage().setItem('fx_people', JSON.stringify(l)); w.StationSession.signedIn({ name, id: null, task: task || '' }); }
    else { tab.storage().setItem('employee_id', 'x-' + name.length); tab.storage().setItem('employee_name', name); w.StationSession.signedIn({ name, id: null }); }
  };
  tab.logout = (name, task) => {
    if (o.multi) { const l = JSON.parse(pc.ls('fx_people') || '[]'); tab.storage().setItem('fx_people', JSON.stringify(l.filter(p => !(p.name === name && (p.task || '') === (task || ''))))); w.StationSession.signedOut('signOut', { name, task: task || '' }); }
    else { tab.storage().removeItem('employee_id'); tab.storage().removeItem('employee_name'); w.StationSession.signedOut('signOut'); }
  };
  tab.who = () => (o.multi ? JSON.parse(pc.ls('fx_people') || '[]') : (pc.ls('employee_id') && pc.ls('employee_name') ? [{ name: pc.ls('employee_name') }] : []));
  tab.input = (type = 'pointerdown') => { const E = /^key/.test(type) ? w.KeyboardEvent : /^(mouse|click|pointer|wheel)/.test(type) ? w.MouseEvent : w.Event; w.document.body.dispatchEvent(new E(type, { bubbles: true, cancelable: true })); };
  tab.hide = () => { tab.hidden = true; w.document.dispatchEvent(new w.Event('visibilitychange')); };
  tab.show = () => { tab.hidden = false; w.document.dispatchEvent(new w.Event('visibilitychange')); w.dispatchEvent(new w.Event('focus')); };
  tab.setOnline = on => { tab.online = on; w.dispatchEvent(new w.Event(on ? 'online' : 'offline')); };
  /** the page is closed or navigated away: pagehide, then its timers are gone */
  tab.close = () => { try { w.dispatchEvent(new w.Event('pagehide')); } catch (_) {} tab.kill(); };
  /** the page crashes or the computer loses power: nothing is sent, nothing runs again */
  tab.kill = () => { tab.dead = true; for (const t of clock.timers) if (t.tab === tab) t.dead = true; clock.timers = clock.timers.filter(t => !t.dead); pc.tabs.delete(tab); clock.tabs.delete(tab); };
  /** the session bodies of one kind (start | beat | end) of this page, newest last */
  tab.kind = k => tab.sessions.filter(s => s.event === k);
  return tab;
}
/** the sessions of the store, one line each: person, station, task, role, start, end, reason (a quick read for a failing check) */
const dumpSessions = () => stationDocs().map(d => `${d.person}@${d.station}/${d.device}${d.task ? ':' + d.task : ''}${d.role ? '[' + d.role + ']' : ''} ${iso(d.startAt).slice(11, 19)}-${d.endAt ? iso(d.endAt).slice(11, 19) : 'open'} ${d.endReason || ''}`).join(' | ');
const open_ = () => stationDocs().filter(d => d.endAt == null);
const endedAt = (name, t0) => stationDocs().filter(d => d.person === name && d.endAt != null && (t0 == null || d.startAt >= t0));

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
      // the end arriving BEFORE the start (the start request was slow, or never came): recorded at once as ended; the late start finds it ended and never reopens it
      const q = SESS(); r = await sess({ id: q.id, event: 'end', person: q.person, station: q.station, computerId: q.computerId, reason: 'signOut', at: wall() }); ok(r.status === 200 && r.body.ended === true, 'an end with no start is recorded: ' + JSON.stringify(r.body));
      const g0 = sdoc(q.id); ok(g0 && g0.endAt != null && g0.endReason === 'signOut' && g0.minutes === 0, 'recorded as a session that ended: ' + JSON.stringify(g0));
      r = await sess(q); ok(r.status === 200 && r.body.ended === true, 'the late start finds it ended: ' + JSON.stringify(r.body)); eq(sdoc(q.id).endAt, g0.endAt, 'and does not reopen it');
      tickWall(30 * MIN); r = await sess(Object.assign({}, q, { event: 'beat' })); eq(r.body.ended, true); eq(sdoc(q.id).endAt, g0.endAt, 'a later beat changes nothing'); eq(open_().filter(d => d._id === q.id).length, 0, 'not open');
      // an end with no usable name stores nothing (a PIN is not a name), and another computer cannot take over the id afterwards by ending it first... only the first writer owns it, as for a start
      for (const person of ['', '   ', PIN, '12 34 56']) { const n = SESS({ person }); r = await sess(Object.assign({}, n, { event: 'end', reason: 'signOut' })); eq(r.status, 200); ok(!sdoc(n.id), `an end with the name ${JSON.stringify(person)} stores nothing`); }
      const t = SESS({ task: 'matching' }); r = await sess(Object.assign({}, t, { event: 'end', reason: 'idle', at: wall() - 5 * MIN })); ok(sdoc(t.id) && sdoc(t.id).task === 'matching' && sdoc(t.id).endReason === 'idle', 'task and reason kept: ' + JSON.stringify(sdoc(t.id)));
      ok(sdoc(t.id).endAt <= wall() && sdoc(t.id).endAt >= wall() - 6 * MIN, 'the end is the time the page names (not later than now)');
      const far = SESS(); await sess(Object.assign({}, far, { event: 'end', at: wall() + 5 * HOUR })); ok(sdoc(far.id).endAt <= wall(), 'an end in the future is clamped to now');
    });
  });
}

/* the server's wall clock and every page's, forward together (no timers: the doors do not run any) */
function tickWall(ms) { clock.mono += ms; }

/* ═════════════════════════ 2 · two-person welding ═════════════════════════ */
const DBG = process.env.ST2_DEBUG ? (...a) => realConsole.log('[dbg]', ...a.map(x => typeof x === 'string' ? x : JSON.stringify(x))) : () => {};
async function welding() {
  const A = { name: 'Tess Welder', task: 'welding' }, B = { name: 'Ray Welder', task: 'matching' }, C = { name: 'Ivy Third', task: 'matching' };
  const mk = (pc, extra) => openTab(pc, Object.assign({ multi: true, station: 'welding', device: 'weld-1' }, extra || {}));
  const key = x => x.name + '|' + (x.task || '');
  const docsOf = (p, t) => stationDocs().filter(d => d.person === p.name && (d.task || '') === (t || p.task || ''));
  const peopleOf = tab => Array.from(tab.SS.people()).map(p => key(p)).sort();
  await section('2 · two-person welding', async () => {
    await check('every order of signing in and out: each person has a session of their own and nobody else is touched', async () => {
      const plans = [['+A', '+B', '-A', '-B'], ['+A', '+B', '-B', '-A'], ['+A', '-A', '+B', '-B'], ['+B', '+A', '-B', '-A'], ['+A', '+B', '-A', '+A', '-B', '-A'], ['+A', '+B', '+B', '+A', '-A', '-A', '-B', '-B'], ['+B', '-B', '+B', '-B', '+A', '-A']];
      for (const plan of plans) {
        world(); const pc = computer('bench'), tab = mk(pc);
        const model = new Map(); const started = []; let ended = 0;
        for (const step of plan) {
          const who = step[1] === 'A' ? A : B, k = key(who);
          await advance(7 * MIN);                                          // beats happen between the steps
          if (step[0] === '+') { if (!model.has(k)) { model.set(k, true); started.push(k); } tab.login(who.name, who.task); }
          else { if (model.has(k)) { model.delete(k); ended++; } tab.logout(who.name, who.task); }
          await advance(1000);
          eq(peopleOf(tab), [...model.keys()].sort(), `people after ${plan.join(' ')} at ${step}`);
          const open = open_().map(d => d.person + '|' + d.task).sort();
          eq(open, [...model.keys()].sort(), `open sessions after ${step} of ${plan.join(' ')}: ${dumpSessions()}`);
          eq(stationDocs().filter(d => d.endAt != null).length, ended, `ended sessions after ${step} of ${plan.join(' ')}`);
        }
        ok(stationDocs().length === started.length, `one document per sign-in: ${stationDocs().length} vs ${started.length}`);
        for (const d of stationDocs()) { ok(d.endReason === 'signOut' && d.endAt >= d.startAt, `ended cleanly: ${dumpSessions()}`); ok(/^welding__weld-1__\w+__(welding|matching)__/.test(d._id), 'session id: ' + d._id); }
        ok(!tab.sessions.some(s => s.event === 'end' && s.reason === 'switched'), 'a second sign-in never ends the first as "switched"');
        eq(tab.errors, [], 'no page errors');
      }
    });
    await check('the same person in both tasks is two sessions; leaving one leaves the other; leaving with no task leaves both', async () => {
      world(); const pc = computer('bench'), tab = mk(pc);
      tab.login(A.name, 'welding'); await advance(MIN); tab.login(A.name, 'matching'); await advance(MIN);
      eq(open_().length, 2); eq(new Set(stationDocs().map(d => d._id)).size, 2);
      tab.logout(A.name, 'welding'); await advance(1000);
      eq(open_().map(d => d.task), ['matching'], dumpSessions());
      tab.login(A.name, 'welding'); await advance(1000); eq(open_().length, 2);
      tab.SS.signedOut('signOut', { name: A.name });                                           // no task: every task of that name
      await advance(1000); eq(open_().length, 0, dumpSessions());
      ok(stationDocs().length === 3, 'three sign-ins, three documents: ' + stationDocs().length);
      // spelled another way it is still the same person and task: no second session
      world(); const pc2 = computer('bench'), t2 = mk(pc2);
      t2.login('Tess Welder', 'matching'); await advance(1000);
      for (const v of ['tess welder', 'TESS  WELDER', ' Tess Welder ']) { t2.SS.signedIn({ name: v, task: 'matching' }); await advance(500); }
      eq(stationDocs().length, 1, 'a respelling is the same person: ' + dumpSessions());
    });
    await check('a double tap on the sign-in or the sign-out never writes twice or ends somebody else', async () => {
      world(); const pc = computer('bench'), tab = mk(pc);
      tab.login(A.name, A.task); tab.login(B.name, B.task); await advance(1000);
      tab.SS.signedIn({ name: A.name, task: A.task }); tab.SS.signedIn({ name: A.name, task: A.task }); await advance(1000);
      eq(stationDocs().length, 2, 'two sign-ins, two documents: ' + dumpSessions());
      tab.logout(A.name, A.task); tab.SS.signedOut('signOut', { name: A.name, task: A.task }); tab.SS.signedOut('signOut', { name: A.name, task: A.task }); await advance(1000);
      eq(open_().map(d => d.person), [B.name], 'only Tess is out: ' + dumpSessions());
      eq(tab.kind('end').length, 1, 'one end sent for one person');
      eq(peopleOf(tab), [key(B)]);
    });
    await check('a reload with two sessions goes on with both: no second start, same ids, beats go on', async () => {
      world(); const pc = computer('bench'); let tab = mk(pc);
      tab.login(A.name, A.task); tab.login(B.name, B.task); await advance(6 * MIN);
      const before = stationDocs().map(d => d._id).sort();
      tab.close(); await advance(20000);
      tab = mk(pc); await advance(2000);
      eq(stationDocs().map(d => d._id).sort(), before, 'the same two sessions, none new: ' + dumpSessions());
      eq(open_().length, 2); eq(peopleOf(tab), [key(A), key(B)].sort());
      eq(tab.kind('start').length, 0, 'the reloaded page starts nothing');
      await advance(6 * MIN); ok(tab.kind('beat').length >= 2, 'both sessions beat again after the reload');
      tab.close();
    });
    await check('a page that crashes with two people signed in: both are closed at their last beat, never later', async () => {
      world(); const pc = computer('bench'); let tab = mk(pc);
      tab.login(A.name, A.task); tab.login(B.name, B.task); await advance(12 * MIN);
      const killed = wall(); tab.kill();
      await advance(40 * MIN);
      tab = mk(pc); await advance(2000);
      const old = stationDocs().filter(d => d.startAt < killed);
      eq(old.length, 2); for (const d of old) { ok(d.endAt != null && d.endAt <= killed + 1000 && d.endAt >= killed - 5.5 * MIN, `ended near the crash: ${dumpSessions()} (crash ${iso(killed).slice(11, 19)})`); ok(['closed', 'signOut', 'idle'].includes(d.endReason), 'reason ' + d.endReason); }
      tab.close();
    });
    await check('midnight ends everybody once, calls the page once per person, and the next morning signs in fresh', async () => {
      world('2026-10-08T03:50:00Z');                                         // 23:50 EDT
      const pc = computer('bench'), tab = mk(pc);
      tab.login(A.name, A.task); tab.login(B.name, B.task); tab.login(A.name, 'matching'); await goTo(Z('2026-10-08T04:05:00Z'));
      eq(open_().length, 0, dumpSessions());
      for (const d of stationDocs()) { eq(d.endReason, 'midnight'); eq(d.endAt, Z('2026-10-08T04:00:00Z'), 'ends at midnight'); }
      eq(tab.signOuts.filter(x => x[0] === 'midnight').length, 3, 'once per person and task: ' + JSON.stringify(tab.signOuts));
      eq(peopleOf(tab), []);
      tab.login(A.name, A.task); await advance(1000); eq(open_().length, 1);
    });
    await check('the board: two people at Welding under two tasks, time on task per task, matched scans counted once, a scan with nobody in Matching never credited, no pieces or orders for Welding', async () => {
      const st = world('2026-10-07T15:00:00Z');                          // 11:00 in New York
      const pc = computer('bench'), tab = mk(pc);
      tab.login(A.name, 'welding'); await advance(30 * MIN); tab.login(B.name, 'matching'); await advance(10 * MIN);
      const scan = (who, rid, extra) => EV(Object.assign({ station: 'welding', device: 'weld-1', person: who, action: 'matched', task: 'matching', orderId: rid, parts: 0, orders: 0, detail: 'phone scan' }, extra || {}));
      const e1 = scan(B.name, '3521000101'), e2 = scan(B.name, '3521000102'), e3 = scan('', '3521000103', { unattributed: true }), e4 = scan(B.name, '3521000101', { detail: 'phone scan · again' });
      const r = await acts([e1, e2, e3, e4, e1]); eq(r.body.written, 4, 'a replay of one scan is stored once: ' + JSON.stringify(r.body));
      await advance(MIN);
      const L = await board({ op: 'live' }); ok(L.status === 200, 'live ' + L.status + ' ' + L.raw.slice(0, 200));
      const w = L.body.stations.find(x => x.key === 'welding'); DBG('welding card', w);
      eq(w.noThroughput, true, 'noThroughput'); eq(w.counts.partsToday, null, 'partsToday'); eq(w.counts.ordersToday, null, 'ordersToday'); eq(w.counts.scansToday, 4, 'four matched scans, the unattributed one included');
      eq(w.today.matched, 4, 'matched'); eq(w.today.unattributed, 1, 'unattributed');
      ok(w.people.length === 2 && w.people.some(p => p.name === A.name && p.task === 'welding') && w.people.some(p => p.name === B.name && p.task === 'matching'), 'two people, each under a task: ' + JSON.stringify(w.people.map(p => [p.name, p.task])));
      ok(Math.abs(w.today.taskMs.welding - 41 * MIN) < 2 * MIN && Math.abs(w.today.taskMs.matching - 11 * MIN) < 2 * MIN, 'time on task per task: ' + JSON.stringify(w.today.taskMs));
      ok(!w.people.some(p => p.name === 'Unattributed' || p.name === ''), 'Unattributed is never a signed-in person');
      const un = (w.matched || []).filter(m => m.unattributed); eq(un.length, 1); eq(un[0].person, '', 'shown as "scanned with nobody in Matching": no person'); ok((w.matched || []).every(m => m.unattributed || m.person === B.name), 'every other scan is Ray\'s, never the welder\'s');
      const O = await board({ op: 'overview', days: 1 }); ok(O.status === 200, 'overview ' + O.status);
      DBG('overview people', O.body.people.map(p => [p.name, p.totals, p.stations]));
      ok(!O.body.people.some(p => p.name === 'Unattributed'), 'Unattributed is not a person of the overview: ' + O.body.people.map(p => p.name));
      eq(O.body.business.totals.parts, 0, 'Welding adds nothing to throughput'); eq(O.body.business.totals.orders, 0, 'no orders');
      const ray = O.body.people.find(p => p.name === B.name), wr = ray && ray.stations.find(x => x.station === 'welding');
      ok(wr && wr.parts === 0 && wr.completes === 0 && wr.orders === 0 && wr.matched === 3, 'Ray\'s welding row: matched 3, no pieces or orders: ' + JSON.stringify(wr));
      const P = await board({ op: 'person', name: B.name, range: 'day' }); ok(P.status === 200, 'person ' + P.status);
      eq(P.body.welding && P.body.welding.matched, 3, 'the person page counts Ray\'s three matched scans'); ok(!P.body.kpis.parts.value, 'no parts for a matcher: ' + JSON.stringify(P.body.kpis.parts));
      const U = await board({ op: 'person', name: 'Unattributed', range: 'day' }); ok(U.status === 200 && !(U.body.found && U.body.kpis && U.body.kpis.parts && U.body.kpis.parts.value > 0), 'no person page invents Unattributed: ' + U.status + ' ' + U.raw.slice(0, 120));
      const Q = await board({ op: 'personOrders', name: B.name, range: 'day', matched: true }); ok(Q.status === 200, 'personOrders matched ' + Q.status);
      eq((Q.body.orders || []).length, 2, 'Ray\'s matched orders: two distinct (the repeat of 3521000101 is one order)');
      tab.close();
    });
    await check('old welding history is still read but never counted as throughput; welding scans and completions of before stay stored', async () => {
      const st = world('2026-10-07T19:00:00Z');
      st.put('Efficiency_Daily', `${TODAY}__Old Welder`, rollDoc(TODAY, 'Old Welder', { welding: statOf({ scans: 8, scanParts: 8, completes: 4, parts: 9, orders: 4, activeMs: 1800000 }), assembly: statOf({ scans: 2, completes: 1, parts: 3, orders: 1 }) }, ['3521000201', '3521000202', '3521000203', '3521000204']));
      st.put('Station_Sessions', 'old-w', sessionDoc('old-w', 'Old Welder', 'welding', nyAt(TODAY, 9), nyAt(TODAY, 12), { device: 'weld-1' }));        // (no task: an old session)
      const O = await board({ op: 'overview', days: 1 }); const p = O.body.people.find(x => x.name === 'Old Welder');
      eq(p.totals.parts, 3, 'only the Assembly pieces count: ' + JSON.stringify(p.totals)); eq(O.body.business.totals.parts, 3);
      const L = await board({ op: 'live' }); const w = L.body.stations.find(x => x.key === 'welding'); eq(w.counts.partsToday, null);
      const P = await board({ op: 'person', name: 'Old Welder', range: 'day' }); eq(P.body.kpis.parts.value, 3); ok(P.body.welding && P.body.welding.hours && P.body.welding.hours.unknown > 0, 'an old session with no task is Welding time with the task not recorded: ' + JSON.stringify(P.body.welding && P.body.welding.hours));
      ok(cur.get('Efficiency_Daily', `${TODAY}__Old Welder`).stations.welding.completes === 4, 'the stored rollup is untouched');
    });
    await check('one person in both tasks: signed time is covered once, time per task counts in both; orders touched only at Welding are no throughput order anywhere', async () => {
      const st = world('2026-10-07T19:00:00Z');
      st.put('Station_Sessions', 'both-1', sessionDoc('both-1', 'Both Tasks', 'welding', nyAt(TODAY, 9), nyAt(TODAY, 10), { device: 'weld-1', task: 'welding' }));
      st.put('Station_Sessions', 'both-2', sessionDoc('both-2', 'Both Tasks', 'welding', nyAt(TODAY, 9), nyAt(TODAY, 10), { device: 'weld-1', task: 'matching' }));
      st.put('Efficiency_Daily', `${TODAY}__Both Tasks`, rollDoc(TODAY, 'Both Tasks', { welding: statOf({ scans: 3, matched: 3, activeMs: 120000 }), assembly: statOf({ scans: 1, completes: 1, parts: 2, orders: 1 }) }, [], { touched: { 3521000301: { welding: true }, 3521000302: { welding: true, assembly: true }, 3521000303: { welding: true } } }));
      const O = await board({ op: 'overview', days: 1 }); const p = O.body.people.find(x => x.name === 'Both Tasks'); ok(p, 'the person');
      eq(p.totals.signedInMin, 60, 'one hour signed in, though two tasks ran in it: ' + p.totals.signedInMin);
      const wr = p.stations.find(x => x.station === 'welding'); ok(wr && wr.taskMin.welding === 60 && wr.taskMin.matching === 60, 'time per task: ' + JSON.stringify(wr));
      const P = await board({ op: 'person', name: 'Both Tasks', range: 'day' }); const Q = await board({ op: 'personOrders', name: 'Both Tasks', range: 'day', limit: 50 });
      DBG('both tasks', P.body.kpis.orders, Q.body.total, p.totals.orders, O.body.business.totals.orders);
      eq(P.body.kpis.orders.value, 1, 'kpi orders: only the order touched at Assembly: ' + JSON.stringify(P.body.kpis.orders));
      eq(p.totals.orders, 1, 'overview person orders'); eq(O.body.business.totals.orders, 1, 'business orders');
      eq(Q.body.total, 1, 'the person\'s order list agrees with the number above it: ' + (Q.body.orders || []).map(o => o.rid));
    });
    await check('one person signed in at two stations at once (two computers, a missed sign-out): signed time is covered once, each station keeps its own hours, the board counts one person', async () => {
      const st = world('2026-10-07T19:00:00Z');
      st.put('Station_Sessions', 'two-1', sessionDoc('two-1', 'Two Places', 'assembly', nyAt(TODAY, 9), nyAt(TODAY, 10), { device: 'assembly-1' }));
      st.put('Station_Sessions', 'two-2', sessionDoc('two-2', 'Two Places', 'welding', nyAt(TODAY, 9, 30), nyAt(TODAY, 10, 30), { device: 'weld-1', task: 'welding' }));
      const O = await board({ op: 'overview', days: 1 }); const p = O.body.people.find(x => x.name === 'Two Places'); ok(p, 'the person is in the overview');
      eq(p.totals.signedInMin, 90, 'the union of 9:00-10:00 and 9:30-10:30 is 90 minutes, not 120: ' + p.totals.signedInMin);
      const row = k => (p.stations || []).find(x => x.station === k) || {}; const mins = r => r.signedInMin != null ? r.signedInMin : r.hoursMin != null ? r.hoursMin : r.minutes;
      ok(mins(row('assembly')) === 60 && mins(row('welding')) === 60, 'each station keeps its own hour: ' + JSON.stringify(p.stations));
      world('2026-10-07T14:00:00Z'); const t = wall();
      cur.put('Station_Sessions', 'two-3', sessionDoc('two-3', 'Two Places', 'assembly', t - 40 * MIN, null, { device: 'assembly-1', lastSeenAt: t - MIN }));
      cur.put('Station_Sessions', 'two-4', sessionDoc('two-4', 'Two Places', 'welding', t - 20 * MIN, null, { device: 'weld-1', task: 'matching', lastSeenAt: t - MIN }));
      const L = await board({ op: 'live' }); eq(L.status, 200); eq(new Set((L.body.signedIn || []).map(x => x.name)).size, 1, 'one person on, at two stations: ' + JSON.stringify((L.body.signedIn || []).map(x => [x.name, x.stationKey])));
    });
    await check('two tabs of a single-person page: another person signing in on one tab is followed by the other tab with ONE new session; the first person ends "switched" once; a tab closed leaves the session going', async () => {
      world(); const pc = computer('bench'), one = () => openTab(pc, { page: 'assembly-1.html', station: 'assembly', device: 'assembly-1' });
      const a = one(); a.login('Ana Tester'); await advance(1000); const b = one(); await advance(1000);
      a.login('Ben Tester'); await advance(70000);
      eq(open_().map(d => d.person), ['Ben Tester'], dumpSessions()); eq(stationDocs().filter(d => d.person === 'Ana Tester').length, 1, 'one session for Ana'); eq(stationDocs().find(d => d.person === 'Ana Tester').endReason, 'switched');
      eq(stationDocs().filter(d => d.person === 'Ben Tester').length, 1, 'one session for Ben, not one per tab: ' + dumpSessions());
      a.close(); await advance(20 * MIN); eq(open_().map(d => d.person), ['Ben Tester'], 'the other tab keeps the session going after one tab is closed: ' + dumpSessions()); ok(open_()[0].lastSeenAt > wall() - 6 * MIN, 'and keeps beating it');
      eq(a.errors.concat(b.errors), []);
    });
    await check('hostile people: a PIN, a respelled task, an odd task or a name with digits never makes a session of the wrong shape', async () => {
      world(); const pc = computer('bench'), tab = mk(pc);
      for (const who of [{ name: PIN, task: 'matching' }, { name: '   ', task: 'matching' }, { name: '', task: 'welding' }, null, undefined, 5, { task: 'welding' }]) tab.SS.signedIn(who);
      await advance(1000); eq(stationDocs().length, 0, 'nobody signed in by a number or an empty name');
      tab.SS.signedIn({ name: `Tess ${PIN}`, task: 'Matching ' }); tab.SS.signedIn({ name: 'Odd Task', task: '__proto__' }); tab.SS.signedIn({ name: 'Long Task', task: 'x'.repeat(40) });
      await advance(1000);
      for (const d of stationDocs()) { ok(!/\d{4}/.test(d.person), 'no digits in ' + d.person); ok(d.task === undefined || d.task === 'welding' || d.task === 'matching', `task stored as ${d.task}`); }
      const tess = stationDocs().find(d => d.person === 'Tess'); ok(tess && tess.task === 'matching', 'a respelled task ("Matching ") is Matching: ' + dumpSessions());
      ok(!tab.reqs.some(r => JSON.stringify(r.body || {}).includes(PIN)), 'the PIN was never sent');
      eq(tab.errors, []);
    });
    // the scanner credit rule: one in Matching, two, none; welders never
    await check('credit: the Matching person, the latest of two, nobody when only welders are in; never a welder', async () => {
      world(); const pc = computer('bench'), tab = mk(pc);
      eq(tab.SS.who(), null, 'nobody in');
      tab.login(A.name, 'welding'); await advance(1000); eq(tab.SS.who(), null, 'a welder alone is nobody to credit');
      tab.login(B.name, 'matching'); await advance(1000); eq(tab.SS.who().person, B.name, 'one matcher: that person');
      tab.login(C.name, 'matching'); await advance(1000); eq(tab.SS.who().person, C.name, 'two matchers: the one who signed in last');
      tab.SS.touch(wall(), { name: B.name, task: 'matching' }); eq(tab.SS.who().person, B.name, 'input from the other makes it theirs');
      await advance(2000); tab.SS.touch(wall()); eq(tab.SS.who().person, B.name, 'a plain input does not move credit');
      await advance(2000); tab.SS.touch(wall(), { name: A.name, task: 'welding' }); eq(tab.SS.who().person, B.name, 'a welder\'s input never takes the credit');
      tab.logout(B.name, 'matching'); await advance(1000); eq(tab.SS.who().person, C.name, 'the other matcher when one leaves');
      tab.logout(C.name, 'matching'); await advance(1000); eq(tab.SS.who(), null, 'none in Matching: nobody (a welder is never credited)');
      eq(tab.SS.who('welding').person, A.name, 'who("welding") is the welder');
      // the same person in both tasks: credited as the Matching one
      tab.login(A.name, 'matching'); await advance(1000); eq(tab.SS.who().person, A.name); eq(tab.SS.who().task, 'matching');
    });
    await check('touch: a time from the future, a bad value or an old relayed scan never makes input from nothing', async () => {
      world(); const pc = computer('bench'), tab = mk(pc);
      tab.login(B.name, B.task); await advance(20 * MIN);
      const before = tab.SS.lastInput();
      for (const v of [NaN, 'x', {}, [], -5, 0, Infinity, -Infinity, null, undefined, 1e15, wall() + 3 * HOUR, wall() + 61000]) { tab.SS.touch(v); ok(tab.SS.lastInput() <= wall() + 1, `touch(${String(v)}) put last input in the future: ${tab.SS.lastInput() - wall()} ms`); }
      ok(tab.SS.lastInput() >= before, 'last input never goes backwards');
      const t = tab.SS.lastInput(); tab.SS.touch(wall() - 3 * HOUR); ok(tab.SS.lastInput() === t || tab.SS.lastInput() >= t, 'an old relayed scan does not move last input back: ' + (tab.SS.lastInput() - t));
    });
  });
}

/* ═════════════════════════ 1 · auto sign-out: the server side (AD2: _stationAdmins.js, _stationAutoSignout.js, the session door) ═════════════════════════ */
const HAVE = {
  ad2: !!ADMINS && exists('netlify/functions/_stationAutoSignout.js'),
  ad1: !!process.env.FORCE_AD1 || /["']idle["']/.test(SCRIPTS.session) && /lastInputAt/.test(SCRIPTS.session) && /stationAdmin/.test(SCRIPTS.session),
  ws1page: /weld_people/.test(read('weld-1.html')),
  ld1: /laser or design/i.test(read('charm-nest-1.html')) || /CNRole/.test(read('charm-nest-1.html')) || /CNRole/.test(read('charm-nest-bridge.js')),
  ws3: /matched/.test(SCRIPTS.queue)
};
const tAt = (m, base = '2026-10-07T13:00:00Z') => Date.parse(base) + m * MIN;
const toWall = t => { if (t > wall()) clock.mono += t - wall(); };
async function autoServer() {
  await section('1 · auto sign-out, the server side', async () => {
    if (!HAVE.ad2) { pending('Rule A and B on the server, the Admin list and its door', 'AD2 (_stationAdmins.js, _stationAutoSignout.js) is not on main yet'); return; }
    const NON = 'Ana Tester';
    const ASM = o => Object.assign(SESS({ station: 'assembly', device: 'assembly-1', person: NON, computerId: 'pc-ASMPC0000001' }), o || {});
    const send = (s, ev, extra) => sess(Object.assign({}, s, { event: ev, at: wall(), sentAt: wall() }, extra || {}));
    const readers = { live: () => board({ op: 'live' }), overview: () => board({ op: 'overview', days: 1 }), person: () => board({ op: 'person', name: NON, range: 'day' }) };
    const startAt = async (m, o = {}) => { toWall(tAt(m)); const s = ASM(o); const r = await send(s, 'start', { lastInputAt: wall() }); eq(r.status, 200, 'start ' + JSON.stringify(r.body)); return s; };
    const beatAt = async (s, m, L, o = {}) => { toWall(tAt(m)); return send(s, 'beat', { lastInputAt: tAt(L) }, o); };
    /** a page that is alive: a beat every 5 minutes from minute `from` to `to` (clock in minutes after 09:00), each reporting the last input `L(minute)`; returns the last answer */
    const alive = async (s, from, to, L, o = {}) => { let r = null; for (let m = from; m <= to; m += 5) { r = await beatAt(s, m, typeof L === 'function' ? L(m) : L, o); if (r.body && r.body.ended) break; } return r; };

    for (const kind of Object.keys(readers)) await check(`a page that died ends at its LAST INPUT, reason idle, when the ${kind} read finds it; reading again changes nothing`, async () => {
      world('2026-10-07T13:00:00Z'); toWall(tAt(0));
      const s = await startAt(0); await beatAt(s, 5, 1);                       // 09:00 start, 09:05 beat, last input 09:01, then the page dies
      toWall(tAt(10)); await readers[kind](); ok(sdoc(s.id).endAt == null, 'still open at 09:10 (the page may only be quiet)');
      toWall(tAt(21)); const r = await readers[kind](); ok(r.status === 200, kind + ' ' + r.status);
      const d = sdoc(s.id); eq(d.endAt, tAt(1), 'ends at the last input 09:01, not at the beat or the moment it was noticed: ' + iso(d.endAt || 0)); eq(d.endReason, 'idle'); eq(d.minutes, 1);
      const before = JSON.stringify(d), writes = cur.writesOf('Station_Sessions').length;
      toWall(tAt(40)); await readers[kind](); await readers.live();
      eq(JSON.stringify(sdoc(s.id)), before, 'an ended session is never rewritten'); eq(cur.writesOf('Station_Sessions').length, writes, 'no second write');
    });
    await check('the 10-minute edge: a page that reports 9:59 of quiet stays, 11 quiet minutes end it at once, at the last input', async () => {
      world(); const s = await startAt(0);
      toWall(tAt(10) + 59000); let r = await send(s, 'beat', { lastInputAt: tAt(1) }); ok(!r.body.ended, 'a beat reporting 9:59 of quiet leaves it open: ' + JSON.stringify(r.body));
      const q = await startAt(11, { computerId: 'pc-ASMPC0000002', person: 'Ben Tester' });
      toWall(tAt(22)); r = await send(q, 'beat', { lastInputAt: tAt(11) }); ok(r.body.ended === true && r.body.endReason === 'idle', 'a beat reporting 11 quiet minutes ends it in the same request: ' + JSON.stringify(r.body));
      eq(sdoc(q.id).endAt, tAt(11), 'at the last input');
    });
    await check('an Admin is never ended for idleness (any spelling), a lookalike is', async () => {
      let n = 0;
      for (const [name, admin] of [['Paul K', true], ['paul k', true], ['  Paul   K ', true], ['Paul K\u0000', true], ['Paul\tK', true], ['PAUL K.', true], ['Paul_K', true], ['Paul', true], ['Paul K 482915', true], ['Pauline', false], ['Paul Kx', false], ['Paula K', false], ['Pаul K', false], ['P a u l K', false], ['Paul Kowalski', false], ['Paulk', false]]) {
        world(); const s = await startAt(0, { person: name, computerId: 'pc-ADM' + String(++n).padStart(9, '0') });
        eq(sdoc(s.id).admin, admin, `${JSON.stringify(name)} is ${admin ? '' : 'not '}an Admin on the document`);
        const r = await alive(s, 5, 60, 0);                                       // an hour of beats, every five minutes, with no input at all
        if (admin) ok(!r.body.ended && sdoc(s.id).endAt == null, `${name}: an hour with no input does not end an Admin: ${JSON.stringify(r.body)}`);
        else ok(r.body.ended === true && sdoc(s.id).endReason === 'idle', `${name}: a non-Admin is ended: ${JSON.stringify(r.body)}`);
      }
    });
    await check('an Admin whose page died is ended by the OLD rule only (closed at the last beat), never idle or closing', async () => {
      world(); const s = await startAt(0, { person: 'Paul K' }); await beatAt(s, 5, 0); toWall(tAt(25)); await readers.live();
      const d = sdoc(s.id); ok(d.endAt == null || (d.endReason === 'closed' && d.endAt === tAt(5)), 'an Admin session is open or closed at the last beat, never idle: ' + JSON.stringify([d.endAt && iso(d.endAt), d.endReason]));
    });
    await check('clock skew: a computer 20 min slow or fast is not wrongly ended, 25 quiet minutes still end it', async () => {
      let n = 0;
      for (const skew of [-20 * MIN, 20 * MIN, -9 * MIN, 9 * MIN]) {
        world(); const s0 = ASM({ computerId: 'pc-SKW' + String(++n).padStart(9, '0') }); toWall(tAt(0));
        const c = () => wall() + skew;                                          // the computer's own clock
        let r = await sess(Object.assign({}, s0, { event: 'start', at: c(), sentAt: c(), lastInputAt: c() })); eq(r.status, 200);
        toWall(tAt(4)); r = await sess(Object.assign({}, s0, { event: 'beat', at: c(), sentAt: c(), lastInputAt: c() - 60000 }));
        ok(!r.body.ended, `skew ${skew / MIN} min: a person who typed a minute ago is not ended: ${JSON.stringify(r.body)}`);
        ok(sdoc(s0.id).lastInputAt <= wall() && sdoc(s0.id).lastInputAt >= wall() - 2 * MIN, `skew ${skew / MIN}: stored last input is the server's time: ${iso(sdoc(s0.id).lastInputAt)} vs ${iso(wall())}`);
        toWall(tAt(30)); r = await sess(Object.assign({}, s0, { event: 'beat', at: c(), sentAt: c(), lastInputAt: c() - 25 * MIN }));
        ok(r.body.ended === true, `skew ${skew / MIN}: 25 quiet minutes end it: ${JSON.stringify(r.body)}`);
      }
    });
    await check('last input never goes backwards (a replayed older beat) and never into the future (a fast clock, a hostile page)', async () => {
      world(); const s = await startAt(0); await beatAt(s, 3, 3); eq(sdoc(s.id).lastInputAt, tAt(3));
      await beatAt(s, 4, 1); ok(sdoc(s.id).lastInputAt >= tAt(3), 'a replayed older beat does not move last input back: ' + iso(sdoc(s.id).lastInputAt));
      for (const L of [wall() + 3 * HOUR, 1e15, -5, 'x', null, NaN]) { toWall(wall() + 1000); await send(s, 'beat', { lastInputAt: L }); const d = sdoc(s.id); ok(d.lastInputAt <= wall() && d.lastInputAt >= d.startAt, `lastInputAt ${String(L)} stored as ${d.lastInputAt}`); }
    });
    await check('closing at 17:00 Toronto, on ordinary days and on both daylight-saving days', async () => {
      let n = 0;
      for (const day of ['2026-10-07', '2026-03-07', '2026-03-08', '2026-03-09', '2026-10-31', '2026-11-01', '2026-11-02', '2026-12-24']) {
        const m = (h, mi) => nyAt(day, h, mi), cid = () => 'pc-CLS' + String(++n).padStart(9, '0');
        const beat = (s, at, L) => { toWall(at); return send(s, 'beat', { lastInputAt: L }); };
        const begin = async (at) => { world(iso(at)); toWall(at); const s = ASM({ computerId: cid() }); await send(s, 'start', { lastInputAt: at }); return s; };
        // typed until 16:45, a beat at 16:52 (7 quiet minutes: stays), the next beat 17:02: no input in the last 10 minutes before 17:00
        let s = await begin(m(16, 40)); await beat(s, m(16, 45), m(16, 45)); let r = await beat(s, m(16, 52), m(16, 45)); ok(!r.body.ended, `${day}: 7 quiet minutes before 17:00 stays: ${JSON.stringify(r.body)}`);
        r = await beat(s, m(17, 2), m(16, 45));
        ok(r.body.ended === true && sdoc(s.id).endAt === m(16, 45), `${day}: ended at the last input 16:45: ${JSON.stringify(r.body)} ${sdoc(s.id).endAt && iso(sdoc(s.id).endAt)} vs ${iso(m(16, 45))}`);
        eq(sdoc(s.id).endReason, 'closing', `${day}: the reason is "closing" (started before 17:00, last beat after it, no input after 16:50)`);
        // typed at 16:55: stays at 17:02, then the idle rule runs from 16:55
        s = await begin(m(16, 40)); await beat(s, m(16, 45), m(16, 45)); await beat(s, m(16, 55), m(16, 55));
        r = await beat(s, m(17, 2), m(16, 55)); ok(!r.body.ended, `${day}: input at 16:55 stays at 17:02: ${JSON.stringify(r.body)}`);
        r = await beat(s, m(17, 7), m(16, 55)); ok(r.body.ended === true && sdoc(s.id).endAt === m(16, 55) && sdoc(s.id).endReason === 'idle', `${day}: then Rule A from 16:55: ${JSON.stringify(r.body)} ${sdoc(s.id).endReason}`);
        // exactly 16:50 is "no input in the last 10 minutes"; 16:51 is input inside them
        s = await begin(m(16, 40)); await beat(s, m(16, 50), m(16, 50)); r = await beat(s, m(17, 0), m(16, 50)); ok(r.body.ended === true && sdoc(s.id).endReason === 'closing' && sdoc(s.id).endAt === m(16, 50), `${day}: input at exactly 16:50 is closing: ${JSON.stringify(r.body)}`);
        s = await begin(m(16, 40)); await beat(s, m(16, 51), m(16, 51)); r = await beat(s, m(17, 0), m(16, 51)); ok(!r.body.ended, `${day}: input at 16:51 stays at 17:00: ${JSON.stringify(r.body)}`);
        r = await beat(s, m(17, 1), m(16, 51)); ok(r.body.ended === true && sdoc(s.id).endReason === 'idle' && sdoc(s.id).endAt === m(16, 51), `${day}: 10 quiet minutes after 16:51 is Rule A, not closing: ${JSON.stringify(r.body)}`);
        // a person who signs in after 17:00 and keeps typing stays (the rule is for sessions that were open at 17:00)
        s = await begin(m(17, 20)); await beat(s, m(17, 25), m(17, 24)); await beat(s, m(17, 30), m(17, 29)); await beat(s, m(17, 35), m(17, 34)); r = await beat(s, m(17, 40), m(17, 38)); ok(!r.body.ended, `${day}: a sign-in at 17:20 stays in while there is input: ${JSON.stringify(r.body)}`);
        // a page that died at 16:46 and is found at 17:30: it ends at its last input, not at 17:00 and not at 17:30 (it was gone before 17:00: plain idle)
        s = await begin(m(16, 40)); await beat(s, m(16, 45), m(16, 45)); toWall(m(17, 30)); await board({ op: 'live' });
        eq(sdoc(s.id).endAt, m(16, 45), `${day}: a dead page found at 17:30 ends at its last input`); eq(sdoc(s.id).endReason, 'idle', `${day}: a page that died before 17:00`);
        // a page that was alive at 17:00 (its beat says so) with no input since 16:40, then died: closing, at 16:40
        s = await begin(m(16, 30)); await beat(s, m(16, 40), m(16, 40)); await beat(s, m(16, 49), m(16, 40)); await beat(s, m(16, 59), m(16, 40)); toWall(m(17, 40)); await board({ op: 'live' });
        ok(sdoc(s.id).endAt === m(16, 40), `${day}: ends at the last input: ${iso(sdoc(s.id).endAt || 0)}`);
      }
    });
    await check('an ended session cannot be ended again, reopened or rewritten by a late client end, a late beat or a second read; the first end stands', async () => {
      world(); const s = await startAt(0); await beatAt(s, 5, 1); toWall(tAt(21)); await readers.live();
      const d0 = sdoc(s.id); eq(d0.endReason, 'idle');
      for (const [ev, extra] of [['end', { reason: 'signOut', lastInputAt: tAt(15) }], ['end', { reason: 'closing', at: tAt(2), lastInputAt: tAt(2) }], ['beat', { lastInputAt: tAt(20) }], ['start', { lastInputAt: tAt(20) }], ['end', { reason: 'midnight' }]]) {
        toWall(wall() + 1000); const r = await send(s, ev, extra); eq(r.body.ended, true, ev + ' says ended'); eq(sdoc(s.id).endAt, d0.endAt); eq(sdoc(s.id).endReason, 'idle'); eq(sdoc(s.id).minutes, d0.minutes);
      }
      world(); const t = await startAt(0); await beatAt(t, 5, 1); toWall(tAt(11)); const r = await send(t, 'end', { reason: 'idle', at: tAt(1), lastInputAt: tAt(1) });
      eq(r.status, 200); eq(sdoc(t.id).endAt, tAt(1), 'a client idle end keeps its own time (the last input, earlier than the last beat)'); eq(sdoc(t.id).endReason, 'idle');
      toWall(tAt(40)); await readers.live(); eq(sdoc(t.id).endAt, tAt(1)); eq(cur.writesOf('Station_Sessions').filter(w => w[1] === t.id).length, 3, 'start, beat, end: and nothing more');
    });
    await check('several reads at once end a dead session exactly once', async () => {
      world(); const s = await startAt(0); await beatAt(s, 5, 1); toWall(tAt(30));
      const before = cur.writesOf('Station_Sessions').filter(w => w[1] === s.id).length;
      await Promise.all([readers.live(), readers.overview(), readers.person(), readers.live()]);
      eq(cur.writesOf('Station_Sessions').filter(w => w[1] === s.id).length - before, 1, 'one write ended it'); eq(sdoc(s.id).endAt, tAt(1));
    });
    await check('a live page is never ended for input it has not reported yet: a beat 11 minutes old is not dead, a new beat keeps it', async () => {
      world(); const s = await startAt(0); await beatAt(s, 5, 1); toWall(tAt(16));
      await readers.live(); ok(sdoc(s.id).endAt == null, 'a page whose last beat is 11 minutes old is not ended yet');
      toWall(tAt(18)); await beatAt(s, 18, 17); await readers.live(); ok(sdoc(s.id).endAt == null, 'a beat at 18 with input at 17 keeps it');
    });
    await check('sandbox and real stay apart: a dead sandbox session is ended only by a sandbox read, a real one only by a real read', async () => {
      world(); const real = await startAt(0, { computerId: 'pc-REALPC000001' });
      toWall(tAt(0)); const sb = ASM({ computerId: 'pc-SANDPC000001', person: 'Sandy Tester' }); await sess(Object.assign({}, sb, { event: 'start', at: wall(), sentAt: wall(), lastInputAt: wall() }), { sandbox: true });
      toWall(tAt(5)); await sess(Object.assign({}, sb, { event: 'beat', at: wall(), sentAt: wall(), lastInputAt: tAt(1) }), { sandbox: true }); await beatAt(real, 5, 1);
      toWall(tAt(30)); await board({ op: 'live' });
      ok(cur.get('Station_Sessions', real.id).endAt === tAt(1), 'the real read ends the real one'); ok(cur.get('Sandbox_Station_Sessions', sb.id).endAt == null, 'and not the sandbox one');
      await board({ op: 'live', sandbox: true }); eq(cur.get('Sandbox_Station_Sessions', sb.id).endAt, tAt(1), 'the sandbox read ends the sandbox one');
    });
    await check('midnight New York still ends everybody beside the new rules: a live non-Admin and an Admin at midnight, an idle one at its last input', async () => {
      const day = '2026-10-07', mid = nyAt('2026-10-08', 0, 0), m = (h, mi) => nyAt(day, h, mi);
      world(iso(m(23, 30)));
      const live = ASM({ computerId: 'pc-MID000000001' }), adm = ASM({ computerId: 'pc-MID000000002', person: 'Paul K' }), idle = ASM({ computerId: 'pc-MID000000003', person: 'Ida Idle' });
      toWall(m(23, 30)); for (const s of [live, adm, idle]) await send(s, 'start', { lastInputAt: m(23, 30) });
      for (const k of [5, 10, 15, 20, 25]) { toWall(m(23, 30) + k * MIN); await send(live, 'beat', { lastInputAt: wall() - MIN }); await send(adm, 'beat', { lastInputAt: m(23, 30) }); if (k <= 10) await send(idle, 'beat', { lastInputAt: m(23, 31) }); }
      toWall(mid - 2 * MIN); await send(live, 'beat', { lastInputAt: wall() - MIN }); await send(adm, 'beat', { lastInputAt: m(23, 30) });
      toWall(mid + 3 * MIN); await send(live, 'beat', { lastInputAt: mid + 2 * MIN }); await send(adm, 'beat', { lastInputAt: m(23, 30) });
      const show = d => JSON.stringify([d.endAt && iso(d.endAt), d.endReason, d.lastSeenAt && iso(d.lastSeenAt), d.lastInputAt && iso(d.lastInputAt)]);
      eq(sdoc(live.id).endReason, 'midnight', 'a live person at midnight: ' + show(sdoc(live.id))); eq(sdoc(live.id).endAt, mid, 'a live person at midnight');
      eq(sdoc(adm.id).endReason, 'midnight', 'the Admin at midnight: ' + show(sdoc(adm.id))); eq(sdoc(adm.id).endAt, mid, 'the Admin is not exempt from midnight');
      toWall(mid + 30 * MIN); AUTO.resetSweep(); await AUTO.sweep({ db: cur.db, force: true });
      eq(sdoc(idle.id).endAt, m(23, 31), 'an idle page found after midnight (by the sweep) ends at its last input, not at midnight'); eq(sdoc(idle.id).endReason, 'idle');
    });
    await check('a session with no lastInputAt (an older page) is still closed by the old rule, at its last beat', async () => {
      world(); toWall(tAt(0)); const s = ASM(); await send(s, 'start'); toWall(tAt(5)); await send(s, 'beat'); toWall(tAt(30)); await readers.live();
      const d = sdoc(s.id); ok(d.endAt == null || (d.endReason === 'closed' && d.endAt === tAt(5)), 'closed at the last beat (or left to the 15-minute reader rule): ' + JSON.stringify([d.endAt && iso(d.endAt), d.endReason]));
    });
    await check('the Admin door: a boolean and nothing else, never the list, never an echo of the name or a PIN; bad bodies 4xx; a flood is locked out', async () => {
      freshKeep(); const q = (body, o) => doorPost(body, o);
      let r = await q({ stationAdmin: 'Paul K' }); ok(r.status === 200 && r.body.ok === true && r.body.admin === true, JSON.stringify(r.body)); eq(Object.keys(r.body).sort().join(), 'admin,ok', 'only ok and admin');
      r = await q({ stationAdmin: 'Tess Welder' }); ok(r.body.ok === true && r.body.admin === false);
      for (const nm of [PIN, '48 29 15', '', '   ', '__proto__', 'constructor', 'toString', 'x'.repeat(201)]) { r = await q({ stationAdmin: nm }); ok((r.status === 200 && r.body.admin === false) || r.status === 400, `name ${JSON.stringify(nm).slice(0, 20)}: ${r.status} ${JSON.stringify(r.body)}`); ok(!JSON.stringify(r.body).includes(PIN) && !JSON.stringify(r.body).includes('Paul'), 'no echo'); }
      for (const nm of [5, null, {}, [], ['Paul K'], true]) { r = await q({ stationAdmin: nm }); ok(r.status === 400 || (r.status === 200 && r.body.admin === false), `non-text ${JSON.stringify(nm)}: ${r.status}`); }
      r = await q({ stationAdmin: 'Paul K', pad: 'x'.repeat(1100) }); eq(r.status, 413);
      let locked = 0; for (let i = 0; i < 60; i++) { r = await q({ stationAdmin: 'Guess ' + i }, { ip: '192.0.2.77' }); if (r.status === 429) locked++; } ok(locked >= 15, 'a flood from one address is locked out: ' + locked);
      r = await q({ stationAdmin: 'Tess' }, { ip: '192.0.2.78' }); eq(r.status, 200, 'another address is not locked');
    });
    await check('the Admin list: a missing or empty or odd document falls back to Paul K and Paul; odd entries are ignored; Tess is never an Admin', async () => {
      for (const doc of [undefined, {}, { names: [] }, { names: 'Paul K' }, { names: { a: 1 } }, { names: null }, { names: [null, 5, {}, [], '135792'] }]) {
        freshKeep(); if (doc !== undefined) cur.put('config', 'stationAdmins', doc); try { ADMINS._reset && ADMINS._reset(); } catch (_) {}
        const a = await ADMINS.isAdmin(cur.db, 'Paul K'), b = await ADMINS.isAdmin(cur.db, 'Tess Welder'), c = await ADMINS.isAdmin(cur.db, 'toString');
        ok(a === true && b === false && c === false, `doc ${JSON.stringify(doc)}: Paul K ${a}, Tess ${b}, toString ${c}`);
      }
      freshKeep(); cur.put('config', 'stationAdmins', { names: ['__proto__', 'constructor', 'Boss Man'] }); try { ADMINS._reset && ADMINS._reset(); } catch (_) {}
      ok((await ADMINS.isAdmin(cur.db, 'Boss Man')) === true && (await ADMINS.isAdmin(cur.db, 'toString')) === false && (await ADMINS.isAdmin(cur.db, 'valueOf')) === false && (await ADMINS.isAdmin(cur.db, 'hasOwnProperty')) === false, 'prototype names in the list do not make other prototype names Admins');
    });
    await check('a list that cannot be read: unknown (null), never Admin; a stored admin flag keeps an Admin exempt while the list is down', async () => {
      freshKeep(); try { ADMINS._reset && ADMINS._reset(); } catch (_) {} cur.fail('config');
      const r = await ADMINS.isAdmin(cur.db, 'Paul K'); ok(r === null || r === true, 'unreadable list and nothing cached: unknown (' + r + ')');
      ok((await ADMINS.isAdmin(cur.db, 'Tess Welder')) !== true, 'Tess is not an Admin because the list is down');
      cur.heal('config');
    });
    const sweepNow = async () => { AUTO.resetSweep(); return AUTO.sweep({ db: cur.db, force: true }); };
    const seedRow = (id, o) => { cur.put('Station_Sessions', id, Object.assign({ id, person: 'Seed Person', employeeId: '', station: 'assembly', device: 'assembly-1', computerId: 'pc-SEED' + id.replace(/\W/g, '').slice(-8).padEnd(8, '0'), computerLabel: '', startAt: tAt(0), lastSeenAt: tAt(5), lastInputAt: tAt(1), endAt: null, endReason: null, minutes: 0 }, o)); };
    await check('the sweep: dead non-Admins end at their LAST INPUT (idle), live ones stay, an Admin and an old page by the old rule; a second sweep writes nothing', async () => {
      world(); const dead = await startAt(0, { person: 'Dee Dead', computerId: 'pc-SWDEAD00001' }); await beatAt(dead, 5, 1);
      const adm = await startAt(0, { person: 'Paul K', computerId: 'pc-SWADMIN0001' }); await beatAt(adm, 5, 0);
      toWall(tAt(0)); const old = ASM({ computerId: 'pc-SWOLDPG0001', person: 'Old Page' }); await send(old, 'start'); toWall(tAt(5)); await send(old, 'beat');
      const live = await startAt(0, { person: 'Lee Live', computerId: 'pc-SWLIVE00001' }); await alive(live, 5, 38, m => m - 1);
      const advLive = await startAt(38, { person: 'Pam K', computerId: 'pc-SWPAULK0001' }); await beatAt(advLive, 38, 38);
      toWall(tAt(40)); const r = await sweepNow(); ok(r.stores && r.stores.real && r.stores.sandbox, 'both stores swept: ' + JSON.stringify(r));
      eq(sdoc(dead.id).endAt, tAt(1), 'a dead page ends at its last input'); eq(sdoc(dead.id).endReason, 'idle'); eq(sdoc(dead.id).minutes, 1);
      eq(sdoc(adm.id).endAt, tAt(5), 'an Admin whose page died: the old rule, at the last beat'); eq(sdoc(adm.id).endReason, 'closed');
      eq(sdoc(old.id).endAt, tAt(5), 'an old page with no last input: the old rule'); eq(sdoc(old.id).endReason, 'closed');
      ok(sdoc(live.id).endAt == null, 'a page with a fresh beat and input stays'); ok(sdoc(advLive.id).endAt == null, 'a person on the list by a lookalike name but fresh beats stays');
      const w = cur.writesOf('Station_Sessions').length, snap = JSON.stringify(cur.all('Station_Sessions'));
      toWall(tAt(41)); const r2 = await sweepNow(); eq(cur.writesOf('Station_Sessions').length, w, 'a second sweep writes nothing: ' + JSON.stringify(r2)); eq(JSON.stringify(cur.all('Station_Sessions')), snap, 'nothing else changed');
      eq(cur.writesOf('Sandbox_Station_Sessions').length, 0, 'the sandbox store was not written');
    });
    await check('the sweep: only endAt, endReason and minutes of an open session change; nothing is deleted; another collection is never written', async () => {
      world(); const a = await startAt(0, { person: 'Fay Field' }); await beatAt(a, 5, 1); toWall(tAt(40));
      const before = cur.get('Station_Sessions', a.id), writes0 = cur.writes.length; await sweepNow();
      const after = cur.get('Station_Sessions', a.id); const changed = Object.keys(after).filter(k => JSON.stringify(after[k]) !== JSON.stringify(before[k])).sort();
      eq(changed, ['endAt', 'endReason', 'minutes'], 'the changed fields: ' + changed);
      const w = cur.writes.slice(writes0); ok(w.length >= 1 && w.every(x => x[0] === 'Station_Sessions' && x[2] !== 'delete'), 'only session writes, no delete: ' + JSON.stringify(w));
    });
    await check('the sweep: yesterday\'s dead sessions (found after midnight) end at their last input or last beat, never at midnight and never at the time of the sweep', async () => {
      const day = '2026-10-07', m = (h, mi) => nyAt(day, h, mi); world(iso(m(23, 30))); toWall(m(23, 30));
      const non = ASM({ computerId: 'pc-YDNON000001', person: 'Yda Non' }), adm = ASM({ computerId: 'pc-YDADM000001', person: 'Paul' });
      for (const s of [non, adm]) await send(s, 'start', { lastInputAt: m(23, 30) });
      for (const k of [5, 10]) { toWall(m(23, 30) + k * MIN); await send(non, 'beat', { lastInputAt: m(23, 31) }); await send(adm, 'beat', { lastInputAt: m(23, 30) }); }
      toWall(nyAt('2026-10-08', 0, 40)); await sweepNow();
      eq(sdoc(non.id).endAt, m(23, 31), 'the person: the last input'); eq(sdoc(non.id).endReason, 'idle');
      eq(sdoc(adm.id).endAt, m(23, 40), 'the Admin: the last beat'); eq(sdoc(adm.id).endReason, 'closed');
    });
    await check('the sweep: the real and the sandbox store are each swept on their own; a store that cannot be read does not stop the other; the handler answers 200 and never throws', async () => {
      world(); const real = await startAt(0, { computerId: 'pc-SWREAL00001' });
      const sb = ASM({ computerId: 'pc-SWSAND00001', person: 'Sandy Tester' }); await sess(Object.assign({}, sb, { event: 'start', at: wall(), sentAt: wall(), lastInputAt: wall() }), { sandbox: true });
      await beatAt(real, 5, 1); await sess(Object.assign({}, sb, { event: 'beat', at: wall(), sentAt: wall(), lastInputAt: tAt(1) }), { sandbox: true });
      toWall(tAt(40)); cur.fail('Station_Sessions');
      const r = await sweepNow(); ok(r.stores.real && r.stores.real.error, 'the real store reports its error: ' + JSON.stringify(r.stores.real)); cur.heal('Station_Sessions');
      eq(cur.get('Sandbox_Station_Sessions', sb.id).endAt, tAt(1), 'the sandbox store was still swept: ' + JSON.stringify(cur.get('Sandbox_Station_Sessions', sb.id))); ok(sdoc(real.id).endAt == null, 'the real one waits for the next look');
      cur.fail('Station_Sessions'); cur.fail('Sandbox_Station_Sessions'); AUTO.resetSweep(); toWall(wall() + 2 * MIN);
      const h = await SWEEPFN.handler({}); ok(h.statusCode === 200 && JSON.parse(h.body).ok === true, 'the handler answers 200 with both stores down: ' + JSON.stringify(h)); cur.heal('Station_Sessions'); cur.heal('Sandbox_Station_Sessions');
      AUTO.resetSweep(); toWall(wall() + 2 * MIN); const h2 = await SWEEPFN.handler({}); eq(h2.statusCode, 200); ok(sdoc(real.id).endAt === tAt(1), 'the next sweep ends it at its last input: ' + iso(sdoc(real.id).endAt || 0));
      ok(/^\*\/5 \* \* \* \*$/.test(SWEEPFN.config.schedule), 'the schedule is every five minutes');
    });
    await check('the sweep with the Admin list unreadable leaves a session without a stored flag open; once the list can be read it is decided', async () => {
      world(); seedRow('unk-1', { person: 'Una Known' }); seedRow('unk-2', { person: 'Paul K' }); seedRow('unk-3', { person: 'Stored Out', admin: false }); toWall(tAt(40)); cur.fail('config');
      await sweepNow(); ok(sdoc('unk-1').endAt == null && sdoc('unk-2').endAt == null, 'unknown means left open (and never ended wrongly)'); eq(sdoc('unk-3').endReason, 'idle', 'a stored admin:false is enough to decide');
      cur.heal('config'); try { ADMINS.forget(cur.db); } catch (_) {} await sweepNow();
      eq(sdoc('unk-1').endReason, 'idle', 'decided later'); eq(sdoc('unk-2').endReason, 'closed', 'Paul K, an Admin: the old rule');
    });
    await check('the sweep never ends more than 100 in a run and never throws on odd rows (no start, a text for a time, endAt 0, a person with no name)', async () => {
      world(); for (let i = 0; i < 130; i++) seedRow('bulk-' + i, { person: 'Bulk ' + String.fromCharCode(65 + (i % 26)) + ' Person' });
      seedRow('odd-1', { startAt: 'soon' }); seedRow('odd-2', { startAt: null }); seedRow('odd-3', { person: '', lastSeenAt: 'x', lastInputAt: 'y' }); seedRow('odd-4', { person: null, startAt: tAt(0), lastSeenAt: {}, lastInputAt: [] }); seedRow('odd-5', { lastInputAt: 1e18 }); seedRow('odd-6', { lastInputAt: -4, startAt: 5 });
      toWall(tAt(40)); const r = await sweepNow(); ok(r.stores.real.written <= 100, 'at most 100 written: ' + JSON.stringify(r.stores.real));
      const ended = n => cur.all('Station_Sessions').filter(d => d.endAt).length; ok(ended() >= 90 && ended() <= 106, 'about 100 ended: ' + ended());
      await advance(31000); AUTO.resetSweep(); const r2 = await AUTO.sweep({ db: cur.db, force: true });
      ok(cur.all('Station_Sessions').filter(d => /^bulk-/.test(d._id)).every(d => d.endAt), 'the rest of the 130 end in the next run: ' + JSON.stringify(r2.stores.real));
      for (const d of cur.all('Station_Sessions').filter(d => d.endAt && /^odd-/.test(d._id))) ok(Number.isFinite(d.endAt) && d.endAt >= 0 && Number.isFinite(d.minutes), 'odd row ' + d._id + ' ended sanely: ' + JSON.stringify(d));
    });
  });
}

/* ═════════════════════════ 1b · auto sign-out, the page side (AD1: the idle and 5 pm timers in station-session.js, the Admin lookup) ═════════════════════════ */
async function autoPage() {
  const NON = 'Ana Tester', ADM = 'Paul K', BEN = 'Ben Tester';
  const single = (pc, o) => openTab(pc, Object.assign({ page: 'assembly-1.html', station: 'assembly', device: 'assembly-1' }, o || {}));
  const multi = (pc, o) => openTab(pc, Object.assign({ multi: true, station: 'welding', device: 'weld-1' }, o || {}));
  const lookups = tab => tab.reqs.filter(r => r.body && r.body.stationAdmin !== undefined);
  const near = (a, b, ms = 1500) => Math.abs(a - b) <= ms;
  const typing = async (tab, every, total, type = 'keydown') => { for (let t = 0; t < total; t += every) { await advance(every); tab.input(type); } };
  const isLookup = rec => rec.body && rec.body.stationAdmin !== undefined;
  await section('1b · auto sign-out, the page side', async () => {
    if (!HAVE.ad1) { pending('Rule A and B in the page, the Admin lookup, sleep, clock jumps, reload, two tabs, background tab, offline', 'AD1 (station-session.js idle/closing timers and the stationAdmin lookup) is not on main yet'); return; }

    await check('Rule A: 10 minutes without input signs a non-Admin out through the page, with the reason idle, once; the end is the LAST INPUT; 9:30 of quiet does not', async () => {
      world(); const pc = computer('a'), tab = single(pc); tab.login(NON); await advance(2 * MIN); tab.input('keydown'); const last = wall();
      await advance(9 * MIN + 30000); eq(tab.signOuts, [], 'not at 9:30 of quiet'); ok(open_().length === 1, 'still open');
      await advance(MIN); eq(tab.signOuts, ['idle'], 'the page signed out with the reason idle');
      const d = endedAt(NON); eq(d.length, 1, dumpSessions()); eq(d[0].endReason, 'idle'); ok(near(d[0].endAt, last), 'ends at the last input: ' + iso(d[0].endAt) + ' vs ' + iso(last));
      await advance(30 * MIN); eq(tab.signOuts, ['idle'], 'once'); eq(tab.kind('end').length, 1, 'one end sent'); eq(tab.errors, []);
    });

    await check('every kind of user input counts (pointer, touch, key, wheel, click); the network coming back and a storage event do not', async () => {
      for (const type of ['pointerdown', 'mousedown', 'touchstart', 'keydown', 'wheel', 'click']) {
        world(); const pc = computer('e'), tab = single(pc); tab.login(NON); await advance(9 * MIN); tab.input(type); await advance(9 * MIN); eq(tab.signOuts, [], `${type} counts as input`);
      }
      for (const quiet of ['online', 'storage']) {
        world(); const pc = computer('q'), tab = single(pc); tab.login(NON); await advance(9 * MIN);
        if (quiet === 'online') tab.w.dispatchEvent(new tab.w.Event('online')); else tab.w.dispatchEvent(new tab.w.StorageEvent('storage', { key: 'other', newValue: 'x' }));
        await advance(2 * MIN); eq(tab.signOuts, ['idle'], `${quiet} is not input`);
      }
    });

    await check('a person who types every 9 minutes for 3 hours is never signed out; every beat carries the last input (page clock, never in its future)', async () => {
      world(); const pc = computer('t'), tab = single(pc); tab.login(NON); await typing(tab, 9 * MIN, 3 * HOUR);
      eq(tab.signOuts, []); ok(open_().length === 1, 'open');
      const beats = tab.kind('beat'); ok(beats.length >= 30, 'beats: ' + beats.length);
      for (const b of beats) { ok(Number(b.lastInputAt) > 1e12, 'a beat carries lastInputAt: ' + JSON.stringify(b)); ok(b.lastInputAt <= (b.sentAt || b.at) + 1000, 'last input is not in the future of the page clock'); ok((b.sentAt || b.at) - b.lastInputAt <= 10 * MIN, 'a typing person\'s last input is under 10 minutes old at every beat'); }
    });

    await check('an Admin is never signed out for idleness (3 hours, no input), asked of the door once per sign-in with the name in the body only, nothing stored; midnight still ends the Admin', async () => {
      world(); const pc = computer('adm'), tab = single(pc); tab.login(ADM); await advance(3 * HOUR);
      eq(tab.signOuts, [], 'the Admin stays'); ok(open_().length === 1, 'open');
      const L = lookups(tab); eq(L.length, 1, 'asked once: ' + L.length); ok(!/Paul/.test(L[0].url), 'the name is not in the URL'); eq(L[0].method, 'POST');
      ok(![...pc.mem.entries()].some(([k, v]) => /"?admin"?\s*[:=]\s*"?true/i.test(k + '=' + v)), 'the answer is not kept in storage: ' + JSON.stringify([...pc.mem.keys()]));
      const mid = nyAt('2026-10-08', 0, 0); await goTo(mid + 3 * MIN);
      eq(tab.signOuts, ['midnight'], 'midnight ends everybody, the Admin too'); const d = endedAt(ADM); eq(d.length, 1); eq(d[0].endReason, 'midnight'); eq(d[0].endAt, mid);
    });

    await check('the Admin lookup that fails in any way means NOT Admin: offline, 500, 403, 404, 429, no answer at all; the person is signed out after 10 quiet minutes', async () => {
      for (const mode of ['offline', '500', '403', '404', '429', 'never']) {
        world(); const pc = computer('f'), tab = single(pc);
        if (mode === 'offline') tab.setOnline(false); else if (mode === 'never') tab.hold = rec => isLookup(rec); else tab.failIf = rec => isLookup(rec) && Number(mode);
        tab.login(ADM); await advance(2000); if (mode === 'offline') tab.setOnline(true);
        await advance(11 * MIN); eq(tab.signOuts, ['idle'], `${mode}: signed out for idleness (fail closed)`); eq(tab.errors, [], mode + ': no page errors');
      }
    });

    await check('a slow Admin answer: the person is not kept waiting, and a late answer (even "true") never breaks the page or signs anybody out twice', async () => {
      world(); const pc = computer('slow'), tab = single(pc); tab.hold = rec => isLookup(rec); tab.login(ADM); await advance(6 * MIN);
      ok(open_().length === 1, 'signed in while the door has not answered'); tab.input('keydown'); await tab.release(); await advance(2 * MIN);
      ok(tab.signOuts.length <= 1, 'at most one sign-out: ' + JSON.stringify(tab.signOuts)); eq(tab.errors, []);
      await advance(15 * MIN); ok(tab.signOuts.length <= 1, 'still at most one: ' + JSON.stringify(tab.signOuts)); ok(endedAt(ADM).length <= 1, dumpSessions());
    });

    await check('closing at 17:00 Toronto (ordinary and both daylight-saving days): quiet since 16:50 is out at 17:00 at the last input; input at 16:55 stays and Rule A takes over; a sign-in at 17:20 stays while typing', async () => {
      for (const day of ['2026-10-07', '2026-03-08', '2026-03-09', '2026-11-01', '2026-11-02']) {
        const m = (h, mi) => nyAt(day, h, mi);
        world(iso(m(16, 30))); let pc = computer('c'), tab = single(pc); tab.login(NON); await goTo(m(16, 50)); tab.input('keydown'); await goTo(m(17, 3));
        eq(tab.signOuts.length, 1, `${day}: signed out by 17:03: ${JSON.stringify(tab.signOuts)}`); ok(/^(closing|idle)$/.test(tab.signOuts[0]), `${day}: reason ${tab.signOuts[0]}`);
        let d = endedAt(NON); eq(d.length, 1, dumpSessions()); ok(near(d[0].endAt, m(16, 50)) && /^(closing|idle)$/.test(d[0].endReason), `${day}: ends at the last input 16:50: ${iso(d[0].endAt)} ${d[0].endReason}`);
        world(iso(m(16, 30))); pc = computer('c2'); tab = single(pc); tab.login(NON); await goTo(m(16, 55)); tab.input('keydown'); await goTo(m(17, 3)); eq(tab.signOuts, [], `${day}: input at 16:55 stays at 17:03`);
        await goTo(m(17, 8)); eq(tab.signOuts, ['idle'], `${day}: then idle from 16:55`); d = endedAt(NON); ok(near(d[0].endAt, m(16, 55)), `${day}: ends at 16:55: ${iso(d[0].endAt)}`);
        world(iso(m(17, 20))); pc = computer('c3'); tab = single(pc); tab.login(NON); await typing(tab, 5 * MIN, 30 * MIN); eq(tab.signOuts, [], `${day}: a sign-in after 17:00 stays while typing`);
      }
    });

    await check('a computer that sleeps 40 minutes (the clock runs on, timers stand still) or freezes: on wake the person is signed out at the LAST INPUT, never at the wake-up, never still signed in', async () => {
      for (const how of ['sleep', 'freeze']) {
        world(); const pc = computer('s'), tab = single(pc); tab.login(NON); await advance(2 * MIN); tab.input('keydown'); const last = wall();
        if (how === 'sleep') await sleepFor(40 * MIN); else await freeze(40 * MIN);
        await advance(70000);
        ok(tab.signOuts.length === 1 && /^(idle|closing)$/.test(tab.signOuts[0]), `${how}: the page signed out for idleness: ${JSON.stringify(tab.signOuts)}`);
        const d = endedAt(NON); eq(d.length, 1, `${how}: ${dumpSessions()}`); ok(d[0].endAt <= last + 1500, `${how}: ends at the last input, not at the wake-up: ${iso(d[0].endAt)} vs ${iso(last)}`); ok(/^(idle|closing)$/.test(d[0].endReason), `${how}: reason ${d[0].endReason}`);
      }
    });

    await check('the computer\'s clock set 3 hours FORWARD under a typing person signs nobody out; set 2 hours BACK under an idle one still ends at the real last input', async () => {
      world(); let pc = computer('cf'), tab = single(pc); tab.login(NON); await advance(2 * MIN); tab.input('keydown'); tab.skew += 3 * HOUR; await advance(2 * MIN);
      eq(tab.signOuts, [], 'a clock set forward is not 3 hours of quiet'); ok(open_().length === 1); tab.input('keydown'); await advance(5 * MIN); eq(tab.signOuts, []);
      world(); pc = computer('cb'); tab = single(pc); tab.login(NON); await advance(2 * MIN); tab.input('keydown'); const real = wall(); tab.skew -= 2 * HOUR; await advance(11 * MIN);
      eq(tab.signOuts, ['idle'], 'ended for idleness after 10 quiet minutes of real time'); const d = endedAt(NON); eq(d.length, 1, dumpSessions());
      ok(near(d[0].endAt, real, 3000), `the end is the real moment of the last input, not 2 hours off: ${iso(d[0].endAt)} vs ${iso(real)}`);
    });

    await check('a page whose own clock is 20 minutes fast or slow: the server end is still the last input in the server\'s time', async () => {
      for (const skew of [20 * MIN, -20 * MIN]) {
        world(); const pc = computer('k'), tab = single(pc, { skew }); tab.login(NON); await advance(2 * MIN); tab.input('keydown'); const real = wall(); await advance(12 * MIN);
        eq(tab.signOuts, ['idle'], `skew ${skew / MIN}: signed out`); const d = endedAt(NON); eq(d.length, 1, dumpSessions()); ok(near(d[0].endAt, real, 4000), `skew ${skew / MIN}: ${iso(d[0].endAt)} vs ${iso(real)}`);
      }
    });

    await check('a page reloaded in the middle of an idle spell keeps the idle clock: 8 quiet minutes, a reload, 3 more: signed out at the ORIGINAL last input', async () => {
      world(); const pc = computer('r'), tab = single(pc); tab.login(NON); await advance(MIN); tab.input('keydown'); const last = wall(); await advance(8 * MIN); tab.close();
      const t2 = single(pc); await advance(3 * MIN); eq(t2.signOuts, ['idle'], 'a reload is not input: the idle spell goes on'); const d = endedAt(NON); eq(d.length, 1, dumpSessions()); ok(near(d[0].endAt, last), 'at the original last input: ' + iso(d[0].endAt) + ' vs ' + iso(last));
      eq(open_().filter(x => x.person === NON).length, 0, 'nothing left open');
    });

    await check('two tabs of one person at one station: input in EITHER keeps the person in; quiet in both ends it once, at the last input of either', async () => {
      world(); const pc = computer('two'), a = single(pc); a.login(NON); await advance(1000); const b = single(pc);
      await advance(9 * MIN); b.input('keydown'); const lastB = wall(); await advance(9 * MIN); eq(a.signOuts, [], 'tab A: the person typed in B 9 minutes ago'); eq(b.signOuts, []); ok(open_().length === 1);
      await advance(2 * MIN); const n = a.signOuts.length + b.signOuts.length; ok(n >= 1 && n <= 2, 'signed out (once per page callback): ' + JSON.stringify([a.signOuts, b.signOuts]));
      const d = endedAt(NON); eq(d.length, 1, dumpSessions()); ok(near(d[0].endAt, lastB), 'at the last input of either tab: ' + iso(d[0].endAt) + ' vs ' + iso(lastB)); eq(a.kind('end').length + b.kind('end').length >= 1, true);
      eq(cur.writesOf('Station_Sessions').filter(w => w[2] === 'set' && w[1] === d[0].id).length >= 2, true);
    });

    await check('input that arrives only as a relayed phone scan (StationSession.touch) keeps the person in', async () => {
      world(); const pc = computer('sc'), tab = single(pc); tab.login(NON); for (let i = 0; i < 6; i++) { await advance(9 * MIN); tab.SS.touch(); } eq(tab.signOuts, [], 'scans every 9 minutes for an hour: no sign-out'); ok(open_().length === 1);
      await advance(11 * MIN); eq(tab.signOuts, ['idle'], 'then 11 quiet minutes');
    });

    await check('a hidden (background) tab: timers run once a minute; it is still signed out within about 11 minutes, at the last input; coming back after a long time signs out at once', async () => {
      world(); let pc = computer('h1'), tab = single(pc); tab.login(NON); await advance(MIN); tab.input('keydown'); const last = wall(); tab.hide(); await advance(13 * MIN);
      eq(tab.signOuts, ['idle'], 'signed out although the tab is hidden'); let d = endedAt(NON); eq(d.length, 1, dumpSessions()); ok(near(d[0].endAt, last), 'at the last input: ' + iso(d[0].endAt) + ' vs ' + iso(last));
      world(); pc = computer('h2'); tab = single(pc, { throttle: false }); tab.login(NON); await advance(MIN); tab.input('keydown'); tab.hide(); await sleepFor(2 * HOUR); tab.show(); await advance(5000);
      eq(tab.signOuts.length, 1, 'back after two hours: signed out at once: ' + JSON.stringify(tab.signOuts)); d = endedAt(NON); ok(d.length === 1 && d[0].endAt < wall() - HOUR, 'the end is the last input, long ago: ' + dumpSessions());
    });

    await check('offline when the idle time comes: the sign-out happens on the page at once, the end is kept and sent on reconnect with the LAST INPUT as its time, once', async () => {
      world(); const pc = computer('o'), tab = single(pc); tab.login(NON); await advance(2 * MIN); tab.input('keydown'); const last = wall(); tab.setOnline(false); await advance(11 * MIN);
      eq(tab.signOuts, ['idle'], 'signed out on the page although offline'); await advance(20 * MIN); tab.setOnline(true); await advance(2 * MIN);
      const d = endedAt(NON); eq(d.length, 1, dumpSessions()); ok(near(d[0].endAt, last, 3000), 'at the last input: ' + iso(d[0].endAt) + ' vs ' + iso(last)); ok(/^(idle|closed)$/.test(d[0].endReason), d[0].endReason);
      await advance(10 * MIN); eq(endedAt(NON).length, 1); eq(tab.signOuts.length, 1);
    });

    await check('20 minutes with no network while typing is not an idle person: still signed in afterwards (a new session when the old one was closed), never the login screen', async () => {
      world(); const pc = computer('n'), tab = single(pc); tab.login(NON); await advance(2 * MIN); tab.setOnline(false); await typing(tab, 4 * MIN, 20 * MIN); tab.setOnline(true); await advance(6 * MIN);
      eq(tab.signOuts, [], 'typed all the time: no sign-out'); ok(open_().filter(d => d.person === NON).length === 1, 'exactly one open session of the person: ' + dumpSessions());
    });

    await check('two people at the Welding page: input counts for both; quiet for 10 minutes ends the non-Admin ONLY through the page\'s per-person sign-out; the Admin carries on', async () => {
      world(); const pc = computer('w'), tab = multi(pc); tab.login('Tess Welder', 'welding'); tab.login(ADM, 'matching'); await advance(2 * MIN); tab.input('keydown'); await advance(9 * MIN); tab.input('pointerdown'); await advance(9 * MIN);
      eq(tab.signOuts, [], 'input counts for both: nobody is out 18 minutes after the first input'); await advance(2 * MIN);
      eq(tab.signOuts.map(x => x[0] + ':' + x[1].name + ':' + x[1].task), ['idle:Tess Welder:welding'], 'only Tess, with her name and task');
      eq(open_().map(d => d.person), [ADM], 'the Admin\'s session is the only one open: ' + dumpSessions()); eq(tab.who().map(p => p.name), [ADM]);
      const t = endedAt('Tess Welder'); eq(t.length, 1); eq(t[0].endReason, 'idle');
      await advance(3 * HOUR); eq(open_().map(d => d.person), [ADM], 'the Admin is still in after 3 more hours'); eq(tab.signOuts.length, 1);
    });

    await check('one person at two stations on two computers: each page ends its own session on its own idle time', async () => {
      world(); const a = single(computer('x1')), w = multi(computer('x2')); a.login(NON); w.login(NON, 'welding'); await advance(2 * MIN); a.input('keydown'); await advance(9 * MIN); w.input('keydown'); await advance(2 * MIN);
      eq(a.signOuts, ['idle'], 'the assembly page is out (10 quiet minutes there)'); eq(w.signOuts, [], 'the welding page had input 2 minutes ago');
      const d = stationDocs().filter(x => x.person === NON); eq(d.filter(x => x.endAt).map(x => x.station), ['assembly'], dumpSessions());
    });

    await check('after an idle sign-out the same person signs in again: a new session, its own idle clock from the new sign-in, the old end stands', async () => {
      world(); const pc = computer('again'), tab = single(pc); tab.login(NON); await advance(12 * MIN); eq(tab.signOuts, ['idle']); const first = endedAt(NON)[0];
      await advance(5 * MIN); tab.login(NON); await advance(8 * MIN); eq(tab.signOuts, ['idle'], 'the new sign-in is not out after 8 minutes'); await advance(3 * MIN); eq(tab.signOuts, ['idle', 'idle']);
      const all = stationDocs().filter(d => d.person === NON); eq(all.length, 2, dumpSessions()); eq(cur.get('Station_Sessions', first.id).endAt, first.endAt, 'the first end stands'); ok(all.every(d => d.endAt && d.endReason === 'idle'));
    });

    await check('midnight New York beside the new rules: a typing person and an idle one end at midnight/at their last input; the Admin at midnight', async () => {
      const day = '2026-10-07', m = (h, mi) => nyAt(day, h, mi), mid = nyAt('2026-10-08', 0, 0);
      world(iso(m(23, 40))); const a = single(computer('m1')), b = single(computer('m2')), c = single(computer('m3'));
      a.login(NON); b.login(BEN); c.login(ADM); await advance(MIN); b.input('keydown'); const lastB = wall();
      await typing(a, 5 * MIN, 25 * MIN); await goTo(mid + 3 * MIN);
      eq(a.signOuts, ['midnight']); eq(c.signOuts, ['midnight']); eq(b.signOuts, ['idle'], 'Ben was idle long before midnight');
      eq(endedAt(NON)[0].endAt, mid); eq(endedAt(ADM)[0].endAt, mid); ok(near(endedAt(BEN)[0].endAt, lastB), 'Ben ends at his last input: ' + iso(endedAt(BEN)[0].endAt));
    });

    await check('sandbox and real stay apart: an idle sign-out of a sandbox page ends only the sandbox session', async () => {
      world(); const real = single(computer('sr')), sb = single(computer('ss'), { sandbox: true }); real.login(NON); sb.login(BEN); await advance(12 * MIN);
      eq(real.signOuts, ['idle']); eq(sb.signOuts, ['idle']); eq(cur.all('Station_Sessions').filter(d => d.person === BEN).length, 0, 'Ben is not in the real store'); eq(cur.all('Sandbox_Station_Sessions').filter(d => d.person === NON).length, 0, 'Ana is not in the sandbox store');
      ok(cur.all('Sandbox_Station_Sessions').every(d => d.endReason === 'idle') && cur.all('Station_Sessions').every(d => d.endReason === 'idle'));
    });

    await check('no PIN, no keystroke, no input content is stored or sent: only times', async () => {
      world(); const pc = computer('priv'), tab = single(pc); tab.login(NON); for (let i = 0; i < 5; i++) { tab.input('keydown'); await advance(30000); }
      const sent = JSON.stringify(tab.reqs.map(r => r.body)); ok(!/keydown|pointerdown|key":|"code"/i.test(sent.replace(/lastInputAt/g, '')), 'no event names or keys in what was sent');
      ok(![...pc.mem.values()].some(v => /keydown|pointerdown/.test(v)), 'nothing about keys in storage');
    });
  });
}

/* ═════════════════════════ 2b · the Welding scanner: who a scan is credited to ═════════════════════════ */
async function scanner() {
  const T = { A: 'Tess Welder', B: 'Ray Welder', C: 'Ivy Third' };
  let scanN = 0;
  const desk = (pc, extra) => {
    const tab = openTab(pc, Object.assign({ multi: true, station: 'welding', device: 'weld-1', scripts: ['session', 'activity', 'queue'] }, extra || {}));
    tab.q = tab.w.StationScanQueue.create({ device: 'weld-1', signedIn: () => tab.who().length > 0, run: async () => {} });
    const login0 = tab.login; tab.login = (...a) => { const r = login0(...a); tab.q.drain(); return r; };   // the page runs the waiting scans after every sign-in (weld-1.html: scanQueue.drain())
    /** a phone scan as the relay document carries it: the real scan time, the scan id, and the moment the phone pushed it */
    tab.scan = (order, o = {}) => { const at = o.at != null ? o.at : wall(), id = o.id || ('s' + (++scanN) + 'x' + (Math.random() * 1e6 | 0)), sent = o.sent != null ? o.sent : wall(); return tab.q.offer(order, { 'Order Number': order, 'Shipping Label Timestamps': iso(at), 'Staff Note': JSON.stringify({ v: 1, id, at, sent, q: 0 }) }); };
    return tab;
  };
  const mdocs = () => cur.all('Station_Activity').filter(d => d.action === 'matched').sort((a, b) => a.at - b.at);
  const flushAct = async () => { await advance(31000); };
  await section('2b · the Welding scanner: who a scan is credited to', async () => {
    if (!HAVE.ws3) { pending('the matched event', 'WS3 is not on main yet'); return; }
    await check('one matcher is credited, two: the one with the latest input, none: Unattributed; a welder never', async () => {
      world(); const pc = computer('bench'), tab = desk(pc);
      tab.login(T.A, 'welding'); await advance(MIN);
      tab.scan('3521000401'); await flushAct();
      let m = mdocs(); eq(m.length, 1); eq(m[0].person, 'Unattributed', 'only a welder is in: nobody in Matching'); eq(m[0].unattributed, true); eq(m[0].task, 'matching'); eq(m[0].session, '');
      tab.login(T.B, 'matching'); await advance(MIN); tab.scan('3521000402'); await flushAct();
      m = mdocs(); eq(m[1].person, T.B, 'one matcher'); eq(m[1].unattributed, undefined); ok(m[1].session.startsWith('welding__weld-1__Ray_Welder__matching__'), 'the credited person\'s session: ' + m[1].session);
      tab.login(T.C, 'matching'); await advance(MIN); tab.scan('3521000403'); await flushAct();
      m = mdocs(); eq(m[2].person, T.C, 'two matchers: the one who signed in last');
      await advance(MIN); tab.SS.touch(wall(), { name: T.B, task: 'matching' }); tab.scan('3521000404'); await flushAct();
      eq(mdocs()[3].person, T.B, 'input from Ray makes him the latest');
      tab.logout(T.B, 'matching'); tab.logout(T.C, 'matching'); await advance(MIN); tab.scan('3521000405'); await flushAct();
      eq(mdocs()[4].person, 'Unattributed', 'everybody left Matching: unattributed');
      for (const d of mdocs()) { eq(d.action, 'matched'); eq(d.orders, 0); eq(d.parts, 0); ok(d.person !== T.A, 'a welder is never credited: ' + d.person); }
      eq(tab.errors, [], 'no page errors');
    });
    await check('the same order within 10 s, the same scan id told twice, and a reload mid-queue are ONE scan; a later repeat is its own scan marked again', async () => {
      world(); const pc = computer('bench'); let tab = desk(pc);
      tab.login(T.B, 'matching'); await advance(MIN);
      eq(tab.scan('3521000501', { id: 'dup1' }), true); eq(tab.scan('3521000501', { id: 'dup1' }), false, 'the same scan id'); await advance(3000); eq(tab.scan('3521000501', { id: 'dup2' }), false, 'the same order 3 s later');
      await flushAct(); eq(mdocs().length, 1);
      await advance(11000); eq(tab.scan('3521000501', { id: 'dup3' }), true, 'a repeat after 10 s'); await flushAct(); const m = mdocs(); eq(m.length, 2); ok(/again/.test(m[1].detail), 'marked again: ' + m[1].detail);
      tab.close(); tab = desk(pc); await advance(MIN); tab.scan('3521000501', { id: 'dup3' }); await flushAct(); eq(mdocs().length, 2, 'a reload and the same scan id told again: still two documents (the matched event remembers the scan id)');
      const r = cur.get('Efficiency_Daily', `${TODAY}__${T.B}`); ok(r && r.stations.welding.matched === 2, 'the rollup counted two: ' + JSON.stringify(r && r.stations.welding));
    });
    await check('hostile relay data: a time in the future, an id with a path, a PIN as an order, a 70-digit order, NaN and objects never break the page or store the wrong thing', async () => {
      world(); const pc = computer('bench'), tab = desk(pc);
      tab.login(T.B, 'matching'); await advance(MIN);
      const q = tab.q;
      for (const relay of [null, undefined, 5, 'x', [], {}, { 'Staff Note': '{not json' }, { 'Staff Note': JSON.stringify({ at: 1e18, sent: 1e18, id: '../../x' }) }, { 'Staff Note': JSON.stringify({ at: -5, sent: NaN, id: { a: 1 } }) }, { 'Shipping Label Timestamps': '9999-12-31T00:00:00Z' }, { 'Staff Note': JSON.stringify({ at: wall() + 5 * HOUR, sent: wall() + 5 * HOUR, id: 'future' }) }]) {
        try { q.offer('3521000' + (600 + (scanN++ % 300)), relay); } catch (e) { throw new Error('offer threw for ' + JSON.stringify(relay) + ': ' + e.message); }
      }
      for (const order of [PIN, '4829', '12345678', '9'.repeat(70), '', '   ', 'abc', '35210006\u0000', { a: 1 }, ['3521000999'], null, undefined]) { try { q.offer(order); } catch (e) { throw new Error('offer threw for ' + String(order) + ': ' + e.message); } }
      await flushAct();
      const m = mdocs(); ok(m.length >= 5, 'the usable scans were stored: ' + m.length);
      for (const d of m) { ok(d.at <= wall() && d.at > wall() - 8 * 86400000, 'the scan time is never in the future or older than a week: ' + iso(d.at)); ok(!/^\d{4,8}$/.test(d.orderId) || d.orderId.length === 10, 'no PIN-like order: ' + d.orderId); ok(d.orderId.length <= 30, 'order id length'); ok(/^[\w.:-]{8,100}$/.test(d.id), 'id ' + d.id); }
      ok(!cur.dump().includes(PIN), 'the PIN-like order was not stored');
      eq(tab.errors, []);
    });
    await check('a scan when nobody is signed in waits, shows its note, and after the next sign-in is recorded once with its real time', async () => {
      world(); const pc = computer('bench'), tab = desk(pc);
      const t0 = wall(); tab.scan('3521000701'); await advance(5000); tab.scan('3521000702'); await advance(MIN);
      eq(tab.q.count(), 2); eq(mdocs().length, 0, 'nothing is recorded while nobody is in (the page has no identity to write under)');
      ok(/waiting for a sign-in/.test(tab.w.document.getElementById('stationScanQueueNote').textContent), 'the note says so');
      tab.login(T.B, 'matching'); await advance(2 * MIN); await flushAct();
      const m = mdocs(); eq(m.length, 2, JSON.stringify(m.map(d => [d.orderId, d.person]))); ok(m[0].at >= t0 - 1000 && m[0].at <= t0 + 2000, 'the real scan time: ' + iso(m[0].at) + ' vs ' + iso(t0));
      eq(new Set(m.map(d => d.id)).size, 2);
    });
    await check('a scan from before midnight is never credited to whoever signs in after it', async () => {
      world('2026-10-08T03:55:00Z');                                         // 23:55 New York
      const pc = computer('bench'), tab = desk(pc);
      tab.scan('3521000801'); await goTo(Z('2026-10-08T04:30:00Z'));         // it waits through midnight
      tab.login(T.B, 'matching'); await advance(2 * MIN); await flushAct();
      const m = mdocs(); eq(m.length, 1); eq(m[0].person, 'Unattributed', 'yesterday\'s scan is not Ray\'s: ' + m[0].person); eq(m[0].day, '2026-10-07', 'it belongs to the day it was scanned');
    });
    await check('a page that crashes with scans recorded but not sent: the next page load sends them, once, under the person who scanned', async () => {
      world(); const pc = computer('bench'); let tab = desk(pc);
      tab.login(T.B, 'matching'); await advance(MIN); tab.scan('3521000901'); tab.scan('3521000902'); await advance(1000);
      ok(JSON.parse(pc.ls('station_activity_q.weld-1') || '[]').length === 2, 'two events wait in storage');
      tab.kill(); await advance(20 * MIN);
      tab = desk(pc); await advance(MIN); await flushAct();
      const m = mdocs(); eq(m.length, 2); ok(m.every(d => d.person === T.B), 'credited to the person who scanned (the event was written when the scan arrived): ' + m.map(d => d.person));
      await advance(5 * MIN); eq(mdocs().length, 2, 'sent once');
    });
    await check('the scan is credited by who was signed in at the SCAN time when the phone sent it late (offline replay)', async () => {
      world(); const pc = computer('bench'), tab = desk(pc);
      tab.login(T.A, 'matching'); await advance(MIN);
      const scanAt = wall(); await advance(4 * MIN);                           // Tess scanned an earring; the phone had no signal
      tab.logout(T.A, 'matching'); await advance(MIN); tab.login(T.B, 'matching'); await advance(2 * MIN);
      tab.scan('3521001001', { at: scanAt, sent: wall(), id: 'late1' });       // the phone is back: it pushes the old scan now
      await flushAct();
      const m = mdocs(); eq(m.length, 1); eq(m[0].person, T.A, 'Tess was in Matching when the earring was scanned; Ray signed in afterwards: ' + m[0].person);
    }, { known: 'D1' });
  });
}

/* ═════════════════════════ 3 · Laser or Design in the Sorter app (LD1: charm-nest-role.js, the page's own sign-in glue, the role on sessions and events) ═════════════════════════ */
/** the Sorter page's own sign-in glue, taken from charm-nest-1.html itself so the test cannot drift from the page */
function sorterGlue() {
  const html = read('charm-nest-1.html'), at = html.indexOf('const roleOn = () => !!(window.CNRole');
  if (at < 0) return null;
  const from = html.lastIndexOf('<script>', at), to = html.indexOf('</script>', at);
  return html.slice(from + '<script>'.length, to);
}
/** the Sorter app on a computer: the real station-session.js, station-activity.js and charm-nest-role.js, the page's real glue, a stand-in for the name bar */
function sorter(pc, o = {}) {
  const tab = openTab(pc, Object.assign({ page: 'charm-nest-1.html', station: 'sorter', device: 'charm-nest-1', scripts: ['session', 'activity', 'charm-nest-role.js'], init: false }, o));
  const w = tab.w, ls = () => w.localStorage; tab.bars = []; tab.toasts = []; tab.bar = null;
  w.CNEmployee = { name: () => ls().getItem('cn.employee') || '', roleBar: spec => { tab.bars.push(spec); tab.bar = spec && spec.state !== 'off' ? spec : null; return true; } };
  w.CN = { toast: t => tab.toasts.push(String(t)) };
  w.B = {}; Object.defineProperty(w.B, 'employee', { configurable: true, enumerable: true, get: () => ls().getItem('cn.employee') || '', set: v => { v = String(v == null ? '' : v).trim(); if (v) ls().setItem('cn.employee', v); else ls().removeItem('cn.employee'); } });
  w.eval(sorterGlue());
  tab.name = n => { w.B.employee = n; };
  tab.asked = () => tab.bars.filter(b => b.state === 'ask').length;
  tab.pick = r => { const b = tab.bar; if (!b || b.state !== 'ask') throw new Error('the question is not on screen: ' + JSON.stringify(tab.bars.map(x => x.state))); b.onPick(r); };
  tab.role = () => w.CNRole.role(); tab.state = () => w.CNRole.state();
  tab.press = (o2) => w.StationActivity.log('scan', Object.assign({ orderId: '3521000' + (200 + (++evN % 700)) }, o2 || {}));
  return tab;
}
async function laserDesign() {
  const DANA = 'Dana Laser', BEN = 'Ben Design';
  const mine = name => stationDocs().filter(d => d.person === name).sort((a, b) => a.startAt - b.startAt);
  const isLookup = rec => rec.body && rec.body.stationAdmin !== undefined;
  await section('3 · Laser or Design in the Sorter app', async () => {
    if (!HAVE.ld1 || !sorterGlue()) { pending('the Laser or Design question', 'LD1 (charm-nest-role.js and the Sorter page glue) is not on main yet'); return; }

    await check('the door keeps a session role only for station laser or design with role equal to the station; every other value is dropped, never an error, never stored raw', async () => {
      world(); let n = 0;
      const cases = [['laser', 'laser', 'laser'], ['design', 'design', 'design'], ['laser', 'design', ''], ['design', 'laser', ''], ['sorter', 'laser', ''], ['welding', 'design', ''], ['assembly', 'laser', ''],
        ['laser', { a: 1 }, ''], ['laser', ['laser'], ''], ['laser', '__proto__', ''], ['laser', 'LASER', ''], ['laser', ' laser', ''], ['laser', 'laser\u0000', ''], ['laser', PIN, ''], ['design', 482915, ''], ['laser', null, ''], ['laser', undefined, '']];
      for (const [station, role, kept] of cases) {
        const s = SESS({ station, device: 'charm-nest-1', person: 'Role Tester', computerId: 'pc-ROLE' + String(++n).padStart(8, '0'), role }); const r = await sess(s);
        eq(r.status, 200, `${station}/${JSON.stringify(role)}: ${JSON.stringify(r.body)}`); const d = sdoc(s.id); eq(d.role === undefined ? '' : d.role, kept, `${station}/${JSON.stringify(role)} stored role`);
        ok(!JSON.stringify(d).includes(PIN), 'no PIN in the document');
      }
    });

    await check('an activity event with a role: kept for laser and design only; one with NO role is simply stored without; a hostile role is dropped, never an error', async () => {
      world(); const evs = [EV({ station: 'laser', role: 'laser' }), EV({ station: 'design', role: 'design' }), EV({ station: 'sorter' }), EV({ station: 'laser', role: 'admin' }), EV({ station: 'laser', role: { x: 1 } }), EV({ station: 'sorter', role: '__proto__' }), EV({ station: 'laser', role: PIN })];
      const r = await acts(evs); eq(r.status, 200, JSON.stringify(r.body)); const d = cur.all('Station_Activity'); eq(d.length, 7, 'every event stored');
      const by = id => d.find(x => x.id === id); eq(by(evs[0].id).role, 'laser'); eq(by(evs[1].id).role, 'design'); for (const i of [2, 3, 4, 5, 6]) ok(by(evs[i].id).role === undefined, `event ${i} has no role stored: ${by(evs[i].id).role}`);
    });

    await check('the Admin (any spelling) is never asked and signs in as before: station sorter, no role, no question, a reload asks the door quietly and goes on with the same session', async () => {
      for (const nm of ['Paul K', 'paul  k', 'PAUL_K', 'Paul']) {
        world(); const pc = computer('adm'), tab = sorter(pc); tab.name(nm); await advance(1500);
        eq(tab.state(), 'admin', nm); eq(tab.asked(), 0, `${nm}: never asked`); eq(tab.role(), ''); const s = tab.kind('start'); eq(s.length, 1, `${nm}: one start`); eq(s[0].station, 'sorter'); ok(!s[0].role, 'no role on the Admin\'s session');
        ok(tab.press() === true, 'an Admin\'s press is recorded'); await advance(31000); const ev = cur.all('Station_Activity'); eq(ev.length, 1); eq(ev[0].station, 'sorter'); ok(ev[0].role === undefined, 'no role on the Admin\'s event');
        tab.close(); const t2 = sorter(pc); await advance(3000); eq(t2.state(), 'admin', 'a reload: the Admin again'); eq(t2.asked(), 0); eq(t2.kind('start').length, 0, 'no second start after a reload'); eq(open_().length, 1, dumpSessions());
        ok(![...pc.mem.values()].some(v => /"admin"\s*:\s*true/.test(v)), 'the Admin answer is never stored');
      }
    });

    await check('a non-Admin is asked ONCE per sign-in: nobody is signed in and nothing is recorded until the answer; then the session and every event carry the role', async () => {
      world(); const pc = computer('d'), tab = sorter(pc); tab.name(DANA); await advance(1500);
      eq(tab.state(), 'ask'); ok(tab.asked() >= 1, 'asked'); eq(tab.kind('start').length, 0, 'nobody is signed in yet'); eq(tab.press(), false, 'no event before the answer'); eq(tab.w.StationActivity.pending(), 0);
      tab.name(DANA); tab.name(DANA); await advance(1000); eq(tab.kind('start').length, 0, 'asking again is not a sign-in');
      tab.pick('laser'); await advance(1500); eq(tab.state(), 'role'); eq(tab.role(), 'laser');
      const s = tab.kind('start'); eq(s.length, 1, 'one start'); eq(s[0].station, 'laser'); eq(s[0].role, 'laser'); eq(s[0].device, 'charm-nest-1'); eq(sdoc(s[0].id).role, 'laser');
      ok(tab.press(), 'a press is recorded'); await advance(31000); const ev = cur.all('Station_Activity'); eq(ev.length, 1); eq(ev[0].station, 'laser'); eq(ev[0].role, 'laser'); eq(ev[0].session, s[0].id);
      const before = tab.asked(); tab.name(DANA); await advance(1000); eq(tab.asked(), before, 'the next approval sets the same name: not asked again'); eq(tab.kind('start').length, 1, 'no second start');
      eq(tab.errors, []);
    });

    await check('role switching, including seven switches in one breath: sessions follow one another and never overlap, only the last is open, each earlier one ended "switched"; nonsense roles change nothing', async () => {
      world(); const pc = computer('sw'), tab = sorter(pc); tab.name(DANA); await advance(1500); tab.pick('laser'); await advance(1500);
      const R = tab.w.CNRole; for (let i = 0; i < 7; i++) R.switchTo(R.other(tab.role())); await advance(5000);
      const all = mine(DANA); eq(all.length, 8, dumpSessions()); eq(all.filter(d => d.endAt == null).length, 1, 'one open');
      for (let i = 0; i < all.length - 1; i++) { ok(all[i].endAt != null && all[i].endAt <= all[i + 1].startAt, `session ${i} ends before session ${i + 1} starts: ${iso(all[i].endAt || 0)} ${iso(all[i + 1].startAt)}`); eq(all[i].endReason, 'switched', 'switched'); }
      ok(['laser', 'design'].includes(all[7].station) && all[7].station === tab.role() && all[7].role === tab.role(), 'the open one is the current role: ' + dumpSessions()); ok(all.every(d => d.station === d.role && d.device === 'charm-nest-1'), 'station is role on every one');
      const was = tab.role(); for (const bad of ['admin', '__proto__', 'constructor', '', null, undefined, 5, {}, ['laser'], 'LASER']) { R.switchTo(bad); R.choose(bad); } await advance(2000); eq(tab.role(), was, 'nothing changed: ' + JSON.stringify([tab.role(), was, tab.w.localStorage.getItem('cn.role')])); eq(mine(DANA).length, 8, 'no new session: ' + dumpSessions());
      R.switchTo(was); eq(mine(DANA).length, 8, 'the same role again: nothing happens');
    });

    await check('a slow, a failing and an out-of-order Admin answer: the person is asked (fail closed), a late answer for an older name never changes the newer person, and nobody is signed in by an answer for somebody else', async () => {
      for (const mode of ['offline', '500', 'slow']) {
        world(); const pc = computer('f'), tab = sorter(pc);
        if (mode === 'offline') tab.setOnline(false); else if (mode === '500') tab.failIf = rec => isLookup(rec) && 500; else tab.hold = rec => isLookup(rec);
        tab.name('Paul K'); await advance(mode === 'slow' ? 7000 : 1500); eq(tab.state(), 'ask', `${mode}: not told is not the Admin: asked`); eq(tab.kind('start').length, 0, `${mode}: no session before the answer`);
        if (mode === 'slow') { await tab.release(); await advance(1000); eq(tab.state(), 'ask', 'a late "true" does not flip a person who was already asked'); }
        eq(tab.errors, [], mode + ': no page errors');
      }
      world(); let pc = computer('race'), tab = sorter(pc); tab.hold = rec => isLookup(rec); tab.name(DANA); await advance(100); tab.name(BEN); await advance(100); eq(tab.parked.length, 2);
      await tab.release([1, 0]); await advance(1500); eq(tab.state(), 'ask', 'Ben is asked'); tab.pick('design'); await advance(1500);
      eq(mine(DANA).length, 0, 'Dana has no session'); eq(mine(BEN).length, 1); eq(mine(BEN)[0].station, 'design'); eq(tab.w.localStorage.getItem('cn.employee'), BEN);
      world(); pc = computer('gone'); tab = sorter(pc); tab.hold = rec => isLookup(rec); tab.name('Paul K'); await advance(100); tab.name(''); await advance(100); await tab.release(); await advance(1500);
      eq(tab.state(), 'none', 'signed out while the door was thinking: the answer for Paul K changes nothing'); eq(stationDocs().length, 0, 'no session for a name that left');
    });

    await check('an Admin who was offline at sign-in picks a role (asked, fail closed); when the network is back the door says Admin: the role session ends "switched" and the sorter session starts', async () => {
      world(); const pc = computer('off'), tab = sorter(pc); tab.setOnline(false); tab.name('Paul K'); await advance(7000); eq(tab.state(), 'ask'); tab.pick('laser'); await advance(2000);
      ok(tab.w.CNRole.unknown(), 'the page knows it was not told'); tab.setOnline(true); await advance(3000);
      eq(tab.state(), 'admin', 'now the Admin'); eq(tab.role(), ''); const all = mine('Paul K'); ok(all.length >= 1, dumpSessions());
      const open = all.filter(d => d.endAt == null); eq(open.length, 1, dumpSessions()); eq(open[0].station, 'sorter'); ok(!open[0].role, 'the Admin\'s open session has no role');
      ok(all.filter(d => d.endAt).every(d => d.endReason === 'switched' && d.station === 'laser'), 'the laser one ended switched: ' + dumpSessions());
    });

    await check('a reload keeps the name and the role and goes on with the SAME session (no second start); after 20 minutes away it is a new session under the same role; the question is not asked again', async () => {
      world(); const pc = computer('rl'), tab = sorter(pc); tab.name(DANA); await advance(1500); tab.pick('design'); await advance(1500); const id = tab.kind('start')[0].id;
      await advance(7 * MIN); tab.close(); const t2 = sorter(pc); await advance(3000);
      eq(t2.state(), 'role'); eq(t2.role(), 'design'); eq(t2.asked(), 0, 'not asked again'); eq(t2.kind('start').length, 0, 'no second start'); ok(t2.kind('beat').every(b => b.id === id), 'the same session goes on');
      t2.close(); await advance(20 * MIN); const t3 = sorter(pc); await advance(3000); eq(t3.role(), 'design'); const all = mine(DANA); ok(all.length === 2 && all[0].endAt && all[1].endAt == null && all[1].station === 'design', 'a new session under the same role: ' + dumpSessions());
    });

    await check('the sign-out (midnight, and AD1\'s idle and closing) takes the role with the name: the next sign-in is asked again, an event is never recorded under yesterday\'s role', async () => {
      world(iso(nyAt('2026-10-07', 23, 50))); const pc = computer('so'), tab = sorter(pc); tab.name(DANA); await advance(1500); tab.pick('laser'); await advance(MIN);
      await goTo(nyAt('2026-10-08', 0, 3)); eq(tab.state(), 'none', 'signed out at midnight'); eq(tab.w.localStorage.getItem('cn.role'), null, 'the role is gone from storage'); ok(tab.toasts.some(t => /midnight/i.test(t)), 'one calm line: ' + JSON.stringify(tab.toasts));
      eq(tab.press(), false, 'no event after the sign-out'); const d = mine(DANA); eq(d.length, 1); eq(d[0].endReason, 'midnight');
      tab.name(DANA); await advance(1500); eq(tab.state(), 'ask', 'asked again at the next sign-in'); eq(mine(DANA).length, 1, 'no new session until the answer');
    });

    await check('two tabs of the Sorter app on one computer: a switch in one is followed by the other without a second session, and a name cleared in one is cleared for both', async () => {
      world(); const pc = computer('two'), a = sorter(pc); a.name(DANA); await advance(1500); a.pick('laser'); await advance(1500); const b = sorter(pc); await advance(3000);
      eq(b.role(), 'laser', 'the second tab knows the role'); a.w.CNRole.switchTo('design'); await advance(2 * MIN);
      eq(mine(DANA).filter(d => d.endAt == null).length, 1, 'one open session after the switch: ' + dumpSessions()); eq(mine(DANA).filter(d => d.endAt == null)[0].station, 'design');
      b.press(); a.press(); await advance(31000); ok(cur.all('Station_Activity').every(e => e.station === 'design' && e.role === 'design'), 'both tabs record under the new role: ' + JSON.stringify(cur.all('Station_Activity').map(e => [e.station, e.role])));
    });

    await check('the Laser and the Design person show on the board under THEIR station, the Admin under Sorting; one name is one person; Sorting never lists the role people', async () => {
      world(); const pc1 = computer('b1'), pc2 = computer('b2'), pc3 = computer('b3'), a = sorter(pc1), b = sorter(pc2), c = sorter(pc3);
      a.name(DANA); b.name(BEN); c.name('Paul K'); await advance(1500); a.pick('laser'); b.pick('design'); await advance(4000);
      const r = await board({ op: 'live' }); eq(r.status, 200); const st = k => (r.body.stations || []).find(s => s.key === k) || {}; const nm = s => (s.people || []).map(p => (typeof p === 'string' ? p : p.name)).sort();
      ok(nm(st('laser')).includes(DANA) && !nm(st('laser')).includes(BEN), 'Laser: ' + nm(st('laser'))); ok(nm(st('design')).includes(BEN) && !nm(st('design')).includes(DANA), 'Design: ' + nm(st('design')));
      ok(nm(st('sorting')).some(n => /^Paul K/.test(n)) && !nm(st('sorting')).includes(DANA) && !nm(st('sorting')).includes(BEN), 'Sorting: ' + nm(st('sorting')));
      eq(new Set((r.body.signedIn || []).map(p => p.name)).size, 3, 'three people on');
    });

    await check('sandbox and real stay apart for the role: a sandbox Sorter writes its role session and events only to the Sandbox_ store', async () => {
      world(); const pc = computer('sb'), tab = sorter(pc, { sandbox: true }); tab.name(DANA); await advance(1500); tab.pick('laser'); await advance(1500); tab.press(); await advance(31000);
      eq(cur.all('Station_Sessions').length, 0, 'nothing in the real sessions'); eq(cur.all('Station_Activity').length, 0, 'nothing in the real events');
      const s = cur.all('Sandbox_Station_Sessions'); eq(s.length, 1); eq(s[0].role, 'laser'); eq(cur.all('Sandbox_Station_Activity').length, 1);
    });

    await check('hostile names at the Sorter: a PIN, digits with letters, markup, 300 characters, a prototype name: never stored raw, never a PIN, never a page error', async () => {
      for (const nm of [PIN, '482915 Dana', 'Dana <img src=x onerror=alert(1)>', 'x'.repeat(300), '__proto__', 'constructor', 'Paul K ' + PIN, 'D\u0000ana']) {
        world(); const pc = computer('h'), tab = sorter(pc); tab.name(nm); await advance(1500); if (tab.state() === 'ask') { tab.pick('laser'); await advance(1500); }
        eq(tab.errors, [], JSON.stringify(nm).slice(0, 30) + ': no page errors'); const dump = cur.dump(); ok(!dump.includes(PIN), JSON.stringify(nm).slice(0, 30) + ': the PIN is nowhere in the store');
        for (const d of stationDocs()) ok(d.person.length <= 80 && !/[\u0000-\u001f]/.test(d.person) && /\p{L}/u.test(d.person), 'a clean name: ' + JSON.stringify(d.person));
      }
    });
  });
}

/* ═════════════════════════ 5 · the Sorting fold ═════════════════════════ */
async function fold() {
  await section('5 · the Sorting fold', async () => {
    const day = '2026-10-07', at = (h, m = 0) => nyAt(day, h, m);
    const seedOld = st => {
      st.put('Efficiency_Daily', `${day}__Fold Tester`, rollDoc(day, 'Fold Tester', {
        sorting: statOf({ scans: 3, scanParts: 3, completes: 2, parts: 10, orders: 2, activeMs: 600000, firstAt: at(9, 5), lastAt: at(11, 40) }),
        sorter: statOf({ scans: 2, scanParts: 2, completes: 1, parts: 5, orders: 1, activeMs: 300000, firstAt: at(10, 5), lastAt: at(10, 50) }),
        qr: statOf({ prints: 4, activeMs: 120000, firstAt: at(10, 30), lastAt: at(10, 44) }) }, [], { touched: { 3521000001: { sorting: true, sorter: true }, 3521000002: { sorting: true }, 3521000003: { sorter: true, qr: true } } }));
      st.put('Efficiency_Daily', `${day}__Sort Only`, rollDoc(day, 'Sort Only', { sorting: statOf({ scans: 1, completes: 1, parts: 4, orders: 1, activeMs: 60000, firstAt: at(9, 10), lastAt: at(9, 40) }) }, ['3521000009']));
      st.put('Station_Sessions', 's-sorting', sessionDoc('s-sorting', 'Fold Tester', 'sorting', at(9), at(12), { device: 'sorting-1', lastSeenAt: at(12), minutes: 180 }));
      st.put('Station_Sessions', 's-sorter', sessionDoc('s-sorter', 'Fold Tester', 'sorter', at(10), at(11), { device: 'charm-nest-1', lastSeenAt: at(11), minutes: 60 }));
      st.put('Station_Sessions', 's-qr', sessionDoc('s-qr', 'Fold Tester', 'qr', at(10, 30), at(10, 45), { device: 'qr-printer', lastSeenAt: at(10, 45), minutes: 15 }));
    };
    await check('the overview shows no Sorter or QR Printer station, adds their counters to Sorting, and counts the time once', async () => {
      const st = world('2026-10-07T19:00:00Z'); seedOld(st);
      const o = await board({ op: 'overview', days: 1 }); ok(o.status === 200, 'overview ' + o.status + ' ' + o.raw.slice(0, 200));
      DBG('overview people', o.body.people.map(p => [p.name, p.totals, p.stations]));
      const p = o.body.people.find(x => x.name === 'Fold Tester'); ok(p, 'the person');
      const keys = p.stations.map(s => s.station);
      ok(!keys.includes('sorter') && !keys.includes('qr'), 'person stations: ' + keys);
      const so = p.stations.find(s => s.station === 'sorting'); ok(so && so.parts === 15 && so.prints === 4 && so.scans === 5, 'Sorting carries all three: ' + JSON.stringify(so));
      eq(p.totals.parts, 15, 'the person\'s parts are counted once');
      const bs = o.body.business.stations.map(s => s.station); ok(!bs.includes('sorter') && !bs.includes('qr'), 'business stations: ' + bs);
      eq(o.body.business.totals.parts, 19, 'the shop total is 15 + 4, not doubled');
      const bso = o.body.business.stations.find(s => s.station === 'sorting'); eq(bso.orders, 4, 'orders touched at Sorting: 3521000001, 2, 3 and 9, each once');
      ok(!JSON.stringify(o.body).match(/"station":"(sorter|qr)"/), 'no row of the overview carries the old keys');
    });
    await check('the person page: one Sorting row, time covered once (the Sorter app inside a Sorting session is not extra time)', async () => {
      const st = world('2026-10-07T19:00:00Z'); seedOld(st);
      const r = await board({ op: 'person', name: 'Fold Tester', range: 'day' }); ok(r.status === 200, 'person ' + r.status + ' ' + r.raw.slice(0, 200));
      DBG('person', Object.keys(r.body), r.body.stations, r.body.series && r.body.series[0], r.body.hours && r.body.hours.station);
      const keys = r.body.stations.map(s => s.station); ok(!keys.includes('sorter') && !keys.includes('qr'), 'stations: ' + keys);
      eq(r.body.kpis.parts.value, 15);
      const sg = r.body.series[r.body.series.length - 1]; eq(sg.day, day);
      eq(sg.signedMs, 180 * MIN, 'signed in 09:00 to 12:00 once, though the Sorter app and the QR Printer ran inside it: ' + sg.signedMs / MIN + ' min');
    });
    await check('the live board: Sorting is the only card, the Sorter app is one of its pages, a Laser person in the Sorter app is on Laser', async () => {
      const st = world('2026-10-07T19:00:00Z'); seedOld(st);
      await doorPost({ session: SESS({ station: 'sorter', device: 'charm-nest-1', person: 'Fold Tester', computerId: 'pc-FOLDPC000001' }) });
      await doorPost({ session: SESS({ station: 'laser', device: 'charm-nest-1', person: 'Laser Lena', computerId: 'pc-FOLDPC000002' }) });
      await doorPost({ session: SESS({ station: 'qr', device: 'qr-printer', person: 'Print Pat', computerId: 'pc-FOLDPC000003' }) });
      await doorPost({ live: { v: 1, event: 'work', station: 'sorter', device: 'charm-nest-1', person: 'Fold Tester', order: { kind: 'order', rid: '3521000042', scannedAt: wall() - 20000 } } });
      const r = await board({ op: 'live' }); ok(r.status === 200, 'live ' + r.status + ' ' + r.raw.slice(0, 200));
      const keys = r.body.stations.map(s => s.key); eq(keys.filter(k => k === 'sorter' || k === 'qr').length, 0, 'no Sorter or QR card: ' + keys);
      const sorting = r.body.stations.find(s => s.key === 'sorting'), laser = r.body.stations.find(s => s.key === 'laser');
      ok(sorting.people.concat(sorting.names || []).some(p => (p.name || p) === 'Fold Tester'), 'Fold Tester is on Sorting: ' + JSON.stringify(sorting.people));
      ok(sorting.people.concat(sorting.names || []).some(p => (p.name || p) === 'Print Pat'), 'the QR Printer person is on Sorting');
      ok(!JSON.stringify(sorting.people).includes('Laser Lena'), 'a Laser person at the Sorter app is not on Sorting');
      ok(JSON.stringify(laser.people).includes('Laser Lena'), 'Laser Lena is on Laser: ' + JSON.stringify(laser.people));
      eq(sorting.current.length, 1, 'the order in hand at the Sorter app is on the Sorting card');
      eq(r.body.signedIn.filter(p => p.name === 'Fold Tester').length, 1, 'one person at two pages of Sorting is signed in once');
    });
    await check('orders: a filter on Sorting finds orders worked at the old Sorter and QR keys, an old filter on those keys never crashes, one order is one row', async () => {
      const st = world('2026-10-07T19:00:00Z'); seedOld(st);
      for (const [id, who] of [['3521000001', 'Fold Tester'], ['3521000003', 'Fold Tester']]) {
        st.put('Station_Activity', 'ev-' + id, { id: 'ev-' + id, station: id === '3521000001' ? 'sorter' : 'qr', device: id === '3521000001' ? 'charm-nest-1' : 'qr-printer', person: who, action: 'complete', orderId: id, parts: 1, orders: 1, detail: '', at: at(10, 20), seq: 1, sincePrevMs: 1000, ts: at(10, 20), serverAt: at(10, 20), day, hour: '10', v: 1 });
      }
      const all = await board({ op: 'personOrders', name: 'Fold Tester', range: 'day', limit: 50 }); ok(all.status === 200, 'personOrders ' + all.status + ' ' + all.raw.slice(0, 160));
      DBG('personOrders', all.body.total, (all.body.orders || []).map(o => Object.keys(o)));
      const sorting = await board({ op: 'personOrders', name: 'Fold Tester', range: 'day', station: 'sorting', limit: 50 });
      eq(sorting.body.total, all.body.total, 'Sorting filter = all of this person\'s orders (they were all worked at Sorting\'s three keys): ' + sorting.body.total + ' vs ' + all.body.total);
      for (const old of ['sorter', 'qr']) { const r = await board({ op: 'personOrders', name: 'Fold Tester', range: 'day', station: old, limit: 50 }); ok(r.status === 200, `an old ${old} filter: ${r.status}`); }
      const ids = (all.body.orders || []).map(o => String(o.orderId || o.id || o.rid)); eq(new Set(ids).size, ids.length, 'no order twice: ' + ids);
      const one = await board({ op: 'orders', orderId: '3521000001' }); ok(one.status === 200, 'orders ' + one.status);
      DBG('orders', Object.keys(one.body), (one.body.steps || []).map(x => [x.station, x.person]));
      ok((one.body.steps || []).every(x => x.station !== 'sorter' && x.station !== 'qr'), 'an order\'s steps show Sorting: ' + JSON.stringify((one.body.steps || []).map(x => x.station)));
    });
    await check('a new event written under the old key counts once in the rollup and shows as Sorting', async () => {
      const st = world('2026-10-07T19:00:00Z');
      const e1 = EV({ station: 'sorter', device: 'charm-nest-1', person: 'Fold Tester', action: 'complete', parts: 3, orders: 1, orderId: '3521000555' });
      const e2 = EV({ station: 'sorting', device: 'sorting-1', person: 'Fold Tester', action: 'complete', parts: 2, orders: 1, orderId: '3521000555' });
      const e3 = EV({ station: 'qr', device: 'qr-printer', person: 'Fold Tester', action: 'print', orderId: '3521000555' });
      await acts([e1, e2, e3, e1, e2]);                                        // (a replay of two of them)
      eq(cur.count('Station_Activity'), 3, 'each event stored once');
      const o = await board({ op: 'overview', days: 1 }); const p = o.body.people.find(x => x.name === 'Fold Tester');
      eq(p.totals.parts, 5); eq(o.body.business.totals.parts, 5);
      const so = p.stations.find(s => s.station === 'sorting'); ok(so && !p.stations.some(s => s.station === 'sorter' || s.station === 'qr'), JSON.stringify(p.stations));
      const pr = await board({ op: 'person', name: 'Fold Tester', range: 'day' }); eq(pr.body.kpis.orders.value, 1, 'the order touched at three keys is one order: ' + JSON.stringify(pr.body.kpis.orders));
      const feed = (o.body.feed || []).map(f => f.station); ok(!feed.includes('sorter') && !feed.includes('qr'), 'the feed shows Sorting: ' + feed);
    });
  });
}

/* ═════════════════════════ runner ═════════════════════════ */
(async () => {
  const t0 = REAL_NOW();
  await hostile();
  await welding();
  await scanner();
  await autoServer();
  await autoPage();
  await laserDesign();
  await fold();
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
