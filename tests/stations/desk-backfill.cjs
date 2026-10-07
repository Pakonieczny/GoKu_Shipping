// The desk numbers of the days recorded BEFORE desks were told apart (Paul, 7 Oct 2026: "the totals for assembly and shipping do not match"), recovered once per day from
// that day's events (op deskBackfill, netlify/functions/_deskBackfill.js). Offline: the REAL modules (_stationActivity, _deskBackfill, employeeEfficiency, the console's view
// models) over one in-memory Firestore with real transaction conflicts (a write that moved a document the transaction read makes it run again), a faked clock, invented names.
//   1  events for assembly-1..4 and shipping-1..3 on days whose rollups have only the kind counters: after the op the desk counters add up to the kind totals, the touched
//      orders name their desk, one write per person-day, reads = the person-days' events once; running it twice changes nothing and reads no event
//   2  a live write racing the backfill (while its events are read, and while it commits) is counted once; a live write afterwards adds on top; a rollup the live path
//      creates is born with the marker
//   3  days older than 31 days, days to come, the sandbox and a wrong passcode are refused before anything is read or written
//   4  events with no device stay in the plain row (the console calls it "desk not recorded", the group total is the sum of its rows)
//   5  bounded and resumable: a few hundred events (or eight person-days) a call; a person-day too big or not lining up is marked and left alone
//   node tests/stations/desk-backfill.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module'), vm = require('vm'), fs = require('fs');
const root = path.join(__dirname, '../..');
const noNested = require('../charm-nest/_noNestedArrays.cjs');

/* ── one in-memory Firestore: nested increments, merge, counted reads and writes, and OPTIMISTIC transactions (a document changed since it was read = run again) ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const SERVER_TS = { __ts: true }, inc = n => ({ __inc: n });
const colls = new Map(), vers = new Map(), reads = [], writes = [], HOOK = { onQuery: null, beforeCommit: null };
let txRuns = 0;
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
const ver = p => vers.get(p) || 0;
const put = (c, id, d, o) => { const doc = apply(col(c).get(id), d, !!(o && o.merge), Date.now()); noNested(doc, c + '/' + id); col(c).set(id, doc); vers.set(c + '/' + id, ver(c + '/' + id) + 1); };
const kind = v => v instanceof Ts ? 'ts' : typeof v, val = v => v instanceof Ts ? v.m : v;
const refOf = (c, id) => ({ c, id, path: c + '/' + id });
const snapOf = r => { const d = col(r.c).get(r.id); reads.push({ c: r.c, doc: r.id }); return { id: r.id, exists: !!d, data: () => d ? clone(d) : undefined, ref: r }; };
function query(name, filters, order, lim, sel) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel), orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel), limit: n => query(name, filters, order, n, sel), select: (...f) => query(name, filters, order, lim, f),
    get: async () => {
      if (HOOK.onQuery) await HOOK.onQuery(name, filters);
      let docs = [...col(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => { const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false; const a = val(x), b = val(v); return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false; });
      if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
      else docs.sort((p, q) => (p.id < q.id ? -1 : p.id > q.id ? 1 : 0));
      if (lim != null) docs = docs.slice(0, lim);
      reads.push({ c: name, n: docs.length, select: sel || null, filters: filters.map(f => f[0]) });
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
    for (let attempt = 0; attempt < 6; attempt++) {
      txRuns++;
      const seen = new Map(), w = [], rd = r => { seen.set(r.path, ver(r.path)); return snapOf(r); };
      const tx = { get: async r => rd(r), getAll: async (...rs) => rs.map(rd), set: (r, d, o) => { w.push([r, d, o]); return tx; } };
      const out = await fn(tx);
      if (HOOK.beforeCommit) await HOOK.beforeCommit(w);
      if ([...seen].some(([p, v]) => ver(p) !== v)) continue;                       // (a document this transaction read was written meanwhile: run again, as Firestore does)
      for (const [r, d, o] of w) { writes.push([r.c, r.id]); put(r.c, r.id, d, o); }
      return out;
    }
    throw new Error('too much contention');
  }
};
const FV = { serverTimestamp: () => SERVER_TS, increment: inc, delete: () => null };
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { Timestamp: Ts, FieldValue: FV }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const SA = require(path.join(root, 'netlify/functions/_stationActivity.js'));
const DB = require(path.join(root, 'netlify/functions/_deskBackfill.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;

const PASS = 'synthetic-desk-pass-7q3m';
process.env.EDIT_PASSCODE = PASS;
const realNow = Date.now;
const NOW = Date.UTC(2026, 9, 7, 18, 0);                                          // 14:00 on 7 Oct in New York (EDT)
Date.now = () => NOW;
console.warn = console.log = console.info = () => {};
const say = s => process.stdout.write(s + '\n');
const eq = (a, b, msg) => assert.deepStrictEqual(a, b, msg);
const TODAY = '2026-10-07', D1 = '2026-10-05', D3 = '2026-10-03';
let ipN = 0;
const send = async (body, key) => { const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ipN % 200) }, body: JSON.stringify(Object.assign({ key: key === undefined ? PASS : key }, body)) }, fakeDb); return { status: r.statusCode, body: JSON.parse(r.body) }; };
const dropCaches = () => { const c = eff._t.cacheOf(fakeDb); c.memo.clear(); c.recent.clear(); c.fails.clear(); EP.resetCache(); };
const reset = () => { colls.clear(); vers.clear(); reads.length = 0; writes.length = 0; txRuns = 0; HOOK.onQuery = HOOK.beforeCommit = null; dropCaches(); };
const zero = () => { reads.length = 0; writes.length = 0; };
const evReads = () => reads.filter(r => r.c === 'Station_Activity').reduce((n, r) => n + (r.n != null ? r.n : 1), 0);
const rollWrites = () => writes.filter(w => w[0] === 'Efficiency_Daily').length;
const nyAt = (day, h, m = 0) => Date.UTC(+day.slice(0, 4), +day.slice(5, 7) - 1, +day.slice(8, 10), h + 4, m);   // (October is EDT)

/* ── the shop's events, stored as the activity door stores them (SA.clean), and the OLD rollups (kind counters only: no `devices`, orders touched as `true`) ── */
let seq = 0, onum = 0;
const storeEv = (day, h, m, station, device, person, action, o = {}) => {
  const raw = { id: `${(device || 'nodev').replace(/\W/g, '')}_${String(++seq).padStart(5, '0')}_${person.replace(/\W/g, '').slice(0, 4)}`, station, device, person, action, orderId: o.orderId || '', parts: o.parts || 0, orders: o.orders || 0,
    at: nyAt(day, h, m), sincePrevMs: 20000, seq, detail: '', session: 'sess-' + seq + '-abcdef', computer: 'pc-ABCDEFGHJKMN' };
  const r = SA.clean(raw, NOW, ''); assert(r.doc, 'event accepted: ' + JSON.stringify(raw));
  put('Station_Activity', r.doc.id, Object.assign({}, r.doc, { ts: new Ts(r.doc.serverAt) }));
  return r.doc;
};
const newOrder = () => String(3521000000 + ++onum);
/** a person's work at one station/device: n orders, each scanned then completed (parts p) */
function work(day, person, station, device, n, p, t0, docs, extra) {
  let t = t0;
  for (let i = 0; i < n; i++) {
    const o = newOrder();
    docs.push(storeEv(day, 9 + Math.floor(t / 60), t % 60, station, device, person, 'scan', { orderId: o, parts: p })); t += 2;
    if (extra && extra.print) { docs.push(storeEv(day, 9 + Math.floor(t / 60), t % 60, station, device, person, 'print', { orderId: o })); t += 1; }
    docs.push(storeEv(day, 9 + Math.floor(t / 60), t % 60, station, device, person, 'complete', { orderId: o, parts: p, orders: 1 })); t += 3;
  }
  return t;
}
const PLAIN = { increment: n => n, serverTimestamp: () => null };
function oldRollup(day, person, docs) {
  const r = SA.rollupPatch(PLAIN, null, day, person, docs, '');
  delete r.devices; delete r.deskBackfilled;
  for (const m of Object.values(r.touched || {})) for (const k of Object.keys(m)) m[k] = true;       // (before desks were told apart an order said only `true`)
  put('Efficiency_Daily', SA.rollupId(day, person), r);
  return r;
}
const PEOPLE = ['Ann A.', 'Ben B.', 'Cy C.', 'Di D.', 'Eve E.', 'Fay F.', 'Gus G.', 'Hal H.', 'Ina I.', 'Jo J.'];
/** one day of the shop: seven desks, a desk change, a page with no number, a mixed person, a person with other stations too */
function seedDay(day) {
  const by = {};
  const docs = p => by[p] || (by[p] = []);
  work(day, 'Ann A.', 'assembly', 'assembly-1', 3, 2, 0, docs('Ann A.'));
  let t = work(day, 'Ben B.', 'assembly', 'assembly-2', 2, 3, 0, docs('Ben B.'));
  docs('Ben B.').push(storeEv(day, 9 + Math.floor(t / 60), t % 60, 'assembly', 'assembly-2', 'Ben B.', 'undo', { orderId: docs('Ben B.')[1].orderId, parts: 3, orders: 1 })); t += 2;
  docs('Ben B.').push(storeEv(day, 9 + Math.floor(t / 60), t % 60, 'assembly', 'assembly-2', 'Ben B.', 'error', {}));
  t = work(day, 'Cy C.', 'assembly', 'assembly-3', 2, 1, 0, docs('Cy C.')); work(day, 'Cy C.', 'assembly', 'assembly-1', 1, 4, t + 5, docs('Cy C.'));
  work(day, 'Di D.', 'assembly', 'assembly-4', 2, 2, 0, docs('Di D.'));
  work(day, 'Eve E.', 'shipping', 'shipping-1', 2, 1, 0, docs('Eve E.'), { print: true });
  work(day, 'Fay F.', 'shipping', 'shipping-2', 3, 2, 0, docs('Fay F.'), { print: true });
  work(day, 'Gus G.', 'shipping', 'shipping-3', 1, 5, 0, docs('Gus G.'));
  work(day, 'Hal H.', 'assembly', 'assembly', 2, 3, 0, docs('Hal H.'));                                   // a page with NO number
  t = work(day, 'Ina I.', 'shipping', 'shipping-2', 1, 2, 0, docs('Ina I.')); work(day, 'Ina I.', 'shipping', '', 2, 2, t + 5, docs('Ina I.'));   // one desk, then events with no device
  t = work(day, 'Jo J.', 'assembly', 'assembly-2', 1, 2, 0, docs('Jo J.'));
  docs('Jo J.').push(storeEv(day, 10, 30, 'welding', 'weld-1', 'Jo J.', 'scan', { orderId: newOrder(), parts: 1 }), storeEv(day, 10, 31, 'sorting', 'sorting-1', 'Jo J.', 'complete', { orderId: newOrder(), parts: 1, orders: 1 }));   // other stations: read, never counted
  for (const [p, d] of Object.entries(by)) oldRollup(day, p, d);
  return by;
}
/** the numbers the desks must hold, worked out from the stored events alone (no code of the writer): { person -> { devices, plain } } over one day */
function oracle(day) {
  const evs = [...col('Station_Activity').values()].filter(d => d.day === day && /^(assembly|shipping)$/.test(d.station)).sort((a, b) => a.at - b.at || (a.id < b.id ? -1 : 1));
  const out = {};
  for (const e of evs) {
    const P = out[e.person] || (out[e.person] = { devices: {}, plainBy: { assembly: {}, shipping: {} }, touched: {} });
    const m = /^(assembly|shipping)-([1-9]\d?)$/.exec(e.device), dk = m && m[1] === e.station ? `${m[1]}-${+m[2]}` : '';
    const d = dk ? (P.devices[dk] || (P.devices[dk] = {})) : P.plainBy[e.station], add = (k, n) => { if (n) d[k] = (d[k] || 0) + n; };
    add('events', 1);
    if (e.action === 'scan' || e.action === 'matched') add('scans', 1);
    else if (e.action === 'complete') { add('completes', 1); add('parts', e.parts); add('orders', e.orders); }
    else if (e.action === 'undo') { add('undoParts', e.parts); add('undoOrders', e.orders); }
    if (dk) d.lastAt = Math.max(d.lastAt || 0, e.at);
    if (e.orderId) { const t = P.touched[e.orderId] || (P.touched[e.orderId] = {}); if (dk) t[e.station] = dk; else if (typeof t[e.station] !== 'string') t[e.station] = true; }
  }
  return out;
}
const rollup = (day, p) => col('Efficiency_Daily').get(SA.rollupId(day, p));
/** every rollup of a day must say what the oracle says: desk counters equal, orders name their desk, marked, desks never above the kind */
function checkDay(day, msg) {
  const O = oracle(day);
  for (const [p, o] of Object.entries(O)) {
    const r = rollup(day, p); assert(r, msg + ' ' + p);
    eq(r.devices || {}, o.devices, `${msg}: ${p}'s desk counters are what the events say`);
    eq(Object.fromEntries(Object.entries(r.touched || {}).filter(([id]) => o.touched[id])), o.touched, `${msg}: ${p}'s touched orders name their desk (or stay true with no desk)`);
    eq(r.deskBackfilled, true, `${msg}: ${p} is marked`);
    for (const kd of ['assembly', 'shipping']) {
      const st = r.stations && r.stations[kd]; if (!st) continue;
      for (const f of ['scans', 'completes', 'parts', 'orders']) {
        const desks = Object.entries(r.devices || {}).filter(([k]) => k.startsWith(kd + '-')).reduce((n, [, v]) => n + (v[f] || 0), 0);
        eq(desks + (o.plainBy[kd][f] || 0), st[f] || 0, `${msg}: ${p} ${kd} ${f}: the desks plus the events with no desk are the kind total`);
      }
    }
  }
  return O;
}
const drain = async (day, label) => {
  const agg = { calls: 0, processed: 0, written: 0, eventsRead: 0, busy: 0, skipped: 0, already: 0, bodies: [] };
  for (let i = 0; i < 20; i++) {
    dropCaches();
    const r = await send({ op: 'deskBackfill', day }); assert.strictEqual(r.status, 200, label + ' ' + JSON.stringify(r.body));
    agg.calls++; agg.bodies.push(r.body);
    for (const k of ['processed', 'written', 'eventsRead', 'busy', 'skipped', 'already']) agg[k] += r.body[k];
    if (!r.body.more) break;
  }
  return agg;
};
const overview = async (day, days = 1) => { dropCaches(); const r = await send({ op: 'overview', day, days }); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); return r.body; };
const stationOf = (ov, k) => ov.business.stations.find(s => s.station === k);
const J = o => JSON.parse(JSON.stringify(o));

(async () => {
  /* ═══ 1 · two old days: the op makes the desks add up, once ═══ */
  reset(); seedDay(TODAY); seedDay(D1);
  const before = await overview(TODAY);
  eq(before.deskPending, [TODAY], 'the overview of today says today still lacks its desk numbers (the days in view only)');
  eq((await overview(TODAY, 7)).deskPending, [TODAY, D1], 'a week in view: both days, newest first');
  const sa0 = stationOf(before, 'assembly'), sh0 = stationOf(before, 'shipping');
  eq([sa0.devices.reduce((n, d) => n + d.parts + d.scans + d.orders, 0), sh0.devices.reduce((n, d) => n + d.parts + d.scans + d.orders, 0)], [0, 0], 'before: every desk is empty');
  eq([sa0.unassigned.parts, sa0.unassigned.scans, sa0.unassigned.orders], [sa0.parts, sa0.scans, sa0.orders], 'before: everything sits in the plain row (nobody can tell the desks)');
  const kindBefore = J({ a: [sa0.parts, sa0.scans, sa0.orders], s: [sh0.parts, sh0.scans, sh0.orders], people: before.people.map(p => [p.name, p.totals.parts, p.totals.scans, p.totals.orders]) });

  const expectReads = day => PEOPLE.reduce((n, p) => n + [...col('Station_Activity').values()].filter(d => d.day === day && d.person === p).length, 0);
  const want = expectReads(TODAY);
  zero(); const a1 = await drain(TODAY, 'day 1');
  eq(a1.calls, 2, 'ten person-days, eight a call: two calls, the second finds the rest');
  eq([a1.bodies[0].more, a1.bodies[1].more, a1.bodies[0].processed, a1.bodies[1].processed], [true, false, 8, 2]);
  eq([a1.written, a1.busy, a1.skipped], [10, 0, 0], 'every person-day written once');
  eq(a1.eventsRead, want, 'one read per event of that day, once (every event of the ten person-days: ' + want + ')');
  eq(evReads(), want, 'the store agrees: the events were read once and nothing else was read from Station_Activity');
  eq(rollWrites(), 10, 'ONE update per person-day document');
  eq(writes.filter(w => w[0] === 'Station_Activity').length, 0, 'no event was written, rewritten or deleted');
  assert(reads.filter(r => r.c === 'Station_Activity').every(r => r.select && r.select.length >= 4 && !r.select.includes('detail') && !r.select.includes('session')), 'events are read with a field mask (only the fields the desks need)');
  assert(reads.filter(r => r.c === 'Station_Activity').every(r => r.filters.join() === 'person,day'), 'through the indexed query the Employee page already uses (person and day)');
  checkDay(TODAY, 'day 1');
  // the kind totals are the desks plus what carries no desk, for every field
  const after = await overview(TODAY);
  const sa1 = stationOf(after, 'assembly'), sh1 = stationOf(after, 'shipping');
  eq(J({ a: [sa1.parts, sa1.scans, sa1.orders], s: [sh1.parts, sh1.scans, sh1.orders], people: after.people.map(p => [p.name, p.totals.parts, p.totals.scans, p.totals.orders]) }), kindBefore, 'the kind totals and the people are exactly what they were: the backfill only moves numbers into the desks');
  for (const [s, k] of [[sa1, 'assembly'], [sh1, 'shipping']]) {
    for (const [f, g] of [['parts', 'parts'], ['scans', 'scans'], ['orders', 'orders']]) {
      const desks = s.devices.reduce((n, d) => n + d[f], 0);
      eq(desks + s.unassigned[g], s[f], `${k} ${f}: the desks and the plain row add up to the kind total`);
    }
  }
  eq([sa1.unassigned.parts, sa1.unassigned.scans, sa1.unassigned.orders], [6, 2, 2], 'Assembly: only Hal (a page with no number) is left in the plain row');
  eq([sh1.unassigned.parts, sh1.unassigned.scans, sh1.unassigned.orders], [4, 2, 2], 'Shipping: only the events with no device are left in the plain row');
  eq(sa1.devices.map(d => d.device + ':' + d.peopleNow.length), ['assembly-1:0', 'assembly-2:0', 'assembly-3:0', 'assembly-4:0']);
  eq(sa1.devices.map(d => d.parts), [2 * 3 + 4, 3 * 2 - 3 + 2, 1 * 2, 2 * 2], 'Assembly 1 (Ann and Cy), 2 (Ben net of his undo, and Jo), 3, 4: pieces finished');
  eq(sh1.devices.map(d => d.parts), [2, 2 * 3 + 2, 5], 'Shipping 1, 2 (Fay and Ina), 3');
  eq(after.deskPending, undefined, 'today is no longer listed');
  eq((await overview(TODAY, 7)).deskPending, [D1], 'only the other day is');
  say('1a the op: desks + plain row = kind totals for every field; kind totals and people unchanged; one write per person-day; ' + want + ' event reads once; field mask and the Employee page\'s own query');

  // twice: nothing changes, no event is read
  const snap = JSON.stringify([...col('Efficiency_Daily')].sort()), vSnap = JSON.stringify([...vers].filter(([k]) => /^Efficiency_Daily/.test(k)).sort());
  zero(); const again = await drain(TODAY, 'twice');
  eq([again.calls, again.bodies[0].pending, again.written, again.eventsRead], [1, 0, 0, 0], 'the second run finds nothing pending');
  eq(evReads(), 0, 'and reads no event'); eq(rollWrites(), 0, 'and writes no rollup');
  eq(JSON.stringify([...col('Efficiency_Daily')].sort()), snap, 'every rollup is exactly as it was');
  eq(JSON.stringify([...vers].filter(([k]) => /^Efficiency_Daily/.test(k)).sort()), vSnap, 'not even a version moved');
  // a lost marker is harmless too: the max-merge never adds twice
  const ann = SA.rollupId(TODAY, 'Ann A.'), annDoc = clone(col('Efficiency_Daily').get(ann));
  col('Efficiency_Daily').get(ann).deskBackfilled = false; vers.set('Efficiency_Daily/' + ann, ver('Efficiency_Daily/' + ann) + 1);
  eq(DB.gapOf(col('Efficiency_Daily').get(ann)), 0, 'Ann\'s desks account for every event, so even without the marker there is nothing pending');
  eq(DB.pending(col('Efficiency_Daily').get(ann)), false);
  col('Efficiency_Daily').set(ann, annDoc);
  // the other day, the same
  const a2 = await drain(D1, 'day 2'); eq([a2.written, a2.busy], [10, 0]); checkDay(D1, 'day 2');
  eq((await overview(TODAY, 7)).deskPending, undefined, 'a week in view: nothing left to ask');
  say('1b twice: nothing pending, zero event reads, zero writes, every document and version unchanged; both days done');

  /* ═══ 2 · a live write racing the backfill ═══ */
  const liveRaw = (person, station, device, o = {}) => Object.assign({ id: `live_${device}_${++seq}_${person.replace(/\W/g, '').slice(0, 3)}`, station, device, person, action: 'scan', orderId: newOrder(), parts: 1, orders: 0, at: NOW - 1000, sincePrevMs: 1000, seq, detail: '', session: 'sess-live-abcdef', computer: 'pc-ABCDEFGHJKMN' }, o);
  const live = async raw => { const r = await SA.add(fakeDb, FV, [raw]); assert.strictEqual(r.written, 1); };
  // 2a · the live event is counted while the backfill is reading the person's events
  reset(); seedDay(TODAY);
  let fired = 0;
  HOOK.onQuery = async (name, filters) => { if (name === 'Station_Activity' && !fired && filters.find(f => f[0] === 'person')) { fired++; await live(liveRaw('Ann A.', 'assembly', 'assembly-1')); } };
  zero(); const r2a = await drain(TODAY, 'race a');
  eq(fired, 1); eq([r2a.busy, r2a.skipped], [0, 0], 'the person-day was read again, not left');
  assert(r2a.eventsRead > want, 'the person-day moved by the live write was read a second time (' + r2a.eventsRead + ' reads against ' + want + ')');
  checkDay(TODAY, 'race a');
  eq(rollup(TODAY, 'Ann A.').devices['assembly-1'].events, 7, 'Ann\'s three orders (6 events) and the live scan: counted ONCE (not 6, not 8)');
  eq(Object.values(rollup(TODAY, 'Ann A.').touched).filter(m => m.assembly === 'assembly-1').length, 4, 'all four orders name their desk');
  say('2a a live write landing while the events are read: read again, counted once');
  // 2b · it lands while the backfill commits (a real conflict: the transaction runs again)
  reset(); seedDay(TODAY);
  fired = 0; const runs0 = txRuns;
  HOOK.beforeCommit = async w => { const x = w.find(([r, d]) => r.c === 'Efficiency_Daily' && d.deskBackfilled === true); if (x && !fired) { fired++; await live(liveRaw(x[0].id.split('__')[1], 'assembly', x[0].id.endsWith('Ann A.') ? 'assembly-1' : 'assembly-2')); } };
  zero(); const r2b = await drain(TODAY, 'race b');
  eq(fired, 1); eq([r2b.busy, r2b.skipped], [0, 0]); assert(txRuns > runs0 + 11, 'the conflict made the transaction run again');
  checkDay(TODAY, 'race b');
  eq(rollWrites(), 10 + 1, 'ten person-day writes by the backfill and the one live write: no write was applied twice');
  say('2b a live write landing while the backfill commits: the conflict re-runs it, counted once');
  // 2c · after the backfill the live path just adds on top; a rollup it creates is born with the marker
  const evN = rollup(TODAY, 'Ben B.').devices['assembly-2'].events;
  await live(liveRaw('Ben B.', 'assembly', 'assembly-2'));
  eq(rollup(TODAY, 'Ben B.').devices['assembly-2'].events, evN + 1, 'a live event after the backfill is one more');
  eq(rollup(TODAY, 'Ben B.').deskBackfilled, true);
  await live(liveRaw('Newbie N.', 'assembly', 'assembly-3'));
  const nb = rollup(TODAY, 'Newbie N.'); eq([nb.deskBackfilled, Object.keys(nb.devices)], [true, ['assembly-3']], 'a rollup the live path creates is born with the marker, desks counted from its first event');
  eq(DB.pending(nb), false);
  zero(); const r2c = await drain(TODAY, 'after'); eq([r2c.bodies[0].pending, r2c.eventsRead], [0, 0], 'nothing pending after live writes either');
  checkDay(TODAY, 'live after');
  say('2c live writes after the backfill add on top; a new rollup is born with the marker');

  /* ═══ 3 · refusals ═══ */
  reset(); seedDay(TODAY);
  const rs = async (b, key) => { dropCaches(); zero(); const r = await send(b, key); return { r, touched: reads.filter(x => /Efficiency_Daily|Station_Activity/.test(x.c)).length + writes.filter(x => /Efficiency_Daily|Station_Activity/.test(x[0])).length }; };
  for (const [day, why] of [['2026-08-30', 'long ago'], ['2026-09-05', '32 days ago']]) { const x = await rs({ op: 'deskBackfill', day }); eq([x.r.status, x.r.body.ok, /older than 31/.test(x.r.body.error), x.touched], [400, false, true, 0], `${why}: refused, nothing read, nothing written`); }
  { const x = await rs({ op: 'deskBackfill', day: '2026-09-06' }); eq([x.r.status, x.r.body.pending, x.r.body.done], [200, 0, true], '31 days ago is the oldest allowed'); }
  for (const [b, why] of [[{ op: 'deskBackfill', day: '2026-10-08' }, 'tomorrow'], [{ op: 'deskBackfill', day: 'yesterday' }, 'not a day'], [{ op: 'deskBackfill' }, 'no day'], [{ op: 'deskBackfill', day: TODAY, sandbox: true }, 'sandbox']]) {
    const x = await rs(b); eq([x.r.status, x.r.body.ok, x.touched], [400, false, 0], `${why}: refused before anything was read`);
  }
  { const x = await rs({ op: 'deskBackfill', day: TODAY }, 'not-the-passcode'); eq([x.r.status, x.touched], [401, 0], 'the same passcode gate as every other op'); }
  // the overview never offers a day the op would refuse
  put('Efficiency_Daily', SA.rollupId('2026-08-30', 'Old O.'), { day: '2026-08-30', person: 'Old O.', v: 1, events: 2, stations: { assembly: { scans: 2 } } });
  const ovOld = await overview('2026-09-20', 30); eq(ovOld.deskPending, undefined, 'a day older than 31 days in view is not offered');
  say('3 refused before anything is read or written: older than 31 days, tomorrow, not a day, the sandbox, a wrong passcode; the overview never offers such a day');

  /* ═══ 4 · events with no device stay in the plain row, labelled for what they are ═══ */
  await drain(TODAY, 'section 4');
  const rest = rollup(TODAY, 'Hal H.'); eq([rest.deskBackfilled, rest.devices, rest.deskSkipped], [true, undefined, undefined], 'Hal\'s events carry no desk: marked done (never asked again), no desk counters invented');
  const ina = rollup(TODAY, 'Ina I.'); eq(Object.keys(ina.devices), ['shipping-2'], 'a mixed person: the desk events go to the desk, the rest stays plain');
  assert(Object.values(ina.touched).filter(m => m.shipping === true).length === 2 && Object.values(ina.touched).filter(m => m.shipping === 'shipping-2').length === 1);
  /* the console: the plain row is "(desk not recorded)", only when it has data; the group's total is the sum of its rows */
  const win = { document: { getElementById: () => null, addEventListener() {}, createElement: () => ({}) }, console }; win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), win);
  const Eff = win.Efficiency;
  const ovT = await overview(TODAY); ovT.deskPending = undefined;
  const rows = J(Eff.stationRowsOf(Eff.norm(J(ovT))));
  const grp = k => { const i = rows.findIndex(r => r.head === k); let j = i + 1; while (j < rows.length && !rows[j].head && /^(assembly|shipping)/.test(rows[j].key)) j++; return { head: rows[i], rows: rows.slice(i + 1, j) }; };
  for (const k of ['assembly', 'shipping']) {
    const g = grp(k);
    for (const f of ['parts', 'scans', 'orders']) eq(g.rows.reduce((n, r) => n + r.s[f], 0), g.head.s[f], `${k}: the group total (${f}) is the sum of its rows`);
    eq(g.rows.filter(r => r.rest).length, 1, `${k}: one quiet plain row (it has data)`);
    eq(g.rows.filter(r => r.desk).length, k === 'assembly' ? 4 : 3);
  }
  const noRest = J(ovT); for (const s of noRest.business.stations) if (s.unassigned) s.unassigned = { parts: 0, scans: 0, orders: 0 }; for (const s of noRest.business.stations) if (s.peopleNow) s.peopleNow = [];
  eq(J(Eff.stationRowsOf(Eff.norm(noRest))).filter(r => r.rest).length, 0, 'a plain row with no data is not drawn');
  assert(/desk not recorded/.test(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8')) && /desk not recorded/.test(fs.readFileSync(path.join(root, 'charm-nest-efficiency-stations.js'), 'utf8')), 'both console files label it');
  say('4 events with no device stay in the plain row, which is labelled "desk not recorded" and drawn only with data; the group total is the sum of its rows');

  /* ═══ 5 · bounded and resumable ═══ */
  reset();
  for (let i = 0; i < 5; i++) { const docs = []; work(D3, 'Bulk ' + 'ABCDE'[i] + '.', 'assembly', `assembly-${(i % 4) + 1}`, 50, 1, 0, docs); }    // 5 people, 100 numbered events each
  for (const p of ['A', 'B', 'C', 'D', 'E']) { const docs = [...col('Station_Activity').values()].filter(d => d.person === `Bulk ${p}.`); oldRollup(D3, `Bulk ${p}.`, docs); }
  const a5 = await drain(D3, 'bulk');
  eq([a5.calls, a5.bodies.map(b => b.processed), a5.bodies.map(b => b.eventsRead)], [2, [3, 2], [300, 200]], 'a call reads at most ~300 events (three person-days of 100), the next call the rest');
  eq([a5.written, a5.eventsRead], [5, 500], 'five person-days, 500 events once');
  checkDay(D3, 'bulk');
  // a person-day too big is marked and left alone, with no event read; one that does not line up with its counters too
  put('Efficiency_Daily', SA.rollupId(D1, 'Huge H.'), { day: D1, person: 'Huge H.', v: 1, events: 2000, stations: { assembly: { scans: 2000 } } });
  const docsM = [storeEv(D1, 9, 0, 'assembly', 'assembly-1', 'Mism M.', 'scan', { orderId: newOrder(), parts: 1 }), storeEv(D1, 9, 5, 'assembly', 'assembly-1', 'Mism M.', 'scan', { orderId: newOrder(), parts: 1 })];
  oldRollup(D1, 'Mism M.', docsM); put('Efficiency_Daily', SA.rollupId(D1, 'Mism M.'), { stations: { assembly: { scans: 1 } } }, { merge: true });   // (two events stored, one counted)
  zero(); const a6 = await drain(D1, 'odd');
  eq([a6.skipped, a6.written, a6.eventsRead], [2, 0, 2], 'too many events (2,000): marked without reading a single event; two events stored for one counted: read, found not to line up, marked, nothing invented');
  eq([rollup(D1, 'Huge H.').deskBackfilled, rollup(D1, 'Huge H.').deskSkipped, rollup(D1, 'Mism M.').deskBackfilled, rollup(D1, 'Mism M.').devices], [true, 'too many events', true, undefined]);
  zero(); eq((await drain(D1, 'odd again')).bodies[0].pending, 0, 'and never asked again');
  say('5 batches of ~300 events, resumable; a person-day too big or not lining up is marked and left alone');

  /* ═══ 6 · the console mounted (jsdom) over the real server: asks once per day in view, repaints with the desk rows, labels the plain row, never asks again ═══ */
  let JSDOM = null;
  for (const d of [process.env.JSDOM_DIR, path.join(root, 'node_modules')].filter(Boolean)) { try { ({ JSDOM } = require(path.join(d, 'jsdom'))); break; } catch (_) {} }
  if (!JSDOM) say('6 (jsdom not found: the mounted console was not run; set JSDOM_DIR)');
  else {
    const mount = async (days, keep) => {
      if (!keep) { reset(); seedDay(TODAY); seedDay(D1); }
      const dom = new JSDOM('<!doctype html><html><body><div class="lib" id="efficiencyView"></div></body></html>', { url: 'http://localhost/', pretendToBeVisual: true, runScripts: 'outside-only' });
      const w = dom.window, ops = [];
      w.Date.now = () => NOW;
      w.sessionStorage.setItem('cn.eff.key', PASS); if (days) w.sessionStorage.setItem('cn.eff.days', String(days));
      w.fetch = async (u, o) => { const b = JSON.parse(o.body); ops.push(b.op + (b.op === 'deskBackfill' ? ':' + b.day : '')); const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ipN % 200) }, body: o.body }, fakeDb); return { ok: r.statusCode === 200, status: r.statusCode, text: async () => r.body }; };
      for (const f of ['charm-nest-efficiency-charts.js', 'charm-nest-efficiency-stations.js', 'charm-nest-efficiency.js']) w.eval(fs.readFileSync(path.join(root, f), 'utf8'));
      Object.assign(w.Efficiency.options, { pollMs: 120, liveMs: 200, growMs: 0, nudgeMs: 50 });
      await new Promise(r => setTimeout(r, 2500));
      return { w, ops, done: () => { try { w.close(); } catch (_) {} } };
    };
    const m1 = await mount(0);
    eq(m1.ops.filter(o => o.startsWith('deskBackfill')), ['deskBackfill:' + TODAY, 'deskBackfill:' + TODAY], 'the day in view is asked for ONCE (two calls: eight person-days, then two), not on any of the many refreshes that followed');
    assert(m1.ops.filter(o => o === 'overview').length >= 4 && m1.ops.indexOf('deskBackfill:' + TODAY) < m1.ops.lastIndexOf('overview'), 'a refresh of the same read followed the recovery');
    const d1 = m1.w.document, q = x => d1.querySelector(x), nf = n => String(n);
    eq(q('.efSR[data-kind="rest"][data-station="assembly"] .efSN').textContent, 'Assembly (desk not recorded)', 'the plain Assembly row says what it is');
    eq(q('.efSR[data-kind="rest"][data-station="shipping"] .efSN').textContent, 'Shipping (desk not recorded)');
    for (const k of ['assembly', 'shipping']) {
      const rowsK = [...d1.querySelectorAll(`.efSR[data-station^="${k}"]`)], sum = c => rowsK.reduce((n, r) => n + (+r.querySelector(`[data-c="${c}"] .efNum`).dataset.v || 0), 0);
      eq(rowsK.length, (k === 'assembly' ? 4 : 3) + 1, k + ': every desk and the one plain row');
      eq(q(`.efSG[data-group="${k}"] .efSGT`).textContent, `${nf(sum('parts'))} ${sum('parts') === 1 ? 'piece' : 'pieces'} · ${nf(sum('orders'))} ${sum('orders') === 1 ? 'order' : 'orders'}`, k + ': the caption is the sum of its rows');
      assert(sum('parts') > 0 && sum('orders') > 0);
    }
    const desk2 = q('.efSR[data-station="assembly-2"]'); eq([+desk2.querySelector('[data-c="parts"] .efNum').dataset.v, desk2.dataset.kind], [5, 'desk'], 'Assembly 2 holds what the events say (Ben and Jo) after the recovery');
    m1.done();
    const m2 = await mount(7);
    eq(m2.ops.filter(o => o.startsWith('deskBackfill')), ['deskBackfill:' + TODAY, 'deskBackfill:' + TODAY, 'deskBackfill:' + D1, 'deskBackfill:' + D1], 'a week in view: each day once, newest first');
    m2.done();
    const m3 = await mount(7, true);
    eq(m3.ops.filter(o => o.startsWith('deskBackfill')), [], 'a console opened after everything was recovered asks for nothing');
    m3.done();
    say('6 the mounted console: asks once per day in view (a week: each day once), repaints with the desk rows, labels the plain row, caption = sum of its rows, asks for nothing afterwards');
  }

  Date.now = realNow; say('OK');
  process.exit(0);
})().catch(e => { Date.now = realNow; process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
